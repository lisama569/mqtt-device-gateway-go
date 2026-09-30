// Package broker is the local IoT gateway: TCP listener, deterministic session
// takeover, wildcard routing, QoS1 offline replay, last-will delivery and
// strict error classification.
//
// Design summary:
//   - A single router goroutine performs all fan-out. Connection goroutines
//     only enqueue an immutable published message onto a bounded channel and
//     continue. Routing therefore NEVER blocks on another device's socket, so
//     a slow or wedged device cannot stall any peer or deadlock the gateway.
//   - Per connection there are two goroutines with one direction each: a
//     reader (parses frames and enforces keep-alive via its read deadline) and
//     a writer loop that selects among inbound frames, outbound QoS0 control
//     frames and an edge-independent "kick" for QoS1 queue activity. There is
//     no shared socket and no deadline-polling race.
//   - One global registry lock (mu) guards clientID -> *conn. It is held only
//     to snapshot target pointers, never while doing I/O.
//   - Takeover rule is "new connection wins": the previous live connection is
//     sent RCTakeover and closed without publishing its will; at every instant
//     at most one connection owns a session.
//   - Will is published exactly once, and only on abnormal termination
//     (protocol error, keep-alive timeout, TCP drop). A clean DISCONNECT and a
//     takeover suppress it.
package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lisama569/mqtt-device-gateway-go/internal/proto"
	"github.com/lisama569/mqtt-device-gateway-go/internal/session"
)

// DefaultMaxPayload bounds accepted frames.
const DefaultMaxPayload = proto.MaxFramePayload

// sendQueueDepth bounds per-connection control/QoS0 buffering.
const sendQueueDepth = 4096

// routeQueueDepth bounds pending fan-out work accepted by the router.
const routeQueueDepth = 1 << 16

// Config tunes a Broker.
type Config struct {
	Addr       string // e.g. "127.0.0.1:0"; empty picks a random local port
	MaxPayload int    // <=0 uses DefaultMaxPayload
	Logger     *slog.Logger
}

// published is an immutable message handed to the router goroutine.
type published struct {
	topic   string
	payload []byte
	qos     byte
}

// Broker accepts device connections.
type Broker struct {
	cfg    Config
	log    *slog.Logger
	ln     net.Listener
	store  *session.Store
	routeQ chan published

	mu    sync.Mutex
	conns map[string]*conn      // live REGISTERED connections by clientID
	all   map[net.Conn]struct{} // every accepted raw conn (incl. handshaking)
	wg    sync.WaitGroup

	closeOnce sync.Once
	closed    chan struct{}
}

// conn is one live device attachment.
type conn struct {
	broker    *Broker
	raw       net.Conn
	sess      *session.Session
	clientID  string
	will      *proto.Will
	keepAlive time.Duration

	// frameCh hands each inbound frame from reader to writer. Unbuffered, so
	// frames are delivered strictly one at a time and can never be reordered
	// against one another or against control traffic.
	frameCh chan *proto.Packet
	kick    chan struct{} // coalesced QoS1 queue hint fired by router

	// pendingCtrl buffers QoS0/control frames (router QoS0, PUBACK/SUBACK); the
	// writer drains it every loop. Guarded by ctrlMu (router vs writer).
	ctrlMu      sync.Mutex
	pendingCtrl []*proto.Packet

	readDone chan struct{} // closed exactly once when reader exits
	readErr  atomic.Value  // terminal reader error (stored before close)

	cancelTakeover context.CancelFunc
	takenOver      atomicBool
	closedCh       chan struct{}
	closeConnOnce  sync.Once
}

// New creates a broker (not listening yet).
func New(cfg Config) *Broker {
	if cfg.MaxPayload <= 0 {
		cfg.MaxPayload = DefaultMaxPayload
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}
	return &Broker{
		cfg:    cfg,
		log:    cfg.Logger,
		store:  session.NewStore(),
		conns:  map[string]*conn{},
		all:    map[net.Conn]struct{}{},
		routeQ: make(chan published, routeQueueDepth),
		closed: make(chan struct{}),
	}
}

// Start binds the listener, starts the router and serves until Close.
func (b *Broker) Start(ctx context.Context) error {
	ln, err := net.Listen("tcp", b.cfg.Addr)
	if err != nil {
		return fmt.Errorf("broker listen: %w", err)
	}
	b.ln = ln
	b.wg.Add(2)
	go b.routerLoop()
	go b.acceptLoop(ctx)
	return nil
}

// Addr reports the bound listen address.
func (b *Broker) Addr() string {
	if b.ln == nil {
		return ""
	}
	return b.ln.Addr().String()
}

// Close stops accepting, closes all connections and waits for cleanup.
func (b *Broker) Close() error {
	var err error
	b.closeOnce.Do(func() {
		close(b.closed)
		if b.ln != nil {
			err = b.ln.Close()
		}
		b.mu.Lock()
		live := make([]*conn, 0, len(b.conns))
		for _, c := range b.conns {
			live = append(live, c)
		}
		allRaw := make([]net.Conn, 0, len(b.all))
		for raw := range b.all {
			allRaw = append(allRaw, raw)
		}
		b.mu.Unlock()
		for _, c := range live {
			c.terminate(reasonServerShutdown, false)
		}
		// Force-close accepted-but-not-registered connections so their handshake
		// reads cannot keep serveConn (and Close) waiting.
		for _, raw := range allRaw {
			_ = raw.Close()
		}
	})
	b.wg.Wait()
	return err
}

// LiveCount reports currently attached devices (tests).
func (b *Broker) LiveCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.conns)
}

func (b *Broker) acceptLoop(ctx context.Context) {
	defer b.wg.Done()
	for {
		raw, err := b.ln.Accept()
		if err != nil {
			select {
			case <-b.closed:
				return
			default:
			}
			if errors.Is(err, net.ErrClosed) {
				return
			}
			b.log.Error("accept failed", "err", err)
			continue
		}
		b.mu.Lock()
		b.all[raw] = struct{}{}
		b.mu.Unlock()
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			b.serveConn(ctx, raw)
			b.mu.Lock()
			delete(b.all, raw)
			b.mu.Unlock()
		}()
	}
}

// routerLoop is the ONLY goroutine that fans messages out to sessions. It can
// therefore block (bounded by session queues / outboxes) without ever holding
// up a device's read loop.
func (b *Broker) routerLoop() {
	defer b.wg.Done()
	for {
		select {
		case m := <-b.routeQ:
			b.route(m.topic, m.payload, m.qos)
		case <-b.closed:
			// Drain nothing on shutdown; QoS1 already accepted is kept in the
			// session store for the next run only if persisted (in-memory here).
			return
		}
	}
}
