package broker

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"

	"github.com/lisama569/mqtt-device-gateway-go/internal/protocol"
	"github.com/lisama569/mqtt-device-gateway-go/internal/session"
)

// Config holds gateway tuning parameters.
type Config struct {
	ListenAddr string
	ReadLimit  int64 // max frame bytes; 0 => protocol.MaxFrameSize
	Logger     *slog.Logger
}

// Broker is the local gateway.
type Broker struct {
	cfg      Config
	listener net.Listener
	logger   *slog.Logger

	store *session.Store

	connsMu sync.Mutex
	conns   map[string]*conn // clientID -> currently owning connection

	closed   bool
	closedMu sync.Mutex
	wg       sync.WaitGroup
}

// New creates a gateway from cfg.
func New(cfg Config) (*Broker, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	readLimit := cfg.ReadLimit
	if readLimit <= 0 {
		readLimit = protocol.MaxFrameSize
	}
	cfg.ReadLimit = readLimit
	return &Broker{
		cfg:    cfg,
		logger: logger,
		store:  session.NewStore(),
		conns:  make(map[string]*conn),
	}, nil
}

// ActiveConns returns the number of connections currently owning a session.
func (b *Broker) ActiveConns() int {
	b.connsMu.Lock()
	defer b.connsMu.Unlock()
	return len(b.conns)
}

// Store exposes the session store (mainly for tests/inspection).
func (b *Broker) Store() *session.Store { return b.store }

// Serve opens the listener (if not already open) and serves until Close.
func (b *Broker) Serve() error {
	ln, err := net.Listen("tcp", b.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", b.cfg.ListenAddr, err)
	}
	return b.ServeListener(ln)
}

// ServeListener serves on an externally created listener.
func (b *Broker) ServeListener(ln net.Listener) error {
	b.listener = ln
	b.logger.Info("gateway listening", "addr", ln.Addr().String())
	for {
		raw, err := ln.Accept()
		if err != nil {
			if b.isClosed() {
				b.wg.Wait()
				return nil
			}
			return fmt.Errorf("accept: %w", err)
		}
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			b.handleConn(raw)
		}()
	}
}

func (b *Broker) isClosed() bool {
	b.closedMu.Lock()
	defer b.closedMu.Unlock()
	return b.closed
}

// Addr returns the listener address (usable after Serve started).
func (b *Broker) Addr() net.Addr {
	if b.listener == nil {
		return nil
	}
	return b.listener.Addr()
}

// Close stops the listener and closes every owned connection. It never
// publishes will messages: gateway shutdown is an orderly event, not an
// abnormal client disconnect.
func (b *Broker) Close() error {
	b.closedMu.Lock()
	if b.closed {
		b.closedMu.Unlock()
		return nil
	}
	b.closed = true
	b.closedMu.Unlock()

	var firstErr error
	if b.listener != nil {
		firstErr = b.listener.Close()
	}
	b.connsMu.Lock()
	active := make([]*conn, 0, len(b.conns))
	for _, c := range b.conns {
		active = append(active, c)
	}
	b.connsMu.Unlock()
	for _, c := range active {
		c.terminate(true, errors.New("gateway shutting down"))
	}
	b.wg.Wait()
	return firstErr
}

// registerOwner atomically makes c the owner of clientID and returns the
// displaced owner (if any) WITHOUT closing it. Ownership is therefore
// unique at every instant; the caller is responsible for closing the
// displaced connection after the new handshake has been acknowledged.
func (b *Broker) registerOwner(clientID string, c *conn) *conn {
	b.connsMu.Lock()
	defer b.connsMu.Unlock()
	old := b.conns[clientID]
	b.conns[clientID] = c
	return old
}

// closeDisplaced finishes a take-over: the old owner is invalidated and
// closed without publishing its will. Called only after the new owner's
// CONNACK has been flushed.
func (b *Broker) closeDisplaced(clientID string, old *conn) {
	if old == nil {
		return
	}
	old.epoch.Add(1) // invalidate any in-flight writes of the old owner
	old.takenOver.Store(true)
	b.logger.Info("session take-over: closing previous connection after new CONNACK",
		"clientID", clientID)
	old.shutdown(errors.New("session taken over by new connection"))
}

// unregister removes c only while it is still the current owner. A
// taken-over old connection cannot evict the new owner's registration.
func (b *Broker) unregister(clientID string, c *conn) {
	b.connsMu.Lock()
	if b.conns[clientID] == c {
		delete(b.conns, clientID)
	}
	b.connsMu.Unlock()
}

// deliverToSubscribers routes a published message to every session with a
// matching subscription, excluding the sender (a client never receives a
// copy of the message it published, including its own will).
//
//	online session + effective QoS0  -> fire-and-forget PUBLISH
//	online session + effective QoS1  -> per-session FIFO pending queue
//	offline persistent + QoS1        -> bounded offline queue (oldest-first drop)
//	offline persistent + QoS0        -> discarded (QoS0 has no storage)
//
// Each target is handled independently; one target's failure can never
// block or corrupt another.
func (b *Broker) deliverToSubscribers(sender string, pkt *protocol.Packet) {
	// Map online owners once under the registry lock; only read fields
	// afterwards, then operate without holding that lock.
	type online struct {
		c    *conn
		sess *session.Session
	}
	onlineOwners := make(map[*session.Session]*conn)
	b.connsMu.Lock()
	for _, c := range b.conns {
		if c.clientID != "" && c.sess != nil && !c.closeOnce.Load() {
			onlineOwners[c.sess] = c
		}
	}
	b.connsMu.Unlock()

	for _, sess := range b.store.All() {
		if sess.ClientID() == sender {
			continue
		}
		subs := sess.Subscriptions()
		matched := false
		granted := uint8(0)
		for filter, maxQoS := range subs {
			if protocol.TopicMatch(filter, pkt.Topic) {
				matched = true
				if maxQoS > granted {
					granted = maxQoS
				}
			}
		}
		if !matched {
			continue
		}
		// Effective QoS = min(publish qos, subscription granted qos).
		effQoS := pkt.QoS
		if granted < effQoS {
			effQoS = granted
		}

		if owner, ok := onlineOwners[sess]; ok {
			owner.deliverOne(sender, pkt, effQoS)
			continue
		}
		// Offline target.
		if effQoS == 0 {
			b.logger.Info("offline subscriber: QoS0 message discarded",
				"clientID", sess.ClientID(), "topic", pkt.Topic,
				"basis", "QoS0 messages are never stored")
			continue
		}
		if !sess.Persistent() {
			b.logger.Info("offline temporary subscriber: message discarded",
				"clientID", sess.ClientID(), "topic", pkt.Topic,
				"basis", "cleanStart sessions have no offline storage")
			continue
		}
		msg := &session.QueuedMessage{
			Topic: pkt.Topic, Payload: append([]byte(nil), pkt.Payload...),
			QoS: effQoS, Retain: pkt.Retain,
		}
		dropped := sess.EnqueueOffline(msg)
		if dropped != nil {
			b.logger.Warn("offline queue overflow: oldest message dropped",
				"clientID", sess.ClientID,
				"droppedTopic", dropped.Topic,
				"keptTopic", pkt.Topic,
				"basis", "bounded queue oldest-first policy",
				"totalDropped", sess.OverflowDropped())
		} else {
			b.logger.Info("message stored for offline persistent session",
				"clientID", sess.ClientID(), "topic", pkt.Topic,
				"queued", sess.OfflineCount())
		}
	}
}
