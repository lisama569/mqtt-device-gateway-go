package broker

import (
	"bufio"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lisama569/mqtt-device-gateway-go/internal/protocol"
	"github.com/lisama569/mqtt-device-gateway-go/internal/session"
)

// conn is one live device connection.
type conn struct {
	broker *Broker
	raw    net.Conn
	w      *bufio.Writer

	epoch atomic.Uint64 // bumped on take-over to invalidate late writers

	clientID  string
	sess      *session.Session
	will      *protocol.WillMessage
	keepAlive time.Duration

	writeToken chan struct{} // token-based writer serialization (capacity 1)

	shutdownCh    chan struct{}
	closeOnce     atomic.Bool
	takenOver     atomic.Bool
	endedNormally atomic.Bool

	ka   *keepaliveState
	kaWg sync.WaitGroup
}

// maxClientIDLen bounds the device identifier length.
const maxClientIDLen = 128

func (b *Broker) newConn(raw net.Conn) *conn {
	c := &conn{
		broker:     b,
		raw:        raw,
		w:          bufio.NewWriterSize(raw, 64*1024),
		writeToken: make(chan struct{}, 1),
		shutdownCh: make(chan struct{}),
	}
	c.writeToken <- struct{}{}
	c.epoch.Store(1)
	return c
}

// handleConn runs the full lifecycle of one transport, from CONNECT wait
// through termination cleanup. It always closes the raw connection.
func (b *Broker) handleConn(raw net.Conn) {
	c := b.newConn(raw)
	defer func() {
		// Safety net: whatever the failure path, terminate runs once.
		if !c.closeOnce.Load() {
			c.terminate(false, errors.New("connection ended without handshake"))
		}
	}()

	// A CONNECT must arrive promptly.
	_ = raw.SetReadDeadline(time.Now().Add(10 * time.Second))
	pkt, err := protocol.ReadFrame(raw)
	if err != nil {
		b.rejectPreHandshake(raw, err)
		return
	}
	_ = raw.SetReadDeadline(time.Time{})

	if pkt.Type != protocol.TypeConnect {
		b.rejectPreHandshake(raw, protocol.NewError(protocol.ErrMalformedPacket,
			"first packet must be CONNECT, got %s", pkt.Type))
		return
	}
	if err := c.handleConnect(pkt); err != nil {
		// CONNACK with the precise reason has already been sent when possible.
		return
	}

	c.readLoop()
}

// rejectPreHandshake sends a best-effort CONNACK for failures before the
// session is known, then closes the transport. No will exists yet.
func (b *Broker) rejectPreHandshake(raw net.Conn, cause error) {
	reason := classifyConnackReason(cause)
	b.logger.Warn("handshake rejected",
		"cause", errorCodeOf(cause), "detail", cause.Error(), "reason", reason)
	w := bufio.NewWriter(raw)
	_ = protocol.WriteFrame(w, &protocol.Packet{Type: protocol.TypeConnack, Reason: reason})
	_ = w.Flush()
	_ = raw.Close()
}

// handleConnect validates the CONNECT, binds/creates the session and
// performs take-over under the registry lock.
func (c *conn) handleConnect(pkt *protocol.Packet) error {
	b := c.broker

	// 1. Protocol identification must be exact and distinguishable.
	if pkt.Protocol != protocol.ProtocolName {
		return c.refuseConnect(protocol.ReasonUnsupportedVersion,
			protocol.NewError(protocol.ErrUnsupportedProtocol,
				"protocol name %q, want %q", pkt.Protocol, protocol.ProtocolName))
	}
	if pkt.Version != protocol.SupportedVersion {
		return c.refuseConnect(protocol.ReasonUnsupportedVersion,
			protocol.NewError(protocol.ErrUnsupportedVersion,
				"protocol version %d, want %d", pkt.Version, protocol.SupportedVersion))
	}
	// 2. Identifier validation: non-empty, bounded, no NUL.
	if len(pkt.ClientID) == 0 || len(pkt.ClientID) > maxClientIDLen {
		return c.refuseConnect(protocol.ReasonIdentifierRejected,
			protocol.NewError(protocol.ErrInvalidClientID,
				"clientID length %d outside 1..%d", len(pkt.ClientID), maxClientIDLen))
	}
	for _, ch := range pkt.ClientID {
		if ch == 0 {
			return c.refuseConnect(protocol.ReasonIdentifierRejected,
				protocol.NewError(protocol.ErrInvalidClientID, "clientID contains NUL byte"))
		}
	}
	// 3. Will (if present) must itself be publishable.
	if pkt.Will != nil {
		if err := protocol.ValidateTopicName(pkt.Will.Topic); err != nil {
			return c.refuseConnect(protocol.ReasonMalformed, err)
		}
		if pkt.Will.QoS > 1 {
			return c.refuseConnect(protocol.ReasonQoSNotSupported,
				protocol.NewError(protocol.ErrQoSNotSupported, "will qos %d > 1", pkt.Will.QoS))
		}
	}
	if pkt.QoS > 1 { // tolerated field on CONNECT: explicitly reject rather than downgrade
		return c.refuseConnect(protocol.ReasonQoSNotSupported,
			protocol.NewError(protocol.ErrQoSNotSupported, "connect qos %d > 1", pkt.QoS))
	}

	// 4. Session binding. This replaces any stored session when CleanStart.
	sess, created := b.store.CreateOrResume(pkt.ClientID, pkt.CleanStart)

	c.clientID = pkt.ClientID
	c.sess = sess
	c.will = pkt.Will
	if pkt.KeepAlive > 0 {
		c.keepAlive = time.Duration(pkt.KeepAlive) * time.Second
	}
	// Start the watchdog BEFORE this conn becomes registry-visible, so a
	// concurrent Broker.Close() can never observe half-initialized
	// keepalive state through terminate().
	c.startKeepalive()

	// 5. Ownership is swapped atomically BEFORE the new CONNACK so that
	// two connections can never both be registered owner for one id; the
	// displaced connection is closed only AFTER the new CONNACK is on the
	// wire (MQTT take-over: the will of the old owner is suppressed).
	displaced := b.registerOwner(pkt.ClientID, c)

	if err := c.send(&protocol.Packet{Type: protocol.TypeConnack, Reason: protocol.ReasonAccepted}); err != nil {
		b.unregister(pkt.ClientID, c)
		if created {
			b.store.Delete(pkt.ClientID, sess)
		}
		return err
	}

	b.closeDisplaced(pkt.ClientID, displaced)
	c.broker.logger.Info("device connected",
		"clientID", pkt.ClientID,
		"cleanStart", pkt.CleanStart,
		"sessionCreated", created,
		"resumed", !created,
		"keepAlive_s", pkt.KeepAlive,
		"will", pkt.Will != nil,
		"tookOver", displaced != nil)

	// 6. Resume delivery: offline backlog first (original order), then
	// retransmit pre-disconnect inflight packets with DUP=1, then pump.
	if !pkt.CleanStart {
		if n := sess.DrainOffline(); n > 0 {
			b.logger.Info("offline queue drained on reconnect",
				"clientID", pkt.ClientID, "messages", n)
		}
		for _, in := range sess.InflightSnapshot() {
			c.resendInflight(in)
		}
	}
	c.pumpSession()
	return nil
}

// refuseConnect logs the precise classification, sends a CONNACK with the
// given reason and marks the connection terminating (no will possible yet).
func (c *conn) refuseConnect(reason uint8, cause error) error {
	c.broker.logger.Warn("CONNECT refused",
		"clientID", c.clientID,
		"cause", errorCodeOf(cause),
		"detail", cause.Error(),
		"reason", reason)
	_ = c.send(&protocol.Packet{Type: protocol.TypeConnack, Reason: reason})
	c.terminate(false, cause) // pre-session connect failure: no will registered
	return cause
}

// readLoop is the main packet loop.
func (c *conn) readLoop() {
	b := c.broker
	for {
		pkt, err := protocol.ReadFrame(c.raw)
		if err != nil {
			if c.isGracefulClose(err) {
				return
			}
			c.terminate(false, classifyReadError(err))
			return
		}
		c.touchKeepalive()

		switch pkt.Type {
		case protocol.TypePublish:
			err = c.handlePublish(pkt)
		case protocol.TypeSubscribe:
			err = c.handleSubscribe(pkt)
		case protocol.TypeUnsubscribe:
			err = c.handleUnsubscribe(pkt)
		case protocol.TypePuback:
			err = c.handlePuback(pkt)
		case protocol.TypePingreq:
			err = c.send(&protocol.Packet{Type: protocol.TypePingresp})
		case protocol.TypeDisconnect:
			b.logger.Info("device disconnect (normal)", "clientID", c.clientID)
			c.terminate(true, nil)
			return
		case protocol.TypeConnect:
			err = protocol.NewError(protocol.ErrMalformedPacket, "second CONNECT on same transport")
		default:
			err = protocol.NewError(protocol.ErrMalformedPacket, "unknown packet type %q", pkt.Type)
		}
		if err != nil {
			// Protocol errors are abnormal terminations: the will (if any)
			// is published exactly once.
			c.terminate(false, err)
			return
		}
	}
}

func (c *conn) isGracefulClose(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed)
}

// classifyReadError keeps "peer vanished" distinct from "peer sent garbage".
func classifyReadError(err error) error {
	var pe *protocol.Error
	if errors.As(err, &pe) {
		return pe
	}
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return protocol.NewError(protocol.ErrConnectionClosed, "transport closed: %v", err)
	}
	return protocol.NewError(protocol.ErrConnectionClosed, "read failure: %v", err)
}

func classifyConnackReason(err error) uint8 {
	var pe *protocol.Error
	if errors.As(err, &pe) {
		switch pe.Code {
		case protocol.ErrUnsupportedProtocol, protocol.ErrUnsupportedVersion:
			return protocol.ReasonUnsupportedVersion
		case protocol.ErrInvalidClientID:
			return protocol.ReasonIdentifierRejected
		case protocol.ErrQoSNotSupported:
			return protocol.ReasonQoSNotSupported
		}
	}
	return protocol.ReasonMalformed
}

func errorCodeOf(err error) string {
	var pe *protocol.Error
	if errors.As(err, &pe) {
		return string(pe.Code)
	}
	return "IO_ERROR"
}
