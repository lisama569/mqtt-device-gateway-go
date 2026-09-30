package client

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lisama569/mqtt-device-gateway-go/internal/protocol"
)

// ConnectConfig configures a Connect call.
type ConnectConfig struct {
	Address    string
	ClientID   string
	CleanStart bool
	KeepAlive  time.Duration
	Will       *protocol.WillMessage
	Dialer     *net.Dialer
}

// MessageHandler receives asynchronous PUBLISH messages.
// packetID is 0 for QoS0; dup marks a broker-side retransmission.
type MessageHandler func(topic string, payload []byte, packetID uint16, dup bool)

// Client is one device's connection to the gateway.
type Client struct {
	conn net.Conn
	w    *bufio.Writer

	writeToken chan struct{}

	handler MessageHandler

	closed    atomic.Bool
	pending   map[uint16]chan error // outbound QoS1 PUBACK waiters
	waits     map[uint16]func(*protocol.Packet) bool
	pendingMu sync.Mutex
	nextID    uint16

	stopPing chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup

	closeErr   error
	closeErrMu sync.Mutex
}

// Connect dials and completes the CONNECT/CONNACK handshake.
func Connect(cfg ConnectConfig, handler MessageHandler) (*Client, error) {
	if cfg.ClientID == "" {
		return nil, errors.New("clientID is required")
	}
	dialer := cfg.Dialer
	if dialer == nil {
		dialer = &net.Dialer{Timeout: 5 * time.Second}
	}
	raw, err := dialer.Dial("tcp", cfg.Address)
	if err != nil {
		return nil, fmt.Errorf("dial gateway: %w", err)
	}
	c := &Client{
		conn:       raw,
		w:          bufio.NewWriterSize(raw, 64*1024),
		writeToken: make(chan struct{}, 1),
		handler:    handler,
		pending:    make(map[uint16]chan error),
		waits:      make(map[uint16]func(*protocol.Packet) bool),
		nextID:     1,
		stopPing:   make(chan struct{}),
	}
	c.writeToken <- struct{}{}

	var keepAlive uint16
	if cfg.KeepAlive > 0 {
		keepAlive = uint16(cfg.KeepAlive / time.Second)
	}
	connPkt := &protocol.Packet{
		Type:       protocol.TypeConnect,
		Protocol:   protocol.ProtocolName,
		Version:    protocol.SupportedVersion,
		CleanStart: cfg.CleanStart,
		KeepAlive:  keepAlive,
		ClientID:   cfg.ClientID,
		Will:       cfg.Will,
	}
	if err := c.writeFrame(connPkt); err != nil {
		_ = raw.Close()
		return nil, fmt.Errorf("send CONNECT: %w", err)
	}
	// Only CONNACK is expected now; bound the wait.
	_ = raw.SetReadDeadline(time.Now().Add(5 * time.Second))
	ack, err := protocol.ReadFrame(raw)
	_ = raw.SetReadDeadline(time.Time{})
	if err != nil {
		_ = raw.Close()
		return nil, fmt.Errorf("read CONNACK: %w", err)
	}
	if ack.Type != protocol.TypeConnack {
		_ = raw.Close()
		return nil, fmt.Errorf("expected CONNACK, got %s", ack.Type)
	}
	if ack.Reason != protocol.ReasonAccepted {
		_ = raw.Close()
		return nil, &ConnectError{Reason: ack.Reason}
	}

	c.wg.Add(1)
	go c.readLoop()
	if cfg.KeepAlive > 0 {
		c.startPing(cfg.KeepAlive)
	}
	return c, nil
}

// ConnectError carries a non-zero CONNACK reason code.
type ConnectError struct{ Reason uint8 }

func (e *ConnectError) Error() string {
	switch e.Reason {
	case protocol.ReasonUnsupportedVersion:
		return "connect rejected: unsupported protocol/version (reason=1)"
	case protocol.ReasonIdentifierRejected:
		return "connect rejected: client identifier rejected (reason=2)"
	case protocol.ReasonQoSNotSupported:
		return "connect rejected: unsupported QoS (reason=4)"
	default:
		return fmt.Sprintf("connect rejected: reason=%d", e.Reason)
	}
}

// Subscribe installs topic filters; returns the per-filter granted QoS/reason.
func (c *Client) Subscribe(filters []protocol.SubFilter) ([]protocol.SubResult, error) {
	if len(filters) == 0 {
		return nil, errors.New("at least one filter is required")
	}
	for _, f := range filters {
		if err := protocol.ValidateFilter(f.Filter); err != nil {
			return nil, err
		}
		if f.QoS > 1 {
			return nil, protocol.NewError(protocol.ErrQoSNotSupported,
				"requested qos %d > 1; the gateway will not downgrade", f.QoS)
		}
	}
	id := c.allocID()
	pkt := &protocol.Packet{Type: protocol.TypeSubscribe, PacketID: id, Filters: filters}
	resp, err := c.roundTrip(pkt, func(p *protocol.Packet) bool {
		return p.Type == protocol.TypeSuback && p.PacketID == id
	})
	if err != nil {
		return nil, err
	}
	return resp.Results, nil
}

// Unsubscribe removes topic filters.
func (c *Client) Unsubscribe(filters []string) error {
	if len(filters) == 0 {
		return errors.New("at least one filter is required")
	}
	sub := make([]protocol.SubFilter, len(filters))
	for i, f := range filters {
		if err := protocol.ValidateFilter(f); err != nil {
			return err
		}
		sub[i] = protocol.SubFilter{Filter: f}
	}
	id := c.allocID()
	_, err := c.roundTrip(&protocol.Packet{
		Type: protocol.TypeUnsubscribe, PacketID: id, Filters: sub,
	}, func(p *protocol.Packet) bool {
		return p.Type == protocol.TypeUnsuback && p.PacketID == id
	})
	return err
}

// Publish sends a message; QoS 1 blocks until PUBACK, transport error,
// or a fixed acknowledgement timeout (the caller then decides to retry;
// the server de-duplicates retries inside its window).
func (c *Client) Publish(topic string, qos uint8, payload []byte) error {
	if err := protocol.ValidateTopicName(topic); err != nil {
		return err
	}
	if qos > 1 {
		return protocol.NewError(protocol.ErrQoSNotSupported,
			"publish qos %d > 1; the gateway rejects QoS 2 explicitly", qos)
	}
	if qos == 0 {
		return c.writeFrame(&protocol.Packet{
			Type: protocol.TypePublish, Topic: topic, QoS: 0, Payload: payload,
		})
	}
	id := c.allocID()
	ch := c.registerAck(id)
	defer c.unregisterAck(id)
	if err := c.writeFrame(&protocol.Packet{
		Type: protocol.TypePublish, Topic: topic, QoS: 1,
		PacketID: id, Payload: payload,
	}); err != nil {
		return err
	}
	select {
	case err := <-ch:
		return err
	case <-time.After(10 * time.Second):
		return errors.New("timeout waiting for PUBACK")
	case <-c.stopPing:
		return net.ErrClosed
	}
}

// Disconnect sends DISCONNECT and closes without triggering the will.
func (c *Client) Disconnect() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	// Best effort: the server treats this as the normal DISCONNECT.
	_ = c.writeFrame(&protocol.Packet{Type: protocol.TypeDisconnect})
	c.stopOnce.Do(func() { close(c.stopPing) })
	err := c.conn.Close()
	c.wg.Wait()
	// Closing our own socket is the intended outcome, not an error.
	if errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

// Wait blocks until the transport has ended (take-over/abnormal close),
// returning the cause.
func (c *Client) Wait() error {
	c.wg.Wait()
	c.closeErrMu.Lock()
	defer c.closeErrMu.Unlock()
	return c.closeErr
}

// Done returns a channel closed when the connection ends.
func (c *Client) Done() <-chan struct{} { return c.stopPing }

// KillForDemo drops the underlying transport WITHOUT sending DISCONNECT,
// simulating abrupt power loss / network failure so the gateway applies
// abnormal-disconnect semantics (will message). Intended for tests/demos.
func (c *Client) KillForDemo() error {
	if c.closed.CompareAndSwap(false, true) {
		c.stopOnce.Do(func() { close(c.stopPing) })
	}
	return c.conn.Close()
}

func (c *Client) readLoop() {
	defer c.wg.Done()
	for {
		pkt, err := protocol.ReadFrame(c.conn)
		if err != nil {
			c.failAll(err)
			if c.closed.CompareAndSwap(false, true) {
				c.closeErrMu.Lock()
				c.closeErr = err
				c.closeErrMu.Unlock()
			}
			c.stopOnce.Do(func() { close(c.stopPing) })
			_ = c.conn.Close()
			return
		}
		switch pkt.Type {
		case protocol.TypePublish:
			if pkt.QoS == 1 {
				// Auto-acknowledge broker deliveries. The application
				// handler is invoked after the ACK is queued to keep the
				// broker's window moving; duplicate delivery (same id,
				// DUP=1) is still handed to the handler with dup=true.
				_ = c.writeFrame(&protocol.Packet{
					Type: protocol.TypePuback, PacketID: pkt.PacketID,
				})
			}
			if c.handler != nil {
				c.handler(pkt.Topic, pkt.Payload, pkt.PacketID, pkt.Dup)
			}
		case protocol.TypePuback:
			c.resolveAck(pkt.PacketID, nil)
		case protocol.TypeSuback, protocol.TypeUnsuback:
			c.pendingMu.Lock()
			fn := c.waits[pkt.PacketID]
			keep := fn != nil && !fn(pkt)
			if !keep {
				delete(c.waits, pkt.PacketID)
			}
			c.pendingMu.Unlock()
		case protocol.TypePingresp:
			// nothing to do
		default:
			// Unrequested SUBACK/UNSUBACK/CONNACK: ignore but stay open.
		}
	}
}

// startPing sends PINGREQ at half the keepalive interval (MQTT convention).
func (c *Client) startPing(keepAlive time.Duration) {
	interval := keepAlive / 2
	if interval < time.Second {
		interval = time.Second
	}
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-c.stopPing:
				return
			case <-t.C:
				if err := c.writeFrame(&protocol.Packet{Type: protocol.TypePingreq}); err != nil {
					return
				}
			}
		}
	}()
}

func (c *Client) allocID() uint16 {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	id := c.nextID
	c.nextID++
	if c.nextID == 0 {
		c.nextID = 1
	}
	return id
}

func (c *Client) registerAck(id uint16) chan error {
	ch := make(chan error, 1)
	c.pendingMu.Lock()
	c.pending[id] = ch
	c.pendingMu.Unlock()
	return ch
}

func (c *Client) unregisterAck(id uint16) {
	c.pendingMu.Lock()
	delete(c.pending, id)
	c.pendingMu.Unlock()
}

func (c *Client) resolveAck(id uint16, err error) {
	c.pendingMu.Lock()
	ch := c.pending[id]
	delete(c.pending, id)
	c.pendingMu.Unlock()
	if ch != nil {
		ch <- err
	}
}

func (c *Client) failAll(err error) {
	c.pendingMu.Lock()
	chs := make([]chan error, 0, len(c.pending))
	for id, ch := range c.pending {
		chs = append(chs, ch)
		delete(c.pending, id)
	}
	c.pendingMu.Unlock()
	for _, ch := range chs {
		ch <- err
	}
}

// roundTrip sends pkt and waits for the first inbound frame accepted by match.
func (c *Client) roundTrip(pkt *protocol.Packet, match func(*protocol.Packet) bool) (*protocol.Packet, error) {
	type result struct {
		pkt *protocol.Packet
		err error
	}
	respCh := make(chan result, 1)

	c.pendingMu.Lock()
	c.waits[pkt.PacketID] = func(p *protocol.Packet) bool {
		if !match(p) {
			return false
		}
		select {
		case respCh <- result{pkt: p}:
		default:
		}
		return true
	}
	c.pendingMu.Unlock()
	defer func() {
		c.pendingMu.Lock()
		delete(c.waits, pkt.PacketID)
		c.pendingMu.Unlock()
	}()

	if err := c.writeFrame(pkt); err != nil {
		return nil, err
	}
	select {
	case r := <-respCh:
		return r.pkt, r.err
	case <-time.After(10 * time.Second):
		return nil, errors.New("timeout waiting for response")
	case <-c.stopPing:
		return nil, net.ErrClosed
	}
}

func (c *Client) writeFrame(pkt *protocol.Packet) error {
	token := <-c.writeToken
	defer func() { c.writeToken <- token }()
	if err := protocol.WriteFrame(c.w, pkt); err != nil {
		return err
	}
	return c.w.Flush()
}
