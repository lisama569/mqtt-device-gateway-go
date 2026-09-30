// Package client is a Go device client for the local IoT gateway, used by the
// demo and the automated tests. It performs the CONNECT handshake, automatic
// keep-alive, QoS1 ack tracking, subscriptions, and both clean and abnormal
// disconnect modes.
package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lisama569/mqtt-device-gateway-go/internal/proto"
)

// Errors returned by the client.
var (
	ErrRejected        = errors.New("client: server rejected request")
	ErrBadFilter       = errors.New("client: invalid topic filter")
	ErrQoSNotSupported = errors.New("client: qos level not supported")
	ErrClosed          = errors.New("client: connection closed")
	ErrPacketIDWindow  = errors.New("client: outbound packet-id window exhausted")
	ErrBadTopic        = errors.New("client: invalid publish topic")
)

// writeDeadline bounds every outbound write.
const writeDeadline = 500 * time.Millisecond

// Options for Dial.
type Options struct {
	ClientID     string
	CleanSession bool
	KeepAlive    uint16 // seconds; 0 disables keep-alive
	WillTopic    string
	WillQoS      byte
	WillPayload  []byte
}

// Message is a delivered application message.
type Message struct {
	Topic    string
	Payload  []byte
	QoS      byte
	PacketID uint16
	Dup      bool
}

// Client is one device connection.
type Client struct {
	conn net.Conn
	logf func(string, ...any)

	writeMu sync.Mutex // serializes EVERY outbound write to conn

	msgCh  chan Message
	errCh  chan error
	doneCh chan struct{}

	closeOnce sync.Once
	closed    atomic.Bool
	discoOnce atomic.Bool // true after clean DISCONNECT was sent

	pubID  uint16
	pubMu  sync.Mutex
	pubWin map[uint16]bool

	subID   uint16
	subMu   sync.Mutex
	ackMu   sync.Mutex
	ackCh   map[uint16]chan proto.ReasonCode
	pubAcks map[uint16]chan struct{}
	grants  map[uint16][]byte

	keepalive time.Duration
}

// Dial connects, performs the CONNECT handshake and returns the client.
// A non-accept reason is returned as an error carrying the reason code.
func Dial(ctx context.Context, addr string, opts Options) (*Client, error) {
	if opts.ClientID == "" {
		return nil, fmt.Errorf("%w: empty client id", ErrBadTopic)
	}
	if opts.WillTopic != "" {
		if opts.WillQoS > 1 {
			return nil, ErrQoSNotSupported
		}
	}
	d := &net.Dialer{}
	raw, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	cp := &proto.ConnectPacket{
		ProtocolName: proto.ProtocolName,
		Version:      proto.SupportedVersion,
		ClientID:     opts.ClientID,
		CleanSession: opts.CleanSession,
		KeepAlive:    opts.KeepAlive,
	}
	if opts.WillTopic != "" {
		cp.Will = &proto.Will{Topic: opts.WillTopic, QoS: opts.WillQoS, Payload: opts.WillPayload}
	}
	_ = raw.SetDeadline(time.Now().Add(10 * time.Second))
	if err := proto.WriteFrame(raw, &proto.Packet{Type: proto.CONNECT, Connect: cp}); err != nil {
		_ = raw.Close()
		return nil, err
	}
	ack, err := proto.ReadFrame(raw)
	if err != nil {
		_ = raw.Close()
		return nil, err
	}
	_ = raw.SetDeadline(time.Time{})
	if ack.Type != proto.CONNACK {
		_ = raw.Close()
		return nil, fmt.Errorf("%w: expected CONNACK got %d", ErrRejected, ack.Type)
	}
	if ack.Code != proto.RCAccepted {
		_ = raw.Close()
		return nil, &ConnectError{Code: ack.Code}
	}

	c := &Client{
		conn:      raw,
		msgCh:     make(chan Message, 256),
		errCh:     make(chan error, 2),
		doneCh:    make(chan struct{}),
		pubWin:    map[uint16]bool{},
		ackCh:     map[uint16]chan proto.ReasonCode{},
		pubAcks:   map[uint16]chan struct{}{},
		grants:    map[uint16][]byte{},
		keepalive: time.Duration(opts.KeepAlive) * time.Second,
		logf:      func(string, ...any) {},
	}
	go c.readLoop()
	if c.keepalive > 0 {
		go c.keepAliveLoop()
	}
	return c, nil
}

// ConnectError reports a failed handshake.
type ConnectError struct{ Code proto.ReasonCode }

func (e *ConnectError) Error() string {
	return fmt.Sprintf("connect rejected: %s", e.Code)
}

// Subscribe registers filters. A malformed/unsupported per-filter result is
// reported through SubscribeError; nothing is silently accepted.
func (c *Client) Subscribe(ctx context.Context, filters map[string]byte) (*SubscribeResult, error) {
	if c.closed.Load() {
		return nil, ErrClosed
	}
	list := make([]proto.SubFilter, 0, len(filters))
	for f, q := range filters {
		if f == "" {
			return nil, ErrBadFilter
		}
		if q > 1 {
			return nil, fmt.Errorf("%w: qos=%d", ErrQoSNotSupported, q)
		}
		list = append(list, proto.SubFilter{Filter: f, QoS: q})
	}
	id := c.allocSubID()
	ch := make(chan proto.ReasonCode, 1)
	c.ackMu.Lock()
	c.ackCh[id] = ch
	c.ackMu.Unlock()

	if err := c.write(&proto.Packet{Type: proto.SUBSCRIBE, Sub: &proto.SubscribePacket{
		PacketID: id, Filters: list,
	}}); err != nil {
		c.removeWaiter(id)
		return nil, err
	}
	select {
	case <-ch:
	case <-ctx.Done():
		c.removeWaiter(id)
		return nil, ctx.Err()
	case <-c.doneCh:
		return nil, ErrClosed
	}
	c.ackMu.Lock()
	g := c.grants[id]
	delete(c.grants, id)
	c.ackMu.Unlock()
	return &SubscribeResult{PacketID: id, Granted: g, Filters: filtersInOrder(list)}, nil
}

// SubscribeResult reports per-filter grants (0x80-bits set => rejected).
type SubscribeResult struct {
	PacketID uint16
	Granted  []byte
	Filters  []string // in granted order
}

// AnyRejected reports whether at least one filter was rejected.
func (r *SubscribeResult) AnyRejected() (int, bool) {
	for i, g := range r.Granted {
		if g&0x80 != 0 {
			return i, true
		}
	}
	return 0, false
}

// Unsubscribe removes filters.
func (c *Client) Unsubscribe(ctx context.Context, filters []string) error {
	if c.closed.Load() {
		return ErrClosed
	}
	if len(filters) == 0 {
		return ErrBadFilter
	}
	id := c.allocSubID()
	ch := make(chan proto.ReasonCode, 1)
	c.ackMu.Lock()
	c.ackCh[id] = ch
	c.ackMu.Unlock()
	if err := c.write(&proto.Packet{Type: proto.UNSUBSCRIBE, Unsub: &proto.UnsubscribePacket{
		PacketID: id, Filters: filters,
	}}); err != nil {
		c.removeWaiter(id)
		return err
	}
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		c.removeWaiter(id)
		return ctx.Err()
	case <-c.doneCh:
		return ErrClosed
	}
}

// Publish sends a message. qos 1 blocks until PUBACK; qos>1 is rejected.
func (c *Client) Publish(ctx context.Context, topic string, payload []byte, qos byte) error {
	if c.closed.Load() {
		return ErrClosed
	}
	if topic == "" || topicContainsWildcard(topic) {
		return ErrBadTopic
	}
	if qos > 1 {
		return fmt.Errorf("%w: qos=%d", ErrQoSNotSupported, qos)
	}
	if len(payload) > proto.MaxFramePayload {
		return fmt.Errorf("%w: payload too large", ErrRejected)
	}
	p := &proto.PublishPacket{Topic: topic, Payload: payload, QoS: qos}
	var ackCh chan struct{}
	if qos == 1 {
		id, err := c.allocPubID()
		if err != nil {
			return err
		}
		defer c.freePubID(id)
		p.PacketID = id
		// Capture the wait channel BEFORE sending; a PUBACK can arrive the
		// instant the frame leaves (even before write returns). The read loop
		// signals this exact channel, so we must not look it up again after the
		// write (it may already have been deleted, yielding a nil channel and a
		// permanent block despite a received PUBACK).
		ackCh = make(chan struct{}, 1)
		c.ackMu.Lock()
		c.pubAcks[id] = ackCh
		c.ackMu.Unlock()
		defer func() {
			c.ackMu.Lock()
			delete(c.pubAcks, id)
			c.ackMu.Unlock()
		}()
	}
	if err := c.write(&proto.Packet{Type: proto.PUBLISH, Pub: p}); err != nil {
		return err
	}
	if qos == 1 {
		select {
		case <-ackCh:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-c.doneCh:
			return ErrClosed
		}
	}
	return nil
}

// Messages returns the inbound message channel.
func (c *Client) Messages() <-chan Message { return c.msgCh }

// Done is closed when the connection has ended.
func (c *Client) Done() <-chan struct{} { return c.doneCh }

// Disconnect performs a clean DISCONNECT: the broker MUST NOT publish the
// will. The frame is written under the write lock (so it cannot interleave
// with another outbound frame) with a bounded deadline, after which the socket
// is closed and every goroutine is released.
func (c *Client) Disconnect() error {
	if c.closed.Load() {
		return ErrClosed
	}
	c.discoOnce.Store(true)
	c.writeFrame(&proto.Packet{Type: proto.DISCONNECT})
	c.markClosed() // closes the socket and doneCh exactly once
	return nil
}

// Kill simulates an abnormal drop (no DISCONNECT): the broker MUST publish the
// will exactly once after detecting the gone socket.
func (c *Client) Kill() error {
	c.markClosed() // closes socket and doneCh exactly once
	return nil
}

// Closed reports whether this client handle is shut.
func (c *Client) Closed() bool { return c.closed.Load() }

// ---- internals ----

func (c *Client) readLoop() {
	defer c.markClosed()
	for {
		f, err := proto.ReadFrame(c.conn)
		if err != nil {
			if !isExpectedClose(err) {
				select {
				case c.errCh <- err:
				default:
				}
			}
			return
		}
		switch f.Type {
		case proto.PUBLISH:
			pub := f.Pub
			c.msgCh <- Message{Topic: pub.Topic, Payload: pub.Payload, QoS: pub.QoS, PacketID: pub.PacketID, Dup: pub.Dup}
			if pub.QoS == 1 {
				c.writeBestEffort(&proto.Packet{Type: proto.PUBACK, PacketID: pub.PacketID})
			}
		case proto.PUBACK:
			c.ackMu.Lock()
			if ch, ok := c.pubAcks[f.PacketID]; ok {
				select {
				case ch <- struct{}{}:
				default:
				}
				delete(c.pubAcks, f.PacketID)
			}
			c.ackMu.Unlock()
		case proto.SUBACK:
			c.ackMu.Lock()
			c.grants[f.PacketID] = f.Granted
			if ch, ok := c.ackCh[f.PacketID]; ok {
				ch <- proto.RCAccepted
				delete(c.ackCh, f.PacketID)
			}
			c.ackMu.Unlock()
		case proto.UNSUBACK:
			c.ackMu.Lock()
			if ch, ok := c.ackCh[f.PacketID]; ok {
				ch <- proto.RCAccepted
				delete(c.ackCh, f.PacketID)
			}
			c.ackMu.Unlock()
		case proto.PINGRESP:
			// nothing to do
		case proto.CONNACK:
			// takeover notification or asynchronous reject
			c.errCh <- &ConnectError{Code: f.Code}
			_ = c.conn.Close()
			return
		default:
			c.errCh <- fmt.Errorf("%w: unexpected type %d", ErrRejected, f.Type)
			return
		}
	}
}

func (c *Client) keepAliveLoop() {
	interval := c.keepalive / 2
	if interval <= 0 {
		interval = time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			c.writeBestEffort(&proto.Packet{Type: proto.PINGREQ})
		case <-c.doneCh:
			return
		}
	}
}

// writeFrame serializes a single complete frame onto the socket. The bounded
// write deadline keeps an unresponsive peer from blocking the caller forever;
// a failed write closes the connection (releasing the read loop/keep-alive).
func (c *Client) writeFrame(p *proto.Packet) error {
	if c.closed.Load() {
		return ErrClosed
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.closed.Load() {
		return ErrClosed
	}
	_ = c.conn.SetWriteDeadline(time.Now().Add(writeDeadline))
	if err := proto.WriteFrame(c.conn, p); err != nil {
		c.markClosed() // releases the read loop and keep-alive
		return err
	}
	return nil
}

// write is the blocking write used by request/response calls.
func (c *Client) write(p *proto.Packet) error { return c.writeFrame(p) }

// writeBestEffort writes a frame whose result the caller ignores (PUBACK for a
// received message, keep-alive PINGREQ). Errors are swallowed because a dead
// connection is detected and cleaned up by writeFrame/read loop anyway.
func (c *Client) writeBestEffort(p *proto.Packet) { _ = c.writeFrame(p) }

func (c *Client) markClosed() {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		_ = c.conn.Close() // unblocks a reader and a blocked writePump
		close(c.doneCh)
	})
	c.closed.Store(true)
}

func (c *Client) allocPubID() (uint16, error) {
	c.pubMu.Lock()
	defer c.pubMu.Unlock()
	if len(c.pubWin) == 65535 {
		return 0, ErrPacketIDWindow
	}
	for {
		c.pubID++
		if c.pubID == 0 {
			c.pubID = 1
		}
		if !c.pubWin[c.pubID] {
			c.pubWin[c.pubID] = true
			return c.pubID, nil
		}
	}
}

func (c *Client) freePubID(id uint16) {
	c.pubMu.Lock()
	delete(c.pubWin, id)
	c.pubMu.Unlock()
}

func (c *Client) allocSubID() uint16 {
	c.subMu.Lock()
	defer c.subMu.Unlock()
	if c.subID == 65535 {
		c.subID = 0
	}
	c.subID++
	return c.subID
}

func (c *Client) removeWaiter(id uint16) {
	c.ackMu.Lock()
	delete(c.ackCh, id)
	delete(c.grants, id)
	c.ackMu.Unlock()
}

func topicContainsWildcard(t string) bool {
	for _, r := range t {
		if r == '+' || r == '#' {
			return true
		}
	}
	return false
}

func isExpectedClose(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed)
}

func filtersInOrder(l []proto.SubFilter) []string {
	out := make([]string, len(l))
	for i, f := range l {
		out[i] = f.Filter
	}
	return out
}
