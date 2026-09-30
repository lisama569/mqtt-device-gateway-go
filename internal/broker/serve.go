package broker

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"time"

	"github.com/lisama569/mqtt-device-gateway-go/internal/proto"
	"github.com/lisama569/mqtt-device-gateway-go/internal/session"
	"github.com/lisama569/mqtt-device-gateway-go/internal/topic"
)

// closeReason records *why* a connection ended, deciding will behavior and
// making logs explainable.
type closeReason int

const (
	reasonProtocolError closeReason = iota // malformed frame / unexpected packet
	reasonOversized
	reasonKeepAliveTimeout
	reasonPeerGone // TCP read returned EOF/reset
	reasonCleanDisconnect
	reasonTakeover
	reasonServerShutdown
)

func (r closeReason) String() string {
	switch r {
	case reasonProtocolError:
		return "protocol-error"
	case reasonOversized:
		return "payload-too-large"
	case reasonKeepAliveTimeout:
		return "keepalive-timeout"
	case reasonPeerGone:
		return "peer-gone"
	case reasonCleanDisconnect:
		return "clean-disconnect"
	case reasonTakeover:
		return "session-takeover"
	case reasonServerShutdown:
		return "server-shutdown"
	default:
		return "unknown"
	}
}

// abnormal reports whether the will must be published for this ending.
func (r closeReason) abnormal() bool {
	switch r {
	case reasonProtocolError, reasonOversized, reasonKeepAliveTimeout, reasonPeerGone:
		return true
	default:
		return false
	}
}

// atomicBool is a tiny atomic flag wrapper.
type atomicBool int32

func (a *atomicBool) set(v bool) {
	var n int32
	if v {
		n = 1
	}
	atomic.StoreInt32((*int32)(a), n)
}
func (a *atomicBool) get() bool { return atomic.LoadInt32((*int32)(a)) != 0 }

const (
	handshakeTimeout = 10 * time.Second
	writeTimeout     = 10 * time.Second
	subscribeFailed  = 0x80 // per-filter failure marker in SUBACK
)

// serveConn performs the handshake, installs the connection and runs exactly
// two goroutines: reader (frames in, keep-alive) and writer (frames out).
func (b *Broker) serveConn(ctx context.Context, raw net.Conn) {
	_ = raw.SetReadDeadline(time.Now().Add(handshakeTimeout))
	frame, err := proto.ReadFrame(raw)
	if err != nil {
		b.rejectBeforeConnect(raw, err)
		return
	}
	if frame.Type != proto.CONNECT || frame.Connect == nil {
		b.log.Warn("handshake: first frame is not CONNECT", "remote", raw.RemoteAddr(), "type", frame.Type)
		_ = raw.Close()
		return
	}
	cp := frame.Connect

	code := validateConnect(cp)
	if code != proto.RCAccepted {
		b.log.Warn("handshake rejected", "client_id", cp.ClientID, "remote", raw.RemoteAddr(), "reason", code)
		_ = writeFrameDirect(raw, &proto.Packet{Type: proto.CONNACK, Code: code})
		_ = raw.Close()
		return
	}

	sess, _ := b.store.GetOrCreate(cp.ClientID, cp.CleanSession)
	c := &conn{
		broker:    b,
		raw:       raw,
		sess:      sess,
		clientID:  cp.ClientID,
		will:      cp.Will,
		keepAlive: time.Duration(cp.KeepAlive) * time.Second,
		frameCh:   make(chan *proto.Packet),
		kick:      make(chan struct{}, 1),
		readDone:  make(chan struct{}),
		closedCh:  make(chan struct{}),
	}
	ctx2, cancel := context.WithCancel(ctx)
	c.cancelTakeover = cancel

	// Deterministic takeover: new connection wins. The old connection gets
	// RCTakeover and is closed; it never publishes a will or touches state.
	b.mu.Lock()
	old := b.conns[cp.ClientID]
	b.conns[cp.ClientID] = c
	b.mu.Unlock()
	if old != nil {
		b.log.Info("session takeover: closing previous connection",
			"client_id", cp.ClientID, "reason", proto.RCTakeover)
		old.takenOver.set(true)
		old.terminate(reasonTakeover, true)
	}

	if err := writeFrameDirect(raw, &proto.Packet{Type: proto.CONNACK, Code: proto.RCAccepted}); err != nil {
		cancel()
		b.cleanup(c, reasonPeerGone)
		return
	}
	b.log.Info("device connected",
		"client_id", c.clientID, "clean_session", cp.CleanSession,
		"keepalive_s", cp.KeepAlive, "has_will", cp.Will != nil)

	go c.reader(ctx2)
	reason := c.writer(ctx2)
	cancel()
	c.terminate(reason, false)
	<-c.readDone
	b.cleanup(c, reason)
}

// reader parses frames and enforces keep-alive. Each frame is handed to the
// writer over an unbuffered channel, so inbound frames are never reordered or
// coalesced with anything. On read error it stores the reason and exits.
func (c *conn) reader(ctx context.Context) {
	defer close(c.readDone)
	if c.keepAlive > 0 {
		_ = c.raw.SetReadDeadline(time.Now().Add(c.keepAlive + c.keepAlive/2))
	} else {
		_ = c.raw.SetReadDeadline(time.Time{})
	}
	for {
		f, err := proto.ReadFrame(c.raw)
		if err != nil {
			c.readErr.Store(err)
			return
		}
		if c.keepAlive > 0 {
			_ = c.raw.SetReadDeadline(time.Now().Add(c.keepAlive + c.keepAlive/2))
		}
		select {
		case c.frameCh <- f:
		case <-c.closedCh:
			return
		case <-ctx.Done():
			return
		}
	}
}

// terminalReadReason returns the classified reason of the reader's exit.
func (c *conn) terminalReadReason() closeReason {
	if v := c.readErr.Load(); v != nil {
		return c.readErrorReason(v.(error))
	}
	return reasonPeerGone
}

// writer is the ONLY routine that writes the socket in steady state. It drains
// ALL outbound work (control frames then ordered QoS1) BEFORE blocking for the
// next inbound frame, so acknowledgements and queued messages are never starved
// by incoming traffic. A kick from the router flushes QoS1 enqueued while
// this peer sends nothing. Inbound frames always take precedence: a ready
// frame is consumed in a non-blocking pass ahead of every blocking wait.
func (c *conn) writer(ctx context.Context) closeReason {
	// 1) Redeliver unacked messages from any previous attachment, Dup=true.
	for _, m := range c.sess.ResendInflight() {
		c.broker.log.Info("replaying unacked message",
			"client_id", c.clientID, "packet_id", m.PacketID, "topic", m.Topic, "dup", true)
		if !c.writePacket(publishPacketFromQueued(m)) {
			return reasonPeerGone
		}
	}
	// 2) Flush messages already queued before attachment completed.
	if r := c.flushQueue(); r != -1 {
		return r
	}

	inboundSeen := map[uint16]bool{}
	for {
		// Priority drain before blocking: control frames (PUBACK/SUBACK/
		// PINGRESP) first, then queued QoS1 in order.
		if r := c.drainCtrl(); r != -1 {
			return r
		}
		if r := c.flushQueue(); r != -1 {
			return r
		}
		// Inbound frame ready? Handle it immediately, ahead of the idle tick.
		select {
		case f, ok := <-c.frameCh:
			if !ok {
				return c.terminalReadReason()
			}
			r, reply := c.handleFrame(ctx, f, inboundSeen)
			if reply != nil && !c.writePacket(reply) {
				return reasonPeerGone
			}
			if r != -1 {
				return r
			}
			continue
		case <-c.readDone:
			return c.terminalReadReason()
		default:
		}

		select {
		case <-ctx.Done():
			return reasonServerShutdown
		case <-c.closedCh:
			return reasonPeerGone
		case <-c.readDone:
			return c.terminalReadReason()
		case <-c.kick:
			// Router queued QoS1 (or control) while the peer was idle; it is
			// drained at the top of the next iteration. Coalesced/edge-free.
		case f, ok := <-c.frameCh:
			if !ok {
				return c.terminalReadReason()
			}
			r, reply := c.handleFrame(ctx, f, inboundSeen)
			if reply != nil && !c.writePacket(reply) {
				return reasonPeerGone
			}
			if r != -1 {
				return r
			}
		}
	}
}

// flushQueue sends every queued QoS1 message in FIFO order.
func (c *conn) flushQueue() closeReason {
	for {
		m, ok := c.sess.PopForSend()
		if !ok {
			return -1
		}
		if !c.writePacket(publishPacketFromQueued(m)) {
			return reasonPeerGone
		}
	}
}

// drainCtrl writes all buffered QoS0/control frames in order.
func (c *conn) drainCtrl() closeReason {
	c.ctrlMu.Lock()
	pending := c.pendingCtrl
	c.pendingCtrl = nil
	c.ctrlMu.Unlock()
	for _, p := range pending {
		if !c.writePacket(p) {
			return reasonPeerGone
		}
	}
	return -1
}

func (c *conn) readErrorReason(err error) closeReason {
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		c.broker.log.Warn("keepalive timeout", "client_id", c.clientID)
		return reasonKeepAliveTimeout
	}
	if isPeerGoneErr(err) {
		return reasonPeerGone
	}
	return classifyError(err)
}

// validateConnect applies handshake-level rules and returns the rejection code.
func validateConnect(cp *proto.ConnectPacket) proto.ReasonCode {
	if cp.ProtocolName != proto.ProtocolName || cp.Version != proto.SupportedVersion {
		return proto.RCUnsupportedProto
	}
	if cp.ClientID == "" {
		return proto.RCBadIdentifier
	}
	if cp.Will != nil {
		if cp.Will.QoS > 1 {
			return proto.RCQoSNotSupported
		}
		if err := topic.ValidatePublishTopic(cp.Will.Topic); err != nil {
			return proto.RCBadTopicFilter
		}
	}
	return proto.RCAccepted
}

func (b *Broker) rejectBeforeConnect(raw net.Conn, err error) {
	cls := classifyError(err)
	b.log.Warn("handshake failed before identification",
		"remote", raw.RemoteAddr(), "reason", cls, "err", errString(err))
	_ = raw.Close()
}

// terminate forcibly closes the socket. When notify is true an explanatory
// CONNACK is attempted first (takeover). To guarantee progress even against a
// peer whose receive window is full (write blocking until closed), the close
// is scheduled concurrently and this method always returns promptly; the
// blocked notify write is abandoned when the deadline elapses.
func (c *conn) terminate(r closeReason, notify bool) {
	c.closeConnOnce.Do(func() {
		if c.cancelTakeover != nil {
			c.cancelTakeover()
		}
		if notify {
			code := proto.RCServerError
			if r == reasonTakeover {
				code = proto.RCTakeover
			}
			// Bound the write so a peer with a full receive window cannot
			// block termination; the deferred Close below ends a stuck write.
			_ = writeFrameDirect(c.raw, &proto.Packet{Type: proto.CONNACK, Code: code})
		}
		_ = c.raw.Close()
		close(c.closedCh)
	})
}

// kickOnce fires the coalesced QoS1 wake (never blocks).
func (c *conn) kickOnce() {
	select {
	case c.kick <- struct{}{}:
	default:
	}
}

// push buffers a QoS0/control frame for the writer. The writer's periodic
// (10ms) idle tick flushes it promptly; the reply to a frame the writer is
// currently handling is drained before the next blocking read. Non-blocking: a
// full control queue means a stalled client closed in isolation.
func (c *conn) push(p *proto.Packet) bool {
	c.ctrlMu.Lock()
	if len(c.pendingCtrl) >= sendQueueDepth {
		c.ctrlMu.Unlock()
		select {
		case <-c.closedCh:
		default:
			c.broker.log.Warn("control queue full, closing only this connection", "client_id", c.clientID)
			c.terminate(reasonProtocolError, false)
		}
		return false
	}
	c.pendingCtrl = append(c.pendingCtrl, p)
	c.ctrlMu.Unlock()
	return true
}

// handleFrame processes one frame. It returns a terminal reason (-1 to keep
// going) and a direct reply that the writer sends synchronously, in the same
// frame-handling turn, so an acknowledgement can never be stranded behind a
// later event.
func (c *conn) handleFrame(ctx context.Context, f *proto.Packet, inboundSeen map[uint16]bool) (closeReason, *proto.Packet) {
	switch f.Type {
	case proto.PUBLISH:
		return c.handleInboundPublish(f, inboundSeen)
	case proto.PUBACK:
		if c.sess.AckPending(f.PacketID) {
			c.broker.log.Info("ack received", "client_id", c.clientID, "packet_id", f.PacketID)
		} else {
			c.broker.log.Info("duplicate or unknown ack ignored",
				"client_id", c.clientID, "packet_id", f.PacketID, "decision", "ignore")
		}
	case proto.SUBSCRIBE:
		return -1, c.buildSubAck(f)
	case proto.UNSUBSCRIBE:
		for _, flt := range f.Unsub.Filters {
			if c.sess.RemoveSubscription(flt) {
				c.broker.log.Info("unsubscribed", "client_id", c.clientID, "filter", flt)
			} else {
				c.broker.log.Info("unsubscribe for unknown filter",
					"client_id", c.clientID, "filter", flt, "decision", "ack-anyway")
			}
		}
		return -1, &proto.Packet{Type: proto.UNSUBACK, PacketID: f.PacketID}
	case proto.PINGREQ:
		return -1, &proto.Packet{Type: proto.PINGRESP}
	case proto.DISCONNECT:
		c.broker.log.Info("clean disconnect", "client_id", c.clientID)
		return reasonCleanDisconnect, nil
	default:
		c.broker.log.Warn("unexpected packet type", "client_id", c.clientID, "type", f.Type)
		return reasonProtocolError, nil
	}
	return -1, nil
}

func (c *conn) handleInboundPublish(f *proto.Packet, inboundSeen map[uint16]bool) (closeReason, *proto.Packet) {
	pub := f.Pub
	if pub.QoS > 1 {
		c.broker.log.Warn("unsupported qos rejected",
			"client_id", c.clientID, "qos", pub.QoS, "topic", pub.Topic, "decision", "close-connection")
		return reasonProtocolError, nil
	}
	if err := topic.ValidatePublishTopic(pub.Topic); err != nil {
		c.broker.log.Warn("invalid publish topic",
			"client_id", c.clientID, "topic", pub.Topic, "decision", "close-connection")
		return reasonProtocolError, nil
	}
	if len(pub.Payload) > c.broker.cfg.MaxPayload {
		c.broker.log.Warn("payload exceeds broker limit",
			"client_id", c.clientID, "size", len(pub.Payload), "limit", c.broker.cfg.MaxPayload)
		return reasonOversized, nil
	}

	dup := false
	if pub.QoS == 1 {
		if inboundSeen[pub.PacketID] {
			dup = true
			c.broker.log.Info("duplicate inbound publish suppressed",
				"client_id", c.clientID, "packet_id", pub.PacketID,
				"topic", pub.Topic, "decision", "re-ack-only")
		} else {
			inboundSeen[pub.PacketID] = true
		}
	}
	if !dup {
		c.broker.submit(pub.Topic, pub.Payload, pub.QoS)
	}
	var reply *proto.Packet
	if pub.QoS == 1 {
		// Acknowledged on every receipt (including a duplicate) but routed once.
		reply = &proto.Packet{Type: proto.PUBACK, PacketID: pub.PacketID}
	}
	return -1, reply
}

// buildSubAck validates and records filters and returns the SUBACK with a
// per-filter grant (0x80-bits set means that filter was rejected, never
// silently accepted).
func (c *conn) buildSubAck(f *proto.Packet) *proto.Packet {
	granted := make([]byte, len(f.Sub.Filters))
	for i, sf := range f.Sub.Filters {
		if err := topic.ValidateFilter(sf.Filter); err != nil {
			c.broker.log.Warn("subscription rejected: bad filter",
				"client_id", c.clientID, "packet_id", f.PacketID,
				"filter", sf.Filter, "code", proto.RCBadTopicFilter)
			granted[i] = subscribeFailed | byte(proto.RCBadTopicFilter)
			continue
		}
		if sf.QoS > 1 {
			c.broker.log.Warn("subscription rejected: unsupported qos",
				"client_id", c.clientID, "packet_id", f.PacketID,
				"filter", sf.Filter, "qos", sf.QoS, "code", proto.RCQoSNotSupported)
			granted[i] = subscribeFailed | byte(proto.RCQoSNotSupported)
			continue
		}
		c.sess.AddSubscription(sf.Filter, sf.QoS)
		granted[i] = sf.QoS
		c.broker.log.Info("subscription accepted",
			"client_id", c.clientID, "packet_id", f.PacketID, "filter", sf.Filter, "qos", sf.QoS)
	}
	return &proto.Packet{Type: proto.SUBACK, PacketID: f.PacketID, Granted: granted}
}

// cleanup runs once per attachment when its writer loop ends.
func (b *Broker) cleanup(c *conn, reason closeReason) {
	c.terminate(reason, false)

	b.mu.Lock()
	owner := b.conns[c.clientID] == c
	if owner {
		delete(b.conns, c.clientID)
	}
	b.mu.Unlock()

	if !owner || c.takenOver.get() {
		b.log.Info("attachment cleaned after takeover", "client_id", c.clientID, "reason", reason)
		return
	}

	switch {
	case reason == reasonCleanDisconnect:
		b.log.Info("device offline (clean)", "client_id", c.clientID, "will_published", false)
	case reason.abnormal() && c.will != nil:
		b.log.Info("abnormal disconnect: publishing will once",
			"client_id", c.clientID, "reason", reason,
			"will_topic", c.will.Topic, "will_qos", c.will.QoS)
		b.submit(c.will.Topic, c.will.Payload, c.will.QoS)
	case reason.abnormal():
		b.log.Info("abnormal disconnect (no will)", "client_id", c.clientID, "reason", reason)
	default:
		b.log.Info("device offline", "client_id", c.clientID, "reason", reason, "will_published", false)
	}
}

func classifyError(err error) closeReason {
	switch {
	case errors.Is(err, proto.ErrOversized):
		return reasonOversized
	case errors.Is(err, proto.ErrUnknownPacket), errors.Is(err, proto.ErrMalformedPayload), errors.Is(err, proto.ErrShortFrame):
		return reasonProtocolError
	default:
		return reasonPeerGone
	}
}

func writeFrameDirect(raw net.Conn, p *proto.Packet) error {
	_ = raw.SetWriteDeadline(time.Now().Add(writeTimeout))
	return proto.WriteFrame(raw, p)
}

func (c *conn) writePacket(p *proto.Packet) bool {
	err := proto.WriteFrame(c.raw, p)

	if err != nil {
		c.broker.log.Warn("writePacket failed", "client_id", c.clientID, "type", p.Type, "pid", p.PacketID, "err", err)
	}
	return err == nil
}

func publishPacketFromQueued(m session.QueuedMessage) *proto.Packet {
	return &proto.Packet{Type: proto.PUBLISH, Pub: &proto.PublishPacket{
		Topic: m.Topic, Payload: m.Payload, QoS: m.QoS,
		PacketID: m.PacketID, Dup: m.Dup,
	}}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
