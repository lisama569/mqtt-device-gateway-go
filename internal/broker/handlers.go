package broker

import (
	"github.com/lisama569/mqtt-device-gateway-go/internal/protocol"
	"github.com/lisama569/mqtt-device-gateway-go/internal/session"
)

// handlePublish processes inbound PUBLISH:
//   - topic/QoS are validated; QoS 2 is rejected explicitly (no downgrade);
//   - QoS 1 requires a non-zero identifier; duplicates inside the dedup
//     window are answered with PUBACK again but NOT re-delivered.
func (c *conn) handlePublish(pkt *protocol.Packet) error {
	if err := protocol.ValidateTopicName(pkt.Topic); err != nil {
		return err
	}
	if pkt.QoS > 1 {
		return protocol.NewError(protocol.ErrQoSNotSupported,
			"clientID=%s published qos=%d; only 0/1 supported (no silent downgrade)",
			c.clientID, pkt.QoS)
	}
	if pkt.QoS == 1 {
		if pkt.PacketID == 0 {
			return protocol.NewError(protocol.ErrMalformedPacket,
				"clientID=%s QoS1 PUBLISH without packet identifier", c.clientID)
		}
		if c.sess.SeenInbound(pkt.PacketID) {
			c.broker.logger.Info("duplicate inbound PUBLISH: re-acknowledging without re-delivery",
				"clientID", c.clientID, "packetID", pkt.PacketID)
			return c.send(&protocol.Packet{
				Type: protocol.TypePuback, PacketID: pkt.PacketID,
			})
		}
	}

	c.broker.logger.Info("route PUBLISH",
		"clientID", c.clientID, "packetID", pkt.PacketID,
		"topic", pkt.Topic, "qos", pkt.QoS, "dup", pkt.Dup, "bytes", len(pkt.Payload))

	// Routing happens synchronously against subscription state; each
	// target's enqueue/transport failure is contained inside deliverOne.
	c.broker.deliverToSubscribers(c.clientID, pkt)

	if pkt.QoS == 1 {
		// Acknowledgement of inbound delivery happens after routing: if this
		// goroutine cannot make progress the client sees the timeout and may
		// retry; duplicate retry is de-duplicated deterministically.
		return c.send(&protocol.Packet{Type: protocol.TypePuback, PacketID: pkt.PacketID})
	}
	return nil
}

// handleSubscribe validates every filter and rejects the whole operation
// on malformed input rather than silently subscribing a subset.
func (c *conn) handleSubscribe(pkt *protocol.Packet) error {
	if pkt.PacketID == 0 {
		return protocol.NewError(protocol.ErrMalformedPacket,
			"clientID=%s SUBSCRIBE without packet identifier", c.clientID)
	}
	if len(pkt.Filters) == 0 {
		return protocol.NewError(protocol.ErrInvalidSubscription,
			"clientID=%s SUBSCRIBE with empty filter list", c.clientID)
	}
	results := make([]protocol.SubResult, len(pkt.Filters))
	for i, f := range pkt.Filters {
		if err := protocol.ValidateFilter(f.Filter); err != nil {
			return err
		}
		if f.QoS > 1 {
			// Per-filter unsupported QoS is reported, not silently lowered:
			// the result carries the failure code for that filter.
			c.broker.logger.Warn("subscription QoS unsupported",
				"clientID", c.clientID, "packetID", pkt.PacketID,
				"filter", f.Filter, "requestedQoS", f.QoS)
			results[i] = protocol.SubResult{Reason: protocol.ReasonSubFilterRejected}
			continue
		}
		c.sess.SetSubscription(f.Filter, f.QoS)
		results[i] = protocol.SubResult{Reason: f.QoS}
		c.broker.logger.Info("subscription installed",
			"clientID", c.clientID, "packetID", pkt.PacketID,
			"filter", f.Filter, "grantedQoS", f.QoS)
	}
	return c.send(&protocol.Packet{
		Type: protocol.TypeSuback, PacketID: pkt.PacketID, Results: results,
	})
}

// handleUnsubscribe removes filters and always confirms with UNSUBACK.
func (c *conn) handleUnsubscribe(pkt *protocol.Packet) error {
	if pkt.PacketID == 0 {
		return protocol.NewError(protocol.ErrMalformedPacket,
			"clientID=%s UNSUBSCRIBE without packet identifier", c.clientID)
	}
	if len(pkt.Filters) == 0 {
		return protocol.NewError(protocol.ErrInvalidSubscription,
			"clientID=%s UNSUBSCRIBE with empty filter list", c.clientID)
	}
	for _, f := range pkt.Filters {
		if err := protocol.ValidateFilter(f.Filter); err != nil {
			return err
		}
		if c.sess.RemoveSubscription(f.Filter) {
			c.broker.logger.Info("subscription removed",
				"clientID", c.clientID, "packetID", pkt.PacketID, "filter", f.Filter)
		} else {
			c.broker.logger.Info("unsubscribe of absent filter",
				"clientID", c.clientID, "packetID", pkt.PacketID, "filter", f.Filter)
		}
	}
	return c.send(&protocol.Packet{Type: protocol.TypeUnsuback, PacketID: pkt.PacketID})
}

// handlePuback completes one outbound inflight packet. Unknown identifiers
// (duplicate or spurious acknowledgement) are logged and ignored; they can
// never remove a different packet from the window.
func (c *conn) handlePuback(pkt *protocol.Packet) error {
	if pkt.PacketID == 0 {
		return protocol.NewError(protocol.ErrMalformedPacket,
			"clientID=%s PUBACK without packet identifier", c.clientID)
	}
	if !c.sess.AckPacketID(pkt.PacketID) {
		c.broker.logger.Warn("spurious/duplicate PUBACK ignored",
			"clientID", c.clientID, "packetID", pkt.PacketID,
			"basis", "identifier not present in inflight window")
		return nil
	}
	c.broker.logger.Info("PUBACK received: inflight slot released",
		"clientID", c.clientID, "packetID", pkt.PacketID,
		"inflight", c.sess.InflightCount())
	c.pumpSession()
	return nil
}

// deliverOne enqueues or immediately sends one routed copy to a target
// session. A failure on this target's transport is confined to this
// session; the abnormal-termination path handles cleanup/will.
func (c *conn) deliverOne(sender string, pkt *protocol.Packet, qos uint8) {
	msg := &session.QueuedMessage{
		Topic: pkt.Topic, Payload: append([]byte(nil), pkt.Payload...),
		QoS: qos, Retain: pkt.Retain,
	}
	if qos == 0 {
		// QoS0: fire-and-forget while online; never queued offline.
		if err := c.directSend(&protocol.Packet{
			Type: protocol.TypePublish, Topic: msg.Topic,
			Payload: msg.Payload, QoS: 0, Retain: msg.Retain,
		}); err != nil {
			c.broker.logger.Warn("QoS0 delivery failed; message not queued",
				"clientID", c.clientID, "topic", msg.Topic, "cause", errorCodeOf(err))
			go c.terminate(false, err)
		}
		return
	}
	// QoS1 to an online owner: join the pending queue behind any backlog
	// so global per-session FIFO order is preserved.
	c.sess.PushPending(msg)
	c.broker.logger.Info("QoS1 message enqueued for subscriber",
		"clientID", c.clientID, "from", sender, "topic", msg.Topic,
		"pending", c.sess.PendingCount())
	c.pumpSession()
}

// pumpSession sends as many queued messages as fit the inflight window.
func (c *conn) pumpSession() {
	for {
		in, ok := c.sess.AllocPacketID()
		if !ok {
			return
		}
		out := &protocol.Packet{
			Type:     protocol.TypePublish,
			Topic:    in.Msg.Topic,
			Payload:  in.Msg.Payload,
			QoS:      1,
			Retain:   in.Msg.Retain,
			PacketID: in.PacketID,
			Dup:      in.Dup,
		}
		if err := c.directSend(out); err != nil {
			// Keep the packet in the inflight window; it is either
			// retransmitted on reconnect (DUP=1) or the terminating path
			// tears this connection down. Put it back at the head of pending
			// would reorder; instead termination will resend it.
			c.broker.logger.Warn("outbound PUBLISH failed; packet retained for retransmit",
				"clientID", c.clientID, "packetID", in.PacketID, "topic", in.Msg.Topic)
			go c.terminate(false, err)
			return
		}
		c.broker.logger.Info("QoS1 PUBLISH delivered",
			"clientID", c.clientID, "packetID", in.PacketID,
			"topic", in.Msg.Topic, "dup", in.Dup,
			"inflight", c.sess.InflightCount())
	}
}

// resendInflight re-emits one pre-disconnect inflight packet with DUP=1.
func (c *conn) resendInflight(in *session.Inflight) {
	out := &protocol.Packet{
		Type: protocol.TypePublish, Topic: in.Msg.Topic, Payload: in.Msg.Payload,
		QoS: 1, Retain: in.Msg.Retain, PacketID: in.PacketID, Dup: true,
	}
	if err := c.directSend(out); err != nil {
		c.broker.logger.Warn("inflight retransmit failed on resume",
			"clientID", c.clientID, "packetID", in.PacketID, "cause", errorCodeOf(err))
		go c.terminate(false, err)
		return
	}
	c.broker.logger.Info("inflight retransmitted with DUP=1",
		"clientID", c.clientID, "packetID", in.PacketID, "topic", in.Msg.Topic)
}
