package broker

import (
	"errors"
	"io"
	"net"
	"strings"
	"syscall"

	"github.com/lisama569/mqtt-device-gateway-go/internal/proto"
	"github.com/lisama569/mqtt-device-gateway-go/internal/topic"
)

// submit hands a published message to the dedicated router goroutine. It is
// non-blocking: the queue is large and bounded; if it ever fills (router
// saturated) the message is dropped with a logged, explainable reason rather
// than stalling the publishing device's read loop.
func (b *Broker) submit(topicName string, payload []byte, qos byte) {
	ev := published{topic: topicName, payload: append([]byte(nil), payload...), qos: qos}
	select {
	case b.routeQ <- ev:
	default:
		b.log.Error("router queue full; dropping message",
			"topic", topicName, "qos", qos, "reason", "backpressure")
	}
}

// route fans one message out to every matching session. It runs only on the
// router goroutine, so it may take its time without blocking any device.
//
// Delivery semantics:
//   - Online subscriber: effective QoS = min(publishQoS, subscribedQoS). QoS1
//     enters the ordered session queue and the connection is kicked; QoS0 goes
//     to its bounded outbox.
//   - Offline persistent subscriber: QoS1 is appended to the ordered offline
//     queue (no duplicates) and flushed on reconnect; QoS0 is dropped.
//   - Offline clean-session subscriber has no state: dropped.
func (b *Broker) route(topicName string, payload []byte, pubQoS byte) {
	type target struct {
		c      *conn
		subQoS byte
	}
	var online []target

	b.mu.Lock()
	for _, c := range b.conns {
		for filter, q := range c.sess.Subscriptions() {
			if topic.Match(filter, topicName) {
				online = append(online, target{c: c, subQoS: q})
				break
			}
		}
	}
	b.mu.Unlock()

	onlineDelivered := 0
	for _, tg := range online {
		eff := pubQoS
		if tg.subQoS < eff {
			eff = tg.subQoS
		}
		if b.deliverTo(tg.c, topicName, payload, eff) {
			onlineDelivered++
		}
	}

	offlineDelivered := b.routeOffline(topicName, payload, pubQoS)

	b.log.Info("message routed",
		"topic", topicName, "qos", pubQoS,
		"online_matches", onlineDelivered, "offline_queued", offlineDelivered,
		"payload_bytes", len(payload))
}

// routeOffline enqueues QoS1 for persistent sessions that are not attached.
func (b *Broker) routeOffline(topicName string, payload []byte, pubQoS byte) int {
	if pubQoS < 1 {
		return 0 // QoS0 is never stored for offline sessions
	}
	queued := 0
	for _, clientID := range b.store.PersistentClientIDs() {
		b.mu.Lock()
		_, live := b.conns[clientID]
		b.mu.Unlock()
		if live {
			continue
		}
		sess, _ := b.store.GetOrCreate(clientID, false)
		matched := false
		for filter := range sess.Subscriptions() {
			if topic.Match(filter, topicName) {
				matched = true
				break
			}
		}
		if !matched {
			continue
		}
		id, ok := sess.Enqueue(topicName, payload, 1)
		if !ok {
			b.log.Error("offline queue rejected: packet-id window full",
				"client_id", clientID, "topic", topicName)
			continue
		}
		queued++
		b.log.Info("message queued for offline device",
			"client_id", clientID, "packet_id", id,
			"topic", topicName, "queue_len", sess.QueuedLen())
	}
	return queued
}

// deliverTo puts one copy toward a live connection. It never performs socket
// I/O: QoS1 goes to the session queue, QoS0 to the bounded outbox.
func (b *Broker) deliverTo(c *conn, topicName string, payload []byte, qos byte) bool {
	if qos >= 1 {
		id, ok := c.sess.Enqueue(topicName, payload, qos)
		if !ok {
			b.log.Error("enqueue failed: packet-id window full",
				"client_id", c.clientID, "topic", topicName)
			return false
		}
		b.log.Info("message queued for delivery",
			"client_id", c.clientID, "packet_id", id, "topic", topicName, "qos", qos)
		c.kickOnce()
		return true
	}
	ok := c.push(&proto.Packet{Type: proto.PUBLISH, Pub: &proto.PublishPacket{
		Topic: topicName, Payload: append([]byte(nil), payload...), QoS: 0,
	}})
	if ok {
		b.log.Info("message delivered qos0", "client_id", c.clientID, "topic", topicName)
	}
	return ok
}

// isPeerGoneErr recognizes a torn-down or half-closed socket as an abnormal
// peer departure (EOF, reset, broken pipe, use of closed connection).
func isPeerGoneErr(err error) bool {
	return errors.Is(err, io.EOF) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, syscall.ECONNRESET) ||
		err != nil && strings.Contains(err.Error(), "broken pipe")
}
