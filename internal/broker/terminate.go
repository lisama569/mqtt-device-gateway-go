package broker

import (
	"io"
	"net"
	"sync"
	"time"

	"github.com/lisama569/mqtt-device-gateway-go/internal/protocol"
)

// directSend serializes frame writes on one connection. A take-over or
// termination closes shutdownCh and the raw socket, so a blocked writer
// cannot leak a goroutine or write onto a replaced transport.
func (c *conn) directSend(pkt *protocol.Packet) error {
	if c.closeOnce.Load() {
		return io.ErrClosedPipe
	}
	select {
	case token := <-c.writeToken:
		defer func() { c.writeToken <- token }()
	case <-c.shutdownCh:
		return net.ErrClosed
	}
	if c.closeOnce.Load() {
		return io.ErrClosedPipe
	}
	if err := protocol.WriteFrame(c.w, pkt); err != nil {
		return err
	}
	return c.w.Flush()
}

// send is the alias used by control-path handlers.
func (c *conn) send(pkt *protocol.Packet) error { return c.directSend(pkt) }

// shutdown force-closes the transport of a taken-over connection so its
// blocked read/write goroutines unwind. The eventual terminate call sees
// takenOver=true and therefore publishes no will and deletes no session.
func (c *conn) shutdown(cause error) {
	_ = c.raw.SetDeadline(time.Now())
	_ = c.raw.Close()
}

// terminate is the exactly-once end-of-life handler.
//
// Decision table (basis logged in every case):
//
//	normal=true (DISCONNECT)      -> no will; temp session deleted
//	taken over by newer CONNECT   -> no will; session ownership unchanged
//	abnormal (error/timeout/EOF)  -> will published exactly once, if any
//	pre-handshake failure         -> no session/ownership, no will
func (c *conn) terminate(normal bool, cause error) {
	if !c.closeOnce.CompareAndSwap(false, true) {
		return
	}
	if normal {
		c.endedNormally.Store(true)
	}
	close(c.shutdownCh)
	c.stopKeepalive() // signals AND waits for the watchdog (non-blocking if called from it)
	_ = c.raw.Close()

	// Unregister FIRST while still holding the session reference; the
	// identity check makes a taken-over old conn unable to evict the new
	// owner. Doing this before will publication also guarantees the dead
	// client never receives its own will.
	c.broker.unregister(c.clientID, c)

	takenOver := c.takenOver.Load()

	switch {
	case c.sess == nil:
		c.broker.logger.Info("connection closed before session established",
			"clientID", c.clientID, "normal", normal, "cause", errorCodeOf(cause))
	case takenOver:
		c.broker.logger.Info("old connection closed by take-over: will suppressed",
			"clientID", c.clientID, "basis", "newer CONNECT owns the session",
			"cause", errorCodeOf(cause))
	case normal:
		if c.sess.Persistent() {
			c.broker.logger.Info("persistent session retained after normal disconnect",
				"clientID", c.clientID,
				"subscriptions", len(c.sess.Subscriptions()),
				"inflight", c.sess.InflightCount(),
				"pending", c.sess.PendingCount(),
				"basis", "will suppressed on normal DISCONNECT")
		} else {
			c.broker.store.Delete(c.clientID, c.sess)
			c.broker.logger.Info("temporary session discarded after normal disconnect",
				"clientID", c.clientID, "basis", "cleanStart session lifetime ends with transport")
		}
	default:
		// Abnormal termination: publish the will exactly once, then settle
		// the session. Persistent sessions stay (offline-queue eligible);
		// temporary sessions are removed, but the will is still published.
		c.publishWill(cause)
		if c.sess.Persistent() {
			c.broker.logger.Info("persistent session retained after abnormal disconnect",
				"clientID", c.clientID,
				"inflight", c.sess.InflightCount(),
				"pending", c.sess.PendingCount(),
				"offline", c.sess.OfflineCount(),
				"basis", "state kept for ordered redelivery on reconnect")
		} else {
			c.broker.store.Delete(c.clientID, c.sess)
			c.broker.logger.Info("temporary session discarded after abnormal disconnect",
				"clientID", c.clientID)
		}
	}
}

// publishWill emits the registered will message once through the normal
// routing path (so offline persistent subscribers queue it in order).
func (c *conn) publishWill(cause error) {
	if c.will == nil {
		c.broker.logger.Info("abnormal disconnect without will",
			"clientID", c.clientID, "cause", errorCodeOf(cause))
		return
	}
	will := *c.will
	c.broker.logger.Warn("abnormal disconnect: publishing will message",
		"clientID", c.clientID, "cause", errorCodeOf(cause),
		"willTopic", will.Topic, "willQoS", will.QoS)
	c.broker.deliverToSubscribers(c.clientID, &protocol.Packet{
		Type:    protocol.TypePublish,
		Topic:   will.Topic,
		Payload: append([]byte(nil), will.Payload...),
		QoS:     will.QoS,
	})
}

// ---- keepalive watchdog ------------------------------------------------
//
// One watchdog goroutine tracks the time of the last inbound packet.
// On timeout it asks for termination ASYNCHRONOUSLY and exits immediately,
// so terminate() may always synchronously join the watchdog without the
// risk of the watchdog joining itself (self-deadlock).

type keepaliveState struct {
	mu     sync.Mutex
	last   time.Time
	stopCh chan struct{}
	stop   sync.Once
}

func (c *conn) startKeepalive() {
	c.ka = &keepaliveState{last: time.Now(), stopCh: make(chan struct{})}
	if c.keepAlive <= 0 {
		return // no watchdog: stopKeepalive is a cheap no-op
	}
	c.kaWg.Add(1)
	timeout := c.keepAlive * 3 / 2
	go func() {
		defer c.kaWg.Done()
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-c.ka.stopCh:
				return
			case <-ticker.C:
				c.ka.mu.Lock()
				expired := time.Since(c.ka.last) > timeout
				c.ka.mu.Unlock()
				if expired {
					cause := protocol.NewError(protocol.ErrKeepAliveTimeout,
						"no packet within 1.5x keepalive (%s)", timeout)
					// Do not call terminate from this goroutine: it would
					// wait here via kaWg. Hand it off and unwind now.
					go c.terminate(false, cause)
					return
				}
			}
		}
	}()
}

func (c *conn) touchKeepalive() {
	if c.ka == nil {
		return
	}
	c.ka.mu.Lock()
	c.ka.last = time.Now()
	c.ka.mu.Unlock()
}

// stopKeepalive signals the watchdog and waits for it to finish. It is
// never invoked from the watchdog goroutine itself (that goroutine exits
// before terminate runs), so the wait cannot self-deadlock.
func (c *conn) stopKeepalive() {
	if c.ka == nil {
		return
	}
	c.ka.stop.Do(func() { close(c.ka.stopCh) })
	c.kaWg.Wait()
}
