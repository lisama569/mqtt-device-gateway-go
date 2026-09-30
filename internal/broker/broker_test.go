package broker_test

import (
	"bufio"
	"encoding/binary"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/mqtt-device-gateway-go/client"
	"github.com/lisama569/mqtt-device-gateway-go/internal/broker"
	"github.com/lisama569/mqtt-device-gateway-go/internal/protocol"
)

const testTimeout = 3 * time.Second

func startBroker(t *testing.T) (*broker.Broker, string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gw, err := broker.New(broker.Config{ListenAddr: ln.Addr().String()})
	if err != nil {
		t.Fatalf("new broker: %v", err)
	}
	go func() { _ = gw.ServeListener(ln) }()
	t.Cleanup(func() { _ = gw.Close() })
	time.Sleep(30 * time.Millisecond)
	return gw, ln.Addr().String()
}

// rawClient speaks frames directly over TCP for negative-path tests.
type rawClient struct {
	t   *testing.T
	raw net.Conn
	w   *bufio.Writer
	r   *bufio.Reader
}

func dialRaw(t *testing.T, addr string) *rawClient {
	t.Helper()
	raw, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return &rawClient{t: t, raw: raw, w: bufio.NewWriter(raw), r: bufio.NewReader(raw)}
}

func (rc *rawClient) write(p *protocol.Packet) {
	rc.t.Helper()
	if err := protocol.WriteFrame(rc.w, p); err != nil {
		rc.t.Fatalf("write frame: %v", err)
	}
	if err := rc.w.Flush(); err != nil {
		rc.t.Fatalf("flush: %v", err)
	}
}

func (rc *rawClient) writeRaw(body []byte) {
	rc.t.Helper()
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(len(body)))
	if _, err := rc.raw.Write(prefix[:]); err != nil {
		rc.t.Fatalf("write prefix: %v", err)
	}
	if _, err := rc.raw.Write(body); err != nil {
		rc.t.Fatalf("write body: %v", err)
	}
}

func (rc *rawClient) read() *protocol.Packet {
	rc.t.Helper()
	_ = rc.raw.SetReadDeadline(time.Now().Add(testTimeout))
	p, err := protocol.ReadFrame(rc.r)
	if err != nil {
		rc.t.Fatalf("read frame: %v", err)
	}
	return p
}

func (rc *rawClient) close() { _ = rc.raw.Close() }

func connectPkt(id string, clean bool) *protocol.Packet {
	return &protocol.Packet{
		Type: protocol.TypeConnect, Protocol: protocol.ProtocolName,
		Version: protocol.SupportedVersion, ClientID: id, CleanStart: clean,
	}
}

func mustConnect(t *testing.T, addr, id string, clean bool, h client.MessageHandler) *client.Client {
	t.Helper()
	c, err := client.Connect(client.ConnectConfig{
		Address: addr, ClientID: id, CleanStart: clean,
	}, h)
	if err != nil {
		t.Fatalf("connect %s: %v", id, err)
	}
	t.Cleanup(func() { _ = c.Disconnect() })
	return c
}

func waitCh(t *testing.T, ch <-chan string, what string) string {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(testTimeout):
		t.Fatalf("timed out waiting for %s", what)
	}
	return ""
}

func TestConnectAcceptsAndRejects(t *testing.T) {
	_, addr := startBroker(t)

	t.Run("accepted", func(t *testing.T) {
		rc := dialRaw(t, addr)
		defer rc.close()
		rc.write(connectPkt("ok", true))
		ack := rc.read()
		if ack.Type != protocol.TypeConnack || ack.Reason != protocol.ReasonAccepted {
			t.Fatalf("want accepted CONNACK, got %+v", ack)
		}
	})

	t.Run("bad protocol name", func(t *testing.T) {
		rc := dialRaw(t, addr)
		defer rc.close()
		p := connectPkt("x", true)
		p.Protocol = "MQTT"
		rc.write(p)
		ack := rc.read()
		if ack.Reason != protocol.ReasonUnsupportedVersion {
			t.Fatalf("want reason 1, got %d", ack.Reason)
		}
	})

	t.Run("bad version", func(t *testing.T) {
		rc := dialRaw(t, addr)
		defer rc.close()
		p := connectPkt("x", true)
		p.Version = 99
		rc.write(p)
		ack := rc.read()
		if ack.Reason != protocol.ReasonUnsupportedVersion {
			t.Fatalf("want reason 1, got %d", ack.Reason)
		}
	})

	t.Run("empty client id", func(t *testing.T) {
		rc := dialRaw(t, addr)
		defer rc.close()
		rc.write(connectPkt("", true))
		ack := rc.read()
		if ack.Reason != protocol.ReasonIdentifierRejected {
			t.Fatalf("want reason 2, got %d", ack.Reason)
		}
	})

	t.Run("first packet not CONNECT", func(t *testing.T) {
		rc := dialRaw(t, addr)
		defer rc.close()
		rc.write(&protocol.Packet{Type: protocol.TypePingreq})
		ack := rc.read()
		if ack.Reason != protocol.ReasonMalformed {
			t.Fatalf("want reason 3, got %d", ack.Reason)
		}
	})

	t.Run("malformed JSON", func(t *testing.T) {
		rc := dialRaw(t, addr)
		defer rc.close()
		rc.writeRaw([]byte("{not json"))
		ack := rc.read()
		if ack.Reason != protocol.ReasonMalformed {
			t.Fatalf("want reason 3, got %d", ack.Reason)
		}
	})

	t.Run("oversized frame", func(t *testing.T) {
		rc := dialRaw(t, addr)
		defer rc.close()
		var prefix [4]byte
		binary.BigEndian.PutUint32(prefix[:], uint32(protocol.MaxFrameSize+1))
		_, _ = rc.raw.Write(prefix[:])
		// Deterministic outcome: the gateway either sends a rejection
		// CONNACK and closes, or closes directly. It must never block
		// waiting to drain the oversized body and must never stay open.
		_ = rc.raw.SetReadDeadline(time.Now().Add(testTimeout))
		p, err := protocol.ReadFrame(rc.r)
		if err == nil {
			if p.Type != protocol.TypeConnack {
				t.Fatalf("want rejection CONNACK, got %s", p.Type)
			}
		}
		// The follow-up read must report a closed transport.
		_ = rc.raw.SetReadDeadline(time.Now().Add(testTimeout))
		if _, err := rc.raw.Read(make([]byte, 4)); err == nil {
			t.Fatal("connection still open after oversized frame")
		}
	})
}

func TestConcurrentSameClientIDTakeOver(t *testing.T) {
	gw, addr := startBroker(t)

	// Old connection is fully established.
	old := mustConnect(t, addr, "dup-dev", true, nil)
	time.Sleep(50 * time.Millisecond)
	if got := gw.ActiveConns(); got != 1 {
		t.Fatalf("active conns before take-over = %d, want 1", got)
	}

	// Hammer reconnects concurrently; exactly one client must own the
	// session at any time. The rule: newest CONNECT wins; the old owner
	// is closed WITHOUT its will being published.
	var wg sync.WaitGroup
	const n = 12
	wg.Add(n)
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			c, err := client.Connect(client.ConnectConfig{
				Address: addr, ClientID: "dup-dev", CleanStart: true,
			}, nil)
			if err != nil {
				errCh <- err
				return
			}
			time.Sleep(time.Duration(i%3) * time.Millisecond)
			_ = c.Disconnect()
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent connect failed: %v", err)
	}

	// Throughout the storm one identifier can have at most one owner.
	// (The registry invariant is structural; sample it after completion too.)
	if got := gw.ActiveConns(); got > 1 {
		t.Fatalf("active conns for one clientID = %d, must never exceed 1", got)
	}

	// The original owner must have been closed by take-over, not by itself.
	done := make(chan error, 1)
	go func() { done <- old.Wait() }()
	select {
	case <-done:
	case <-time.After(testTimeout):
		t.Fatal("old connection was not closed by take-over")
	}
}

func TestPersistentSessionOfflineReplayInOrder(t *testing.T) {
	_, addr := startBroker(t)

	sub1 := mustConnect(t, addr, "sub", false, nil)
	res, err := sub1.Subscribe([]protocol.SubFilter{{Filter: "t/data", QoS: 1}})
	if err != nil || res[0].Reason != 1 {
		t.Fatalf("subscribe: res=%+v err=%v", res, err)
	}
	if err := sub1.KillForDemo(); err != nil { // abnormal: will-less, session retained
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	pub := mustConnect(t, addr, "pub", true, nil)
	const N = 5
	for i := 0; i < N; i++ {
		if err := pub.Publish("t/data", 1, []byte("m"+strconv.Itoa(i))); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	_ = pub.Disconnect()

	// Reconnect BEFORE reading: replay happens on the new transport.
	var gotMu sync.Mutex
	var got []string
	var ids []uint16
	sub2 := mustConnect(t, addr, "sub", false, func(topic string, p []byte, id uint16, dup bool) {
		gotMu.Lock()
		got = append(got, string(p))
		ids = append(ids, id)
		gotMu.Unlock()
	})
	t.Cleanup(func() { _ = sub2.Disconnect() })

	deadline := time.Now().Add(testTimeout)
	for time.Now().Before(deadline) {
		gotMu.Lock()
		n := len(got)
		gotMu.Unlock()
		if n >= N {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	gotMu.Lock()
	defer gotMu.Unlock()
	if len(got) != N {
		t.Fatalf("replayed %d of %d messages: %v", len(got), N, got)
	}
	for i := 0; i < N; i++ {
		if got[i] != "m"+strconv.Itoa(i) {
			t.Fatalf("order/dup broken: %v", got)
		}
	}
	idsSeen := map[uint16]bool{}
	for _, id := range ids {
		if id == 0 || idsSeen[id] {
			t.Fatalf("packet id invalid or repeated: %v", ids)
		}
		idsSeen[id] = true
	}
}

func TestDuplicateInboundQoS1AckedOnce(t *testing.T) {
	_, addr := startBroker(t)
	delivered := make(chan string, 8)
	sink := mustConnect(t, addr, "sink", false, func(topic string, p []byte, id uint16, dup bool) {
		delivered <- string(p) + "/" + strconv.FormatBool(dup)
	})
	if _, err := sink.Subscribe([]protocol.SubFilter{{Filter: "t/x", QoS: 1}}); err != nil {
		t.Fatal(err)
	}

	rc := dialRaw(t, addr)
	defer rc.close()
	rc.write(connectPkt("dup-pub", true))
	if ack := rc.read(); ack.Reason != protocol.ReasonAccepted {
		t.Fatalf("connect rejected: %d", ack.Reason)
	}
	pub := &protocol.Packet{Type: protocol.TypePublish, Topic: "t/x", QoS: 1, PacketID: 42, Payload: []byte("v")}
	rc.write(pub)
	firstAck := rc.read()
	if firstAck.Type != protocol.TypePuback || firstAck.PacketID != 42 {
		t.Fatalf("want PUBACK 42, got %+v", firstAck)
	}
	// Exact retransmit (same identifier) -> another PUBACK, one delivery.
	rc.write(pub)
	secondAck := rc.read()
	if secondAck.Type != protocol.TypePuback || secondAck.PacketID != 42 {
		t.Fatalf("want duplicate PUBACK 42, got %+v", secondAck)
	}
	select {
	case v := <-delivered:
		if v != "v/false" {
			t.Fatalf("delivery = %q", v)
		}
	case <-time.After(testTimeout):
		t.Fatal("first publish not delivered")
	}
	select {
	case v := <-delivered:
		t.Fatalf("duplicate was re-delivered: %s", v)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestQoS2RejectedAndInvalidSubscriptions(t *testing.T) {
	_, addr := startBroker(t)
	rc := dialRaw(t, addr)
	defer rc.close()
	rc.write(connectPkt("strict", true))
	if ack := rc.read(); ack.Reason != protocol.ReasonAccepted {
		t.Fatalf("connect: %d", ack.Reason)
	}
	// QoS2 publish must terminate the connection abnormally (no downgrade).
	rc.write(&protocol.Packet{Type: protocol.TypePublish, Topic: "a", QoS: 2})
	_ = rc.raw.SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 16)
	if _, err := rc.raw.Read(buf); err == nil {
		t.Fatal("connection stayed open after QoS2 publish")
	}

	// Malformed filters are refused, not silently accepted.
	rc2 := dialRaw(t, addr)
	defer rc2.close()
	rc2.write(connectPkt("strict2", true))
	_ = rc2.read()
	rc2.write(&protocol.Packet{
		Type: protocol.TypeSubscribe, PacketID: 1,
		Filters: []protocol.SubFilter{{Filter: "a/#/b", QoS: 1}},
	})
	_ = rc2.raw.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := rc2.raw.Read(make([]byte, 8)); err == nil {
		t.Fatal("connection stayed open after malformed SUBSCRIBE")
	}

	// Empty filter list is also a protocol error.
	rc3 := dialRaw(t, addr)
	defer rc3.close()
	rc3.write(connectPkt("strict3", true))
	_ = rc3.read()
	rc3.write(&protocol.Packet{Type: protocol.TypeSubscribe, PacketID: 2})
	_ = rc3.raw.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := rc3.raw.Read(make([]byte, 8)); err == nil {
		t.Fatal("connection stayed open after empty SUBSCRIBE")
	}
}

func TestWillOnAbnormalOnlyAndExactlyOnce(t *testing.T) {
	_, addr := startBroker(t)
	willCh := make(chan string, 8)

	// Watcher: persistent so it can observe will delivery.
	w := mustConnect(t, addr, "watcher", false, func(topic string, p []byte, id uint16, dup bool) {
		if topic == "evt/will" {
			willCh <- string(p)
		}
	})
	_, err := w.Subscribe([]protocol.SubFilter{{Filter: "evt/#", QoS: 1}})
	if err != nil {
		t.Fatal(err)
	}

	t.Run("normal disconnect publishes no will", func(t *testing.T) {
		c, err := client.Connect(client.ConnectConfig{
			Address: addr, ClientID: "normal-dev", CleanStart: true,
			Will: &protocol.WillMessage{Topic: "evt/will", QoS: 1, Payload: []byte("NORMAL?")},
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Disconnect(); err != nil {
			t.Fatal(err)
		}
		select {
		case v := <-willCh:
			t.Fatalf("will published on normal DISCONNECT: %s", v)
		case <-time.After(400 * time.Millisecond):
		}
	})

	t.Run("take-over publishes no will", func(t *testing.T) {
		c1 := mustConnect(t, addr, "take-dev", true, nil)
		// Attach a will via raw reconnect semantics is complex; the SDK
		// Connect with will then a second same-ID Connect must suppress it.
		_ = c1.Disconnect()
		c2, err := client.Connect(client.ConnectConfig{
			Address: addr, ClientID: "take2-dev", CleanStart: true,
			Will: &protocol.WillMessage{Topic: "evt/will", QoS: 1, Payload: []byte("TAKEOVER?")},
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
		c3, err := client.Connect(client.ConnectConfig{
			Address: addr, ClientID: "take2-dev", CleanStart: true,
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
		_ = c2.Wait() // old transport closed by take-over
		_ = c3.Disconnect()
		select {
		case v := <-willCh:
			t.Fatalf("will published on take-over: %s", v)
		case <-time.After(400 * time.Millisecond):
		}
	})

	t.Run("abnormal disconnect publishes will exactly once", func(t *testing.T) {
		c, err := client.Connect(client.ConnectConfig{
			Address: addr, ClientID: "abn-dev", CleanStart: true,
			Will: &protocol.WillMessage{Topic: "evt/will", QoS: 1, Payload: []byte("BYE")},
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.KillForDemo(); err != nil {
			t.Fatal(err)
		}
		if v := waitCh(t, willCh, "will message"); v != "BYE" {
			t.Fatalf("will payload=%q want BYE", v)
		}
		select {
		case v := <-willCh:
			t.Fatalf("will delivered twice: %s", v)
		case <-time.After(500 * time.Millisecond):
		}
	})
}

func TestWildcardRoutingAndFaultIsolation(t *testing.T) {
	_, addr := startBroker(t)
	a := make(chan string, 8)
	b := make(chan string, 8)
	ca := mustConnect(t, addr, "a", true, func(topic string, p []byte, _ uint16, _ bool) {
		a <- topic + ":" + string(p)
	})
	cb := mustConnect(t, addr, "b", true, func(topic string, p []byte, _ uint16, _ bool) {
		b <- topic + ":" + string(p)
	})
	if _, err := ca.Subscribe([]protocol.SubFilter{{Filter: "floor/+/temp", QoS: 1}}); err != nil {
		t.Fatal(err)
	}
	if _, err := cb.Subscribe([]protocol.SubFilter{{Filter: "floor/2/#", QoS: 1}}); err != nil {
		t.Fatal(err)
	}

	pub := mustConnect(t, addr, "pub", true, nil)
	if err := pub.Publish("floor/1/temp", 1, []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := pub.Publish("floor/2/temp", 1, []byte("2")); err != nil {
		t.Fatal(err)
	}
	if err := pub.Publish("floor/2/humidity", 1, []byte("3")); err != nil {
		t.Fatal(err)
	}

	// a: floor/+/temp -> floor/1/temp, floor/2/temp (not humidity)
	for i, want := range []string{"floor/1/temp:1", "floor/2/temp:2"} {
		if got := waitCh(t, a, "subscriber a delivery"); got != want {
			t.Fatalf("a delivery %d = %q want %q", i, got, want)
		}
	}
	// b: floor/2/# -> temp and humidity under floor/2 (not floor/1)
	for i, want := range []string{"floor/2/temp:2", "floor/2/humidity:3"} {
		if got := waitCh(t, b, "subscriber b delivery"); got != want {
			t.Fatalf("b delivery %d = %q want %q", i, got, want)
		}
	}

	// Fault isolation: a misbehaving raw connection sending garbage must
	// not affect subscriber b's ability to keep receiving.
	bad := dialRaw(t, addr)
	defer bad.close()
	bad.write(connectPkt("bad", true))
	_ = bad.read()
	bad.writeRaw([]byte("@@@@ not json @@@@"))
	time.Sleep(100 * time.Millisecond) // let the broker drop the bad conn

	if err := pub.Publish("floor/2/humidity", 1, []byte("4")); err != nil {
		t.Fatalf("publish after neighbor failure: %v", err)
	}
	if got := waitCh(t, b, "b delivery after neighbor failure"); got != "floor/2/humidity:4" {
		t.Fatalf("fault isolation broken, got %q", got)
	}
}

func TestKeepAliveTimeoutTriggersWill(t *testing.T) {
	_, addr := startBroker(t)
	willCh := make(chan string, 4)
	watcher := mustConnect(t, addr, "ka-watcher", false, func(topic string, p []byte, _ uint16, _ bool) {
		if topic == "ka/will" {
			willCh <- string(p)
		}
	})
	if _, err := watcher.Subscribe([]protocol.SubFilter{{Filter: "ka/#", QoS: 1}}); err != nil {
		t.Fatal(err)
	}

	// Raw connection with keepalive=1s that then goes silent. The client
	// SDK would send PINGREQ; here we deliberately never do.
	rc := dialRaw(t, addr)
	defer rc.close()
	rc.write(&protocol.Packet{
		Type: protocol.TypeConnect, Protocol: protocol.ProtocolName,
		Version: protocol.SupportedVersion, ClientID: "ka-dev",
		CleanStart: true, KeepAlive: 1,
		Will: &protocol.WillMessage{Topic: "ka/will", QoS: 1, Payload: []byte("KA-BYE")},
	})
	if ack := rc.read(); ack.Reason != protocol.ReasonAccepted {
		t.Fatalf("connect rejected: %d", ack.Reason)
	}
	if v := waitCh(t, willCh, "will after keepalive timeout"); v != "KA-BYE" {
		t.Fatalf("will=%q want KA-BYE", v)
	}
	select {
	case v := <-willCh:
		t.Fatalf("will published more than once: %s", v)
	case <-time.After(500 * time.Millisecond):
	}
}

func TestInflightRetransmitWithDupOnReconnect(t *testing.T) {
	_, addr := startBroker(t)

	// Raw subscriber: subscribe at QoS1 but never PUBACK, so every message
	// stays in the broker's inflight window.
	rc := dialRaw(t, addr)
	defer rc.close()
	rc.write(connectPkt("slow", false))
	if ack := rc.read(); ack.Reason != protocol.ReasonAccepted {
		t.Fatalf("connect: %d", ack.Reason)
	}
	rc.write(&protocol.Packet{
		Type: protocol.TypeSubscribe, PacketID: 1,
		Filters: []protocol.SubFilter{{Filter: "f/x", QoS: 1}},
	})
	suback := rc.read()
	if suback.Type != protocol.TypeSuback || suback.Results[0].Reason != 1 {
		t.Fatalf("suback: %+v", suback)
	}

	pub := mustConnect(t, addr, "pub", true, nil)
	if err := pub.Publish("f/x", 1, []byte("only")); err != nil {
		t.Fatal(err)
	}
	first := rc.read()
	if first.Type != protocol.TypePublish || first.Dup || first.PacketID == 0 {
		t.Fatalf("first delivery must be DUP=0 with valid id, got %+v", first)
	}
	firstID := first.PacketID

	// Abrupt disconnect WITHOUT acknowledging: packet remains inflight.
	rc.close()
	time.Sleep(80 * time.Millisecond)

	// Reconnect (persistent). The unacked packet is re-sent with the SAME
	// packet identifier and DUP=1, before anything new.
	rc2 := dialRaw(t, addr)
	defer rc2.close()
	rc2.write(connectPkt("slow", false))
	if ack := rc2.read(); ack.Reason != protocol.ReasonAccepted {
		t.Fatalf("reconnect: %d", ack.Reason)
	}
	redelivered := rc2.read()
	if redelivered.Type != protocol.TypePublish {
		t.Fatalf("want PUBLISH on resume, got %s", redelivered.Type)
	}
	if !redelivered.Dup {
		t.Fatal("retransmitted packet must carry DUP=1")
	}
	if redelivered.PacketID != firstID {
		t.Fatalf("retransmitted id=%d must equal original %d", redelivered.PacketID, firstID)
	}
	if string(redelivered.Payload) != "only" {
		t.Fatalf("payload=%q", redelivered.Payload)
	}
	// Exactly one replay, no duplicate.
	_ = rc2.raw.SetReadDeadline(time.Now().Add(400 * time.Millisecond))
	if p, err := protocol.ReadFrame(rc2.r); err == nil {
		t.Fatalf("unexpected extra frame on resume: %+v", p)
	}
}

func TestEffectiveQoSDowngradeBySubscription(t *testing.T) {
	_, addr := startBroker(t)
	got := make(chan uint8, 4)
	sub := mustConnect(t, addr, "qsub", true, func(topic string, p []byte, id uint16, dup bool) {
		if id == 0 {
			got <- 0
		} else {
			got <- 1
		}
	})
	// Subscription grants only QoS0; publisher sends QoS1.
	if _, err := sub.Subscribe([]protocol.SubFilter{{Filter: "q/t", QoS: 0}}); err != nil {
		t.Fatal(err)
	}
	pub := mustConnect(t, addr, "qpub", true, nil)
	if err := pub.Publish("q/t", 1, []byte("d")); err != nil {
		t.Fatal(err)
	}
	select {
	case q := <-got:
		if q != 0 {
			t.Fatalf("effective delivery qos=%d, want downgrade to 0", q)
		}
	case <-time.After(testTimeout):
		t.Fatal("no delivery")
	}
}
