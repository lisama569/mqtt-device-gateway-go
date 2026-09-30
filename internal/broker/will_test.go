package broker_test

import (
	"context"
	"testing"
	"time"

	"github.com/lisama569/mqtt-device-gateway-go/internal/client"
	"github.com/lisama569/mqtt-device-gateway-go/internal/proto"
)

// 异常断开（Kill）：遗嘱必须发布且只发布一次。
func TestWillPublishedOnceOnAbnormalDisconnect(t *testing.T) {
	b := testBroker(t)
	ctx := context.Background()

	watcher, err := client.Dial(ctx, b.Addr(), client.Options{ClientID: "watcher", CleanSession: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = watcher.Disconnect() })
	if _, err := watcher.Subscribe(ctx, map[string]byte{"will/dying": 1}); err != nil {
		t.Fatal(err)
	}

	dying, err := client.Dial(ctx, b.Addr(), client.Options{
		ClientID: "dying", CleanSession: true,
		WillTopic: "will/dying", WillQoS: 1, WillPayload: []byte("last-words"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := dying.Kill(); err != nil {
		t.Fatal(err)
	}

	recv(t, watcher, "last-words")

	// 不应出现第二条遗嘱
	select {
	case m := <-watcher.Messages():
		t.Fatalf("will delivered more than once: %q", m.Payload)
	case <-time.After(300 * time.Millisecond):
	}
}

// 正常断开（DISCONNECT）：遗嘱绝不发布。
func TestWillSuppressedOnCleanDisconnect(t *testing.T) {
	b := testBroker(t)
	ctx := context.Background()

	watcher, err := client.Dial(ctx, b.Addr(), client.Options{ClientID: "watcher2", CleanSession: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = watcher.Disconnect() })
	if _, err := watcher.Subscribe(ctx, map[string]byte{"will/clean": 1}); err != nil {
		t.Fatal(err)
	}

	dying, err := client.Dial(ctx, b.Addr(), client.Options{
		ClientID: "dying2", CleanSession: true,
		WillTopic: "will/clean", WillQoS: 1, WillPayload: []byte("should-not-appear"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := dying.Disconnect(); err != nil {
		t.Fatal(err)
	}

	select {
	case m := <-watcher.Messages():
		t.Fatalf("will must not be published on clean disconnect, got %q", m.Payload)
	case <-time.After(300 * time.Millisecond):
	}
}

// 会话接管：被替换的旧连接不应发布遗嘱（设备并未死亡，只是换了连接）。
func TestWillSuppressedOnTakeover(t *testing.T) {
	b := testBroker(t)
	ctx := context.Background()

	watcher, err := client.Dial(ctx, b.Addr(), client.Options{ClientID: "watcher3", CleanSession: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = watcher.Disconnect() })
	if _, err := watcher.Subscribe(ctx, map[string]byte{"will/take": 1}); err != nil {
		t.Fatal(err)
	}

	first, err := client.Dial(ctx, b.Addr(), client.Options{
		ClientID: "take-dev", CleanSession: false,
		WillTopic: "will/take", WillQoS: 1, WillPayload: []byte("no-will"),
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := client.Dial(ctx, b.Addr(), client.Options{
		ClientID: "take-dev", CleanSession: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Disconnect() })
	_ = first

	select {
	case m := <-watcher.Messages():
		t.Fatalf("takeover must suppress old will, got %q", m.Payload)
	case <-time.After(300 * time.Millisecond):
	}
}

// 重复 PUBLISH（相同报文标识）只路由一次，但每次都要应答。
func TestDuplicateInboundPublishRoutedOnce(t *testing.T) {
	b := testBroker(t)
	ctx := context.Background()

	sub, err := client.Dial(ctx, b.Addr(), client.Options{ClientID: "dup-sub", CleanSession: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Disconnect() })
	if _, err := sub.Subscribe(ctx, map[string]byte{"dup/t": 1}); err != nil {
		t.Fatal(err)
	}

	raw := rawDial(t, b.Addr())
	_ = protoWriteConnect(raw, "dup-raw")
	connack, err := proto.ReadFrame(raw)
	if err != nil || connack.Type != proto.CONNACK || connack.Code != proto.RCAccepted {
		t.Fatalf("handshake: %+v err=%v", connack, err)
	}

	// 构造一条 QoS1 PUBLISH（PacketID=55），发两次，第二次置 Dup。
	mk := func(dup bool) []byte {
		return encodePublishRaw("dup/t", []byte("once"), 55, dup)
	}
	writeBytes(t, raw, mk(false))
	writeBytes(t, raw, mk(true))

	// 两次都应得到 PUBACK 55
	for i := 0; i < 2; i++ {
		ack, err := proto.ReadFrame(raw)
		if err != nil {
			t.Fatalf("ack %d: %v", i, err)
		}
		if ack.Type != proto.PUBACK || ack.PacketID != 55 {
			t.Fatalf("ack %d = type %d id %d", i, ack.Type, ack.PacketID)
		}
	}

	// 订阅端只收到一次消息
	recv(t, sub, "once")
	select {
	case m := <-sub.Messages():
		t.Fatalf("duplicate message routed: %q", m.Payload)
	case <-time.After(300 * time.Millisecond):
	}
}
