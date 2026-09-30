package broker_test

import (
	"context"
	"testing"
	"time"

	"github.com/lisama569/mqtt-device-gateway-go/internal/client"
)

// TestOfflineReplayPublishersComplete 验证：设备离线期间，由独立 goroutine 连续
// 发布的 QoS1 消息都能拿到发布方的 PUBACK（防止发布链路应答丢失/重排）。
func TestOfflineReplayPublishersComplete(t *testing.T) {
	b := testBroker(t)
	ctx := context.Background()
	sub, err := client.Dial(ctx, b.Addr(), client.Options{ClientID: "P", CleanSession: false})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Subscribe(ctx, map[string]byte{"t/x": 1}); err != nil {
		t.Fatal(err)
	}
	_ = sub.Kill()
	waitFor(t, func() bool { return b.LiveCount() == 0 })

	pub, err := client.Dial(ctx, b.Addr(), client.Options{ClientID: "S", CleanSession: true})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		for i := 0; i < 3; i++ {
			if err := pub.Publish(ctx, "t/x", []byte{byte('A' + i)}, 1); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("missing PUBACK for offline messages")
	}
	_ = pub.Disconnect()
}
