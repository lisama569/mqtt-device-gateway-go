package broker_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/mqtt-device-gateway-go/internal/client"
)

// 同一设备标识并发接入：新连接必须胜出，旧连接收到 RCTakeover 并关闭；
// 任何时刻只有一个连接持有该会话。
func TestConcurrentTakeover(t *testing.T) {
	b := testBroker(t)
	ctx := context.Background()

	const n = 20
	var wg sync.WaitGroup
	winners := make(chan *client.Client, n)
	losers := make(chan int, n)
	start := make(chan struct{})

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			c, err := client.Dial(ctx, b.Addr(), client.Options{
				ClientID: "same-device", CleanSession: false,
			})
			if err != nil {
				losers <- i
				return
			}
			winners <- c
		}(i)
	}
	close(start)
	wg.Wait()
	close(winners)
	close(losers)

	var live []*client.Client
	for c := range winners {
		live = append(live, c)
	}
	if len(live) == 0 {
		t.Fatal("no surviving connection")
	}

	// 服务端视角：同一 clientID 只能有一个在线连接。
	deadline := time.Now().Add(testTimeout)
	for time.Now().Before(deadline) {
		if b.LiveCount() == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := b.LiveCount(); got != 1 {
		t.Fatalf("live connections for same id = %d, want 1", got)
	}

	// 等待所有非获胜者的客户端感知关闭；只有一个能继续正常收发。
	time.Sleep(100 * time.Millisecond)
	open := 0
	for _, c := range live {
		if !c.Closed() {
			open++
		}
	}
	if open != 1 {
		t.Fatalf("open handles = %d, want exactly 1", open)
	}
}

// 持久会话离线补发：离线期间的 QoS1 消息按顺序、不重不漏地补发。
func TestPersistentOfflineReplay(t *testing.T) {
	b := testBroker(t)
	ctx := context.Background()

	sub, err := client.Dial(ctx, b.Addr(), client.Options{ClientID: "persist", CleanSession: false})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Subscribe(ctx, map[string]byte{"t/x": 1}); err != nil {
		t.Fatal(err)
	}
	if err := sub.Kill(); err != nil { // 异常断开，保留会话与订阅
		t.Fatal(err)
	}
	waitFor(t, func() bool { return b.LiveCount() == 0 })

	pub, err := client.Dial(ctx, b.Addr(), client.Options{ClientID: "sender", CleanSession: true})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if err := pub.Publish(ctx, "t/x", []byte{byte('A' + i)}, 1); err != nil {
			t.Fatal(err)
		}
	}

	sub2, err := client.Dial(ctx, b.Addr(), client.Options{ClientID: "persist", CleanSession: false})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		recv(t, sub2, string(rune('A'+i)))
	}
	// 再不应有重复投递
	select {
	case m := <-sub2.Messages():
		t.Fatalf("unexpected duplicate message: %q dup=%v pid=%d", m.Payload, m.Dup, m.PacketID)
	case <-time.After(300 * time.Millisecond):
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}
