// Command localdemo wires in-process devices against a local gateway to
// demonstrate sessions, QoS1 delivery, offline backlog and will messages
// without any external service. Run with: go run ./examples/localdemo
package main

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	"github.com/lisama569/mqtt-device-gateway-go/client"
	"github.com/lisama569/mqtt-device-gateway-go/internal/broker"
	"github.com/lisama569/mqtt-device-gateway-go/internal/protocol"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))

	gw, err := broker.New(broker.Config{ListenAddr: "127.0.0.1:0", Logger: logger})
	must(err)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	must(err)
	go func() { _ = gw.ServeListener(ln) }()
	defer func() { _ = gw.Close() }()
	addr := ln.Addr().String()
	time.Sleep(50 * time.Millisecond)

	// 1. Persistent subscriber, then it goes offline WITHOUT DISCONNECT.
	var mu sync.Mutex
	got := make([]string, 0)
	sub, err := client.Connect(client.ConnectConfig{
		Address: addr, ClientID: "sensor-1", CleanStart: false,
	}, func(topic string, payload []byte, id uint16, dup bool) {
		mu.Lock()
		got = append(got, fmt.Sprintf("%s=%s(id=%d,dup=%t)", topic, payload, id, dup))
		mu.Unlock()
	})
	must(err)
	_, err = sub.Subscribe([]protocol.SubFilter{{Filter: "home/+/temp", QoS: 1}})
	must(err)
	fmt.Println("[demo] sensor-1 subscribed (persistent session), now closing transport abruptly")
	must(killTransport(sub)) // abnormal close: no DISCONNECT

	// 2. A different device publishes while sensor-1 is offline.
	pub, err := client.Connect(client.ConnectConfig{
		Address: addr, ClientID: "publisher", CleanStart: true,
	}, nil)
	must(err)
	for _, msg := range []string{"21.0", "21.5", "22.1"} {
		must(pub.Publish("home/room-a/temp", 1, []byte(msg)))
	}
	fmt.Println("[demo] publisher sent 3 QoS1 messages while sensor-1 was offline")
	must(pub.Disconnect())

	// 3. sensor-1 reconnects with the same persistent session: the
	// backlog is replayed in order, no gaps and no duplicates.
	sub2, err := client.Connect(client.ConnectConfig{
		Address: addr, ClientID: "sensor-1", CleanStart: false,
	}, func(topic string, payload []byte, id uint16, dup bool) {
		mu.Lock()
		got = append(got, fmt.Sprintf("%s=%s(id=%d,dup=%t)", topic, payload, id, dup))
		mu.Unlock()
	})
	must(err)
	defer func() { _ = sub2.Disconnect() }()
	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	fmt.Printf("[demo] sensor-1 received after reconnect (%d messages, in order):\n", len(got))
	for _, m := range got {
		fmt.Println("       -", m)
	}
	mu.Unlock()

	// 4. Will message demonstration: watcher subscribes; fragile device
	// connects with a will and then vanishes.
	willGot := make(chan string, 1)
	watcher, err := client.Connect(client.ConnectConfig{
		Address: addr, ClientID: "watcher", CleanStart: true,
	}, func(topic string, payload []byte, _ uint16, _ bool) {
		if topic == "status/fragile" {
			select {
			case willGot <- string(payload):
			default:
			}
		}
	})
	must(err)
	_, err = watcher.Subscribe([]protocol.SubFilter{{Filter: "status/+", QoS: 1}})
	must(err)

	fragile, err := client.Connect(client.ConnectConfig{
		Address: addr, ClientID: "fragile", CleanStart: true,
		Will: &protocol.WillMessage{
			Topic: "status/fragile", QoS: 1, Payload: []byte("lost"),
		},
	}, nil)
	must(err)
	must(killTransport(fragile)) // abnormal -> will fires exactly once
	select {
	case w := <-willGot:
		fmt.Println("[demo] watcher received will message:", w)
	case <-time.After(2 * time.Second):
		fmt.Println("[demo] ERROR: will not received")
	}
	_ = watcher.Disconnect()
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

// killTransport drops the underlying TCP connection without sending
// DISCONNECT, simulating power loss / network failure.
func killTransport(c *client.Client) error { return c.KillForDemo() }
