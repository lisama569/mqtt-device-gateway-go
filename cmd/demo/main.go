// Command demo starts an in-process gateway and drives simulated Go devices
// through the main behaviors: wildcard subscription, QoS1 publish/ack,
// duplicate suppression, session takeover, ordered offline replay, clean vs
// abnormal disconnect and the last-will message.
//
// Run: go run ./cmd/demo
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/lisama569/mqtt-device-gateway-go/internal/broker"
	"github.com/lisama569/mqtt-device-gateway-go/internal/client"
)

func must(err error, what string) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", what, err)
		os.Exit(1)
	}
}

func recv(ctx context.Context, c *client.Client, label string) string {
	select {
	case m := <-c.Messages():
		fmt.Printf("  [%s] <- topic=%q payload=%q qos=%d pid=%d dup=%v\n",
			label, m.Topic, m.Payload, m.QoS, m.PacketID, m.Dup)
		return string(m.Payload)
	case <-time.After(2 * time.Second):
		fmt.Printf("  [%s] TIMEOUT waiting for message\n", label)
		return ""
	case <-ctx.Done():
		return ""
	}
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
	b := broker.New(broker.Config{Addr: "127.0.0.1:0", Logger: logger})
	ctx := context.Background()
	must(b.Start(ctx), "start")
	defer b.Close()
	addr := b.Addr()

	fmt.Println("== 1. wildcard subscription and QoS1 delivery ==")
	sub, err := client.Dial(ctx, addr, client.Options{ClientID: "monitor", CleanSession: true})
	must(err, "dial monitor")
	res, err := sub.Subscribe(ctx, map[string]byte{"sensors/+/temp": 1})
	must(err, "subscribe")
	fmt.Printf("  subscribed, granted=%v\n", res.Granted)

	pub, err := client.Dial(ctx, addr, client.Options{ClientID: "sensor-a", CleanSession: true})
	must(err, "dial sensor-a")
	must(pub.Publish(ctx, "sensors/room1/temp", []byte("21.5"), 1), "publish")
	recv(ctx, sub, "monitor")

	fmt.Println("== 2. malformed filter is rejected, not silently accepted ==")
	bad, err := sub.Subscribe(ctx, map[string]byte{"sensors/#/x": 1})
	must(err, "subscribe bad")
	fmt.Printf("  rejected filter grant byte = 0x%02x\n", bad.Granted[0])

	fmt.Println("== 3. last will on abnormal drop; suppressed on clean disconnect ==")
	watch, err := client.Dial(ctx, addr, client.Options{ClientID: "watcher", CleanSession: true})
	must(err, "dial watcher")
	_, err = watch.Subscribe(ctx, map[string]byte{"status/+": 1})
	must(err, "subscribe watcher")
	dying, err := client.Dial(ctx, addr, client.Options{
		ClientID: "dying", CleanSession: true,
		WillTopic: "status/dying", WillQoS: 1, WillPayload: []byte("goodbye"),
	})
	must(err, "dial dying")
	must(dying.Kill(), "kill (abnormal, no DISCONNECT)")
	recv(ctx, watch, "watcher")

	clean, err := client.Dial(ctx, addr, client.Options{
		ClientID: "clean", CleanSession: true,
		WillTopic: "status/clean", WillQoS: 1, WillPayload: []byte("should-not-appear"),
	})
	must(err, "dial clean")
	must(clean.Disconnect(), "clean DISCONNECT")
	select {
	case m := <-watch.Messages():
		fmt.Printf("  UNEXPECTED will after clean disconnect: %q\n", m.Payload)
	case <-time.After(300 * time.Millisecond):
		fmt.Println("  no will published after clean disconnect (correct)")
	}

	fmt.Println("== 4. session takeover (new connection wins, no duplicated state) ==")
	first, err := client.Dial(ctx, addr, client.Options{ClientID: "roaming", CleanSession: false})
	must(err, "dial roaming #1")
	_, err = client.Dial(ctx, addr, client.Options{ClientID: "roaming", CleanSession: false})
	must(err, "dial roaming #2")
	select {
	case <-first.Done():
		fmt.Println("  old connection closed by takeover (correct)")
	case <-time.After(time.Second):
		fmt.Println("  WARN: old connection not observed closing")
	}
	time.Sleep(100 * time.Millisecond)
	fmt.Printf("  live device count: %d (one owner per client id)\n", b.LiveCount())

	fmt.Println("== 5. ordered offline replay for a persistent session ==")
	dev, err := client.Dial(ctx, addr, client.Options{ClientID: "offline-dev", CleanSession: false})
	must(err, "dial offline-dev")
	_, err = dev.Subscribe(ctx, map[string]byte{"jobs/q": 1})
	must(err, "subscribe offline-dev")
	must(dev.Kill(), "drop offline-dev")
	time.Sleep(150 * time.Millisecond) // let the broker notice the drop

	sender, err := client.Dial(ctx, addr, client.Options{ClientID: "jobs-publisher", CleanSession: true})
	must(err, "dial publisher")
	for _, p := range []string{"job-1", "job-2", "job-3"} {
		must(sender.Publish(ctx, "jobs/q", []byte(p), 1), "publish offline")
	}

	back, err := client.Dial(ctx, addr, client.Options{ClientID: "offline-dev", CleanSession: false})
	must(err, "reconnect offline-dev")
	var got []string
	for i := 0; i < 3; i++ {
		got = append(got, recv(ctx, back, "offline-dev"))
	}
	fmt.Printf("  replayed in order: %v\n", got)

	fmt.Println("\nDEMO COMPLETE")
}
