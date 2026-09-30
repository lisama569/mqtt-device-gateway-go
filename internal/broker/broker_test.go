package broker_test

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/lisama569/mqtt-device-gateway-go/internal/broker"
	"github.com/lisama569/mqtt-device-gateway-go/internal/client"
)

const testTimeout = 5 * time.Second

func testBroker(t *testing.T) *broker.Broker {
	t.Helper()
	log := slog.New(slog.NewTextHandler(&testLogWriter{t: t}, nil))
	b := broker.New(broker.Config{Addr: "127.0.0.1:0", Logger: log})
	if err := b.Start(context.Background()); err != nil {
		t.Fatalf("broker start: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func recv(t *testing.T, c *client.Client, want string) {
	t.Helper()
	select {
	case m := <-c.Messages():
		if string(m.Payload) != want {
			t.Fatalf("got %q want %q on %s", m.Payload, want, m.Topic)
		}
	case <-time.After(testTimeout):
		t.Fatalf("did not receive message %q", want)
	}
}

func TestBasicPubSubQoS1(t *testing.T) {
	b := testBroker(t)
	ctx := context.Background()

	sub, err := client.Dial(ctx, b.Addr(), client.Options{ClientID: "sub-1", CleanSession: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Disconnect() })

	res, err := sub.Subscribe(ctx, map[string]byte{"sensors/+": 1})
	if err != nil {
		t.Fatal(err)
	}
	if idx, bad := res.AnyRejected(); bad {
		t.Fatalf("filter %d rejected: %#x", idx, res.Granted[idx])
	}

	pub, err := client.Dial(ctx, b.Addr(), client.Options{ClientID: "pub-1", CleanSession: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pub.Disconnect() })

	if err := pub.Publish(ctx, "sensors/a", []byte("hello"), 1); err != nil {
		t.Fatalf("publish qos1: %v", err)
	}
	recv(t, sub, "hello")
}

type testLogWriter struct{ t *testing.T }

func (w *testLogWriter) Write(p []byte) (int, error) {
	w.t.Logf("BROKER %s", p)
	return len(p), nil
}

var _ = fmt.Sprintf
