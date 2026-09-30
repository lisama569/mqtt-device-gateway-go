package client_test

import (
	"net"
	"testing"
	"time"

	"github.com/lisama569/mqtt-device-gateway-go/client"
	"github.com/lisama569/mqtt-device-gateway-go/internal/broker"
	"github.com/lisama569/mqtt-device-gateway-go/internal/protocol"
)

func startTestBroker(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gw, err := broker.New(broker.Config{ListenAddr: ln.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = gw.ServeListener(ln) }()
	t.Cleanup(func() { _ = gw.Close() })
	time.Sleep(30 * time.Millisecond)
	return ln.Addr().String()
}

func TestSDKSubscribeAndRoundTrip(t *testing.T) {
	addr := startTestBroker(t)
	c, err := client.Connect(client.ConnectConfig{
		Address: addr, ClientID: "sdk1", CleanStart: true,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Disconnect()
	res, err := c.Subscribe([]protocol.SubFilter{
		{Filter: "a/+/c", QoS: 0},
		{Filter: "d/#", QoS: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 2 || res[0].Reason != 0 || res[1].Reason != 1 {
		t.Fatalf("suback results=%+v", res)
	}
	if err := c.Unsubscribe([]string{"a/+/c"}); err != nil {
		t.Fatal(err)
	}
}

func TestSDKRejectsQoS2AndEmptyFilters(t *testing.T) {
	addr := startTestBroker(t)
	c, err := client.Connect(client.ConnectConfig{
		Address: addr, ClientID: "sdk2", CleanStart: true,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Disconnect()

	if err := c.Publish("a/b", 2, []byte("x")); err == nil {
		t.Fatal("QoS2 publish must be rejected, not downgraded")
	}
	if _, err := c.Subscribe(nil); err == nil {
		t.Fatal("empty subscription must be rejected")
	}
	if _, err := c.Subscribe([]protocol.SubFilter{{Filter: "bad/#/x", QoS: 1}}); err == nil {
		t.Fatal("malformed filter must be rejected")
	}
}

func TestSDKTakeOverClosesOldClient(t *testing.T) {
	addr := startTestBroker(t)
	old, err := client.Connect(client.ConnectConfig{
		Address: addr, ClientID: "same", CleanStart: true,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	newC, err := client.Connect(client.ConnectConfig{
		Address: addr, ClientID: "same", CleanStart: true,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer newC.Disconnect()

	done := make(chan error, 1)
	go func() { done <- old.Wait() }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("old SDK client not closed by take-over")
	}
}
