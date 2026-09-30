package client

import (
	"context"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/lisama569/mqtt-device-gateway-go/internal/proto"
)

func dummyServer(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				_, _ = proto.ReadFrame(c)
				_ = proto.WriteFrame(c, &proto.Packet{Type: proto.CONNACK, Code: proto.RCAccepted})
				buf := make([]byte, 1)
				for {
					if _, err := c.Read(buf); err != nil {
						return
					}
				}
			}(c)
		}
	}()
	return ln
}

func TestClientGoroutineCleanup(t *testing.T) {
	ln := dummyServer(t)
	defer ln.Close()
	time.Sleep(50 * time.Millisecond)
	base := runtime.NumGoroutine()
	for i := 0; i < 50; i++ {
		c, err := Dial(context.Background(), ln.Addr().String(), Options{ClientID: "x", CleanSession: true})
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Disconnect(); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(200 * time.Millisecond)
	runtime.GC()
	if after := runtime.NumGoroutine(); after > base+2 {
		buf := make([]byte, 1<<16)
		n := runtime.Stack(buf, true)
		t.Fatalf("goroutine leak: base=%d after=%d\n%s", base, after, buf[:n])
	}
}
