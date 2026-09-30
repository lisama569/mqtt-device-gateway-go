package broker_test

import (
	"sync"
	"testing"
	"time"

	"github.com/lisama569/mqtt-device-gateway-go/client"
)

func TestStressTakeOverAndLeak(t *testing.T) {
	gw, addr := startBroker(t)
	var wg sync.WaitGroup
	for round := 0; round < 5; round++ {
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				c, err := client.Connect(client.ConnectConfig{
					Address: addr, ClientID: "hot", CleanStart: true,
				}, nil)
				if err == nil {
					time.Sleep(time.Millisecond)
					_ = c.Disconnect()
				}
			}(i)
		}
		wg.Wait()
	}
	time.Sleep(200 * time.Millisecond)
	if n := gw.ActiveConns(); n > 1 {
		t.Fatalf("active conns=%d, want <=1 (no leak)", n)
	}
	t.Logf("active conns after storm: %d", gw.ActiveConns())
}
