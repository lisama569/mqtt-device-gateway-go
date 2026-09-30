// Command gateway runs the local IoT device gateway as a TCP server.
//
// Usage:
//
//	gateway [-addr 127.0.0.1:9100] [-max-payload 1048576]
//
// It implements a small length-prefixed binary MQTT-like protocol (LGW1).
// Everything runs locally with no external services; device state, sessions
// and the offline queue are kept in memory.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/lisama569/mqtt-device-gateway-go/internal/broker"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9100", "listen address")
	maxPayload := flag.Int("max-payload", broker.DefaultMaxPayload, "maximum frame payload in bytes")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	b := broker.New(broker.Config{Addr: *addr, MaxPayload: *maxPayload, Logger: logger})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := b.Start(ctx); err != nil {
		logger.Error("failed to start", "err", err)
		os.Exit(1)
	}
	logger.Info("gateway listening", "addr", b.Addr(), "max_payload", *maxPayload)

	<-ctx.Done()
	logger.Info("shutting down")
	if err := b.Close(); err != nil {
		logger.Error("shutdown error", "err", err)
		os.Exit(1)
	}
}
