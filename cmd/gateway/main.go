// Command gateway runs the local IoT device gateway.
package main

import (
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/lisama569/mqtt-device-gateway-go/internal/broker"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:18830", "listen address")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	gw, err := broker.New(broker.Config{ListenAddr: *addr, Logger: logger})
	if err != nil {
		logger.Error("gateway init failed", "err", err)
		os.Exit(1)
	}
	errCh := make(chan error, 1)
	go func() { errCh <- gw.Serve() }()

	logger.Info("gateway started; press Ctrl-C to stop", "addr", *addr)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-sigCh:
		logger.Info("signal received, shutting down", "signal", sig)
	case err := <-errCh:
		if err != nil {
			logger.Error("serve failed", "err", err)
			os.Exit(1)
		}
	}
	if err := gw.Close(); err != nil {
		logger.Error("gateway close error", "err", err)
	}
	logger.Info("gateway stopped")
}
