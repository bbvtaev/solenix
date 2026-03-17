package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	pulse "github.com/bbvtaev/pulse-core"
	"github.com/bbvtaev/pulse-core/collector"
	"github.com/bbvtaev/pulse-core/server"
)

func main() {
	configPath := flag.String("config", "", "path to pulse.yaml config file")
	flag.Parse()

	var cfg pulse.Config
	var err error

	if *configPath != "" {
		cfg, err = pulse.LoadConfig(*configPath)
		if err != nil {
			slog.Error("failed to load config", "path", *configPath, "err", err)
			os.Exit(1)
		}
		slog.Info("loaded config", "path", *configPath)
	} else {
		cfg = pulse.DefaultConfig()
		slog.Info("using default config")
	}

	db, err := pulse.Open(cfg)
	if err != nil {
		slog.Error("failed to open DB", "err", err)
		os.Exit(1)
	}
	defer db.Close()

	slog.Info("pulse-core started",
		"mode", cfg.Mode,
		"wal", cfg.WALPath,
		"grpc_addr", cfg.GRPCAddr,
		"retention", cfg.RetentionDuration,
		"auth", cfg.Auth.Enabled,
	)

	srv := server.New(db)
	go func() {
		slog.Info("gRPC server listening", "addr", cfg.GRPCAddr)
		if err := srv.Listen(cfg.GRPCAddr); err != nil {
			slog.Error("gRPC server error", "err", err)
			os.Exit(1)
		}
	}()

	if cfg.Mode == pulse.ModeSelfHosted && cfg.HTTPAddr != "" {
		httpSrv := server.NewHTTP(db)
		go func() {
			slog.Info("UI available", "url", "http://localhost"+cfg.HTTPAddr)
			if err := httpSrv.ListenHTTP(cfg.HTTPAddr); err != nil {
				slog.Error("HTTP server error", "err", err)
			}
		}()
	}

	if cfg.Collector.Enabled {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		c := collector.New(db, cfg.Collector)
		go c.Run(ctx)
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit

	slog.Info("shutting down", "signal", sig)
}
