package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	pulse "github.com/bbvtaev/pulse-core"
	"github.com/bbvtaev/pulse-core/collector"
	sdk "github.com/bbvtaev/pulse-core/sdk/go"
	"github.com/bbvtaev/pulse-core/server"
)

func main() {
	configPath := flag.String("config", "", "path to pulse.yaml config file")
	demo := flag.Bool("demo", false, "run SDK demo after startup (write, query, subscribe)")
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
		"data_dir", cfg.DataDir,
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

	if *demo {
		go runSDKDemo(cfg.GRPCAddr)
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit

	slog.Info("shutting down", "signal", sig)
}

// runSDKDemo демонстрирует использование Go SDK.
// Запускается с флагом -demo.
func runSDKDemo(addr string) {
	// Даём серверу время запуститься
	time.Sleep(200 * time.Millisecond)

	client, err := sdk.NewClient(addr)
	if err != nil {
		slog.Error("demo: connect failed", "err", err)
		return
	}
	defer client.Close()

	// ── Health ────────────────────────────────────────────────────────────────
	status, version, err := client.Health()
	if err != nil {
		slog.Error("demo: health check failed", "err", err)
		return
	}
	fmt.Printf("\n=== pulse SDK demo === server %s (%s)\n\n", version, status)

	// ── Subscribe (запускаем до Write, чтобы не пропустить точки) ─────────────
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	ch, err := client.Subscribe(ctx, "demo.temperature", sdk.Labels{"sensor": "s1"})
	if err != nil {
		slog.Error("demo: subscribe failed", "err", err)
		return
	}
	go func() {
		for p := range ch {
			fmt.Printf("[subscribe] demo.temperature  value=%.2f  ts=%d\n", p.Value, p.Timestamp)
		}
	}()

	// ── Write ─────────────────────────────────────────────────────────────────
	labels := sdk.Labels{"sensor": "s1", "location": "office"}
	values := []float64{21.3, 22.1, 20.8, 23.5}

	fmt.Println("→ writing", len(values), "points to demo.temperature ...")
	for _, v := range values {
		if err := client.Write("demo.temperature", labels, v); err != nil {
			slog.Error("demo: write failed", "err", err)
			return
		}
		time.Sleep(50 * time.Millisecond)
	}

	// ── WriteBatch ────────────────────────────────────────────────────────────
	now := time.Now().UnixNano()
	batch := []sdk.Point{
		{Timestamp: now - int64(2*time.Second), Value: 19.9},
		{Timestamp: now - int64(1*time.Second), Value: 20.4},
		{Timestamp: now, Value: 21.0},
	}
	fmt.Println("→ writing batch of", len(batch), "points ...")
	if err := client.WriteBatch("demo.temperature", labels, batch); err != nil {
		slog.Error("demo: write_batch failed", "err", err)
		return
	}

	time.Sleep(100 * time.Millisecond)

	// ── Query ─────────────────────────────────────────────────────────────────
	fmt.Println("→ querying all demo.temperature data ...")
	results, err := client.Query("demo.temperature", nil, 0, 0)
	if err != nil {
		slog.Error("demo: query failed", "err", err)
		return
	}
	for _, s := range results {
		fmt.Printf("[query] metric=%s labels=%v  points=%d\n", s.Metric, s.Labels, len(s.Points))
		for _, p := range s.Points {
			fmt.Printf("        ts=%-20d  value=%.2f\n", p.Timestamp, p.Value)
		}
	}
}
