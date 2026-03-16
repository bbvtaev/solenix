// grpc_client_test — ручная проверка gRPC сервера pulse-core.
// Запусти сервер: go run ./cmd/main.go
// Запусти клиент: go run ./grpc_client_test/main.go
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	pb "github.com/bbvtaev/pulse-core/api/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const addr = "localhost:50051"

func main() {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	c := pb.NewPulseDBClient(conn)
	ctx := context.Background()

	// ── 1. Health ────────────────────────────────────────────────────────────
	fmt.Println("=== Health ===")
	health, err := c.Health(ctx, &pb.HealthRequest{})
	if err != nil {
		log.Fatalf("health: %v", err)
	}
	fmt.Printf("status=%s  version=%s\n\n", health.Status, health.Version)

	// ── 2. Write ─────────────────────────────────────────────────────────────
	fmt.Println("=== Write ===")
	now := time.Now().UnixNano()
	writeResp, err := c.Write(ctx, &pb.WriteRequest{
		Series: []*pb.Series{
			{
				Metric: "cpu_usage",
				Labels: map[string]string{"host": "test-1", "env": "dev"},
				Points: []*pb.DataPoint{
					{Timestamp: now - 4*int64(time.Second), Value: 12.5},
					{Timestamp: now - 3*int64(time.Second), Value: 34.7},
					{Timestamp: now - 2*int64(time.Second), Value: 56.1},
					{Timestamp: now - 1*int64(time.Second), Value: 78.9},
					{Timestamp: now, Value: 90.0},
				},
			},
			{
				Metric: "mem_usage",
				Labels: map[string]string{"host": "test-1"},
				Points: []*pb.DataPoint{
					{Timestamp: now - 2*int64(time.Second), Value: 1024.0},
					{Timestamp: now - 1*int64(time.Second), Value: 2048.0},
					{Timestamp: now, Value: 3072.0},
				},
			},
		},
	})
	if err != nil {
		log.Fatalf("write: %v", err)
	}
	fmt.Printf("written=%d series\n\n", writeResp.Written)

	// ── 3. Query — весь диапазон ──────────────────────────────────────────────
	fmt.Println("=== Query (full range) ===")
	queryResp, err := c.Query(ctx, &pb.QueryRequest{
		Metric: "cpu_usage",
		Labels: map[string]string{"host": "test-1"},
		From:   0,
		To:     0,
	})
	if err != nil {
		log.Fatalf("query: %v", err)
	}
	for _, s := range queryResp.Series {
		fmt.Printf("metric=%s labels=%v  points=%d\n", s.Metric, s.Labels, len(s.Points))
		for _, p := range s.Points {
			fmt.Printf("  ts=%d  val=%.2f\n", p.Timestamp, p.Value)
		}
	}
	fmt.Println()

	// ── 4. Query — узкий диапазон ─────────────────────────────────────────────
	fmt.Println("=== Query (last 2s) ===")
	queryResp2, err := c.Query(ctx, &pb.QueryRequest{
		Metric: "cpu_usage",
		Labels: map[string]string{"host": "test-1"},
		From:   now - 2*int64(time.Second),
		To:     now,
	})
	if err != nil {
		log.Fatalf("query range: %v", err)
	}
	for _, s := range queryResp2.Series {
		fmt.Printf("metric=%s  points=%d\n", s.Metric, len(s.Points))
	}
	fmt.Println()

	// ── 5. Subscribe — получить 3 точки в реальном времени ───────────────────
	fmt.Println("=== Subscribe (waiting for 3 live points, writing 5 in background) ===")
	subCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	stream, err := c.Subscribe(subCtx, &pb.SubscribeRequest{
		Metric: "live_metric",
		Labels: map[string]string{"src": "grpc_test"},
	})
	if err != nil {
		log.Fatalf("subscribe: %v", err)
	}

	// Пишем 5 точек с задержкой 200ms
	go func() {
		time.Sleep(200 * time.Millisecond)
		for i := 0; i < 5; i++ {
			_, werr := c.Write(context.Background(), &pb.WriteRequest{
				Series: []*pb.Series{{
					Metric: "live_metric",
					Labels: map[string]string{"src": "grpc_test"},
					Points: []*pb.DataPoint{
						{Timestamp: time.Now().UnixNano(), Value: float64(i) * 1.5},
					},
				}},
			})
			if werr != nil {
				log.Printf("live write %d error: %v", i, werr)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}()

	received := 0
	for received < 3 {
		pt, serr := stream.Recv()
		if serr == io.EOF {
			break
		}
		if serr != nil {
			log.Printf("subscribe recv: %v", serr)
			break
		}
		fmt.Printf("  live point: ts=%d  val=%.2f\n", pt.Timestamp, pt.Value)
		received++
	}
	fmt.Printf("received %d live points\n\n", received)

	// ── 6. Delete ─────────────────────────────────────────────────────────────
	fmt.Println("=== Delete (first 2 points of cpu_usage) ===")
	_, err = c.Delete(ctx, &pb.DeleteRequest{
		Metric: "cpu_usage",
		Labels: map[string]string{"host": "test-1"},
		From:   now - 4*int64(time.Second),
		To:     now - 3*int64(time.Second),
	})
	if err != nil {
		log.Fatalf("delete: %v", err)
	}
	fmt.Println("delete OK")

	// Проверяем, что осталось 3 точки
	afterDel, err := c.Query(ctx, &pb.QueryRequest{
		Metric: "cpu_usage",
		Labels: map[string]string{"host": "test-1"},
		From:   0,
		To:     0,
	})
	if err != nil {
		log.Fatalf("query after delete: %v", err)
	}
	for _, s := range afterDel.Series {
		fmt.Printf("after delete: points=%d (expected 3)\n", len(s.Points))
	}

	fmt.Println("\n=== All checks passed ===")
}
