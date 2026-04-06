// Package solenix предоставляет Go SDK для solenix-core.
//
// Пример использования:
//
//	client, err := solenix.NewClient("127.0.0.1:8731")
//	if err != nil { log.Fatal(err) }
//	defer client.Close()
//
//	client.Push("cpu.usage", solenix.Labels{"host": "srv1"}, 72.5)
//
//	results, _ := client.Query("cpu.usage", nil, 0, 0)
//	for _, s := range results {
//	    fmt.Println(s.Metric, s.Points)
//	}
package solenix

import (
	"context"
	"fmt"
	"time"

	pb "github.com/bbvtaev/solenix-core/api/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
)

// Labels — псевдоним для удобства.
type Labels = map[string]string

// Point — одна точка временного ряда.
type Point struct {
	Timestamp int64
	Value     float64
}

// SeriesResult — результат Query для одной серии.
type SeriesResult struct {
	Metric string
	Labels Labels
	Points []Point
}

// Client — gRPC клиент для solenix-core.
type Client struct {
	conn    *grpc.ClientConn
	rpc     pb.SolenixDBClient
	timeout time.Duration
}

// NewClient подключается к серверу solenix-core по адресу addr (например "127.0.0.1:8731").
func NewClient(addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  50 * time.Millisecond,
				Multiplier: 1.5,
				MaxDelay:   5 * time.Second,
			},
			MinConnectTimeout: 5 * time.Second,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("solenix: connect to %s: %w", addr, err)
	}
	return &Client{
		conn:    conn,
		rpc:     pb.NewSolenixDBClient(conn),
		timeout: 5 * time.Second,
	}, nil
}

// Close закрывает соединение.
func (c *Client) Close() error {
	return c.conn.Close()
}

// Push записывает одно значение с текущим временем (UnixNano).
func (c *Client) Push(metric string, labels Labels, value float64) error {
	return c.PushBatch(metric, labels, []Point{
		{Timestamp: time.Now().UnixNano(), Value: value},
	})
}

// PushBatch записывает несколько точек с произвольными timestamp.
func (c *Client) PushBatch(metric string, labels Labels, points []Point) error {
	pbPoints := make([]*pb.DataPoint, len(points))
	for i, p := range points {
		pbPoints[i] = &pb.DataPoint{Timestamp: p.Timestamp, Value: p.Value}
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	_, err := c.rpc.Push(ctx, &pb.PushRequest{
		Series: []*pb.Series{{
			Metric: metric,
			Labels: labels,
			Points: pbPoints,
		}},
	})
	return err
}

// AggPoint — одна агрегированная точка.
type AggPoint struct {
	Timestamp int64
	Value     float64
}

// AggResult — результат QueryAgg для одной серии.
type AggResult struct {
	Metric string
	Labels Labels
	Window string
	Points []AggPoint
}

// QueryAgg запрашивает агрегированные данные по временным окнам.
// window — строка duration: "1m", "5m", "1h".
// agg — тип агрегации: "avg", "min", "max", "sum", "count".
func (c *Client) QueryAgg(metric string, labels Labels, from, to int64, window, agg string) ([]AggResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	resp, err := c.rpc.QueryAgg(ctx, &pb.QueryAggRequest{
		Metric: metric,
		Labels: labels,
		From:   from,
		To:     to,
		Window: window,
		Agg:    agg,
	})
	if err != nil {
		return nil, err
	}

	results := make([]AggResult, len(resp.Series))
	for i, s := range resp.Series {
		pts := make([]AggPoint, len(s.Points))
		for j, p := range s.Points {
			pts[j] = AggPoint{Timestamp: p.Timestamp, Value: p.Value}
		}
		results[i] = AggResult{Metric: s.Metric, Labels: s.Labels, Window: s.Window, Points: pts}
	}
	return results, nil
}

// Query запрашивает данные. from/to в Unix nanoseconds; 0 означает без ограничения.
func (c *Client) Query(metric string, labels Labels, from, to int64) ([]SeriesResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	resp, err := c.rpc.Query(ctx, &pb.QueryRequest{
		Metric: metric,
		Labels: labels,
		From:   from,
		To:     to,
	})
	if err != nil {
		return nil, err
	}

	results := make([]SeriesResult, len(resp.Series))
	for i, s := range resp.Series {
		pts := make([]Point, len(s.Points))
		for j, p := range s.Points {
			pts[j] = Point{Timestamp: p.Timestamp, Value: p.Value}
		}
		results[i] = SeriesResult{Metric: s.Metric, Labels: s.Labels, Points: pts}
	}
	return results, nil
}

// Subscribe возвращает канал с новыми точками в реальном времени.
// Подписка активна пока ctx не отменён.
func (c *Client) Subscribe(ctx context.Context, metric string, labels Labels) (<-chan Point, error) {
	stream, err := c.rpc.Subscribe(ctx, &pb.SubscribeRequest{
		Metric: metric,
		Labels: labels,
	})
	if err != nil {
		return nil, err
	}

	ch := make(chan Point, 256)
	go func() {
		defer close(ch)
		for {
			p, err := stream.Recv()
			if err != nil {
				return
			}
			select {
			case ch <- Point{Timestamp: p.Timestamp, Value: p.Value}:
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch, nil
}

// Metrics возвращает список всех метрик в БД.
func (c *Client) Metrics() ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	resp, err := c.rpc.Metrics(ctx, &pb.MetricsRequest{})
	if err != nil {
		return nil, err
	}
	return resp.Metrics, nil
}

// Health возвращает статус и версию сервера.
func (c *Client) Health() (status, version string, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	resp, err := c.rpc.Health(ctx, &pb.HealthRequest{})
	if err != nil {
		return "", "", err
	}
	return resp.Status, resp.Version, nil
}
