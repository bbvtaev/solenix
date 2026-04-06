// load-gen — генератор погодных метрик для solenix-core.
//
// Симулирует данные с метеостанций нескольких городов:
//
//	weather_temperature_celsius   — температура воздуха
//	weather_humidity_percent      — влажность
//	weather_pressure_hpa          — атмосферное давление
//	weather_wind_speed_ms         — скорость ветра
//	weather_wind_direction_deg    — направление ветра
//	weather_precipitation_mm      — осадки
//	weather_uv_index              — УФ-индекс
//	weather_visibility_km         — видимость
//
// Флаги:
//
//	-addr     адрес сервера       (default: 127.0.0.1:8731)
//	-rate     тиков в секунду     (default: 2)
//	-stations количество станций  (default: все)
//
// Управление (stdin): [+] ускорить  [-] замедлить  [q] выйти
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"os"
	"sync/atomic"
	"time"

	pb "github.com/bbvtaev/solenix-core/api/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ── Метеостанция ──────────────────────────────────────────────────────────────

type station struct {
	city    string
	country string
	lat     float64 // широта — влияет на базовую температуру

	// внутреннее состояние (плавно меняется)
	temp       float64
	humidity   float64
	pressure   float64
	windSpeed  float64
	windDir    float64
	precip     float64
	uvIndex    float64
	visibility float64
}

// Реальные города с их широтами
var defaultStations = []struct {
	city, country string
	lat           float64
}{
	{"Moscow", "RU", 55.75},
	{"London", "GB", 51.50},
	{"Tokyo", "JP", 35.68},
	{"New York", "US", 40.71},
	{"Dubai", "AE", 25.20},
	{"Sydney", "AU", -33.86},
	{"Reykjavik", "IS", 64.13},
	{"Singapore", "SG", 1.35},
}

func newStation(city, country string, lat float64, rng *rand.Rand) *station {
	// Базовая температура зависит от широты и сезона
	baseTemp := 25 - math.Abs(lat)/90*40 + rng.Float64()*5

	return &station{
		city:       city,
		country:    country,
		lat:        lat,
		temp:       baseTemp,
		humidity:   40 + rng.Float64()*40,
		pressure:   1005 + rng.Float64()*20,
		windSpeed:  rng.Float64() * 10,
		windDir:    rng.Float64() * 360,
		precip:     0,
		uvIndex:    rng.Float64() * 5,
		visibility: 5 + rng.Float64()*15,
	}
}

// tick обновляет состояние станции и возвращает серии для записи.
func (s *station) tick(rng *rand.Rand, t float64) []*pb.Series {
	now := time.Now().UnixNano()
	labels := map[string]string{
		"city":    s.city,
		"country": s.country,
	}

	// ── Температура: суточный цикл + шум ──
	hour := (t / 3600)                            // условный час
	diurnal := 4 * math.Sin((hour-14)*math.Pi/12) // пик в 14:00
	s.temp += (diurnal*0.01 + (rng.Float64()-0.5)*0.3)
	baseTemp := 25 - math.Abs(s.lat)/90*40
	// Медленный дрейф к базовому значению
	s.temp += (baseTemp - s.temp) * 0.001
	s.temp = clamp(s.temp, -50, 55)

	// ── Влажность: обратная зависимость от температуры ──
	s.humidity += (rng.Float64()-0.5)*1.5 - (s.temp-20)*0.02
	s.humidity = clamp(s.humidity, 5, 100)

	// ── Давление: медленные волны ──
	s.pressure += math.Sin(t/3600)*0.05 + (rng.Float64()-0.5)*0.3
	s.pressure = clamp(s.pressure, 970, 1040)

	// ── Ветер: порывистый ──
	s.windSpeed += (rng.Float64() - 0.5) * 1.2
	s.windSpeed = clamp(s.windSpeed, 0, 35)
	s.windDir += (rng.Float64() - 0.5) * 15
	if s.windDir < 0 {
		s.windDir += 360
	}
	if s.windDir >= 360 {
		s.windDir -= 360
	}

	// ── Осадки: случайные эпизоды ──
	if rng.Float64() < 0.05 {
		s.precip = rng.Float64() * 5 // начало осадков
	} else {
		s.precip *= 0.85 // затухание
	}
	s.precip = clamp(s.precip, 0, 50)

	// ── УФ-индекс: зависит от широты, 0 ночью ──
	dayFactor := math.Max(0, math.Sin((hour-6)*math.Pi/12))
	s.uvIndex = dayFactor * (10 - math.Abs(s.lat)/90*5) * (0.8 + rng.Float64()*0.4)
	s.uvIndex = clamp(s.uvIndex, 0, 11)

	// ── Видимость: падает при осадках и высокой влажности ──
	s.visibility = 20 - s.precip*0.5 - (s.humidity-50)*0.05 + (rng.Float64()-0.5)*1
	s.visibility = clamp(s.visibility, 0.1, 25)

	return []*pb.Series{
		pt("weather_temperature_celsius", labels, now, round2(s.temp)),
		pt("weather_humidity_percent", labels, now, round2(s.humidity)),
		pt("weather_pressure_hpa", labels, now, round2(s.pressure)),
		pt("weather_wind_speed_ms", labels, now, round2(s.windSpeed)),
		pt("weather_wind_direction_deg", labels, now, round2(s.windDir)),
		pt("weather_precipitation_mm", labels, now, round2(s.precip)),
		pt("weather_uv_index", labels, now, round2(s.uvIndex)),
		pt("weather_visibility_km", labels, now, round2(s.visibility)),
	}
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func pt(metric string, labels map[string]string, ts int64, value float64) *pb.Series {
	return &pb.Series{
		Metric: metric,
		Labels: labels,
		Points: []*pb.DataPoint{{Timestamp: ts, Value: value}},
	}
}

func clamp(v, min, max float64) float64 {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}

func round2(v float64) float64 {
	return math.Round(v*100) / 100
}

// ── Main ──────────────────────────────────────────────────────────────────────

func main() {
	addr := flag.String("addr", "127.0.0.1:8731", "gRPC server address")
	rate := flag.Int("rate", 2, "ticks per second")
	nstation := flag.Int("stations", len(defaultStations), "number of stations (1–8)")
	flag.Parse()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("connect", "err", err)
		os.Exit(1)
	}
	defer conn.Close()

	c := pb.NewSolenixDBClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	health, err := c.Health(ctx, &pb.HealthRequest{})
	cancel()
	if err != nil {
		slog.Error("server not reachable", "addr", *addr, "err", err)
		os.Exit(1)
	}
	slog.Info("connected", "server", health.Version)

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	n := clampInt(*nstation, 1, len(defaultStations))
	stations := make([]*station, n)
	for i := 0; i < n; i++ {
		def := defaultStations[i]
		stations[i] = newStation(def.city, def.country, def.lat, rng)
	}

	slog.Info("weather generator started",
		"stations", n,
		"cities", stationNames(stations),
		"rate", fmt.Sprintf("%d ticks/s", *rate),
	)
	fmt.Println("Controls: [+] double rate  [-] halve rate  [q] quit")

	var currentRate atomic.Int64
	currentRate.Store(int64(*rate))

	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			switch scanner.Text() {
			case "+":
				r := currentRate.Load() * 2
				currentRate.Store(r)
				fmt.Printf("→ rate: %d ticks/s\n", r)
			case "-":
				r := currentRate.Load() / 2
				if r < 1 {
					r = 1
				}
				currentRate.Store(r)
				fmt.Printf("→ rate: %d ticks/s\n", r)
			case "q":
				os.Exit(0)
			}
		}
	}()

	var totalWritten, totalErrors atomic.Int64

	go func() {
		prev := int64(0)
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			cur := totalWritten.Load()
			slog.Info("stats",
				"series/s", fmt.Sprintf("%.1f", float64(cur-prev)/10.0),
				"total", cur,
				"errors", totalErrors.Load(),
			)
			prev = cur
		}
	}()

	lastRate := int64(0)
	var ticker *time.Ticker

	for {
		r := currentRate.Load()
		if r != lastRate {
			if ticker != nil {
				ticker.Stop()
			}
			ticker = time.NewTicker(time.Duration(float64(time.Second) / float64(r)))
			lastRate = r
		}

		<-ticker.C
		t := float64(time.Now().UnixNano()) / float64(time.Second)

		for _, s := range stations {
			series := s.tick(rng, t)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, err := c.Push(ctx, &pb.PushRequest{Series: series})
			cancel()
			if err != nil {
				totalErrors.Add(1)
			} else {
				totalWritten.Add(int64(len(series)))
			}
		}
	}
}

func stationNames(stations []*station) []string {
	names := make([]string, len(stations))
	for i, s := range stations {
		names[i] = s.city
	}
	return names
}

func clampInt(v, min, max int) int {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}
