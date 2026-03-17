// Package collector собирает системные метрики хоста и пишет их в pulse DB.
// Аналог prometheus/node_exporter, встроенный в ядро.
//
// Метрики:
//   pulse_cpu_usage_percent     — загрузка CPU по ядрам
//   pulse_mem_used_bytes        — использованная память
//   pulse_mem_available_bytes   — доступная память
//   pulse_mem_total_bytes       — общий объём памяти
//   pulse_disk_read_bytes_total — прочитано с диска (counter)
//   pulse_disk_write_bytes_total— записано на диск (counter)
//   pulse_net_rx_bytes_total    — получено байт по сети (counter)
//   pulse_net_tx_bytes_total    — отправлено байт по сети (counter)
//   pulse_go_goroutines         — количество горутин процесса
//   pulse_go_heap_used_bytes    — heap allocation
//   pulse_go_gc_pause_ns        — пауза последнего GC
package collector

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
	psnet "github.com/shirou/gopsutil/v3/net"

	pulse "github.com/bbvtaev/pulse-core"
)

const job = "pulse-core"

// Collector периодически собирает системные метрики и пишет в DB.
type Collector struct {
	db       *pulse.DB
	interval time.Duration
	hostname string
}

func New(db *pulse.DB, cfg pulse.CollectorConfig) *Collector {
	interval := cfg.Interval
	if interval <= 0 {
		interval = 15 * time.Second
	}
	return &Collector{
		db:       db,
		interval: interval,
		hostname: hostname(),
	}
}

// Run запускает коллектор и блокируется до отмены контекста.
func (c *Collector) Run(ctx context.Context) {
	slog.Info("collector started", "interval", c.interval, "host", c.hostname)

	// Первый сбор сразу при старте
	c.collect()

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.collect()
		}
	}
}

func (c *Collector) collect() {
	now := time.Now().UnixNano()
	labels := map[string]string{"host": c.hostname, "job": job}

	c.collectCPU(now, labels)
	c.collectMem(now, labels)
	c.collectDisk(now, labels)
	c.collectNet(now, labels)
	c.collectGoRuntime(now, labels)
}

// ── CPU ───────────────────────────────────────────────────────────────────────

func (c *Collector) collectCPU(now int64, base map[string]string) {
	// percpu=true — одно значение на ядро
	percents, err := cpu.Percent(0, true)
	if err != nil {
		slog.Debug("cpu collect error", "err", err)
		return
	}
	for i, pct := range percents {
		l := merge(base, map[string]string{"cpu": fmt.Sprintf("cpu%d", i)})
		c.write("pulse_cpu_usage_percent", l, now, round(pct))
	}

	// Суммарная загрузка всех ядер
	total, err := cpu.Percent(0, false)
	if err == nil && len(total) > 0 {
		c.write("pulse_cpu_usage_percent", merge(base, map[string]string{"cpu": "total"}), now, round(total[0]))
	}
}

// ── Memory ────────────────────────────────────────────────────────────────────

func (c *Collector) collectMem(now int64, base map[string]string) {
	v, err := mem.VirtualMemory()
	if err != nil {
		slog.Debug("mem collect error", "err", err)
		return
	}
	c.write("pulse_mem_total_bytes",     base, now, float64(v.Total))
	c.write("pulse_mem_used_bytes",      base, now, float64(v.Used))
	c.write("pulse_mem_available_bytes", base, now, float64(v.Available))
	c.write("pulse_mem_usage_percent",   base, now, round(v.UsedPercent))
}

// ── Disk I/O ──────────────────────────────────────────────────────────────────

func (c *Collector) collectDisk(now int64, base map[string]string) {
	counters, err := disk.IOCounters()
	if err != nil {
		slog.Debug("disk collect error", "err", err)
		return
	}
	for device, stat := range counters {
		l := merge(base, map[string]string{"device": device})
		c.write("pulse_disk_read_bytes_total",  l, now, float64(stat.ReadBytes))
		c.write("pulse_disk_write_bytes_total", l, now, float64(stat.WriteBytes))
		c.write("pulse_disk_read_ops_total",    l, now, float64(stat.ReadCount))
		c.write("pulse_disk_write_ops_total",   l, now, float64(stat.WriteCount))
	}
}

// ── Network ───────────────────────────────────────────────────────────────────

func (c *Collector) collectNet(now int64, base map[string]string) {
	counters, err := psnet.IOCounters(true) // pernic=true
	if err != nil {
		slog.Debug("net collect error", "err", err)
		return
	}
	for _, stat := range counters {
		if stat.Name == "lo" || stat.Name == "lo0" {
			continue // loopback не интересен
		}
		l := merge(base, map[string]string{"iface": stat.Name})
		c.write("pulse_net_rx_bytes_total",   l, now, float64(stat.BytesRecv))
		c.write("pulse_net_tx_bytes_total",   l, now, float64(stat.BytesSent))
		c.write("pulse_net_rx_packets_total", l, now, float64(stat.PacketsRecv))
		c.write("pulse_net_tx_packets_total", l, now, float64(stat.PacketsSent))
	}
}

// ── Go runtime ────────────────────────────────────────────────────────────────

func (c *Collector) collectGoRuntime(now int64, base map[string]string) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	c.write("pulse_go_goroutines",      base, now, float64(runtime.NumGoroutine()))
	c.write("pulse_go_heap_used_bytes", base, now, float64(ms.HeapInuse))
	c.write("pulse_go_heap_sys_bytes",  base, now, float64(ms.HeapSys))
	c.write("pulse_go_gc_pause_ns",     base, now, float64(ms.PauseNs[(ms.NumGC+255)%256]))
	c.write("pulse_go_gc_total",        base, now, float64(ms.NumGC))
	c.write("pulse_go_alloc_bytes",     base, now, float64(ms.Alloc))
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func (c *Collector) write(metric string, labels map[string]string, ts int64, value float64) {
	if err := c.db.WriteBatch(metric, labels, []pulse.Point{{Timestamp: ts, Value: value}}); err != nil {
		slog.Debug("collector write error", "metric", metric, "err", err)
	}
}

func merge(base, extra map[string]string) map[string]string {
	out := make(map[string]string, len(base)+len(extra))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range extra {
		out[k] = v
	}
	return out
}

func round(v float64) float64 {
	return float64(int(v*100)) / 100
}

func hostname() string {
	name, err := getHostname()
	if err != nil {
		return "localhost"
	}
	return name
}
