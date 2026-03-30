package chunk

import (
	"math/rand"
	"testing"

	"github.com/bbvtaev/pulse-core/internal/model"
)

func TestRoundtrip_Regular(t *testing.T) {
	pts := make([]model.Point, 100)
	ts := int64(1_700_000_000_000)
	for i := range pts {
		pts[i] = model.Point{
			Timestamp: ts + int64(i)*15_000,
			Value:     float64(i) * 0.1,
		}
	}
	checkRoundtrip(t, pts)
}

func TestRoundtrip_Constant(t *testing.T) {
	pts := make([]model.Point, 100)
	ts := int64(1_700_000_000_000)
	for i := range pts {
		pts[i] = model.Point{Timestamp: ts + int64(i)*15_000, Value: 42.0}
	}
	checkRoundtrip(t, pts)
}

func TestRoundtrip_Random(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	pts := make([]model.Point, 100)
	ts := int64(1_700_000_000_000)
	for i := range pts {
		ts += int64(rng.Intn(60_000) + 1_000)
		pts[i] = model.Point{Timestamp: ts, Value: rng.Float64() * 1000}
	}
	checkRoundtrip(t, pts)
}

func TestRoundtrip_Single(t *testing.T) {
	checkRoundtrip(t, []model.Point{{Timestamp: 1_700_000_000_000, Value: 3.14}})
}

func TestRoundtrip_Two(t *testing.T) {
	checkRoundtrip(t, []model.Point{
		{Timestamp: 1_700_000_000_000, Value: 1.0},
		{Timestamp: 1_700_000_015_000, Value: 2.0},
	})
}

func TestCompression_Ratio(t *testing.T) {
	pts := make([]model.Point, 100)
	ts := int64(1_700_000_000_000)
	for i := range pts {
		pts[i] = model.Point{
			Timestamp: ts + int64(i)*15_000,
			Value:     float64(i) * 0.5,
		}
	}
	compressed := EncodePoints(pts)
	raw := len(pts) * 16
	if len(compressed) >= raw/4 {
		t.Errorf("compression ratio too low: compressed=%d bytes, raw=%d bytes (want < %d)",
			len(compressed), raw, raw/4)
	}
}

func checkRoundtrip(t *testing.T, pts []model.Point) {
	t.Helper()
	compressed := EncodePoints(pts)
	got, err := DecodePoints(compressed, len(pts))
	if err != nil {
		t.Fatalf("DecodePoints error: %v", err)
	}
	if len(got) != len(pts) {
		t.Fatalf("len mismatch: got %d, want %d", len(got), len(pts))
	}
	for i := range pts {
		if got[i].Timestamp != pts[i].Timestamp {
			t.Errorf("[%d] timestamp: got %d, want %d", i, got[i].Timestamp, pts[i].Timestamp)
		}
		if got[i].Value != pts[i].Value {
			t.Errorf("[%d] value: got %v, want %v", i, got[i].Value, pts[i].Value)
		}
	}
}
