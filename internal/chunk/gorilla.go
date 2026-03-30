package chunk

import (
	"fmt"
	"io"
	"math"
	"math/bits"

	"github.com/bbvtaev/pulse-core/internal/model"
)

// bitWriter упаковывает биты MSB-first в байтовый срез.
type bitWriter struct {
	buf   []byte
	cur   byte
	nBits uint8 // заполненных бит в cur (0..8)
}

func (w *bitWriter) writeBit(b bool) {
	if w.nBits == 8 {
		w.buf = append(w.buf, w.cur)
		w.cur = 0
		w.nBits = 0
	}
	if b {
		w.cur |= 1 << (7 - w.nBits)
	}
	w.nBits++
}

func (w *bitWriter) writeBits(v uint64, n uint8) {
	for i := int(n) - 1; i >= 0; i-- {
		w.writeBit((v>>uint(i))&1 == 1)
	}
}

func (w *bitWriter) bytes() []byte {
	out := make([]byte, len(w.buf))
	copy(out, w.buf)
	if w.nBits > 0 {
		out = append(out, w.cur)
	}
	return out
}

// bitReader читает биты MSB-first из байтового среза.
type bitReader struct {
	buf   []byte
	pos   int   // следующий байт для загрузки
	cur   byte
	nBits uint8 // оставшихся бит в cur
}

func (r *bitReader) readBit() (bool, error) {
	if r.nBits == 0 {
		if r.pos >= len(r.buf) {
			return false, io.ErrUnexpectedEOF
		}
		r.cur = r.buf[r.pos]
		r.pos++
		r.nBits = 8
	}
	b := (r.cur >> (r.nBits - 1)) & 1
	r.nBits--
	return b == 1, nil
}

func (r *bitReader) readBits(n uint8) (uint64, error) {
	var v uint64
	for i := uint8(0); i < n; i++ {
		b, err := r.readBit()
		if err != nil {
			return 0, err
		}
		v <<= 1
		if b {
			v |= 1
		}
	}
	return v, nil
}

// xorState хранит состояние XOR-кодирования между точками.
type xorState struct {
	prevValBits  uint64
	prevLeading  uint8
	prevTrailing uint8
	hasBlock     bool
}

// encodeTimestampDoD кодирует delta-of-delta временной метки.
//
//	dod == 0           → '0'           (1 бит)
//	-63 ≤ dod ≤ 64    → '10' + 7 бит  (9 бит)
//	-255 ≤ dod ≤ 256  → '110' + 9 бит (12 бит)
//	-2047 ≤ dod ≤ 2048 → '1110' + 12 б (16 бит)
//	иначе             → '1111' + 64 б  (68 бит)
func encodeTimestampDoD(w *bitWriter, dod int64) {
	switch {
	case dod == 0:
		w.writeBit(false)
	case dod >= -63 && dod <= 64:
		w.writeBit(true)
		w.writeBit(false)
		w.writeBits(uint64(dod+63), 7)
	case dod >= -255 && dod <= 256:
		w.writeBit(true)
		w.writeBit(true)
		w.writeBit(false)
		w.writeBits(uint64(dod+255), 9)
	case dod >= -2047 && dod <= 2048:
		w.writeBit(true)
		w.writeBit(true)
		w.writeBit(true)
		w.writeBit(false)
		w.writeBits(uint64(dod+2047), 12)
	default:
		w.writeBit(true)
		w.writeBit(true)
		w.writeBit(true)
		w.writeBit(true)
		w.writeBits(uint64(dod), 64)
	}
}

func decodeTimestampDoD(r *bitReader) (int64, error) {
	b, err := r.readBit()
	if err != nil {
		return 0, err
	}
	if !b {
		return 0, nil
	}
	b, err = r.readBit()
	if err != nil {
		return 0, err
	}
	if !b {
		v, err := r.readBits(7)
		if err != nil {
			return 0, err
		}
		return int64(v) - 63, nil
	}
	b, err = r.readBit()
	if err != nil {
		return 0, err
	}
	if !b {
		v, err := r.readBits(9)
		if err != nil {
			return 0, err
		}
		return int64(v) - 255, nil
	}
	b, err = r.readBit()
	if err != nil {
		return 0, err
	}
	if !b {
		v, err := r.readBits(12)
		if err != nil {
			return 0, err
		}
		return int64(v) - 2047, nil
	}
	v, err := r.readBits(64)
	if err != nil {
		return 0, err
	}
	return int64(v), nil
}

// encodeValue кодирует значение через XOR с предыдущим.
func encodeValue(w *bitWriter, valBits uint64, st *xorState) {
	xorVal := valBits ^ st.prevValBits
	st.prevValBits = valBits

	if xorVal == 0 {
		w.writeBit(false)
		return
	}
	w.writeBit(true)

	leading := uint8(bits.LeadingZeros64(xorVal))
	trailing := uint8(bits.TrailingZeros64(xorVal))

	if !st.hasBlock || leading < st.prevLeading || trailing < st.prevTrailing {
		w.writeBit(true) // новый блок
		// 5 бит могут хранить 0..31; cap leading чтобы не выйти за границу
		if leading > 31 {
			leading = 31
		}
		meaningful := 64 - leading - trailing
		w.writeBits(uint64(leading), 5)
		w.writeBits(uint64(meaningful-1), 6) // meaningful от 1 до 64
		w.writeBits(xorVal>>trailing, meaningful)
		st.prevLeading = leading
		st.prevTrailing = trailing
		st.hasBlock = true
	} else {
		w.writeBit(false) // переиспользовать блок
		prevMeaningful := 64 - st.prevLeading - st.prevTrailing
		w.writeBits(xorVal>>st.prevTrailing, prevMeaningful)
	}
}

func decodeValue(r *bitReader, st *xorState) (uint64, error) {
	b, err := r.readBit()
	if err != nil {
		return 0, err
	}
	if !b {
		// XOR == 0, значение не изменилось
		return st.prevValBits, nil
	}

	b, err = r.readBit()
	if err != nil {
		return 0, err
	}

	var xorVal uint64
	if b {
		// новый блок
		leadingV, err := r.readBits(5)
		if err != nil {
			return 0, err
		}
		meaningfulV, err := r.readBits(6)
		if err != nil {
			return 0, err
		}
		leading := uint8(leadingV)
		meaningful := uint8(meaningfulV) + 1
		trailing := 64 - leading - meaningful
		payload, err := r.readBits(meaningful)
		if err != nil {
			return 0, err
		}
		xorVal = payload << trailing
		st.prevLeading = leading
		st.prevTrailing = trailing
		st.hasBlock = true
	} else {
		// переиспользовать предыдущий блок
		if !st.hasBlock {
			return 0, fmt.Errorf("gorilla: reuse-block before any block defined")
		}
		prevMeaningful := 64 - st.prevLeading - st.prevTrailing
		payload, err := r.readBits(prevMeaningful)
		if err != nil {
			return 0, err
		}
		xorVal = payload << st.prevTrailing
	}

	result := xorVal ^ st.prevValBits
	st.prevValBits = result
	return result, nil
}

// EncodePoints сжимает срез точек алгоритмом Gorilla.
//
// Формат:
//
//	ts[0]   int64   (64 бита)
//	val[0]  float64 (64 бита)
//	delta1  int64   (64 бита)  — ts[1]-ts[0], начало DoD
//	val[1]  XOR-encoded
//	для i≥2: DoD(ts[i]), XOR(val[i])
func EncodePoints(points []model.Point) []byte {
	if len(points) == 0 {
		return []byte{}
	}

	w := &bitWriter{}
	w.writeBits(uint64(points[0].Timestamp), 64)
	w.writeBits(math.Float64bits(points[0].Value), 64)

	if len(points) == 1 {
		return w.bytes()
	}

	delta1 := points[1].Timestamp - points[0].Timestamp
	w.writeBits(uint64(delta1), 64)

	st := &xorState{prevValBits: math.Float64bits(points[0].Value)}
	encodeValue(w, math.Float64bits(points[1].Value), st)

	prevTS := points[1].Timestamp
	prevDelta := delta1

	for i := 2; i < len(points); i++ {
		delta := points[i].Timestamp - prevTS
		encodeTimestampDoD(w, delta-prevDelta)
		encodeValue(w, math.Float64bits(points[i].Value), st)
		prevDelta = delta
		prevTS = points[i].Timestamp
	}

	return w.bytes()
}

// DecodePoints распаковывает n точек из сжатых данных.
func DecodePoints(data []byte, n int) ([]model.Point, error) {
	if n == 0 {
		return nil, nil
	}

	r := &bitReader{buf: data}
	points := make([]model.Point, n)

	tsBits, err := r.readBits(64)
	if err != nil {
		return nil, fmt.Errorf("gorilla: ts[0]: %w", err)
	}
	valBits, err := r.readBits(64)
	if err != nil {
		return nil, fmt.Errorf("gorilla: val[0]: %w", err)
	}
	points[0] = model.Point{
		Timestamp: int64(tsBits),
		Value:     math.Float64frombits(valBits),
	}

	if n == 1 {
		return points, nil
	}

	deltaBits, err := r.readBits(64)
	if err != nil {
		return nil, fmt.Errorf("gorilla: delta1: %w", err)
	}
	delta := int64(deltaBits)
	points[1].Timestamp = points[0].Timestamp + delta

	st := &xorState{prevValBits: math.Float64bits(points[0].Value)}
	vb, err := decodeValue(r, st)
	if err != nil {
		return nil, fmt.Errorf("gorilla: val[1]: %w", err)
	}
	points[1].Value = math.Float64frombits(vb)

	prevDelta := delta

	for i := 2; i < n; i++ {
		dod, err := decodeTimestampDoD(r)
		if err != nil {
			return nil, fmt.Errorf("gorilla: ts[%d]: %w", i, err)
		}
		delta = dod + prevDelta
		points[i].Timestamp = points[i-1].Timestamp + delta
		prevDelta = delta

		vb, err = decodeValue(r, st)
		if err != nil {
			return nil, fmt.Errorf("gorilla: val[%d]: %w", i, err)
		}
		points[i].Value = math.Float64frombits(vb)
	}

	return points, nil
}
