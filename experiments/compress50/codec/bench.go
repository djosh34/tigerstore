package main

import (
	"bytes"
	"encoding/csv"
	"errors"
	"runtime"
	"strconv"
	"syscall"
	"time"
)

type metrics struct {
	operations, values int64
	wall, cpu          int64
	allocs, allocBytes uint64
}

func cpuNS() (int64, error) {
	var u syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &u); err != nil {
		return 0, err
	}
	return u.Utime.Sec*1e9 + u.Utime.Usec*1e3 + u.Stime.Sec*1e9 + u.Stime.Usec*1e3, nil
}

var sink int

func timed(c *codec, inputs []input, compressed [][]byte, encode bool, duration time.Duration, deadline time.Time) (metrics, error) {
	var m metrics
	if time.Now().Add(duration).After(deadline) {
		return m, errors.New("benchmark budget exhausted")
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	cpu0, err := cpuNS()
	if err != nil {
		return m, err
	}
	start := time.Now()
	stop := start.Add(duration)
	for i := 0; ; i++ {
		ix := i % len(inputs)
		var out []byte
		if encode {
			out, err = c.encode(inputs[ix].data)
		} else {
			out, err = c.decode(compressed[ix])
		}
		if err != nil {
			return m, errors.New("codec failed during timing")
		}
		sink ^= len(out)
		m.operations++
		m.values += int64(inputs[ix].values)
		// Only stop at a corpus boundary. Every input has equal pass coverage,
		// including when one pass takes longer than the requested duration.
		if (i+1)%len(inputs) == 0 {
			now := time.Now()
			if now.After(deadline) {
				return m, errors.New("benchmark budget exhausted")
			}
			if !now.Before(stop) {
				break
			}
		}
	}
	runtime.KeepAlive(sink)
	m.wall = time.Since(start).Nanoseconds()
	cpu1, err := cpuNS()
	if err != nil {
		return m, err
	}
	m.cpu = cpu1 - cpu0
	runtime.ReadMemStats(&after)
	m.allocs = after.Mallocs - before.Mallocs
	m.allocBytes = after.TotalAlloc - before.TotalAlloc
	return m, nil
}
func num(v int64) string       { return strconv.FormatInt(v, 10) }
func decimal(v float64) string { return strconv.FormatFloat(v, 'f', 6, 64) }
func ratio(a, b int64) string {
	if b == 0 {
		return ""
	}
	return decimal(float64(a) / float64(b))
}

var benchHeader = []string{"dataset", "codec", "level", "dictionary_bytes", "batch_target", "order", "repetition", "values", "batches", "value_bytes", "framed_bytes", "compressed_bytes", "compressed_per_value_byte", "compressed_per_framed_byte", "compressed_plus_dictionary_per_value_byte", "encode_operations", "encode_values_timed", "encode_wall_ns_per_value", "encode_cpu_ns_per_value", "encode_wall_ns_per_batch", "encode_cpu_ns_per_batch", "encode_allocs_per_batch", "encode_alloc_bytes_per_batch", "decode_operations", "decode_values_timed", "decode_wall_ns_per_value", "decode_cpu_ns_per_value", "decode_wall_ns_per_batch", "decode_cpu_ns_per_batch", "decode_allocs_per_batch", "decode_alloc_bytes_per_batch"}

func appendMetrics(row []string, m metrics) []string {
	return append(row, num(m.operations), num(m.values), ratio(m.wall, m.values), ratio(m.cpu, m.values), ratio(m.wall, m.operations), ratio(m.cpu, m.operations), decimal(float64(m.allocs)/float64(m.operations)), decimal(float64(m.allocBytes)/float64(m.operations)))
}

func benchmark(w *csv.Writer, dataset string, inputs []input, c *codec, batch int, order string, duration time.Duration, deadline time.Time) error {
	var values, raw, framed, compressedBytes int64
	compressed := make([][]byte, len(inputs))
	for i, in := range inputs {
		if time.Now().After(deadline) {
			return errors.New("benchmark budget exhausted during validation")
		}
		encoded, err := c.encode(in.data)
		if err != nil {
			return errors.New("encode failed during validation")
		}
		compressed[i] = bytes.Clone(encoded)
		decoded, err := c.decode(encoded)
		if err != nil {
			return errors.New("decode failed during validation")
		}
		if !bytes.Equal(in.data, decoded) {
			return errors.New("roundtrip mismatch")
		}
		if batch > 1 {
			if err := validateFrames(decoded, in.values); err != nil {
				return err
			}
		}
		values += int64(in.values)
		framed += int64(len(in.data))
		compressedBytes += int64(len(encoded))
	}
	raw = framed
	if batch > 1 {
		raw -= 4 * values
	}
	// Warm state and output buffers on all inputs before timing. Validation above
	// includes the largest input, so output capacities are already large enough.
	for rep := 1; rep <= 5; rep++ {
		enc, err := timed(c, inputs, compressed, true, duration, deadline)
		if err != nil {
			return err
		}
		dec, err := timed(c, inputs, compressed, false, duration, deadline)
		if err != nil {
			return err
		}
		row := []string{dataset, c.name, strconv.Itoa(c.level), strconv.Itoa(len(c.dictionary)), strconv.Itoa(batch), order, strconv.Itoa(rep), num(values), strconv.Itoa(len(inputs)), num(raw), num(framed), num(compressedBytes), ratio(compressedBytes, raw), ratio(compressedBytes, framed), ratio(compressedBytes+int64(len(c.dictionary)), raw)}
		row = appendMetrics(row, enc)
		row = appendMetrics(row, dec)
		if err := w.Write(row); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}
