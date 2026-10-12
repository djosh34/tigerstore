package main

import (
	"crypto/sha256"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"time"

	"github.com/klauspost/compress/dict"
	"github.com/klauspost/compress/zstd"
)

type dataset struct {
	name                string
	records             []record
	batches, dictionary bool
}
type writerFile struct {
	f *os.File
	w *csv.Writer
}

func output(dir, name string, header []string) (*writerFile, error) {
	f, err := os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return nil, fmt.Errorf("cannot create aggregate output")
	}
	w := csv.NewWriter(f)
	if err := w.Write(header); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &writerFile{f, w}, nil
}
func (w *writerFile) close() error {
	w.w.Flush()
	err := w.w.Error()
	closeErr := w.f.Close()
	if err != nil {
		return err
	}
	return closeErr
}
func metadata(w *csv.Writer, k, v string) error { return w.Write([]string{k, v}) }

func trainingSamples(train []record) ([][]byte, int) {
	// Hash ordering avoids a resource/key-prefix bias when the cap is reached.
	ordered := append([]record(nil), train...)
	sort.Slice(ordered, func(i, j int) bool {
		a, b := sha256.Sum256(ordered[i].Key), sha256.Sum256(ordered[j].Key)
		return string(a[:]) < string(b[:])
	})
	var samples [][]byte
	total := 0
	for _, r := range ordered {
		if len(r.Value) < 8 {
			continue
		}
		n := min(len(r.Value), 256<<10)
		if total+n > 8<<20 || len(samples) >= 4096 {
			continue
		}
		samples = append(samples, r.Value[:n])
		total += n
	}
	return samples, total
}
func run(inputPath, outDir string, duration time.Duration) (retErr error) {
	if runtime.GOMAXPROCS(0) != 1 {
		return fmt.Errorf("set GOMAXPROCS=1 outside the executable and pin its CPU with taskset")
	}
	start := time.Now()
	deadline := start.Add(230 * time.Second)
	f, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("cannot open input")
	}
	records, err := load(f)
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return fmt.Errorf("cannot close input")
	}
	if err := os.MkdirAll(outDir, 0700); err != nil {
		return fmt.Errorf("cannot create output directory")
	}
	var files []*writerFile
	defer func() {
		for _, f := range files {
			if err := f.close(); retErr == nil && err != nil {
				retErr = err
			}
		}
	}()
	meta, err := output(outDir, "metadata.csv", []string{"name", "value"})
	if err != nil {
		return err
	}
	files = append(files, meta)
	mw := meta.w
	pairs := [][2]string{{"status", "started"}, {"go_version", runtime.Version()}, {"goarch", runtime.GOARCH}, {"gomaxprocs", "1"}, {"repetitions", "5"}, {"target_ms_per_direction", num(duration.Milliseconds())}, {"run_budget_seconds", "230"}, {"training_fraction_rule", "sha256(key): big-endian first u64 modulo 5 equals 0"}, {"split_policy", "train20 excluded from dictionary evaluation; test80 uses all other keys"}, {"shuffle_seed", "42"}}
	for _, p := range pairs {
		if err := metadata(mw, p[0], p[1]); err != nil {
			return err
		}
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range info.Deps {
			if err := metadata(mw, "dependency:"+dep.Path, dep.Version); err != nil {
				return err
			}
		}
	}
	train := filter(records, func(r record) bool { return r.train })
	test := filter(records, func(r record) bool { return !r.train })
	if err := metadata(mw, "all_values", strconv.Itoa(len(records))); err != nil {
		return err
	}
	if err := metadata(mw, "train_values", strconv.Itoa(len(train))); err != nil {
		return err
	}
	if err := metadata(mw, "test_values", strconv.Itoa(len(test))); err != nil {
		return err
	}
	inv, err := output(outDir, "inventory.csv", []string{"split", "resource", "format", "size_bin", "values", "key_bytes", "value_bytes", "key_plus_value_bytes", "min_value_bytes", "max_value_bytes"})
	if err != nil {
		return err
	}
	files = append(files, inv)
	if err := inventory(inv.w, records); err != nil {
		return err
	}
	samples, sampleBytes := trainingSamples(train)
	if err := metadata(mw, "training_sample_count", strconv.Itoa(len(samples))); err != nil {
		return err
	}
	if err := metadata(mw, "training_sample_bytes", strconv.Itoa(sampleBytes)); err != nil {
		return err
	}
	var codecs []*codec
	defer func() {
		for _, c := range codecs {
			if err := c.close(); retErr == nil && err != nil {
				retErr = err
			}
		}
	}()
	for _, spec := range []struct {
		name  string
		level int
	}{{"zstd", 1}, {"zstd", 3}, {"lz4", 0}, {"snappy", 0}} {
		c, err := newCodec(spec.name, spec.level, nil)
		if err != nil {
			return fmt.Errorf("codec initialization failed")
		}
		codecs = append(codecs, c)
	}
	for _, level := range []int{1, 3} {
		key := fmt.Sprintf("zstd_%d_dictionary", level)
		if len(samples) < 2 || sampleBytes < 1024 {
			if err := metadata(mw, key+"_status", "skipped_insufficient_train_data"); err != nil {
				return err
			}
			continue
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("benchmark budget exhausted before training")
		}
		cpu0, err := cpuNS()
		if err != nil {
			return err
		}
		t := time.Now()
		dictionary, buildErr := dict.BuildZstdDict(samples, dict.Options{Output: io.Discard, MaxDictSize: 32 << 10, HashBytes: 6, ZstdDictID: uint32(0xCE000000 + level), ZstdLevel: zstd.EncoderLevelFromZstd(level)})
		elapsed := time.Since(t)
		cpu1, err := cpuNS()
		if err != nil {
			return err
		}
		if err := metadata(mw, key+"_build_wall_ns", num(elapsed.Nanoseconds())); err != nil {
			return err
		}
		if err := metadata(mw, key+"_build_cpu_ns", num(cpu1-cpu0)); err != nil {
			return err
		}
		if buildErr != nil {
			if err := metadata(mw, key+"_status", "skipped_training_failed_no_raw_dictionary_fallback"); err != nil {
				return err
			}
			continue
		}
		if err := metadata(mw, key+"_bytes", strconv.Itoa(len(dictionary))); err != nil {
			return err
		}
		if err := metadata(mw, key+"_status", "trained"); err != nil {
			return err
		}
		c, err := newCodec("zstd", level, dictionary)
		if err != nil {
			return fmt.Errorf("trained dictionary codec initialization failed")
		}
		codecs = append(codecs, c)
	}
	mw.Flush()
	if err := mw.Error(); err != nil {
		return err
	}
	sets := []dataset{{"all", records, true, false}, {"train20", train, false, false}, {"test80", test, true, true}}
	for _, bin := range []string{"near_1KiB_896_to_1152", "near_10KiB_9216_to_11264"} {
		b := bin
		sets = append(sets, dataset{b + "_all", filter(records, func(r record) bool { return r.bin == b }), true, false}, dataset{b + "_test80", filter(test, func(r record) bool { return r.bin == b }), true, true})
	}
	for _, n := range []int{1024, 10240} {
		for _, repeat := range []bool{false, true} {
			kind := "random"
			if repeat {
				kind = "repeated"
			}
			sets = append(sets, dataset{fmt.Sprintf("synthetic_%s_%d", kind, n), synthetic(n, repeat), true, true})
		}
	}
	bw, err := output(outDir, "benchmark.csv", benchHeader)
	if err != nil {
		return err
	}
	files = append(files, bw)
	configurations := 0
	for _, set := range sets {
		if len(set.records) == 0 {
			if err := metadata(mw, "empty_dataset:"+set.name, "1"); err != nil {
				return err
			}
			continue
		}
		sizes := []int{1}
		if set.batches {
			sizes = append(sizes, 16, 64, 256)
		}
		for _, batch := range sizes {
			orders := []string{"single"}
			if batch > 1 {
				orders = []string{"shuffled", "resource_sorted"}
				if set.records[0].resource == "synthetic" {
					orders = []string{"shuffled"}
				}
			}
			for _, order := range orders {
				inputs, err := pack(set.records, batch, order)
				if err != nil {
					return err
				}
				for _, c := range codecs {
					if len(c.dictionary) > 0 && !set.dictionary {
						continue
					}
					if err := benchmark(bw.w, set.name, inputs, c, batch, order, duration, deadline); err != nil {
						return err
					}
					configurations++
				}
			}
		}
	}
	if err := metadata(mw, "configurations_completed", strconv.Itoa(configurations)); err != nil {
		return err
	}
	if err := metadata(mw, "run_wall_ns", num(time.Since(start).Nanoseconds())); err != nil {
		return err
	}
	return metadata(mw, "status", "complete")
}

func main() {
	flag.CommandLine.SetOutput(io.Discard)
	inputPath := flag.String("input", "", "etcdctl JSON dump path")
	outDir := flag.String("out", "", "aggregate CSV output directory")
	duration := flag.Duration("duration", 40*time.Millisecond, "target per encode/decode repetition, 40ms to 100ms")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: codec-helper -input /private/dump.json -out /results [-duration 40ms]")
	}
	flag.Parse()
	if *inputPath == "" || *outDir == "" || flag.NArg() != 0 || *duration < 40*time.Millisecond || *duration > 100*time.Millisecond {
		flag.Usage()
		os.Exit(2)
	}
	if err := run(*inputPath, *outDir, *duration); err != nil {
		fmt.Fprintln(os.Stderr, "codec-helper:", err)
		os.Exit(1)
	}
	fmt.Fprintln(os.Stderr, "codec-helper: complete; aggregate CSV files written")
}
