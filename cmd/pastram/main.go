// Command pastram measures a memory-limited cluster. The runner owns the
// processes and cgroups; this command owns the expected value of every key.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/djosh34/tigerstore/internal/k8ssize"
)

type config struct {
	endpoints, out, mode string
	bytes                int64
	duration             time.Duration
}

type measurement struct {
	Phase        string  `json:"phase"`
	Objects      int     `json:"objects"`
	LiveBytes    int64   `json:"live_bytes"`
	Calls        int     `json:"calls"`
	Seconds      float64 `json:"seconds"`
	OpsPerSecond float64 `json:"ops_per_second"`
	P50Us        int64   `json:"p50_us"`
	P99Us        int64   `json:"p99_us"`
	Failure      string  `json:"failure,omitempty"`
}

type workload struct {
	cfg     config
	cl      *clientv3.Client
	rows    []int
	objects int
	live    int64
	changed map[int]int64
	rng     *rand.Rand
	latency []int64
}

func main() {
	var c config
	flag.StringVar(&c.endpoints, "endpoints", "http://127.0.0.1:20100", "comma-separated endpoints")
	flag.StringVar(&c.out, "out", "past-ram", "report directory")
	flag.StringVar(&c.mode, "mode", "proof", "proof, max or cache")
	flag.Int64Var(&c.bytes, "bytes", 3*(512<<20), "minimum live value bytes before mixed load")
	flag.DurationVar(&c.duration, "duration", 3*time.Minute, "mixed-load measurement")
	flag.Parse()
	if err := run(c); err != nil {
		if _, writeErr := fmt.Fprintln(os.Stderr, err); writeErr != nil {
			os.Exit(2)
		}
		os.Exit(1)
	}
}

func run(c config) (err error) {
	if c.bytes <= 0 || c.duration <= 0 || (c.mode != "proof" && c.mode != "max" && c.mode != "cache") {
		return errors.New("need positive bytes and duration, mode proof, max or cache")
	}
	if err := os.MkdirAll(c.out, 0o755); err != nil {
		return err
	}
	cl, err := clientv3.New(clientv3.Config{Endpoints: strings.Split(c.endpoints, ","), DialTimeout: 10 * time.Second, MaxCallRecvMsgSize: 8 << 20})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, cl.Close()) }()
	w := &workload{cfg: c, cl: cl, changed: map[int]int64{}, rng: rand.New(rand.NewPCG(148, 7))}
	for i, b := range k8ssize.Buckets {
		for range b.Count {
			w.rows = append(w.rows, i)
		}
	}
	start := time.Now()
	for c.mode == "max" || w.live < c.bytes {
		var ops []clientv3.Op
		var added int64
		for i := w.objects; i < w.objects+64; i++ {
			v := w.value(i, 0)
			ops = append(ops, clientv3.OpPut(w.key(i), string(v)))
			added += int64(len(v))
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_, putErr := cl.Txn(ctx).Then(ops...).Commit()
		cancel()
		if putErr != nil {
			m := measurement{Phase: "preload-failed", Objects: w.objects, LiveBytes: w.live, Seconds: time.Since(start).Seconds(), Failure: putErr.Error()}
			return errors.Join(putErr, w.report(m))
		}
		w.objects += len(ops)
		w.live += added
		last, err := json.Marshal(measurement{Phase: "preload", Objects: w.objects, LiveBytes: w.live, Seconds: time.Since(start).Seconds()})
		if err != nil {
			return err
		}
		path := filepath.Join(c.out, "last-acknowledged.json")
		if err := os.WriteFile(path+".tmp", last, 0o644); err != nil {
			return err
		}
		if err := os.Rename(path+".tmp", path); err != nil {
			return err
		}
		if w.objects%8192 == 0 {
			if err := w.report(measurement{Phase: "preload", Objects: w.objects, LiveBytes: w.live, Seconds: time.Since(start).Seconds()}); err != nil {
				return err
			}
		}
	}
	if err := w.report(measurement{Phase: "preloaded", Objects: w.objects, LiveBytes: w.live, Seconds: time.Since(start).Seconds()}); err != nil {
		return err
	}
	// A linearizable count through each member waits for its apply path.
	// All three must have the complete preload before the kill timer starts.
	for _, endpoint := range cl.Endpoints() {
		one, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 10 * time.Second})
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		r, readErr := one.Get(ctx, "/registry/", clientv3.WithPrefix(), clientv3.WithCountOnly())
		cancel()
		if err := errors.Join(readErr, one.Close()); err != nil {
			return err
		}
		if r.Count != int64(w.objects) {
			return fmt.Errorf("preload on %s: %d objects, want %d", endpoint, r.Count, w.objects)
		}
	}
	// The runner kills and restarts a node after this marker, during mixed load.
	if err := os.WriteFile(filepath.Join(c.out, "ready"), []byte("mixed\n"), 0o644); err != nil {
		return err
	}
	if err := waitMarker(filepath.Join(c.out, "mixed-started")); err != nil {
		return err
	}
	start = time.Now()
	for time.Since(start) < c.duration {
		at := time.Now()
		if err := w.mixed(); err != nil {
			return errors.Join(err, w.report(measurement{Phase: "mixed-failed", Objects: w.objects, LiveBytes: w.live, Failure: err.Error()}))
		}
		w.latency = append(w.latency, time.Since(at).Microseconds())
	}
	seconds := time.Since(start).Seconds()
	slices.Sort(w.latency)
	if err := w.report(measurement{
		Phase: "mixed", Objects: w.objects, LiveBytes: w.live, Calls: len(w.latency), Seconds: seconds,
		OpsPerSecond: float64(len(w.latency)) / seconds, P50Us: w.latency[len(w.latency)/2], P99Us: w.latency[(len(w.latency)-1)*99/100],
	}); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(c.out, "mixed-done"), []byte("done\n"), 0o644); err != nil {
		return err
	}
	if err := waitMarker(filepath.Join(c.out, "mixed-sampled")); err != nil {
		return err
	}
	// A paginated scan through each member checks exact bytes against our
	// generated history, not against another replica or a server hash.
	for _, endpoint := range cl.Endpoints() {
		one, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 10 * time.Second, MaxCallRecvMsgSize: 8 << 20})
		if err != nil {
			return err
		}
		err = w.verify(one)
		err = errors.Join(err, one.Close())
		if err != nil {
			return errors.Join(fmt.Errorf("verify %s: %w", endpoint, err), w.report(measurement{Phase: "verify-failed", Failure: err.Error()}))
		}
	}
	return w.report(measurement{Phase: "verified", Objects: w.objects, LiveBytes: w.live})
}

// The controller samples before load starts and after it ends. Verification
// waits so its disk reads cannot enter the cache experiment's measurement.
func waitMarker(path string) error {
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		_, err := os.Stat(path)
		if err == nil {
			return nil
		}
		if !os.IsNotExist(err) || time.Now().After(deadline) {
			return fmt.Errorf("waiting for controller marker %s: %w", path, err)
		}
	}
}

func (w *workload) bucket(i int) k8ssize.Bucket {
	return k8ssize.Buckets[w.rows[i%len(w.rows)]]
}

func (w *workload) key(i int) string {
	return fmt.Sprintf("/registry/%s/past-ram/%09d", w.bucket(i).Resource, i)
}

func (w *workload) value(i int, generation int64) []byte {
	var seed [16]byte
	binary.LittleEndian.PutUint64(seed[:8], uint64(i))
	binary.LittleEndian.PutUint64(seed[8:], uint64(generation))
	sum := sha256.Sum256(seed[:])
	v := make([]byte, w.bucket(i).Size)
	for at := 0; at < len(v); at += len(sum) {
		copy(v[at:], sum[:])
	}
	return v
}

func (w *workload) check(k, v []byte) error {
	var i int
	if _, err := fmt.Sscanf(string(k[strings.LastIndexByte(string(k), '/')+1:]), "%d", &i); err != nil {
		return err
	}
	if i < 0 || i >= w.objects || string(k) != w.key(i) || w.changed[i] < 0 || !bytes.Equal(v, w.value(i, w.changed[i])) {
		return fmt.Errorf("value mismatch for %q", k)
	}
	return nil
}

func (w *workload) mixed() error {
	i := w.rng.IntN(w.objects)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// 40% point reads, 10% paginated LIST, 40% revision-CAS updates,
	// 10% deletes. A later update recreates a deleted key.
	op := w.rng.IntN(10)
	if w.cfg.mode == "cache" {
		op = 0
		// Eight reads in ten target the first percent of the keys.
		if w.rng.IntN(10) < 8 {
			i = w.rng.IntN(max(1, w.objects/100))
		}
	}
	if op == 4 {
		r, err := w.cl.Get(ctx, w.key(i), clientv3.WithRange("/registry0"), clientv3.WithLimit(32))
		if err != nil {
			return err
		}
		for _, kv := range r.Kvs {
			if err := w.check(kv.Key, kv.Value); err != nil {
				return err
			}
		}
		return nil
	}
	r, err := w.cl.Get(ctx, w.key(i))
	if err != nil {
		return err
	}
	var mod int64
	if w.changed[i] < 0 {
		if len(r.Kvs) != 0 {
			return fmt.Errorf("deleted key %s returned", w.key(i))
		}
	} else {
		if len(r.Kvs) != 1 {
			return fmt.Errorf("missing key %s", w.key(i))
		}
		if err := w.check(r.Kvs[0].Key, r.Kvs[0].Value); err != nil {
			return err
		}
		mod = r.Kvs[0].ModRevision
	}
	if op < 4 {
		return nil
	}
	generation := int64(len(w.latency) + 1)
	mutation := clientv3.OpPut(w.key(i), string(w.value(i, generation)))
	if op == 9 {
		mutation = clientv3.OpDelete(w.key(i))
		generation = -1
	}
	resp, err := w.cl.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision(w.key(i)), "=", mod)).Then(mutation).Commit()
	if err != nil {
		// Do not retry an ambiguous write or call it a definite rejection.
		return fmt.Errorf("mutation outcome unknown for %s: %w", w.key(i), err)
	}
	if !resp.Succeeded {
		return fmt.Errorf("CAS rejected for exclusively owned key %s", w.key(i))
	}
	if w.changed[i] < 0 && generation >= 0 {
		w.live += int64(w.bucket(i).Size)
	} else if w.changed[i] >= 0 && generation < 0 {
		w.live -= int64(w.bucket(i).Size)
	}
	w.changed[i] = generation
	return nil
}

func (w *workload) verify(cl *clientv3.Client) error {
	var spans [][2]int
	for first := 0; first < w.objects; {
		end := first + 1
		for end < min(first+128, w.objects) && w.bucket(end).Resource == w.bucket(first).Resource {
			end++
		}
		spans = append(spans, [2]int{first, end})
		first = end
	}
	type scanResult struct {
		count int
		live  int64
		err   error
	}
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	results := make(chan scanResult, 8)
	for worker := range 8 {
		go func() {
			var result scanResult
			defer func() { results <- result }()
			for i := worker; i < len(spans); i += 8 {
				callCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
				span := spans[i]
				r, err := cl.Get(callCtx, w.key(span[0]), clientv3.WithRange(w.key(span[1]-1)+"\x00"))
				cancel()
				if err != nil {
					result.err = err
					stop()
					return
				}
				for _, kv := range r.Kvs {
					if err := w.check(kv.Key, kv.Value); err != nil {
						result.err = err
						stop()
						return
					}
					result.count++
					result.live += int64(len(kv.Value))
				}
			}
		}()
	}
	var count int
	var live int64
	var scanErr error
	for range 8 {
		r := <-results
		count += r.count
		live += r.live
		scanErr = errors.Join(scanErr, r.err)
	}
	if scanErr != nil {
		return scanErr
	}
	want := w.objects
	for _, generation := range w.changed {
		if generation < 0 {
			want--
		}
	}
	if count != want || live != w.live {
		return fmt.Errorf("scan got %d objects, %d bytes; want %d, %d", count, live, want, w.live)
	}
	countCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	r, err := cl.Get(countCtx, "/registry/", clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		return err
	}
	if r.Count != int64(want) {
		return fmt.Errorf("unexpected keys: total count %d, expected %d", r.Count, want)
	}
	return nil
}

func (w *workload) report(m measurement) (err error) {
	f, err := os.OpenFile(filepath.Join(w.cfg.out, "progress.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	if err := json.NewEncoder(f).Encode(m); err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(m)
}
