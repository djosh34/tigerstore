// Separate compatibility/slow-reader diagnostics, never a timing generator.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"
)

const (
	limit        = 1536 * 1024
	slowAttempts = 4096
)

func raftSize(key, value []byte) int {
	return proto.Size(&pb.InternalRaftRequest{
		Header: &pb.RequestHeader{ID: 1 << 63},
		Put:    &pb.PutRequest{Key: key, Value: value},
	})
}

func valueOfSize(n int) []byte {
	value := make([]byte, n)
	for i := 0; i < n; i += sha256.Size {
		sum := sha256.Sum256([]byte(fmt.Sprintf("alloc50-large-%d", i)))
		copy(value[i:], sum[:])
	}
	return value
}

// slowCommittedPuts distinguishes committed writes from charged allocator
// capacity. A singleton stream can pin an entire eight-slot arena region.
func slowCommittedPuts(r *pb.RangeResponse, key, value []byte) (int64, error) {
	if r == nil || r.Header == nil || r.Count != 1 || len(r.Kvs) != 1 || r.Kvs[0] == nil {
		return 0, errors.New("slow key Range missing value or header")
	}
	kv := r.Kvs[0]
	if !bytes.Equal(kv.Key, key) || !bytes.Equal(kv.Value, value) || kv.Version < 1 || kv.Version > slowAttempts {
		return 0, errors.New("slow key Range has invalid value or committed count")
	}
	return kv.Version, nil
}

func run() (resultErr error) {
	endpoint := flag.String("endpoint", "", "leader client endpoint")
	certdir := flag.String("certdir", "", "certificate directory")
	debugURL := flag.String("debug-url", "", "leader observation endpoint")
	out := flag.String("out", "", "raw JSON output")
	strategy := flag.String("strategy", "base", "base, default, ring or arena")
	flag.Parse()
	mode := uint64(0)
	switch *strategy {
	case "ring":
		mode = 1
	case "arena":
		mode = 2
	case "base", "default":
	default:
		return errors.New("invalid strategy")
	}
	report := map[string]any{
		"valid": false, "slow_requests": slowAttempts, "slow_planned_puts": slowAttempts, "reply_reads_after_handshake": 0,
		"stream_receive_window": 0, "request_limit_bytes": limit,
	}
	var slow *slowSender
	// On failure, diagnose before cleanup closes the raw socket. The writer
	// must always be joined, including failed healthy requests or snapshots.
	defer func() {
		if resultErr != nil {
			report["valid"] = false
			report["failure"] = resultErr.Error()
			file, err := captureGoroutines(*debugURL, *out, "failure")
			if err != nil {
				report["failure_goroutines_error"] = err.Error()
			} else {
				report["failure_goroutines"] = file
			}
		}
		if slow != nil {
			if !slow.closing.Load() {
				report["sender_before_cleanup"] = slow.snapshot()
			}
			final, err := slow.closeAndWait(3 * time.Second)
			recordSender(report, final)
			if err != nil {
				resultErr = errors.Join(resultErr, err)
				report["valid"] = false
				report["cleanup_error"] = err.Error()
			}
		}
		b, err := json.MarshalIndent(report, "", "  ")
		if err == nil {
			err = os.WriteFile(*out, append(b, '\n'), 0o644)
		}
		resultErr = errors.Join(resultErr, err)
	}()
	ca, err := os.ReadFile(*certdir + "/ca.pem")
	if err != nil {
		return err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return errors.New("bad CA")
	}
	cert, err := tls.LoadX509KeyPair(*certdir+"/client.pem", *certdir+"/client-key.pem")
	if err != nil {
		return err
	}
	cfg := &tls.Config{
		RootCAs: pool, Certificates: []tls.Certificate{cert},
		MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13, NextProtos: []string{"h2"},
	}
	conn, err := grpc.NewClient(*endpoint, grpc.WithTransportCredentials(credentials.NewTLS(cfg)))
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	kv := pb.NewKVClient(conn)
	put := func(key, value []byte) (*pb.PutResponse, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return kv.Put(ctx, &pb.PutRequest{Key: key, Value: value})
	}
	get := func(key []byte) (*pb.RangeResponse, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return kv.Range(ctx, &pb.RangeRequest{Key: key})
	}
	snapshots := map[string]retainedSnapshot{}
	retained := func(phase string) error {
		client := &http.Client{Timeout: 15 * time.Second}
		r, err := client.Get(*debugURL + "/debug/retained")
		if err != nil {
			return err
		}
		b, readErr := io.ReadAll(r.Body)
		closeErr := r.Body.Close()
		if err := errors.Join(readErr, closeErr); err != nil {
			return err
		}
		if r.StatusCode != http.StatusOK {
			return fmt.Errorf("retained: %s", r.Status)
		}
		var j retainedSnapshot
		if err := json.Unmarshal(b, &j); err != nil {
			return err
		}
		report[phase] = j
		snapshots[phase] = j
		if !j.ForcedGC {
			return errors.New("retention snapshot did not force GC")
		}
		return validateAllocator(j.Allocator, mode)
	}
	key := []byte("/alloc50/large")
	value := valueOfSize(limit)
	n := limit
	for raftSize(key, value[:n]) > limit {
		n--
	}
	boundary := []map[string]any{}
	for _, size := range []int{n - 1, n, n + 1, limit} {
		response, err := put(key, value[:size])
		expectedOK := size <= n
		row := map[string]any{"payload_bytes": size, "internal_raft_request_bytes": raftSize(key, value[:size]), "expected_ok": expectedOK}
		if err != nil {
			row["error"] = err.Error()
		}
		if response != nil && response.Header != nil {
			row["revision"] = response.Header.Revision
		}
		boundary = append(boundary, row)
		report["boundary"] = boundary
		if expectedOK {
			if err != nil || response == nil || response.Header == nil {
				return errors.Join(errors.New("below/equal boundary Put failed or missing header"), err)
			}
		} else if err == nil || !errors.Is(rpctypes.Error(err), rpctypes.ErrRequestTooLarge) {
			return errors.Join(errors.New("above boundary should be request-too-large"), err)
		}
		r, err := get(key)
		if err != nil || r == nil || len(r.Kvs) != 1 {
			return errors.Join(errors.New("boundary Range failed or missing value"), err)
		}
		want := size
		if !expectedOK {
			want = n
		}
		if !bytes.Equal(r.Kvs[0].Value, value[:want]) {
			return errors.New("large value corrupted or rejected Put mutated state")
		}
	}
	report["accepted_payload_bytes"] = n
	if err := retained("before_slow"); err != nil {
		return err
	}
	slow, err = stalled(*endpoint, cfg, []byte("/alloc50/slow"))
	if err != nil {
		return err
	}
	started := time.Now()
	var quotaAt time.Time
	quotaSamples := []map[string]any{}
	pollQuota := func() error {
		if err := slow.check(); err != nil {
			return err
		}
		if mode == 0 {
			return nil
		}
		body, err := observation(*debugURL, "/debug/allocator", 3*time.Second)
		if err != nil {
			return err
		}
		var a map[string]uint64
		if err := json.Unmarshal(body, &a); err != nil {
			return err
		}
		if err := validateAllocator(a, mode); err != nil {
			return err
		}
		now := time.Now()
		quotaSamples = append(quotaSamples, map[string]any{"elapsed_s": now.Sub(started).Seconds(), "allocator": a})
		report["quota_samples"] = quotaSamples
		pinned := a["held_slots"] >= a["per_connection_slots"] && a["denied_count"] > snapshots["before_slow"].Allocator["denied_count"]
		if quotaAt.IsZero() {
			if now.Sub(started) > 5*time.Second {
				return errors.New("managed quota/refusal not observed within five seconds")
			}
			if pinned {
				quotaAt = now
				report["quota_observed_elapsed_s"] = now.Sub(started).Seconds()
			}
		} else if !pinned {
			return errors.New("managed quota was released before intentional close")
		}
		return nil
	}
	// Healthy work starts immediately, even if the single raw writer blocks.
	healthyKey, healthy := []byte("/alloc50/healthy"), valueOfSize(1024)
	successes := 0
	var previous int64
	healthyPut := func() error {
		if err := slow.check(); err != nil {
			return err
		}
		r, err := put(healthyKey, healthy)
		if err != nil || r == nil || r.Header == nil || r.Header.Revision <= previous {
			return errors.Join(errors.New("healthy Put while stalled failed or invalid revision"), err)
		}
		previous = r.Header.Revision
		successes++
		report["healthy_successes_while_stalled"] = successes
		if successes%16 == 0 {
			return pollQuota()
		}
		return nil
	}
	for phase := range 3 {
		if phase > 0 && mode != 0 {
			// Establish quota by five seconds, then sustain it for at least one
			// second. Keep healthy Puts progressing throughout that hold.
			for quotaAt.IsZero() {
				if err := pollQuota(); err != nil {
					return err
				}
				if err := healthyPut(); err != nil {
					return err
				}
			}
			until := quotaAt.Add(time.Duration(phase) * 500 * time.Millisecond)
			for time.Now().Before(until) {
				if err := pollQuota(); err != nil {
					return err
				}
				if err := healthyPut(); err != nil {
					return err
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
		for range 512 {
			if err := healthyPut(); err != nil {
				return err
			}
		}
		if err := pollQuota(); err != nil {
			return err
		}
		if err := retained(fmt.Sprintf("slow_phase_%d", phase)); err != nil {
			return err
		}
		report[fmt.Sprintf("sender_phase_%d", phase)] = slow.snapshot()
	}
	if mode == 0 {
		for !slow.snapshot().AllDataSent {
			if err := slow.check(); err != nil {
				return err
			}
			if time.Since(started) > 5*time.Second {
				return errors.New("baseline sender did not complete all planned Puts")
			}
			if err := healthyPut(); err != nil {
				return err
			}
		}
	} else {
		if quotaAt.IsZero() || time.Since(quotaAt) < time.Second {
			return errors.New("managed quota not sustained for one second")
		}
		report["quota_sustained_s"] = time.Since(quotaAt).Seconds()
	}
	if mode != 0 {
		last := snapshots["slow_phase_2"].Allocator
		prior := snapshots["slow_phase_1"].Allocator
		if last["held_slots"] < last["per_connection_slots"] {
			return fmt.Errorf("slow client did not pin its slot quota: %v", last)
		}
		if last["held_slots"] != prior["held_slots"] || last["held_bytes"] != prior["held_bytes"] {
			return fmt.Errorf("managed retention did not stabilize: prior=%v after=%v", prior, last)
		}
		if last["denied_count"] <= snapshots["before_slow"].Allocator["denied_count"] {
			return errors.New("slow workload did not exercise quota backpressure")
		}
		report["allocator_saturation_verified"] = true
	}
	report["healthy_successes_while_stalled"] = successes
	report["healthy_last_revision"] = previous
	// The slow connection never reads replies, so stream attempts and charged
	// slot reservations cannot be presented as successful Puts. Its key starts
	// absent and each committed overwrite increments Version exactly once.
	slowKey := []byte("/alloc50/slow")
	slowRange, err := get(slowKey)
	if err != nil {
		return errors.Join(errors.New("healthy Range of slow key failed"), err)
	}
	committed, err := slowCommittedPuts(slowRange, slowKey, valueOfSize(1024))
	if err != nil {
		return err
	}
	report["slow_committed_puts"] = committed
	report["slow_range_header"] = slowRange.Header
	report["slow_last_mod_revision"] = slowRange.Kvs[0].ModRevision
	if err := slow.check(); err != nil {
		return err
	}
	beforeClose := slow.snapshot()
	report["sender_before_close"] = beforeClose
	if !beforeClose.AllDataSent {
		file, err := captureGoroutines(*debugURL, *out, "blocked-prefix")
		if err != nil {
			return err
		}
		report["blocked_prefix_goroutines"] = file
	}
	final, err := slow.closeAndWait(3 * time.Second)
	recordSender(report, final)
	if err != nil {
		return err
	}
	if err := validateSender(final, committed, mode != 0); err != nil {
		return err
	}
	report["sender_outcome_verified"] = true
	time.Sleep(200 * time.Millisecond)
	postClose, err := put(healthyKey, healthy)
	if err != nil || postClose == nil || postClose.Header == nil || postClose.Header.Revision <= previous {
		return errors.Join(errors.New("healthy Put after slow close failed or invalid revision"), err)
	}
	report["healthy_revision_after_close"] = postClose.Header.Revision
	r, err := get(healthyKey)
	if err != nil || r == nil || len(r.Kvs) != 1 || !bytes.Equal(r.Kvs[0].Value, healthy) {
		return errors.Join(errors.New("healthy final Range failed or wrong value"), err)
	}
	deadline := time.Now().Add(5 * time.Second)
	recovery := []retainedSnapshot{}
	for {
		if err := retained("after_slow_close"); err != nil {
			return err
		}
		latest := snapshots["after_slow_close"]
		recovery = append(recovery, latest)
		report["recovery_samples"] = recovery
		before := snapshots["before_slow"].Allocator
		if mode == 0 || (latest.Allocator["held_slots"] <= before["held_slots"] && latest.Allocator["held_bytes"] <= before["held_bytes"]) {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("managed slots not released after close: %v", latest.Allocator)
		}
		time.Sleep(50 * time.Millisecond)
	}
	report["allocator_recovery_verified"] = mode != 0
	report["valid"] = true
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
