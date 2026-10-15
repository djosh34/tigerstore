package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/wal"
)

// running is one node that runs until stop, which returns what Run
// returned.
type running struct {
	n    *Node
	stop func() error
}

// runClean starts a single node in dir with a disk budget, 32 KiB
// segments and a checkpoint every 16 KiB of log or so.
func runClean(t *testing.T, dir string, budget int64) running {
	t.Helper()
	cfg := core.DefaultConfig(1, []uint64{1})
	cfg.WindowBytes, cfg.WindowMaxBytes = 4<<10, 8<<10
	cfg.UnappliedBytes = 1 << 20
	cfg.CheckpointBytes, cfg.CheckpointRatio = 16<<10, 2
	n, err := New(Config{Core: cfg, DataDir: dir, Tick: 50 * time.Millisecond, SegmentBytes: 32 << 10, DiskBudget: budget, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	ran := make(chan error, 1)
	go func() {
		err := n.Run(ctx, noPeers{})
		if err != nil {
			t.Logf("the node stopped: %v", err)
		}
		ran <- err
	}()
	r := running{n: n, stop: sync.OnceValue(func() error {
		cancel()
		return <-ran
	})}
	t.Cleanup(func() { _ = r.stop() })
	return r
}

// bigValue is a value of about 1 KiB.
func bigValue(key string, round int) []byte {
	return fmt.Appendf(nil, "%s of round %d %01000d", key, round, 0)
}

// putAll writes keys in one transaction and returns its revision.
func putAll(n *Node, round int, keys ...string) (int64, error) {
	txn := &pb.TxnRequest{}
	for _, k := range keys {
		txn.Success = append(txn.Success, &pb.RequestOp{Request: &pb.RequestOp_RequestPut{
			RequestPut: &pb.PutRequest{Key: []byte(k), Value: bigValue(k, round)},
		}})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r, err := n.Do(ctx, txn)
	if err != nil {
		return 0, err
	}
	return r.(*pb.TxnResponse).Header.Revision, nil
}

func keys(prefix string, n int) []string {
	var out []string
	for i := range n {
		out = append(out, fmt.Sprintf("%s%02d", prefix, i))
	}
	return out
}

// checkValues reads every key and compares it with the round that wrote
// it last.
func checkValues(n *Node, want map[string]int) error {
	r, err := do(n, &pb.RangeRequest{Key: []byte{0}, RangeEnd: []byte{0}})
	if err != nil {
		return err
	}
	kvs := r.(*pb.RangeResponse).Kvs
	if len(kvs) != len(want) {
		return fmt.Errorf("%d keys, want %d", len(kvs), len(want))
	}
	for _, kv := range kvs {
		if string(kv.Value) != string(bigValue(string(kv.Key), want[string(kv.Key)])) {
			return fmt.Errorf("key %s holds %.40q, want round %d", kv.Key, kv.Value, want[string(kv.Key)])
		}
	}
	return nil
}

func walFiles(t *testing.T, dir string) (segs, packs int) {
	t.Helper()
	ents, err := os.ReadDir(filepath.Join(dir, "wal"))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		switch {
		case strings.HasSuffix(e.Name(), ".wal"):
			segs++
		case strings.HasPrefix(e.Name(), "pack-"):
			packs++
		}
	}
	return segs, packs
}

// TestCleaningCycles writes 50 cold keys once, then overwrites 50 hot keys
// and compacts, round after round, on a node with a 1 MiB disk budget.
// Cleaning must keep the disk within it without a NOSPACE answer, copy the
// cold values into packs, keep the map of where values are from growing
// with the history, and every value must read back, also after a restart.
func TestCleaningCycles(t *testing.T) {
	const budget = 1 << 20
	dir := t.TempDir()
	r := runClean(t, dir, budget)
	want := map[string]int{}
	if _, err := do(r.n, &pb.PutRequest{Key: []byte("start"), Value: bigValue("start", 0)}); err != nil {
		t.Fatal(err)
	}
	want["start"] = 0
	cold, hot := keys("c", 50), keys("h", 50)
	if _, err := putAll(r.n, 0, cold...); err != nil {
		t.Fatal(err)
	}
	for _, k := range cold {
		want[k] = 0
	}
	var maps []int64
	for round := 1; round <= 200; round++ {
		rev, err := putAll(r.n, round, hot...)
		if err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
		for _, k := range hot {
			want[k] = round
		}
		if _, err := do(r.n, &pb.CompactionRequest{Revision: rev}); err != nil {
			t.Fatalf("compact: %v", err)
		}
		if round%20 == 0 {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			m, err := r.n.Metrics(ctx)
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("round %d: index %d B, window %d B, map %d B, disk %d B, cleaned %d B",
				round, m.IndexBytes, m.WindowBytes, m.MapBytes, m.DiskBytes, m.CleanedBytes)
			if m.DiskBytes > budget {
				t.Fatalf("round %d: the log's files take %d bytes, past the budget", round, m.DiskBytes)
			}
			maps = append(maps, m.MapBytes)
		}
	}
	if err := checkValues(r.n, want); err != nil {
		t.Fatal(err)
	}
	// The map's size swings with the number of segments; with nothing
	// removed it would grow with the history, about tenfold here.
	if first, second := slices.Max(maps[:len(maps)/2]), slices.Max(maps[len(maps)/2:]); second > 2*first {
		t.Fatalf("the map grew from at most %d to %d bytes", first, second)
	}
	segs, packs := walFiles(t, dir)
	if packs == 0 {
		t.Fatal("no pack was made")
	}
	if _, err := os.Stat(filepath.Join(dir, "wal", wal.SegmentName(1))); !os.IsNotExist(err) {
		t.Fatalf("the first segment, with the cold values, is still there: %v", err)
	}
	t.Logf("%d segments and %d packs on disk", segs, packs)
	if err := r.stop(); err != nil {
		t.Fatal(err)
	}
	r = runClean(t, dir, budget)
	if err := checkValues(r.n, want); err != nil {
		t.Fatalf("after a restart: %v", err)
	}
}

// TestNoSpace writes new keys on a node with a 512 KiB disk budget until
// it answers NOSPACE to 10 rounds in a row, as etcd does past its quota.
// Every other answer is a success, the node never stops, and its files
// stay near the budget. Reads and deletes still work, and a lease grant
// gets NOSPACE too. With a larger budget, the node takes writes again.
func TestNoSpace(t *testing.T) {
	const budget = 512 << 10
	dir := t.TempDir()
	r := runClean(t, dir, budget)
	if _, err := do(r.n, &pb.PutRequest{Key: []byte("start"), Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	refused := 0
	for round := 0; refused < 10; round++ {
		if round == 200 {
			t.Fatalf("no NOSPACE in %d rounds of new keys", round)
		}
		_, err := putAll(r.n, round, keys(fmt.Sprintf("r%03d-", round), 20)...)
		switch {
		case err == nil:
			refused = 0
		case errors.Is(err, rpctypes.ErrGRPCNoSpace):
			refused++
		default:
			t.Fatalf("round %d: %v", round, err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := r.n.Do(ctx, &pb.LeaseGrantRequest{TTL: 10}); !errors.Is(err, rpctypes.ErrGRPCNoSpace) {
		t.Fatalf("lease grant at NOSPACE: %v", err)
	}
	for _, op := range []proto.Message{
		&pb.RangeRequest{Key: []byte("start")},
		&pb.DeleteRangeRequest{Key: []byte("start")},
	} {
		if _, err := r.n.Do(ctx, op); err != nil {
			t.Fatalf("%T at NOSPACE: %v", op, err)
		}
	}
	m, err := r.n.Metrics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if m.DiskBytes > budget+64<<10 {
		t.Fatalf("the log's files take %d bytes, past the budget of %d", m.DiskBytes, budget)
	}
	if err := r.stop(); err != nil {
		t.Fatalf("the node stopped: %v", err)
	}
	r = runClean(t, dir, 4*budget)
	if _, err := do(r.n, &pb.PutRequest{Key: []byte("again"), Value: []byte("v")}); err != nil {
		t.Fatalf("put with a larger budget: %v", err)
	}
}

// TestSharedValues writes one 32 KiB value, then 100 puts of its key with
// ignore_value, which all name that value, then other keys until cleaning
// copies it. The pack must hold it once.
func TestSharedValues(t *testing.T) {
	const budget = 512 << 10
	dir := t.TempDir()
	r := runClean(t, dir, budget)
	big := []byte(fmt.Sprintf("%032768d", 1))
	// One deadline and no retry, since a retried put that committed would
	// add a version.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := r.n.Do(ctx, &pb.PutRequest{Key: []byte("big"), Value: big}); err != nil {
		t.Fatal(err)
	}
	for range 100 {
		if _, err := r.n.Do(ctx, &pb.PutRequest{Key: []byte("big"), IgnoreValue: true}); err != nil {
			t.Fatal(err)
		}
	}
	for round := 0; ; round++ {
		if round == 100 {
			t.Fatalf("nothing was cleaned in %d rounds", round)
		}
		if _, err := putAll(r.n, round, keys(fmt.Sprintf("r%03d-", round), 10)...); err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
		m, err := r.n.Metrics(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if m.CleanedBytes == 0 {
			continue
		}
		// Each round writes about 10 KiB of other values.
		if want := int64(len(big) + (round+1)*12<<10); m.CleanedBytes > want {
			t.Fatalf("cleaning wrote %d bytes into packs, more than the %d bytes of all live values", m.CleanedBytes, want)
		}
		break
	}
	resp, err := r.n.Do(ctx, &pb.RangeRequest{Key: []byte("big")})
	if err != nil {
		t.Fatal(err)
	}
	if kvs := resp.(*pb.RangeResponse).Kvs; len(kvs) != 1 || string(kvs[0].Value) != string(big) || kvs[0].Version != 101 {
		t.Fatalf("big reads back as %v", kvs)
	}
}

// TestChoose picks the files of one pass.
func TestChoose(t *testing.T) {
	seg := func(n uint64, size int64) wal.CleanFile { return wal.CleanFile{N: n, Size: size} }
	pack := func(n uint64, size int64) wal.CleanFile { return wal.CleanFile{Pack: true, N: n, Size: size} }
	files := []wal.CleanFile{seg(1, 100), seg(2, 100), seg(3, 100), pack(7, 100), pack(8, 100)}
	for _, tc := range []struct {
		name  string
		live  []int64 // per file of files
		short bool
		upto  uint64
		drop  []uint64
	}{
		{"mostly dead segments from the front", []int64{10, 50, 0, 100, 100}, false, 3, nil},
		{"a mostly live segment stops the rest", []int64{10, 60, 0, 100, 100}, false, 1, nil},
		{"short of space, live segments go too", []int64{10, 60, 0, 100, 100}, true, 3, nil},
		{"mostly dead packs", []int64{100, 0, 0, 50, 51}, false, 0, []uint64{7}},
		{"a pass copies at most cleanLive", []int64{cleanLive, 1, 0, 0, 0}, true, 1, []uint64{7, 8}},
		{"the first file fits however large", []int64{2 * cleanLive, 0, 0, 0, 0}, true, 3, []uint64{7, 8}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			live := map[wal.FileID]int64{}
			fs := slices.Clone(files)
			for i, v := range tc.live {
				live[fs[i].FileID] = v
				fs[i].Size = max(fs[i].Size, v)
			}
			rm, victims := choose(fs, live, tc.short)
			if rm.upto != tc.upto || !slices.Equal(rm.drop, tc.drop) {
				t.Fatalf("choose = segments up to %d, packs %v; want %d, %v", rm.upto, rm.drop, tc.upto, tc.drop)
			}
			if want := int(tc.upto) + len(tc.drop); len(victims) != want {
				t.Fatalf("%d victims, want %d", len(victims), want)
			}
		})
	}
}
