package wl

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/sys/unix"
)

// A range at a past revision, linearizable or not, and a watch that
// catches up with the values before its events, read the history's
// values that the page cache does not hold off the loop (#704), and get
// them whole. The log is written with O_DIRECT, so its values reach the
// page cache only when read; the test drops the pages anyway. Each read
// has keys of its own: the loop keeps what it fetched for a while. A
// range counts the keys it does not return without reading their values:
// a count-only range reads none, and one with a limit only those it
// returns, and the one past it etcd reads.
func TestValuesOutOfThePageCacheAreReadOffTheLoop(t *testing.T) {
	loopOf := watchLoops(t)
	tc := newTestCluster(t)
	c := tc.clientOf(t, tc.endpoints[0])
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rng := rand.New(rand.NewPCG(1, 2))
	value := func() []byte {
		b := make([]byte, 3000)
		for i := range b {
			b[i] = byte(rng.IntN(256))
		}
		return b
	}
	const keys = 20
	old := map[string][]byte{}
	var first, last int64
	for _, g := range "abcd" {
		for i := range keys {
			k := fmt.Sprintf("%c%02d", g, i)
			old[k] = value()
			r, err := c.Put(ctx, k, string(old[k]))
			if err != nil {
				t.Fatal(err)
			}
			if g == 'c' && first == 0 {
				first = r.Header.Revision
			}
			last = r.Header.Revision
		}
	}
	// Every key is put again, and more is written after, so that the old
	// values lie in the log's durable part, which a read takes from disk.
	for k := range old {
		if _, err := c.Put(ctx, k, string(value())); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 200 {
		if _, err := c.Put(ctx, fmt.Sprintf("other%03d", i), string(value())); err != nil {
			t.Fatal(err)
		}
	}
	check := func(what string, k, v []byte) {
		t.Helper()
		if !bytes.Equal(v, old[string(k)]) {
			t.Fatalf("%s: %s has %d bytes, not the %d it was put with", what, k, len(v), len(old[string(k)]))
		}
	}
	for prefix, serializable := range map[string]bool{"a": true, "b": false} {
		dropPageCache(t, tc.dir)
		before := fetchesOf(loopOf(tc.ids[0]))
		opts := []clientv3.OpOption{clientv3.WithPrefix(), clientv3.WithRev(last)}
		if serializable {
			opts = append(opts, clientv3.WithSerializable())
		}
		r, err := c.Get(ctx, prefix, opts...)
		if err != nil {
			t.Fatal(err)
		}
		if len(r.Kvs) != keys {
			t.Fatalf("a range at revision %d got %d keys, want %d", last, len(r.Kvs), keys)
		}
		for _, kv := range r.Kvs {
			check(fmt.Sprintf("a range at revision %d (serializable %v)", last, serializable), kv.Key, kv.Value)
		}
		if fetchesOf(loopOf(tc.ids[0])) == before {
			t.Fatalf("a range at revision %d (serializable %v) read no value off the loop", last, serializable)
		}
	}
	dropPageCache(t, tc.dir)
	before := fetchesOf(loopOf(tc.ids[0]))
	wctx, wcancel := context.WithCancel(ctx)
	defer wcancel()
	got := 0
	for resp := range c.Watch(wctx, "c", clientv3.WithPrefix(), clientv3.WithRev(first), clientv3.WithPrevKV()) {
		if err := resp.Err(); err != nil {
			t.Fatal(err)
		}
		for _, ev := range resp.Events {
			if got < keys {
				check("a watch's event", ev.Kv.Key, ev.Kv.Value)
			} else {
				check("the value before a watch's event", ev.PrevKv.Key, ev.PrevKv.Value)
			}
			got++
		}
		if got == 2*keys {
			break
		}
	}
	if got != 2*keys {
		t.Fatalf("the watch got %d events, want %d", got, 2*keys)
	}
	if fetchesOf(loopOf(tc.ids[0])) == before {
		t.Fatal("the watch read no value off the loop")
	}

	for _, limit := range []int64{0, 1} {
		dropPageCache(t, tc.dir)
		before := fetchedOf(loopOf(tc.ids[0]))
		opts := []clientv3.OpOption{clientv3.WithPrefix(), clientv3.WithRev(last)}
		want, read := 0, 0 // the records returned, and the values read
		if limit == 0 {
			opts = append(opts, clientv3.WithCountOnly())
		} else {
			opts = append(opts, clientv3.WithLimit(limit))
			want, read = int(limit), int(limit)+1
		}
		r, err := c.Get(ctx, "d", opts...)
		if err != nil {
			t.Fatal(err)
		}
		if r.Count != keys || len(r.Kvs) != want {
			t.Fatalf("a range with limit %d (0: count only) at revision %d: count %d and %d records, want %d and %d", limit, last, r.Count, len(r.Kvs), keys, want)
		}
		for _, kv := range r.Kvs {
			check("a range with a limit", kv.Key, kv.Value)
		}
		if got := fetchedOf(loopOf(tc.ids[0])) - before; got > read {
			t.Fatalf("a range with limit %d (0: count only) read %d values off the loop, want at most %d", limit, got, read)
		}
	}
}

// fetchedOf asks l's loop how many values it read off the loop.
func fetchedOf(l *loop) int {
	got := make(chan int, 1)
	l.post(func() error {
		got <- l.n.stats.fetched
		return nil
	})
	return <-got
}

// dropPageCache has the kernel drop the cached pages of every log segment
// and checkpoint under dir.
func dropPageCache(t *testing.T, dir string) {
	t.Helper()
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".seg") && !strings.HasSuffix(path, ".ckpt") {
			return err
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		return unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED)
	})
	if err != nil {
		t.Fatal(err)
	}
}

// fetchesOf asks l's loop how many times it read values off the loop.
func fetchesOf(l *loop) int {
	got := make(chan int, 1)
	l.post(func() error {
		got <- l.n.stats.fetches
		return nil
	})
	return <-got
}
