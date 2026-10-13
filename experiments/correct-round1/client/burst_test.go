package main

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

func TestCoverageExpectedClients(t *testing.T) {
	h := offlineFixture()
	if _, err := coverage(h, 256); err == nil {
		t.Fatal("three clients satisfied a 256-client coverage floor")
	}
	for i := 3; i < 256; i++ {
		h.Ops = append(h.Ops, history.Op{Client: i, Call: int64(i * 10), Return: int64(i*10 + 1), Req: history.Request{Range: &history.RangeRequest{Key: "a"}}, Resp: &history.Response{Revision: 5, Range: &history.RangeResponse{}}})
	}
	if _, err := coverage(h, 256); err != nil {
		t.Fatal(err)
	}
}

func TestBurstBarrier(t *testing.T) {
	dir := t.TempDir()
	c := config{clients: 256, burstWrites: 8, burstReady: filepath.Join(dir, "ready.json"), burstRelease: filepath.Join(dir, "release")}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	b := new(burstGate)
	var wg sync.WaitGroup
	errs := make(chan error, 256)
	for range 256 {
		wg.Go(func() { errs <- b.wait(ctx, c) })
	}
	for {
		ready, err := fileExists(c.burstReady)
		if err != nil {
			t.Fatal(err)
		}
		if ready {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	if len(errs) != 0 {
		t.Fatal("a worker passed the barrier before release")
	}
	if err := os.WriteFile(c.burstRelease, []byte("release"), 0600); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if b.ready != 256 {
		t.Fatalf("%d workers ready", b.ready)
	}
}
