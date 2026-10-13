package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/djosh34/tigerstore/history"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type burstGate struct {
	mu              sync.Mutex
	ready, complete int
	failed          int
}

func (b *burstGate) finish(c config, failures int) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.complete++
	b.failed += failures
	if b.complete == c.clients {
		return save(c.burstReady+".complete.json", map[string]any{"clients_complete": b.complete, "failed_calls": b.failed})
	}
	return nil
}

func fileExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}

func (r *recorder) trigger(ctx context.Context, cs []*clientv3.Client, c config, prefix string) error {
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		ready, err := fileExists(c.triggerFile)
		if err != nil {
			return err
		}
		if ready {
			var target struct {
				Endpoint string `json:"endpoint"`
			}
			if err := load(c.triggerFile, &target); err != nil {
				return err
			}
			for i, ep := range csv(c.endpoints) {
				if ep == target.Endpoint {
					identity := fmt.Sprintf("s%d-enospc-trigger:", c.seed)
					value := identity + strings.Repeat("t", 1024*1024-len(identity))
					op := r.call(cs[i], 40000, history.Request{Put: &history.PutRequest{Key: prefix + "k0", Value: value, PrevKV: true}}, c.requestTimeout)
					return save(filepath.Join(c.out, "trigger.json"), op)
				}
			}
			return errors.New("trigger endpoint is not a configured member")
		}
		select {
		case <-ctx.Done():
			return errors.New("trigger Put was never requested")
		case <-tick.C:
		}
	}
}

func (b *burstGate) wait(ctx context.Context, c config) error {
	b.mu.Lock()
	b.ready++
	if b.ready == c.clients {
		if err := save(c.burstReady, map[string]any{"clients_ready": b.ready, "writes_per_client": c.burstWrites}); err != nil {
			b.mu.Unlock()
			return err
		}
	}
	b.mu.Unlock()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		released, err := fileExists(c.burstRelease)
		if err != nil || released {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}
