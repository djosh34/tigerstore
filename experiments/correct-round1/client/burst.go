package main

import (
	"context"
	"errors"
	"os"
	"sync"
	"time"
)

type burstGate struct {
	mu              sync.Mutex
	ready, complete int
}

func fileExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
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
