//go:build sim

package behavior_test

import (
	"context"
	"testing"
	"time"
)

func TestContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		cancel()
	}()

	<-ctx.Done()
}

func TestContextCancelInner(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	inner, innerCancel := context.WithCancel(ctx)
	defer innerCancel()

	cancelDone := make(chan struct{})

	go func() {
		cancel()
		close(cancelDone)
	}()

	<-inner.Done()
	<-cancelDone
}

func TestContextTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	before := time.Now()
	<-ctx.Done()
	after := time.Now()

	if delay := after.Sub(before); delay != 5*time.Second {
		t.Error("bad delay", delay)
	}
}

func TestContextDeadline(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
	defer cancel()

	before := time.Now()
	<-ctx.Done()
	after := time.Now()

	if delay := after.Sub(before); delay != 10*time.Second {
		t.Error("bad delay", delay)
	}
}

func TestContextTimeoutInnerStricter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	inner, innerCancel := context.WithTimeout(ctx, 2*time.Second)
	defer innerCancel()

	before := time.Now()
	<-inner.Done()
	after := time.Now()

	if delay := after.Sub(before); delay != 2*time.Second {
		t.Error("bad delay", delay)
	}
}

func TestContextTimeoutInnerLooser(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	inner, innerCancel := context.WithTimeout(ctx, 10*time.Second)
	defer innerCancel()

	before := time.Now()
	<-inner.Done()
	after := time.Now()

	if delay := after.Sub(before); delay != 5*time.Second {
		t.Error("bad delay", delay)
	}
}
