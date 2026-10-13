package main

import (
	"context"
	"path/filepath"
	"testing"
)

func TestBurstCompletionMarker(t *testing.T) {
	c := config{clients: 2, burstReady: filepath.Join(t.TempDir(), "ready.json")}
	b := new(burstGate)
	if err := b.finish(c, 1); err != nil {
		t.Fatal(err)
	}
	if ready, err := fileExists(c.burstReady + ".complete.json"); ready || err != nil {
		t.Fatalf("premature completion marker: %v %v", ready, err)
	}
	if err := b.finish(c, 0); err != nil {
		t.Fatal(err)
	}
	var marker map[string]int
	if err := load(c.burstReady+".complete.json", &marker); err != nil {
		t.Fatal(err)
	}
	if marker["clients_complete"] != 2 || marker["failed_calls"] != 1 {
		t.Fatalf("wrong completion marker: %v", marker)
	}
}

func TestUnrequestedTriggerIsFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := new(recorder)
	c := config{triggerFile: filepath.Join(t.TempDir(), "missing")}
	if err := r.trigger(ctx, nil, c, "prefix/"); err == nil {
		t.Fatal("missing trigger marker passed")
	}
}
