package gosimruntime

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"pgregory.net/rapid"
)

func TestTimerHeap(t *testing.T) {
	baseTime := time.Date(2010, 5, 1, 10, 3, 1, 100, time.UTC).UnixNano()

	heap := newTimerHeap()
	first := &Timer{when: baseTime + int64(0*time.Second), pos: -1}
	heap.add(first)
	if first.pos != 0 {
		t.Errorf("expected pos 0, got %d", first.pos)
	}
	a := &Timer{when: baseTime + int64(1*time.Second), pos: -1}
	heap.add(a)
	heap.add(&Timer{when: baseTime + int64(2*time.Second), pos: -1})
	b := &Timer{when: baseTime + int64(3*time.Second), pos: -1}
	heap.add(b)
	if l := heap.len(); l != 4 {
		t.Errorf("expected heap.len() = 4, got %d", l)
	}

	if e := heap.pop(); e.when != baseTime+int64(0*time.Second) {
		t.Errorf("expected t+0s, got %v", e.when-baseTime)
	}
	if e := heap.peek(); e.when != baseTime+int64(1*time.Second) {
		t.Errorf("expected t+1s, got %v", e.when-baseTime)
	}

	heap.adjust(a, baseTime+int64(4*time.Second))

	if e := heap.pop(); e.when != baseTime+int64(2*time.Second) {
		t.Errorf("expected t+2s, got %v", e.when-baseTime)
	}

	heap.remove(b)
	heap.add(&Timer{when: baseTime + int64(5*time.Second), pos: -1})

	if e := heap.pop(); e.when != baseTime+int64(4*time.Second) {
		t.Errorf("expected t+4s, got %v", e.when-baseTime)
	}

	if e := heap.pop(); e.when != baseTime+int64(5*time.Second) {
		t.Errorf("expected t+5s, got %v", e.when-baseTime)
	}
}

func TestCheckTimerHeap(t *testing.T) {
	rapid.Check(t, checkTimerHeap)
}

func checkTimerHeap(t *rapid.T) {
	heap := newTimerHeap()
	var model []*Timer

	var machines []*Machine
	for i := 0; i < 5; i++ {
		machines = append(machines, &Machine{label: fmt.Sprint(i)})
	}

	actions := make(map[string]func(t *rapid.T))

	actions["add"] = func(t *rapid.T) {
		when := rapid.Int64().Draw(t, "when")
		machine := rapid.SampledFrom(machines).Draw(t, "machine")
		timer := &Timer{when: when, Machine: machine, pos: -1}
		model = append(model, timer)
		heap.add(timer)
	}

	actions["peek"] = func(t *rapid.T) {
		if heap.len() == 0 {
			t.Skip()
		}
		got := heap.peek()
		for _, other := range model {
			if other.when < got.when {
				t.Errorf("found earlier when %d than returned %d", other.when, got.when)
			}
		}
	}

	actions["pop"] = func(t *rapid.T) {
		if heap.len() == 0 {
			t.Skip()
		}
		got := heap.pop()
		for _, other := range model {
			if other.when < got.when {
				t.Errorf("found earlier when %d than returned %d", other.when, got.when)
			}
		}
		if got.pos != -1 {
			t.Error("expected pos -1 after pop")
		}
		model = slices.DeleteFunc(model, func(t *Timer) bool { return t == got })
	}

	actions["adjust"] = func(t *rapid.T) {
		if heap.len() == 0 {
			t.Skip()
		}
		timer := rapid.SampledFrom(model).Draw(t, "timer")
		when := rapid.Int64().Draw(t, "when")
		heap.adjust(timer, when)
	}

	actions["delete-machine"] = func(t *rapid.T) {
		machine := rapid.SampledFrom(machines).Draw(t, "machine")
		heap.removemachine(machine)
		model = slices.DeleteFunc(model, func(t *Timer) bool { return t.Machine == machine })
	}

	actions[""] = func(t *rapid.T) {
		if expected, actual := len(model), heap.len(); expected != actual {
			t.Errorf("length mismatch: expected %d, got %d", expected, actual)
		}
		for _, timer := range model {
			if timer.pos < 0 || timer.pos >= len(heap.timers) || heap.timers[timer.pos] != timer {
				t.Errorf("wrong pos for timer") // XXX: helpful information somehow?
			}
		}
	}

	t.Repeat(actions)
}
