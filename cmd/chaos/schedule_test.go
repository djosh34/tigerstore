package main

import (
	"reflect"
	"slices"
	"testing"
)

func TestPlanIsSeededAndCoversEveryFault(t *testing.T) {
	a, b, other := plan(5, allFaults), plan(5, allFaults), plan(6, allFaults)
	if !reflect.DeepEqual(a, b) {
		t.Fatal("one seed made two schedules")
	}
	if reflect.DeepEqual(a, other) {
		t.Fatal("two seeds made one schedule")
	}
	var got []string
	for _, s := range a {
		got = append(got, s.Fault)
		if s.Hold <= 0 || s.Gap <= 0 {
			t.Fatalf("step %+v has no hold or gap", s)
		}
	}
	slices.Sort(got)
	want := slices.Sorted(slices.Values(allFaults))
	if !slices.Equal(got, want) {
		t.Fatalf("faults %v, want each of %v once", got, want)
	}
}
