package main

import "testing"

// TestFeatureGates checks --feature-gates against etcd's parsing: known
// gates, AllAlpha with a gate named on its own winning, and etcd's errors.
func TestFeatureGates(t *testing.T) {
	g := newFeatureGates()
	if g["InitialCorruptCheck"] || !g["FastLeaseKeepAlive"] {
		t.Fatalf("defaults: %v", g)
	}
	if err := g.Set("AllAlpha=true,CompactHashCheck=false, InitialCorruptCheck = true"); err != nil {
		t.Fatal(err)
	}
	if !g["InitialCorruptCheck"] || g["CompactHashCheck"] || !g["LeaseCheckpoint"] || !g["FastLeaseKeepAlive"] {
		t.Fatalf("after AllAlpha: %v", g)
	}
	for value, want := range map[string]string{
		"NoSuchGate=true":          "unrecognized feature gate: NoSuchGate",
		"InitialCorruptCheck":      "missing bool value for InitialCorruptCheck",
		"InitialCorruptCheck=yes!": `invalid value of InitialCorruptCheck=yes!, err: strconv.ParseBool: parsing "yes!": invalid syntax`,
	} {
		if err := newFeatureGates().Set(value); err == nil || err.Error() != want {
			t.Errorf("%q: %v, want %q", value, err, want)
		}
	}
}
