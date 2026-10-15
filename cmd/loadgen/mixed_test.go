package main

import (
	"bytes"
	"math"
	"math/rand/v2"
	"testing"
)

func TestK8sSizes(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	var count int
	var total float64
	for _, b := range k8sSizes {
		if b.count <= 0 || b.min < 8 || b.max < b.min || b.mean < float64(b.min) || b.mean > float64(b.max) {
			t.Fatalf("bad bin: %+v", b)
		}
		const samples = 100000
		sum := 0
		for range samples {
			n := b.draw(r)
			if n < b.min || n > b.max {
				t.Fatalf("%+v drew %d", b, n)
			}
			sum += n
		}
		got := float64(sum) / samples
		// Six standard errors using the maximum variance of any bounded draw.
		variance := (b.mean - float64(b.min)) * (float64(b.max) - b.mean)
		tolerance := 6*math.Sqrt(variance/samples) + 0.01
		if math.Abs(got-b.mean) > tolerance {
			t.Fatalf("%s %s mean %.3f want %.3f", b.resource, b.bin, got, b.mean)
		}
		count += b.count
		total += b.mean * float64(b.count)
	}
	if count != 6261 || math.Abs(total-9217907) > 0.001 {
		t.Fatalf("dump totals %d %.3f", count, total)
	}
	draws := make([]int, len(k8sSizes))
	for range 200000 {
		draws[drawBin(r, "")]++
	}
	for i, b := range k8sSizes {
		probability := float64(b.count) / 6261
		want := 200000 * probability
		tolerance := 6*math.Sqrt(want*(1-probability)) + 1
		if math.Abs(float64(draws[i])-want) > tolerance {
			t.Fatalf("%s bin share %d want %.0f", b.resource, draws[i], want)
		}
		for range 100 {
			if k8sSizes[drawBin(r, b.resource)].resource != b.resource {
				t.Fatal("resource draw crossed resources")
			}
		}
	}
}

func TestK8sSizesHaveInteriorDraws(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 4))
	seen := map[int]bool{}
	b := k8sSizes[21] // Events: 490..890 bytes, not only those endpoints.
	for range 1000 {
		seen[b.draw(r)] = true
	}
	if len(seen) < 50 {
		t.Fatalf("only %d distinct sizes", len(seen))
	}
}

func TestMixedScopes(t *testing.T) {
	for _, r := range k8sResources() {
		key := mixedName(r, 2, 3, 4)
		parts := bytes.Split(key, []byte("/"))
		want := 5
		if clusterScoped(r) {
			want = 4
		}
		if len(parts) != want || !bytes.HasPrefix(key, mixedPrefix(r, 2, 3)) {
			t.Fatalf("wrong scope for %s: %s", r, key)
		}
	}
	for _, r := range []string{"apiservices", "clusterroles", "minions", "namespaces", "storageclasses", "csinodes"} {
		if !clusterScoped(r) {
			t.Fatalf("%s should be cluster scoped", r)
		}
	}
	if clusterScoped("pods") {
		t.Fatal("pods should be namespaced")
	}
}

func TestMixedGroupKeysSorted(t *testing.T) {
	cfg := config{keys: 20000, conns: 6, mix: defaultMix, seed: 1}
	wk := newWorker(nil, 1)
	w, err := newMixedLoad(wk, cfg, 0, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for range 5000 {
		w.newKey()
	}
	for _, g := range w.groups {
		for i := 1; i < len(g.keys); i++ {
			if bytes.Compare(g.keys[i-1].key, g.keys[i].key) >= 0 {
				t.Fatalf("group %s: %s before %s", g.prefix, g.keys[i-1].key, g.keys[i].key)
			}
		}
	}
}

func TestMixedOwnership(t *testing.T) {
	seen := map[string]int{}
	cfg := config{keys: 2000, conns: 6, mix: defaultMix, seed: 1}
	for owner := range cfg.conns {
		wk := newWorker(nil, uint64(owner+1))
		w, err := newMixedLoad(wk, cfg, owner, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		// Include newly created keys, not only the preload.
		for range 100 {
			w.live = append(w.live, w.newKey())
		}
		for _, k := range w.live {
			key := string(k.key)
			if previous, ok := seen[key]; ok {
				t.Fatalf("key %s owned by %d and %d", key, previous, owner)
			}
			seen[key] = owner
			if !bytes.HasPrefix(k.key, w.groups[k.group].prefix) {
				t.Fatal("wrong group")
			}
		}
		for _, g := range w.groups {
			for key, other := range seen {
				if other != owner && bytes.HasPrefix([]byte(key), g.prefix) {
					t.Fatal("LIST prefix shared across workers")
				}
			}
		}
	}
}

func TestMixedValidation(t *testing.T) {
	k := &mixedKey{key: []byte("/registry/pods/ns/name"), value: []byte("random value"), create: 3, mod: 8, version: 2}
	response := func(key *mixedKey, count, more uint64) []byte {
		m := newMsg()
		m.sub(rangeKVs, mixedKV(key))
		m.varint(rangeCount, count)
		m.varint(rangeMore, more)
		return m.b
	}
	if err := checkMixedRange(response(k, 1, 0), []*mixedKey{k}, 500); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"key", "value", "create", "mod", "version"} {
		bad := *k
		switch field {
		case "key":
			bad.key = []byte("wrong")
		case "value":
			bad.value = []byte("wrong")
		case "create":
			bad.create++
		case "mod":
			bad.mod++
		case "version":
			bad.version++
		}
		if err := checkMixedRange(response(&bad, 1, 0), []*mixedKey{k}, 500); err == nil {
			t.Fatalf("accepted wrong %s", field)
		}
	}
	if err := checkMixedRange(response(k, 2, 0), []*mixedKey{k}, 500); err == nil {
		t.Fatal("accepted wrong count")
	}
	if err := checkMixedRange(response(k, 1, 1), []*mixedKey{k}, 500); err == nil {
		t.Fatal("accepted wrong more")
	}
	if err := checkMixedRange(nil, []*mixedKey{k}, 500); err == nil {
		t.Fatal("accepted missing key")
	}
}

func TestMixedWatchPairing(t *testing.T) {
	for _, watchFirst := range []bool{false, true} {
		l := &mixedLedger{expected: make(map[int64]mixedEvent), observed: make(map[int64]mixedEvent), start: 1, stop: 100}
		k := &mixedKey{key: []byte("/registry/pods/ns/name"), value: []byte("old"), create: 2, mod: 2, version: 1}
		prev := *k
		k.value = []byte("new")
		k.mod = 3
		k.version++
		observed := mixedEvent{digest: eventDigest(k, &prev, false), at: 30}
		if watchFirst {
			l.pair(3, observed, false)
		}
		l.expect(3, k, &prev, false, 10)
		if !watchFirst {
			l.pair(3, observed, false)
		}
		if len(l.errs) != 0 || len(l.expected)+len(l.observed) != 0 || l.lat.n != 1 || l.lat.min != 20 {
			t.Fatalf("pair failed: %+v", l)
		}
		l.expect(4, k, &prev, true, 20)
		observed.digest[0]++
		l.pair(4, observed, false)
		if len(l.errs) != 1 {
			t.Fatal("accepted wrong watch event")
		}
	}
}

func TestMixedMix(t *testing.T) {
	for _, s := range []string{"", "25,10,40,15", "25,10,40,15,11", "25,10,40,26,-1", "25,10,x,15,10"} {
		if _, err := parseMix(s); err == nil {
			t.Fatalf("accepted %q", s)
		}
	}
	if _, err := parseMix(defaultMix); err != nil {
		t.Fatal(err)
	}
}

func TestMixedArrivalScheduleReset(t *testing.T) {
	for _, preloadArrivals := range []int{10, 1000} {
		wk := newWorker(nil, 17)
		for range preloadArrivals {
			_ = wk.gap(300)
		}
		wk.arrivals = newArrivalRNG(17)
		fresh := newWorker(nil, 17)
		for range 100 {
			if wk.gap(500) != fresh.gap(500) {
				t.Fatal("preload completion changed the measurement schedule")
			}
		}
	}
}

func TestMixedRateFlags(t *testing.T) {
	for _, flag := range []string{"-rate", "-preload-rate"} {
		for _, v := range []string{"NaN", "Inf", "-1"} {
			if _, err := parseFlags([]string{"-workload", "mixed", flag, v}); err == nil {
				t.Fatalf("accepted %s %s", flag, v)
			}
		}
	}
	cfg, err := parseFlags([]string{"-workload", "mixed"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.mix != defaultMix || cfg.preloadRate != 300 {
		t.Fatal("wrong defaults")
	}
}

func TestVariableValuePoolWrap(t *testing.T) {
	v := values{pool: make([]byte, 100), size: 8, off: 80}
	if len(v.next()) != 8 {
		t.Fatal("short value")
	}
	v.size = 30
	if len(v.next()) != 30 || v.off != 30 {
		t.Fatal("variable size did not wrap")
	}
}
