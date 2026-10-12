package main

import (
	"bytes"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/dict"
	"github.com/klauspost/compress/zstd"
)

func fixture(t *testing.T, n int) []record {
	t.Helper()
	kvs := make([]map[string]string, n)
	for i := range kvs {
		kvs[i] = map[string]string{"key": base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("/registry/pods/private-name-%04d", i))), "value": base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf(`{"kind":"Pod","metadata":{"name":"private-name-%04d"},"spec":{"containers":[{"image":"test","env":"%s"}]}}`, i, strings.Repeat("payload ", 120))))}
	}
	b, err := json.Marshal(map[string]any{"kvs": kvs})
	if err != nil {
		t.Fatal("marshal fixture")
	}
	rs, err := load(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	return rs
}

func testDictionary(t *testing.T) []byte {
	t.Helper()
	rs := fixture(t, 200)
	train := filter(rs, func(r record) bool { return r.train })
	samples, _ := trainingSamples(train)
	b, err := dict.BuildZstdDict(samples, dict.Options{Output: io.Discard, MaxDictSize: 4096, HashBytes: 6, ZstdDictID: 12345, ZstdLevel: zstd.SpeedFastest})
	if err != nil {
		t.Fatal("dictionary training failed")
	}
	return b
}

func TestCodecRoundtrips(t *testing.T) {
	dictionary := testDictionary(t)
	random := make([]byte, 10240)
	_, _ = rand.New(rand.NewSource(1)).Read(random)
	inputs := [][]byte{nil, {}, []byte("a"), bytes.Repeat([]byte("abcdef"), 10240), random}
	for _, spec := range []struct {
		name       string
		level      int
		dictionary []byte
	}{{"zstd", 1, nil}, {"zstd", 3, nil}, {"zstd", 1, dictionary}, {"zstd", 3, dictionary}, {"lz4", 0, nil}, {"snappy", 0, nil}} {
		c, err := newCodec(spec.name, spec.level, spec.dictionary)
		if err != nil {
			t.Fatal("new codec failed")
		}
		for _, in := range inputs {
			encoded, err := c.encode(in)
			if err != nil {
				t.Fatal("encode failed")
			}
			decoded, err := c.decode(encoded)
			if err != nil || !bytes.Equal(in, decoded) {
				t.Fatalf("roundtrip failed codec=%s level=%d input_bytes=%d", spec.name, spec.level, len(in))
			}
		}
		if err := c.close(); err != nil {
			t.Fatal("close codec failed")
		}
	}
}

func TestLoadAndSplit(t *testing.T) {
	a, b := fixture(t, 400), fixture(t, 400)
	if !reflect.DeepEqual(a, b) {
		t.Fatal("nondeterministic split")
	}
	train := filter(a, func(r record) bool { return r.train })
	test := filter(a, func(r record) bool { return !r.train })
	if len(train) < 50 || len(train) > 110 || len(train)+len(test) != 400 {
		t.Fatal("bad hash split")
	}
	for _, r := range test {
		if r.train {
			t.Fatal("test leakage")
		}
	}
	samples, _ := trainingSamples(train)
	for _, s := range samples {
		found := false
		for _, r := range train {
			if bytes.Equal(s, r.Value) {
				found = true
				break
			}
		}
		if !found {
			t.Fatal("sample outside training split")
		}
	}
	for _, bad := range []string{`{"kvs":[{"key":"SENSITIVE_NOT_BASE64"}]}`, `{"kvs":[]}`, `{"kvs":[{"key":"YQ=="},{"key":"YQ=="}]}`, `{"kvs":[{"key":"YQ=="}]} {}`} {
		_, err := load(strings.NewReader(bad))
		if err == nil {
			t.Fatal("invalid input accepted")
		}
		if strings.Contains(err.Error(), "SENSITIVE") {
			t.Fatal("input leaked in error")
		}
	}
}

func TestResourcePrivacy(t *testing.T) {
	for key, want := range map[string]string{"/registry/secrets/private-namespace/private-name": "secrets", "/registry/SENSITIVE/private-name": "other", "/SENSITIVE/key": "other", "/registry/apiregistration.k8s.io/apiservices/private-name": "apiservices", "/registry/services/endpoints/private-name": "endpoints"} {
		if got := resource([]byte(key)); got != want {
			t.Fatal("resource classification mismatch")
		}
	}
}

func TestBinsAndSynthetic(t *testing.T) {
	for n, want := range map[int]string{0: "empty", 895: "1_to_895", 896: "near_1KiB_896_to_1152", 1024: "near_1KiB_896_to_1152", 1152: "near_1KiB_896_to_1152", 1153: "1153_to_9215", 9216: "near_10KiB_9216_to_11264", 10240: "near_10KiB_9216_to_11264", 11264: "near_10KiB_9216_to_11264", 11265: "11265_plus"} {
		if sizeBin(n) != want {
			t.Fatal("bin boundary mismatch")
		}
	}
	for _, n := range []int{1024, 10240} {
		for _, repeat := range []bool{false, true} {
			for _, r := range synthetic(n, repeat) {
				if len(r.Value) != n {
					t.Fatal("synthetic length mismatch")
				}
			}
		}
	}
	if format([]byte{'k', '8', 's', 0, 1}) != "kubernetes_protobuf" || format([]byte(`{"a":1}`)) != "json" || format([]byte{1, 2}) != "raw_other" {
		t.Fatal("format classification mismatch")
	}
}

func TestPacking(t *testing.T) {
	rs := fixture(t, 70)
	for _, batch := range []int{16, 64, 256} {
		for _, order := range []string{"shuffled", "resource_sorted"} {
			a, err := pack(rs, batch, order)
			if err != nil {
				t.Fatal(err)
			}
			b, err := pack(rs, batch, order)
			if err != nil || !reflect.DeepEqual(a, b) {
				t.Fatal("nondeterministic packing")
			}
			count := 0
			for _, in := range a {
				if err := validateFrames(in.data, in.values); err != nil {
					t.Fatal(err)
				}
				count += in.values
			}
			if count != len(rs) {
				t.Fatal("packing lost values")
			}
		}
	}
	for _, bad := range [][]byte{{1}, {5, 0, 0, 0}, {0, 0, 0, 0, 1}} {
		if validateFrames(bad, 1) == nil {
			t.Fatal("invalid frames accepted")
		}
	}
}

func TestTimedWholePassesAndCSV(t *testing.T) {
	rs := fixture(t, 70)
	inputs, err := pack(rs, 16, "shuffled")
	if err != nil {
		t.Fatal(err)
	}
	c, err := newCodec("snappy", 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := c.close(); err != nil {
			t.Fatal("close codec failed")
		}
	}()
	var compressed [][]byte
	values := int64(0)
	for _, in := range inputs {
		encoded, err := c.encode(in.data)
		if err != nil {
			t.Fatal(err)
		}
		compressed = append(compressed, bytes.Clone(encoded))
		values += int64(in.values)
	}
	for _, encode := range []bool{true, false} {
		m, err := timed(c, inputs, compressed, encode, 40*time.Millisecond, time.Now().Add(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		if m.operations < int64(len(inputs)) || m.operations%int64(len(inputs)) != 0 || m.values != (m.operations/int64(len(inputs)))*values {
			t.Fatal("timed partial or uneven corpus pass")
		}
		if m.wall < int64(40*time.Millisecond) || m.cpu <= 0 {
			t.Fatal("bad timings")
		}
	}
	var out bytes.Buffer
	w := csv.NewWriter(&out)
	if err := w.Write(benchHeader); err != nil {
		t.Fatal(err)
	}
	if err := benchmark(w, "test", inputs, c, 16, "shuffled", 40*time.Millisecond, time.Now().Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	rows, err := csv.NewReader(&out).ReadAll()
	if err != nil || len(rows) != 6 {
		t.Fatal("bad benchmark CSV")
	}
	for _, row := range rows {
		if len(row) != len(benchHeader) {
			t.Fatal("CSV schema mismatch")
		}
	}
}

func TestInventoryIsAggregate(t *testing.T) {
	rs := fixture(t, 30)
	rs = append(rs, record{Key: []byte("/registry/SENSITIVE_KEY/SENSITIVE_NAME"), Value: []byte("SENSITIVE_VALUE"), resource: "other", format: "raw_other", bin: "1_to_895"})
	var out bytes.Buffer
	w := csv.NewWriter(&out)
	if err := inventory(w, rs); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "SENSITIVE") || strings.Contains(out.String(), "private-name") || strings.Contains(out.String(), "payload") {
		t.Fatal("inventory leaked source data")
	}
	rows, err := csv.NewReader(&out).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, r := range rows {
		if r[0] == "all" && r[1] == "*" && r[2] == "*" && r[3] == "*" {
			if r[4] != "31" {
				t.Fatal("inventory count mismatch")
			}
			found = true
		}
	}
	if !found {
		t.Fatal("missing aggregate inventory")
	}
}
