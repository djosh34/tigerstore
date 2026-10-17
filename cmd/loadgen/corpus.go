package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
)

// loadCorpus reads actual stored Kubernetes values. It keeps their mixed
// lengths, protobuf envelopes and JSON unchanged. A paired random run
// uses the same lengths, with an independent byte stream per connection.
func loadCorpus(path string) ([][]byte, error) {
	b, err := os.ReadFile(path)
	if err != nil { return nil, err }
	var dump struct { KVs []struct { Key, Value []byte } }
	if err := json.Unmarshal(b, &dump); err != nil { return nil, err }
	var values [][]byte
	var total, excluded int
	for _, kv := range dump.KVs {
		if !bytes.HasPrefix(kv.Key, []byte("/registry/")) || len(kv.Value) < 8 || len(kv.Value) > 50<<10 {
			excluded++
			continue
		}
		values = append(values, kv.Value)
		total += len(kv.Value)
	}
	if len(values) == 0 { return nil, errors.New("empty Kubernetes corpus") }
	if _, err := fmt.Fprintf(os.Stderr, "Kubernetes corpus: %d values, %d bytes, mean %.2f B, %d excluded outside /registry/ or 8..51200 B\n", len(values), total, float64(total)/float64(len(values)), excluded); err != nil { return nil, err }
	return values, nil
}

type corpusPutLoad struct {
	putLoad
	templates []*template
	nextTemplate int
}

func newCorpusPut(b base, values [][]byte, random bool, seed uint64) (*corpusPutLoad, error) {
	w := &corpusPutLoad{putLoad: putLoad{base: b}, nextTemplate: -1}
	rng := rand.NewChaCha8([32]byte{byte(seed), byte(seed >> 8), byte(seed >> 16), byte(seed >> 24)})
	for _, v := range values {
		if random {
			v = make([]byte, len(v))
			if _, err := rng.Read(v); err != nil { return nil, err }
		}
		m := newMsg()
		m.key(putKey, b.ks.size)
		keyAt := m.keys[0]
		m.keys = nil // Only the small key in the first DATA frame is patched.
		m.bytes(putValue, v)
		t, err := newTemplate("/etcdserverpb.KV/Put", m, true)
		if err != nil { return nil, err }
		if keyAt+b.ks.size+grpcPrefixLen > maxDataFrame { return nil, errors.New("corpus key crosses a frame") }
		t.keys = []int{frameHeaderLen+grpcPrefixLen+keyAt}
		w.templates = append(w.templates, t)
	}
	return w, nil
}

func (w *corpusPutLoad) start(c *conn, st *stream) bool {
	if w.nextTemplate < 0 { w.nextTemplate = w.rng.IntN(len(w.templates)) }
	t := w.templates[w.nextTemplate]
	if !c.canStart(t, w.limit) { return false }
	k := w.own[w.rng.IntN(len(w.own))]
	st.slot = k
	r := c.startStream(st, t)
	copy(r[t.keys[0]:], w.ks.key(k))
	w.nextTemplate = -1
	return true
}
