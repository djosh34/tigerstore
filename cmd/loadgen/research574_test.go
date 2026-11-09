package main

import (
	"google.golang.org/protobuf/encoding/protowire"
	"testing"
)

func TestResearch574Census(t *testing.T) {
	w := &mixedLoad{wk: &worker{stats: stats{winStart: 1, winStop: 100}}, calls: map[uint32]mixedCall{}, census: newCensus574()}
	kv := protowire.AppendTag(nil, kvKey, protowire.BytesType)
	kv = protowire.AppendBytes(kv, []byte("a"))
	kv = appendVarintField(kv, kvModRevision, 7)
	response := func(revision uint64) []byte {
		hdr := appendVarintField(nil, headerRevision, revision)
		b := protowire.AppendTag(nil, respHeader, protowire.BytesType)
		b = protowire.AppendBytes(b, hdr)
		b = protowire.AppendTag(b, rangeKVs, protowire.BytesType)
		b = protowire.AppendBytes(b, kv)
		return appendVarintField(b, rangeCount, 1)
	}
	for i, rev := range []uint64{8, 9, 9} {
		st := &stream{id: uint32(i + 1), intended: 10}
		w.calls[st.id] = mixedCall{op: mixedList, key: &mixedKey{group: 1}}
		m := protowire.AppendTag(nil, rangeStreamResponse, protowire.BytesType)
		m = protowire.AppendBytes(m, response(rev))
		w.censusMessage(st, m)
		w.censusDone(st, nil)
	}
	st := &stream{id: 4, intended: 10}
	w.calls[4] = mixedCall{op: mixedGet}
	w.censusMessage(st, response(10))
	w.censusDone(st, nil)
	want := map[string]int64{"list": 3, "list_kvs": 3, "list_repeat_prefix_global_revision": 1, "list_repeat_prefix_page_content": 2, "list_same_as_previous_prefix_page": 2, "value_reads": 4, "value_read_repeat_key_mod": 3, "get": 1, "get_repeat_value_from_get_or_list": 1}
	for key, n := range want {
		if got := w.census.Counts[key]; got != n {
			t.Errorf("%s=%d, want %d", key, got, n)
		}
	}
}
