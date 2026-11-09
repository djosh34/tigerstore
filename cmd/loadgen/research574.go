package main

// Research-only response census. It changes no requests and times no cache.
// Maps are unbounded, start cold at each phase, and are private to a worker.
import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"google.golang.org/protobuf/encoding/protowire"
	"hash"
	"os"
)

type censusRead struct {
	h           hash.Hash
	revision    uint64
	versions    []string
	more, count uint64
}
type census574 struct {
	Reads                                                 map[uint32]*censusRead
	SeenGET, SeenValue, SeenLISTRevision, SeenLISTContent map[string]bool
	LastLIST                                              map[int]string
	Counts                                                map[string]int64
}

func newCensus574() *census574 {
	return &census574{Reads: map[uint32]*censusRead{}, SeenGET: map[string]bool{}, SeenValue: map[string]bool{}, SeenLISTRevision: map[string]bool{}, SeenLISTContent: map[string]bool{}, LastLIST: map[int]string{}, Counts: map[string]int64{}}
}
func (w *mixedLoad) censusMessage(st *stream, m []byte) {
	if w.census == nil || st.intended < w.wk.stats.winStart || st.intended >= w.wk.stats.winStop {
		return
	}
	call := w.calls[st.id]
	if call.op != mixedGet && call.op != mixedList {
		return
	}
	if call.op == mixedList {
		var err error
		m, err = bytesField(m, rangeStreamResponse)
		if err != nil {
			panic(err)
		}
	}
	r := w.census.Reads[st.id]
	if r == nil {
		r = &censusRead{h: sha256.New()}
		w.census.Reads[st.id] = r
	}
	err := fields(m, func(n protowire.Number, v uint64, b []byte) error {
		switch n {
		case respHeader:
			r.revision, _ = varintField(b, headerRevision)
		case rangeKVs:
			// Length-prefix the full KV bytes: value, revisions, lease, key.
			r.h.Write(protowire.AppendBytes(nil, b))
			key, _ := bytesField(b, kvKey)
			mod, _ := varintField(b, kvModRevision)
			r.versions = append(r.versions, fmt.Sprintf("%s@%d", key, mod))
		case rangeMore:
			r.more = v
		case rangeCount:
			r.count = v
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
}
func (w *mixedLoad) censusDone(st *stream, err error) {
	c := w.census
	if c == nil || st.intended < w.wk.stats.winStart || st.intended >= w.wk.stats.winStop {
		return
	}
	call := w.calls[st.id]
	names := []string{"get", "rev", "list", "update", "create", "delete"}
	c.Counts[names[call.op]]++
	r := c.Reads[st.id]
	delete(c.Reads, st.id)
	if err != nil {
		c.Counts["errors"]++
		return
	}
	if r == nil {
		return
	}
	if call.op == mixedGet {
		for _, key := range r.versions {
			if c.SeenGET[key] {
				c.Counts["get_repeat_key_mod"]++
			}
			c.SeenGET[key] = true
			if c.SeenValue[key] {
				c.Counts["get_repeat_value_from_get_or_list"]++
			}
		}
	} else {
		group := call.key.group
		rev := fmt.Sprintf("%d@%d", group, r.revision)
		content := fmt.Sprintf("%d@%x:%d:%d", group, r.h.Sum(nil), r.more, r.count)
		if c.SeenLISTRevision[rev] {
			c.Counts["list_repeat_prefix_global_revision"]++
		}
		c.SeenLISTRevision[rev] = true
		if c.SeenLISTContent[content] {
			c.Counts["list_repeat_prefix_page_content"]++
		}
		c.SeenLISTContent[content] = true
		if c.LastLIST[group] == content {
			c.Counts["list_same_as_previous_prefix_page"]++
		}
		c.LastLIST[group] = content
		c.Counts["list_kvs"] += int64(len(r.versions))
		if r.more != 0 {
			c.Counts["list_more"]++
		}
	}
	for _, key := range r.versions {
		if c.SeenValue[key] {
			c.Counts["value_read_repeat_key_mod"]++
		}
		c.SeenValue[key] = true
		c.Counts["value_reads"]++
	}
}
func (s *session) censusReport() {
	if os.Getenv("RESEARCH574") == "" {
		return
	}
	sum := map[string]int64{}
	for _, w := range s.mixed {
		if w.census != nil {
			for k, v := range w.census.Counts {
				sum[k] += v
			}
		}
	}
	b, _ := json.Marshal(sum)
	fmt.Fprintf(os.Stderr, "RESEARCH574 %s\n", b)
}
