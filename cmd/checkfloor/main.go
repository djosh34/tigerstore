// Throwaway #448 native benchmark. JSON decoding is outside the timers.
package main

import (
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/djosh34/tigerstore/check"
	"github.com/djosh34/tigerstore/history"
)

func main() {
	repeat := flag.Int("repeat", 3, "repetitions; report median wall time and allocated bytes")
	names := flag.String("checkers", "floor,durable,revisions,watches,witness-log,witness-revisions", "checker list")
	synthetic := flag.Bool("synthetic", false, "also compare acknowledged / ambiguous single-key histories")
	flag.Parse()
	for _, file := range flag.Args() {
		h, err := read(file)
		if err != nil {
			panic(err)
		}
		run(file, h, *repeat, *names)
	}
	if *synthetic {
		for _, n := range []int{128, 256, 512, 1024, 2048} {
			for _, amb := range []bool{false, true} {
				h := generate(n, amb)
				run(fmt.Sprintf("synthetic-%d-amb-%v", n, amb), h, *repeat, *names)
			}
		}
	}
}
func run(file string, h *history.History, reps int, names string) {
	fns := map[string]func(*history.History) error{
		"floor": check.Floor448, "durable": check.Durable, "revisions": check.Revisions, "watches": check.Watches,
		"witness-log": func(h *history.History) error {
			return check.Witnessed(h, check.FromLog, 60*time.Second, check.MaxSteps(2000000))
		},
		"witness-revisions": func(h *history.History) error {
			return check.Witnessed(h, check.FromRevisions, 60*time.Second, check.MaxSteps(2000000))
		},
		"linearizable": func(h *history.History) error { return check.Linearizable(h, 2*time.Second, check.MaxSteps(2000000)) },
	}
	a, e, obs := 0, 0, 0
	keys := map[string]bool{}
	for _, op := range h.Ops {
		if op.Err != nil && op.Err.Ambiguous {
			a++
		}
		if op.Effect != nil {
			e++
		}
		if op.Req.Put != nil {
			keys[op.Req.Put.Key] = true
		}
	}
	for _, w := range h.Watches {
		for _, r := range w.Responses {
			obs += len(r.Events)
		}
	}
	for _, name := range strings.Split(names, ",") {
		var times, alloc []float64
		var err error
		for range reps {
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			start := time.Now()
			err = fns[name](h)
			elapsed := time.Since(start)
			runtime.ReadMemStats(&after)
			times = append(times, float64(elapsed.Nanoseconds())/float64(len(h.Ops)))
			alloc = append(alloc, float64(after.TotalAlloc-before.TotalAlloc)/float64(len(h.Ops)))
			if err != nil {
				break
			}
		}
		slices.Sort(times)
		slices.Sort(alloc)
		errstr := ""
		if err != nil {
			errstr = err.Error()
		}
		_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"file": file, "checker": name, "n": len(h.Ops), "a": a, "effects": e, "log": len(h.Log), "watch_events": obs, "reps": len(times), "ns_op_median": times[len(times)/2], "allocated_B_op_median": alloc[len(alloc)/2], "error": errstr})
	}
	runtime.KeepAlive(h)
}
func read(file string) (*history.History, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var r io.Reader = f
	if strings.HasSuffix(file, ".gz") {
		z, e := gzip.NewReader(f)
		if e != nil {
			return nil, e
		}
		defer z.Close()
		r = z
	}
	h := new(history.History)
	err = json.NewDecoder(r).Decode(h)
	return h, err
}
func generate(n int, amb bool) *history.History {
	h := new(history.History)
	for i := range n {
		q := history.Request{Put: &history.PutRequest{Key: "key", Value: fmt.Sprintf("unique-%d", i)}}
		op := history.Op{Client: i, Call: int64(2*i + 1), Return: int64(2*i + 2), Req: q}
		if amb {
			op.Call = 1
			op.Return = 2
			op.Err = &history.Error{Ambiguous: true, Message: "timeout"}
		} else {
			op.Resp = &history.Response{Revision: int64(i + 2), Put: &history.PutResponse{}}
		}
		h.Ops = append(h.Ops, op)
		h.Log = append(h.Log, q)
	}
	h.Target = int64(n + 1)
	h.Ops = append(h.Ops, history.Op{Client: n, Call: int64(2*n + 3), Return: int64(2*n + 4), Req: history.Request{Range: &history.RangeRequest{Key: "key"}}, Resp: &history.Response{Revision: h.Target, Range: &history.RangeResponse{Count: 1, KVs: []history.KeyValue{{Key: "key", Value: fmt.Sprintf("unique-%d", n-1), CreateRevision: 2, ModRevision: h.Target, Version: int64(n)}}}}})
	return h
}
