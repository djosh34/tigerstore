//go:build !(sim || simio)

package wl

import (
	"bytes"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
)

// turnWatch samples, measure only (#704), what the loop's thread waits
// in while a turn runs long: its state, its kernel wait channel and its
// system call, from /proc, every 2 ms once a turn has run 30 ms.
type turnWatch struct {
	tid   int
	start atomic.Int64
	mu    sync.Mutex
	turn  int64
	seen  map[string]int
}

func newTurnWatch() *turnWatch {
	w := &turnWatch{tid: unix.Gettid(), seen: map[string]int{}}
	go w.run()
	return w
}

func (w *turnWatch) begin(t time.Time) { w.start.Store(t.UnixNano()) }
func (w *turnWatch) end()              { w.start.Store(0) }

func (w *turnWatch) run() {
	base := fmt.Sprintf("/proc/self/task/%d/", w.tid)
	for {
		s := w.start.Load()
		if s == 0 || time.Now().UnixNano()-s < int64(30*time.Millisecond) {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		key := w.sample(base)
		w.mu.Lock()
		if w.turn != s {
			w.turn = s
			clear(w.seen)
		}
		w.seen[key]++
		w.mu.Unlock()
		time.Sleep(2 * time.Millisecond)
	}
}

func (w *turnWatch) sample(base string) string {
	state := "?"
	if b, err := os.ReadFile(base + "stat"); err == nil {
		if i := bytes.LastIndexByte(b, ')'); i >= 0 && i+2 < len(b) {
			state = string(b[i+2])
		}
	}
	wchan := "?"
	if b, err := os.ReadFile(base + "wchan"); err == nil {
		wchan = string(b)
	}
	sc := "?"
	if b, err := os.ReadFile(base + "syscall"); err == nil {
		f := strings.Fields(string(b))
		if len(f) > 0 {
			sc = f[0]
		}
	}
	return state + "/" + wchan + "/" + sc
}

// report is what the samples of the turn that began at t saw.
func (w *turnWatch) report(t time.Time) string {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.turn != t.UnixNano() {
		return "no samples"
	}
	keys := make([]string, 0, len(w.seen))
	for k := range w.seen {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return w.seen[keys[i]] > w.seen[keys[j]] })
	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "%s x%d; ", k, w.seen[k])
	}
	return b.String()
}
