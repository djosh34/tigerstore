package sim

import (
	"compress/gzip"
	"os"
	"strconv"
	"testing"
	"unsafe"

	"github.com/djosh34/tigerstore/history"
	"github.com/djosh34/tigerstore/internal/simrun"
)

// TestMemory448 deliberately bypasses the native race-only gate for research.
// Kubernetes native can deadlock (#439); Actions imposes a real timeout.
func TestMemory448(t *testing.T) {
	if !simrun.InGosim && os.Getenv("NATIVE448") != "1" && simrun.Drive(t, 1) {
		return
	}
	var cfg Config
	switch os.Getenv("MEMCASE448") {
	case "default":
		cfg = DefaultConfig(1)
	case "short":
		cfg = kubeConfig(1, false)
	default:
		cfg = kubeConfig(1, true)
	}
	if n, e := strconv.Atoi(os.Getenv("OBJECTS448")); e == nil && n > 0 {
		cfg.Objects = n
		cfg.Faults.DiskBudget = max(16<<20, int64(n)*8<<10)
		cfg.Faults.DiskCapacity = int(2 * cfg.Faults.DiskBudget)
	}
	mark("MEM448.baseline case=%s", os.Getenv("MEMCASE448"))
	res := run(t, cfg)
	mark("MEM448.result")
	if os.Getenv("COMPACT448") == "1" {
		c := &count448{}
		z, _ := gzip.NewWriterLevel(c, gzip.BestSpeed)
		writeHistory(z, res)
		_ = z.Close()
		mark("COMPACT448 gzip-text-history=%d ops=%d", c.n, len(res.History))
	}
	checkers(t, res)
	mark("MEM448.finished record=%d history-op=%d event=%d keyvalue=%d", unsafe.Sizeof(Record{}), unsafe.Sizeof(history.Op{}), unsafe.Sizeof(history.Event{}), unsafe.Sizeof(history.KeyValue{}))
}

type count448 struct{ n int64 }

func (c *count448) Write(p []byte) (int, error) { c.n += int64(len(p)); return len(p), nil }
