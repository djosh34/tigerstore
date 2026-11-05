package floor

import (
	"fmt"
	"golang.org/x/sys/unix"
	"os"
	"testing"
)

// A tmpfs overwrite plus fdatasync measures CPU/kernel-copy cost, NOT durable
// media throughput or an ext4 fsync. No creation/allocation in timed section.
func BenchmarkFloorLogWrite(b *testing.B) {
	f, err := os.CreateTemp("/dev/shm", "floor-log-")
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(f.Name())
	defer f.Close()
	for _, batch := range []int{64, 256, 1536} {
		data := Batch(false, batch)
		if _, err := f.WriteAt(data, 0); err != nil {
			b.Fatal(err)
		}
		if err := f.Sync(); err != nil {
			b.Fatal(err)
		}
		b.Run(fmt.Sprintf("b%d", batch), func(b *testing.B) {
			for b.Loop() {
				n, err := unix.Pwrite(int(f.Fd()), data, 0)
				if err != nil || n != len(data) {
					b.Fatalf("write %d %v", n, err)
				}
				if err := unix.Fdatasync(int(f.Fd())); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*batch), "ns/item")
		})
	}
}
