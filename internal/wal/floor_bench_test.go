package wal

import (
	"fmt"
	"github.com/djosh34/tigerstore/internal/floor"
	"testing"
)

var floorRecord []byte

func BenchmarkFloorRecord(b *testing.B) {
	for _, txn := range []bool{false, true} {
		for _, batch := range []int{64, 256, 1536} {
			name := "put"
			if txn {
				name = "txn"
			}
			data := floor.Batch(txn, batch)
			var h [entryHdrSize]byte
			buf := make([]byte, 0, len(data)+recHdrSize+entryHdrSize)
			var crc uint32
			b.Run(fmt.Sprintf("%s/b%d", name, batch), func(b *testing.B) {
				for b.Loop() {
					buf, crc = appendRecord(buf[:0], crc, recEntry, h[:], data)
				}
				floorRecord = buf
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*batch), "ns/item")
			})
		}
	}
}
