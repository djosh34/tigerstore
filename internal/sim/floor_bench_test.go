//go:build !sim

package sim

import "testing"

// BenchmarkFloor runs the floor natively, one run per iteration.
func BenchmarkFloor(b *testing.B) {
	var total FloorResult
	for i := 0; b.Loop(); i++ {
		r, err := Floor(uint64(i+1), floorLoad())
		if err != nil {
			b.Fatal(err)
		}
		total.add(r)
	}
	b.ReportMetric(total.CallsPerCoreSecond(), "calls/core-s")
	b.ReportMetric(float64(total.Answered)/float64(b.N), "calls/op")
	b.ReportMetric(float64(total.Failed)/float64(b.N), "failed/op")
}
