//go:build difftest

package difftest

import (
	"context"
	"os"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// diag272Hold lets the asynchronous storage migration run before CANCEL.
// The original test usually cancels before the version monitor wakes up.
// No retry hides an endpoint loss. This hook is diagnostic branch code only.
func diag272Hold(r *run) {
	if os.Getenv("DIAG_272_HOLD") == "" {
		return
	}
	r.t.Helper()
	deadline := time.Now().Add(12 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(r.ctx, time.Second)
		st, err := r.c.maint.Status(ctx, &pb.StatusRequest{})
		cancel()
		if err != nil {
			r.t.Fatalf("#272: endpoint lost while waiting for storage downgrade: %v", err)
		}
		if st.StorageVersion == "3.6.0" {
			r.t.Log("#272: storage downgrade to 3.6.0 completed; endpoint still answers")
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	r.t.Fatal("#272: storage did not downgrade to 3.6.0 within 12s")
}
