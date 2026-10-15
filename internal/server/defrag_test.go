package server

import (
	"testing"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// TestHealthDuringDefrag checks the status StopServiceOnDefrag gives the
// health service: NOT_SERVING while a defragmentation runs, with a
// change signaled at its start and end.
func TestHealthDuringDefrag(t *testing.T) {
	h := newHealthState()
	st, changed := h.status()
	if st != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("at first: %v", st)
	}
	h.defrags(1)
	<-changed
	st, changed = h.status()
	if st != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("during a defragmentation: %v", st)
	}
	h.defrags(1)
	h.defrags(-1)
	if st, _ := h.status(); st != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("while one of two defragmentations runs: %v", st)
	}
	h.defrags(-1)
	<-changed
	if st, _ := h.status(); st != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("after the defragmentations: %v", st)
	}
}
