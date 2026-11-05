package sim

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/stats"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// TestRetryPauseBlocks checks, under gosim, that the clientv3 clients'
// stats handler makes a transparent retry wait and a first attempt not.
// The spin of #440 needed a call to meet a transport in the moment it was
// closing; 64 short seeds with clientv3 clients and resets every 300 ms
// did not meet it without the pause. Seed 202610250141001 of the long
// profile on ad989ad9 did, and runs 42099847472 and 42099856592 show it
// ending only with the pause (#440).
func TestRetryPauseBlocks(t *testing.T) {
	if simrun.Race {
		t.Skip("the pause is for gosim")
	}
	if simrun.Drive(t, 1) {
		return
	}
	for _, c := range []struct {
		retry bool
		want  time.Duration
	}{{false, 0}, {true, time.Millisecond}} {
		start := time.Now()
		retryPause{}.HandleRPC(context.Background(), &stats.Begin{IsTransparentRetryAttempt: c.retry})
		if got := time.Since(start); got != c.want {
			t.Errorf("an attempt with transparent retry %v waited %v, want %v", c.retry, got, c.want)
		}
	}
}
