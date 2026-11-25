package sim

import "testing"

// The liveness rule asks all its calls of a whole window, and its share of
// one cut short: seed 519 of TestCampaign's 4 s of 6 ask 34, which its 37
// calls meet (#851).
func TestTheLivenessRuleAsksItsShareOfAWindowCutShort(t *testing.T) {
	const full = 6 * Second
	for _, c := range []struct {
		full, window, want int64
	}{
		{full, full, liveCalls},
		{full, 10 * Second, liveCalls},
		{full, 4 * Second, 34},
		{full, Millisecond, 1},
		{full, 0, 0},
		{full, -Second, 0},
		{0, 4 * Second, liveCalls},
		{-Second, 4 * Second, liveCalls},
	} {
		if got := liveNeed(c.full, c.window); got != c.want {
			t.Errorf("liveNeed(%d ms, %d ms) = %d, want %d", c.full/Millisecond, c.window/Millisecond, got, c.want)
		}
	}
}
