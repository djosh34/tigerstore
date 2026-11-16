//go:build simio || sim || planted

package sim

// wlFaultsConfig is DefaultConfig on wl nodes (Config.wl) without the
// faults of main's engine alone, which wl does not run yet: a snapshot
// fetch's faults and rot of a value in a segment (TestWLRunsTheFaults).
func wlFaultsConfig(seed uint64) Config {
	cfg := DefaultConfig(seed)
	cfg.wl, cfg.FormUnderFaults = true, true
	f := &cfg.Faults
	f.FlipSnapshot, f.FetchCrash, f.SlowFetch, f.Rot = false, false, false, false
	return cfg
}
