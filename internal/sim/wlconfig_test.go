//go:build simio || sim || planted

package sim

// wlFaultsConfig is DefaultConfig on wl nodes (Config.wl) without the
// faults wl does not run yet: a crash of a snapshot's source partway
// through its stream, and rot of a value in a segment
// (TestWLRunsTheFaults). A snapshot stream's damage and late start run
// through wl's hook (wlHooks.SnapshotChunk).
func wlFaultsConfig(seed uint64) Config {
	cfg := DefaultConfig(seed)
	cfg.wl, cfg.FormUnderFaults = true, true
	f := &cfg.Faults
	f.FetchCrash, f.Rot = false, false
	return cfg
}

// wlMembershipConfig is membershipConfig on wl nodes, without the faults
// wlFaultsConfig leaves out.
func wlMembershipConfig(seed uint64) Config {
	cfg := membershipConfig(seed)
	cfg.wl, cfg.FormUnderFaults = true, true
	f := &cfg.Faults
	f.FetchCrash, f.Rot = false, false
	return cfg
}
