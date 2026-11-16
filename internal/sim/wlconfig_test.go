//go:build simio || sim || planted

package sim

// wlFaultsConfig is DefaultConfig on wl nodes (Config.wl) without the
// fault wl does not run yet: a crash of a snapshot's source partway
// through its stream (TestWLRunsTheFaults). A snapshot stream's damage and late start run
// through wl's hook (wlHooks.SnapshotChunk).
func wlFaultsConfig(seed uint64) Config {
	cfg := DefaultConfig(seed)
	cfg.wl, cfg.FormUnderFaults = true, true
	cfg.Faults.FetchCrash = false
	return cfg
}

// wlMembershipConfig is membershipConfig on wl nodes, without the faults
// wlFaultsConfig leaves out.
func wlMembershipConfig(seed uint64) Config {
	cfg := membershipConfig(seed)
	cfg.wl, cfg.FormUnderFaults = true, true
	cfg.Faults.FetchCrash = false
	return cfg
}

// wlStalledLeaderConfig is stalledLeaderConfig (planted_test.go) on wl
// nodes: no faults but the leader's disk, stalled at 5 s until healing.
func wlStalledLeaderConfig(seed uint64) Config {
	cfg := DefaultConfig(seed)
	cfg.wl, cfg.FormUnderFaults = true, true
	cfg.Faults = Faults{}
	cfg.StallLeaderAt = 5 * Second
	cfg.LiveFrom = cfg.StallLeaderAt + 10*Second
	cfg.Duration = 25 * Second
	return cfg
}

// wlDamagedSnapshotConfig is damagedSnapshotConfig on wl nodes: a
// follower held down past the leader's window fetches a snapshot, whose
// first stream is damaged. The disk budget is DefaultConfig's: it is wl's
// checkpoint interval (wlnode.go), and without it no checkpoint cuts the
// log, so the follower catches up from the log and no snapshot is sent.
func wlDamagedSnapshotConfig(seed uint64) Config {
	cfg := damagedSnapshotConfig(seed)
	cfg.wl, cfg.FormUnderFaults = true, true
	cfg.Faults.DiskBudget = DefaultConfig(seed).Faults.DiskBudget
	return cfg
}
