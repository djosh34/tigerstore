package sim

// wlFaultsConfig is DefaultConfig on wl nodes (Config.wl), every fault
// main's runs among them (TestWLRunsTheFaults). A snapshot stream's damage and late start run
// through wl's hook (wlHooks.SnapshotChunk).
func wlFaultsConfig(seed uint64) Config {
	cfg := DefaultConfig(seed)
	cfg.wl, cfg.FormUnderFaults = true, true
	return cfg
}

// wlMembershipConfig is membershipConfig on wl nodes, which form their
// cluster first in 7 seeds of 8 (formsFirst), as main's do.
func wlMembershipConfig(seed uint64) Config {
	cfg := membershipConfig(seed)
	cfg.wl = true
	return cfg
}

// wlLinearizableConfig is linearizableConfig on wl nodes, which form
// their cluster first in 7 seeds of 8 (formsFirst), as main's do.
func wlLinearizableConfig(seed uint64) Config {
	cfg := linearizableConfig(seed)
	cfg.wl = true
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
