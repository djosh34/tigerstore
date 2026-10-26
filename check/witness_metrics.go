package check

// WitnessStats counts work, not guesses about elapsed time. It is local to
// one check; callers must not share it between concurrent checks.
type WitnessStats struct {
	Ops, Keys, Ambiguous, Resolved, Top                                       int
	Steps                                                                     int64
	GapFrames, CandidateScans, Candidates, Fits, FitsAccepted                 int64
	RejectModel, RejectUnchanged, RejectLife, RejectRequired, RejectReachable int64
	Backtracks, MaxBackRevisionDistance, TotalBackRevisionDistance            int64
	MaxOpenGaps, Saves, Restores, SnapshotKeyCells, SnapshotUsedEntries       int64
	LifeChecks, ReachableKeyChecks, MayCandidates, OptionalScans              int64
	CandidateKeyIndexed, CandidateKeyAvoided                                  int64
	IndexSeconds, SearchSeconds                                               float64
}
