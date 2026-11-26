package check

// Scratch-only evidence counters. This file and its call are never to merge.
import "encoding/json"

type eventEvidence struct {
	Gaps, Nonempty, Probes int64
	Reasons                [eventRejectReasons]int64
}

var eventEvidenceTotal eventEvidence
var eventEvidenceByRev = map[int64]*eventEvidence{}

func measureEventCertificate(rev int64, pool int, cert eventCertificate) {
	x := eventEvidenceByRev[rev]
	if x == nil {
		x = &eventEvidence{}
		eventEvidenceByRev[rev] = x
	}
	for _, out := range []*eventEvidence{&eventEvidenceTotal, x} {
		out.Gaps++
		if pool > 0 {
			out.Nonempty++
		}
		out.Probes += int64(pool)
		for k, n := range cert.reasons {
			out.Reasons[k] += int64(n)
		}
	}
}

func EventCertificateMeasurements() []byte {
	b, _ := json.MarshalIndent(struct {
		Total      eventEvidence
		Reasons    []string
		ByRevision map[int64]*eventEvidence
	}{eventEvidenceTotal, []string{"empty-delete", "blocked-delete", "blocked-put", "missing-lease", "read-only-cas", "compaction"}, eventEvidenceByRev}, "", "  ")
	return b
}
