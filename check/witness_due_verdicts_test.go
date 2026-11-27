package check

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

// TestDueRecordedVerdicts compares full errors, not just pass/fail, with
// origin/main before #857. The oracle includes the saved hard histories,
// planted corruption, and random histories both intact and with stale reads.
// To record it, run this test on the pre-change tree with
// WITNESS_RECORD_VERDICTS=testdata/witness-due-verdicts.json.
func TestDueRecordedVerdicts(t *testing.T) {
	got := map[string]string{}
	visitDueRecordedHistories(t, func(name string, h *history.History) {
		err := Witnessed(h, FromRevisions, time.Minute, snapshotOracle(t))
		got[name] = "pass"
		if err != nil {
			got[name] = err.Error()
		}
	})
	checkDueRecordedVerdicts(t, got)
}

func visitDueRecordedHistories(t *testing.T, visit func(string, *history.History)) {
	t.Helper()
	check := func(name string, h *history.History) {
		h.Log = nil
		for i := range h.Ops {
			h.Ops[i].Effect = nil
		}
		visit(name, h)
	}
	for _, name := range []string{
		"planted-B01a-1.json", "seed-5058843.json", "seed-5068434.json",
		"seed-59113215.json", "seed-77148.json", "seed-888737.json",
		"witness-425.json.gz", "witness-control-320-1529.json.gz", "witness-control-80-201.json.gz",
	} {
		f, err := os.Open("testdata/" + name)
		if err != nil {
			t.Fatal(err)
		}
		var h history.History
		decoder := json.NewDecoder(f)
		if strings.HasSuffix(name, ".gz") {
			z, err := gzip.NewReader(f)
			if err != nil {
				t.Fatal(err)
			}
			decoder = json.NewDecoder(z)
		}
		err = decoder.Decode(&h)
		if closeErr := f.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
		if err != nil {
			t.Fatal(err)
		}
		check(name, &h)
	}
	rng := rand.New(rand.NewPCG(857, 2))
	for i := range 80 {
		_, h := narrowingHistory(rng)
		check(fmt.Sprintf("random-%d", i), h)
		if staleRead(rng, h) {
			check(fmt.Sprintf("stale-%d", i), h)
		}
		check(fmt.Sprintf("leased-%d", i), leasedHistory(rng))
	}
}

func checkDueRecordedVerdicts(t *testing.T, got map[string]string) {
	t.Helper()
	if path := os.Getenv("WITNESS_RECORD_VERDICTS"); path != "" {
		data, err := json.MarshalIndent(got, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, append(data, '\n'), 0o600); err != nil {
			t.Fatal(err)
		}
		return
	}
	data, err := os.ReadFile("testdata/witness-due-verdicts.json")
	if err != nil {
		t.Fatal(err)
	}
	var want map[string]string
	if err := json.Unmarshal(data, &want); err != nil {
		t.Fatal(err)
	}
	failed := 0
	for name, verdict := range want {
		if got[name] != verdict {
			t.Errorf("%s: got %q, want %q", name, got[name], verdict)
		}
		if verdict != "pass" {
			failed++
		}
	}
	if len(got) != len(want) || failed < 40 {
		t.Fatalf("got %d histories, want %d with at least 40 failures (got %d)", len(got), len(want), failed)
	}
	t.Logf("%d verdicts and full explanations match the old witness, including %d failures", len(got), failed)
}
