package sim

// NEVER MERGE: single authorized diagnostic on the exact 2f61154a runtime.
// Production, the gate's 360M limit, and its 55-minute backstop are unchanged.
import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/check"
	"github.com/djosh34/tigerstore/history"
	"github.com/djosh34/tigerstore/internal/simrun"
	"github.com/djosh34/tigerstore/internal/wallclock"
)

const diagnostic5000Steps int64 = 3_600_000_000
const diagnostic5000Hashes = "trace e00e0dcb16ede8a6 history e1e8f03d15eec062 node1 c285b978ca019c65"

func diagnostic5000(t *testing.T, res *Result) {
	t.Helper()
	// runWith already captured these BEFORE checks; no different history is
	// ever diagnosed. Native runImage separately verifies the full image SHA.
	got := runHashes(res)
	if got != diagnostic5000Hashes {
		t.Fatalf("DIAGNOSTIC HASH MISMATCH: got %s; want %s; no witness check performed", got, diagnostic5000Hashes)
	}
	t.Logf("DIAGNOSTIC HASH MATCH: %s", got)
	h, err := res.checkable(true)
	if err != nil {
		t.Fatal(err)
	}
	if len(h.Ops) != 231042 {
		t.Fatalf("DIAGNOSTIC input operations %d, want 231042; no witness check performed", len(h.Ops))
	}
	// Stream directly to a driver-owned output, NOT a large in-memory gosim
	// artifact. Each operation/request is encoded individually, not all 4+ GB
	// at once. The job compresses and retains the file even on a failed check.
	w, closeOutput, err := simrun.CreateOutput("diagnostic-history")
	if err != nil {
		t.Fatal(err)
	}
	err = diagnosticHistory(w, h)
	if closeErr := closeOutput(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	input, _ := json.Marshal(map[string]any{"diagnostic_only": true, "hashes": got, "image_sha256": "8d3ebb224e4485da376267714341192bda05af43d9409656b7854f35f521f50b", "operations": len(h.Ops), "step_budget": diagnostic5000Steps, "gate_step_budget": witnessStepLimit, "wall_backstop": "55m"})
	simrun.Artifact("sim-out/diagnostic-input.json", input)
	var used int64
	started := wallclock.Now()
	err = check.Witnessed(h, check.FromRevisions, time.Minute, check.MaxSteps(diagnostic5000Steps), wallStop, check.StepsUsed(&used))
	wall := time.Duration(wallclock.Now() - started)
	classification := "pass-witness-gap"
	if errors.Is(err, check.ErrWitnessTimeout) {
		classification = "inconclusive-timeout"
	} else if err != nil {
		classification = "violation"
	}
	message := ""
	if err != nil {
		message = err.Error()
	}
	verdict, _ := json.Marshal(map[string]any{"diagnostic_only": true, "hashes_verified": true, "classification": classification, "passed": err == nil, "steps": used, "budget": diagnostic5000Steps, "search_wall_seconds": wall.Seconds(), "error": message})
	simrun.Artifact("sim-out/diagnostic-verdict.json", verdict)
	t.Logf("DIAGNOSTIC VERDICT: %s", verdict)
	if err != nil {
		t.Error(err)
	}
}

func diagnosticArray[T any](w io.Writer, values []T) error {
	if _, err := io.WriteString(w, "["); err != nil {
		return err
	}
	encoder := json.NewEncoder(w)
	for i := range values {
		if i > 0 {
			if _, err := io.WriteString(w, ","); err != nil {
				return err
			}
		}
		if err := encoder.Encode(values[i]); err != nil {
			return err
		}
	}
	_, err := io.WriteString(w, "]")
	return err
}

func TestDiagnosticHistoryEncoding(t *testing.T) {
	h := &history.History{Ops: []history.Op{{Client: 1, Call: 2, Return: 3}}, Target: 7, Start: &history.Start{Rev: 4, KVs: []history.KeyValue{{Key: "k", Value: "v\x00x"}}}}
	var out strings.Builder
	if err := diagnosticHistory(&out, h); err != nil {
		t.Fatal(err)
	}
	var got history.History
	if err := json.Unmarshal([]byte(out.String()), &got); err != nil {
		t.Fatal(err)
	}
	before, _ := json.Marshal(h)
	after, _ := json.Marshal(&got)
	if string(before) != string(after) {
		t.Fatalf("history stream changed input: %s vs %s", before, after)
	}
}

func diagnosticHistory(w io.Writer, h *history.History) error {
	if _, err := io.WriteString(w, "{\"ops\":"); err != nil {
		return err
	}
	if err := diagnosticArray(w, h.Ops); err != nil {
		return err
	}
	if _, err := io.WriteString(w, ",\"watches\":"); err != nil {
		return err
	}
	if err := diagnosticArray(w, h.Watches); err != nil {
		return err
	}
	if _, err := io.WriteString(w, ",\"log\":"); err != nil {
		return err
	}
	if err := diagnosticArray(w, h.Log); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, ",\"target\":%d,\"start\":", h.Target); err != nil {
		return err
	}
	if err := json.NewEncoder(w).Encode(h.Start); err != nil {
		return err
	}
	_, err := io.WriteString(w, "}\n")
	return err
}
