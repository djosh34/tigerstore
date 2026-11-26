//go:build !sim && !simio

package wl

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// Exercise the real suite selection, keep/stop functions and EXIT trap,
// with fake log output instead of building or starting any server.
func TestDifftestLogsOnlyNameAnActiveSuite(t *testing.T) {
	b, err := os.ReadFile("../../scripts/difftest.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := string(b)
	prefix, _, ok := strings.Cut(script, "# in_use")
	if !ok {
		t.Fatal("harness prefix missing")
	}
	loopStart := strings.Index(script, "for suite in")
	selection := script[loopStart : loopStart+strings.Index(script[loopStart:], "\tcase $server in")]
	tail := script[strings.LastIndex(script, "\talive\n"):]
	for _, only := range []string{"TestBoundaries", "NeverRun"} {
		t.Run(only, func(t *testing.T) {
			keep := t.TempDir()
			body := "set -- tigerstore\n" + prefix + "\nalive() { :; }\n" + selection + "mkdir -p \"$work/data\"\nprintf passed > \"$work/data/node1.log\"\n" + tail
			cmd := exec.Command("bash", "-c", body)
			cmd.Env = append(os.Environ(), "KEEP="+keep, "ONLY="+only, "RACE=")
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("harness: %v: %s", err, out)
			}
			entries, err := os.ReadDir(keep)
			if err != nil {
				t.Fatal(err)
			}
			want := 0
			if only == "TestBoundaries" {
				want = 1
				b, err := os.ReadFile(filepath.Join(keep, only, "data/node1.log"))
				if err != nil || string(b) != "passed" {
					t.Fatalf("missing active suite logs: %s, %v", b, err)
				}
			}
			if len(entries) != want || want == 1 && entries[0].Name() != only {
				t.Fatalf("ONLY=%s retained %v; want only the suite that actually ran", only, entries)
			}
		})
	}
}

func TestDifftestEtcdKeepsOnlyFailureLogs(t *testing.T) {
	b, err := os.ReadFile("../../scripts/difftest.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := string(b)
	prefix, _, ok := strings.Cut(script, "# in_use")
	if !ok {
		t.Fatal("harness prefix missing")
	}
	keep := t.TempDir()
	body := "set -- etcd\n" + prefix + `
mkdir -p "$work/data"
test=TestBoundaries
active_suite=$test
printf passed > "$work/data/node1.log"
keep 0
test=TestSweep
active_suite=$test
printf failed > "$work/data/node1.log"
exit 7
`
	cmd := exec.Command("bash", "-c", body)
	cmd.Env = append(os.Environ(), "KEEP="+keep)
	if err := cmd.Run(); err == nil {
		t.Fatal("the harness lost the failing suite's exit status")
	}
	entries, err := os.ReadDir(keep)
	if err != nil || len(entries) != 1 || entries[0].Name() != "TestSweep" {
		t.Fatalf("etcd retained successful logs or lost its unfinished failure: %v, %v", entries, err)
	}
	b, err = os.ReadFile(filepath.Join(keep, "TestSweep/data/node1.log"))
	if err != nil || string(b) != "failed" {
		t.Fatalf("failed etcd logs: %s, %v", b, err)
	}
}

func TestDifftestArtifactRetentionPolicy(t *testing.T) {
	b, err := os.ReadFile("../../.github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	before, _, ok := strings.Cut(s, "name: difftest-${{ strategy.job-index }}")
	if !ok {
		t.Fatal("difftest artifact step missing")
	}
	step := s[strings.LastIndex(before, "      - if:"):]
	step = strings.SplitN(step, "\n\n", 2)[0]
	for _, want := range []string{
		"if: always() && (matrix.server == 'tigerstore' || failure())",
		"retention-days: ${{ failure() && 30 || 14 }}",
	} {
		if !strings.Contains(step, want) {
			t.Errorf("artifact policy lacks %q:\n%s", want, step)
		}
	}
}
