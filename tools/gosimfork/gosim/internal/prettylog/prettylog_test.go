package prettylog_test

import (
	"bytes"
	"errors"
	"flag"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/jellevandenhooff/gosim/internal/gosimtool"
	"github.com/jellevandenhooff/gosim/internal/prettylog"
	"github.com/jellevandenhooff/gosim/metatesting"
)

var (
	rewriteInput = flag.Bool("rewriteInput", false, "rewrite input files")
	rewrite      = flag.Bool("rewrite", false, "rewrite golden files")
)

func format(input []byte) []byte {
	var buffer bytes.Buffer
	writer := prettylog.NewWriter(&buffer)

	lines := bytes.SplitAfter(input, []byte("\n"))
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		writer.Write(line)
	}

	return buffer.Bytes()
}

func TestPrettyLog(t *testing.T) {
	files, err := os.ReadDir("./testdata")
	if err != nil {
		t.Fatal(err)
	}

	if *rewriteInput {
		mt := metatesting.ForOtherPackage(t, gosimtool.Module+"/internal/tests/behavior")
		out, err := mt.Run(t, &metatesting.RunConfig{
			Test:     "TestLogForPrettyTest",
			ExtraEnv: []string{"TESTINGFAIL=1"},
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path.Join("testdata", "simple.txt"), out.LogOutput, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	for _, entry := range files {
		name := entry.Name()
		if strings.HasSuffix(name, ".golden") {
			continue
		}
		if !strings.HasSuffix(name, ".txt") {
			continue
		}

		t.Run(name, func(t *testing.T) {
			input, err := os.ReadFile(path.Join("testdata", name))
			if err != nil {
				t.Fatal(err)
			}
			output, err := os.ReadFile(path.Join("testdata", name+".golden"))
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}

			expected := format(input)
			if testing.Verbose() {
				os.Stdout.Write(expected)
			}
			if *rewrite {
				if err := os.WriteFile(path.Join("testdata", name+".golden"), expected, 0o644); err != nil {
					t.Error(err)
				}
			} else {
				if diff := cmp.Diff(string(output), string(expected)); diff != "" {
					t.Error(diff)
				}
			}
		})
	}
}
