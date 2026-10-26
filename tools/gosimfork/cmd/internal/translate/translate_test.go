package translate

import (
	"cmp"
	"flag"
	"go/format"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	gocmp "github.com/google/go-cmp/cmp"
	"golang.org/x/tools/txtar"

	"github.com/jellevandenhooff/gosim/internal/gosimtool"
)

var (
	rewrite    = flag.Bool("rewrite", false, "rewrite golden outputs")
	useworkdir = flag.Bool("useworkdir", false, "write in testdata/workdir instead of tempdir")
)

// TestTranslate runs gosim translate on all go files specified in testdata/.
//
// Each file in testdata that ends in .translate.txt is a directory that will be
// translated.  All files in all directories are put in one module and
// translated at the same time because otherwise this test gets very slow.
//
// The expected output of the translation is stored with the testdata/ (next to
// each input file) and checked or rewritten depending on the -rewrite flag.
//
// TODO: this test relies on a compiled and up-to-date gosim binary from the
// script test... this is brittle and will be a pain at some point...
func TestTranslate(t *testing.T) {
	entries, err := os.ReadDir("./testdata")
	if err != nil {
		t.Fatal(err)
	}

	// prepare work directory
	var workDir string
	if *useworkdir {
		wd, err := os.Getwd()
		if err != nil {
			t.Fatal(err)
		}
		workDir = path.Join(wd, "testdata/workdir")
		if err := os.RemoveAll(workDir); err != nil {
			// XXX: only empty files inside workdir?
			t.Fatal(err)
		}
		if err := os.MkdirAll(workDir, 0o755); err != nil {
			t.Fatal(err)
		}
	} else {
		workDir = t.TempDir()
	}

	toolModDir, err := gosimtool.FindGoModDir()
	if err != nil {
		t.Fatal(err)
	}
	// The gosim module, two directories up from this one, prepared the
	// copy of itself with gosim prepare-selftest.
	curModDir := filepath.Join(toolModDir, "../..")

	extractedModDir := filepath.Join(curModDir, gosimtool.OutputDirectory, "scripttest", "mod")

	// add test dependencies to the copied gosim module
	if err := filepath.Walk(extractedModDir, func(path string, info fs.FileInfo, err error) error {
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	// put a fake go.mod in the work directory
	if err := gosimtool.MakeGoModForTest(extractedModDir, workDir, []string{
		"github.com/google/go-cmp",
		"github.com/mattn/go-isatty",
		"golang.org/x/mod",
		"golang.org/x/sync",
		"golang.org/x/sys",
	}); err != nil {
		t.Fatal(err)
	}

	// TODO: dedup this code with scripttest, somehow
	binPath := filepath.Join(curModDir, gosimtool.OutputDirectory, "scripttest", "bin", "gosim")
	envVars := append(os.Environ(), "GOSIMCACHE="+filepath.Join(curModDir, gosimtool.OutputDirectory))

	// parse all inputs and write them to work directory
	archiveByPackage := make(map[string]*txtar.Archive)
	desiredFilesByPackage := make(map[string][]txtar.File)
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".translate.txt") {
			continue
		}
		pkgName := strings.TrimSuffix(entry.Name(), ".translate.txt")

		archive, err := txtar.ParseFile(path.Join("./testdata", entry.Name()))
		if err != nil {
			t.Error(err)
		}

		// store archive to check against later
		archiveByPackage[pkgName] = archive

		for _, file := range archive.Files {
			// don't write output files
			if strings.HasPrefix(file.Name, "translated/") {
				continue
			}

			p := path.Join(workDir, pkgName, file.Name)
			if err := os.MkdirAll(path.Dir(p), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(p, file.Data, 0o644); err != nil {
				t.Fatal(err)
			}

			// keep a formatted version of this file to check against later
			formatted, err := format.Source(file.Data)
			if err != nil {
				t.Fatal(err)
			}
			desiredFilesByPackage[pkgName] = append(desiredFilesByPackage[pkgName], txtar.File{
				Name: file.Name,
				Data: formatted,
			})
		}
	}

	// run the tool in the work directory
	// TODO: use a race version, optionally
	cmd := exec.Command(binPath, "translate", "./...")
	cmd.Dir = workDir
	cmd.Env = envVars
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			os.Exit(exitErr.ExitCode())
		}
		log.Fatal(err)
	}

	cfg := gosimtool.BuildConfig{
		GOOS: "linux",
		// TODO: varying the architecture for snapshots is fine as long as we don't translate arch-specific stuff; cross-arch tests should catch problems...
		GOARCH: runtime.GOARCH,
		Race:   false,
	}
	outputPath := filepath.Join(workDir, gosimtool.OutputDirectory, "translated", cfg.AsDirname(), "test")

	// read all output files
	if err := filepath.WalkDir(outputPath, func(path string, d fs.DirEntry, _ error) error {
		if !d.Type().IsRegular() {
			return nil
		}

		trimmedPath := strings.TrimPrefix(path, outputPath+"/")

		outBytes, err := os.ReadFile(path)
		if err != nil {
			return err
		}

		pkgName := trimmedPath[:strings.Index(trimmedPath, "/")]
		if _, ok := desiredFilesByPackage[pkgName]; !ok {
			t.Errorf("help unknown package %q for path %q", pkgName, path)
		}
		desiredFilesByPackage[pkgName] = append(desiredFilesByPackage[pkgName], txtar.File{
			Name: "translated/" + trimmedPath,
			Data: outBytes,
		})
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// check each input file one-by-one
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".translate.txt") {
			continue
		}
		pkgName := strings.TrimSuffix(entry.Name(), ".translate.txt")

		desiredFiles := desiredFilesByPackage[pkgName]

		slices.SortStableFunc(desiredFiles, func(a, b txtar.File) int {
			plainA := strings.TrimPrefix(a.Name, "translated/"+pkgName+"/")
			plainB := strings.TrimPrefix(b.Name, "translated/"+pkgName+"/")
			return cmp.Compare(plainA, plainB)
		})

		archive := archiveByPackage[pkgName]

		if *rewrite {
			newArchive := &txtar.Archive{
				Comment: archive.Comment,
				Files:   desiredFiles,
			}
			if err := os.WriteFile(path.Join("./testdata", entry.Name()), txtar.Format(newArchive), 0o644); err != nil {
				t.Error(err)
			}
		} else {
			if diff := gocmp.Diff(archive.Files, desiredFiles); diff != "" {
				t.Error(diff)
			}
		}
	}
}

func TestRenameFile(t *testing.T) {
	cfg := gosimtool.BuildConfig{
		GOOS:   "linux",
		GOARCH: "amd64",
	}

	testcases := []struct {
		in, out string
	}{
		{
			in:  "foo/bar.go",
			out: "foo/bar.go",
		},
		{
			in:  "foo/bar_test.go",
			out: "foo/bar_test.go",
		},
		{
			in:  "foo/bar_linux.go",
			out: "foo/bar_linux_.go",
		},
		{
			in:  "foo/bar_linux_test.go",
			out: "foo/bar_linux__test.go",
		},
		{
			in:  "foo/bar_linux_amd64.go",
			out: "foo/bar_linux_amd64_.go",
		},
		{
			in:  "foo/bar_linux_amd64_test.go",
			out: "foo/bar_linux_amd64__test.go",
		},
		{
			in:  "foo/bar_amd64.go",
			out: "foo/bar_amd64.go",
		},
		{
			in:  "foo/bar_amd64_test.go",
			out: "foo/bar_amd64_test.go",
		},

		{
			in:  "foo/bar_darwin.go",
			out: "foo/bar_darwin.go",
		},
		{
			in:  "foo/bar_darwin_test.go",
			out: "foo/bar_darwin_test.go",
		},
	}

	for _, testcase := range testcases {
		got := renameFile(cfg, testcase.in)
		if got != testcase.out {
			t.Errorf("rename %q: expected %q, got %q", testcase.in, testcase.out, got)
		}
	}
}
