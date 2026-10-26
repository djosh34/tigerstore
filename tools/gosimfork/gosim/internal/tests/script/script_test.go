package script_test

import (
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/rogpeppe/go-internal/gotooltest"
	"github.com/rogpeppe/go-internal/testscript"

	"github.com/jellevandenhooff/gosim/internal/gosimtool"
)

func TestScript(t *testing.T) {
	p := testscript.Params{
		Dir: "testdata",
	}
	gotooltest.Setup(&p)

	curModDir, err := gosimtool.FindGoModDir()
	if err != nil {
		t.Fatal(err)
	}
	extractedModDir := filepath.Join(curModDir, gosimtool.OutputDirectory, "scripttest", "mod")

	// add test dependencies to the copied gosim module
	if err := filepath.Walk(extractedModDir, func(path string, info fs.FileInfo, err error) error {
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	origSetup := p.Setup
	p.Setup = func(e *testscript.Env) error {
		// put a fake go.mod in the work directory
		// this include a copy of the gosim module (not the entire module because we want to minimize test dependencies)
		if err := gosimtool.MakeGoModForTest(extractedModDir, e.WorkDir, []string{
			"github.com/google/go-cmp",
			"github.com/mattn/go-isatty",
			"golang.org/x/mod",
			"golang.org/x/sync",
			"golang.org/x/sys",
		}); err != nil {
			return err
		}

		// add /bin to PATH in tests
		binDir := filepath.Join(curModDir, gosimtool.OutputDirectory, "scripttest", "bin")
		// add dependencies on /bin
		if err := filepath.Walk(binDir, func(path string, info fs.FileInfo, err error) error {
			return nil
		}); err != nil {
			log.Fatal(err)
		}
		e.Setenv("PATH", binDir+string(filepath.ListSeparator)+e.Getenv("PATH"))
		e.Setenv("GOSIMTOOL", filepath.Join(binDir, "gosim"))

		// allow the module to access our shared cache
		e.Vars = append(e.Vars, "GOSIMCACHE="+filepath.Join(curModDir, gosimtool.OutputDirectory))

		if origSetup != nil {
			return origSetup(e)
		}
		return nil
	}
	testscript.Run(t, p)
}

func TestMain(m *testing.M) {
	os.Exit(testscript.RunMain(m, nil))
}
