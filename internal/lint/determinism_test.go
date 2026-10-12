package lint

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// corePackages run inside the simulator, so they must behave the same on
// every run of a seed. Their non-test files may not start goroutines, read
// the clock, use global randomness or reach the operating system. The
// drivers in other packages do those things and feed the results in as
// events.
var corePackages = []string{
	"internal/core",
	"internal/mvcc",
	"internal/sim",
	"internal/simfs",
	"internal/wal",
}

// forbiddenImports reach the clock, the operating system, other goroutines
// or a random source the simulator does not control.
var forbiddenImports = map[string]string{
	"time":                  "read time from ticks given by the driver",
	"os":                    "do I/O through an adapter",
	"os/exec":               "do I/O through an adapter",
	"os/signal":             "do I/O through an adapter",
	"syscall":               "do I/O through an adapter",
	"net":                   "send through the network adapter",
	"net/http":              "send through the network adapter",
	"io/ioutil":             "do I/O through an adapter",
	"crypto/rand":           "take randomness from the driver",
	"math/rand":             "use a math/rand/v2 generator seeded by the driver",
	"runtime":               "the core must not depend on the scheduler",
	"sync":                  "the core is single-threaded and takes no locks",
	"sync/atomic":           "the core is single-threaded and takes no locks",
	"unsafe":                "the core must not depend on memory layout",
	"golang.org/x/sys/unix": "do I/O through an adapter",
}

// seededRand are the math/rand/v2 names that do not touch the global
// generator.
var seededRand = map[string]bool{
	"New": true, "NewPCG": true, "NewChaCha8": true, "NewZipf": true,
	"Rand": true, "PCG": true, "ChaCha8": true, "Source": true, "Zipf": true,
}

// violations returns the determinism violations in one file.
func violations(fset *token.FileSet, f *ast.File) []string {
	var out []string
	report := func(n ast.Node, msg string) {
		out = append(out, fmt.Sprintf("%s: %s", fset.Position(n.Pos()), msg))
	}
	randName := ""
	for _, imp := range f.Imports {
		path, err := strconv.Unquote(imp.Path.Value)
		if err != nil {
			report(imp, err.Error())
			continue
		}
		if why, bad := forbiddenImports[path]; bad || strings.HasPrefix(path, "net/") || strings.HasPrefix(path, "golang.org/x/sys/") {
			if why == "" {
				why = "do I/O through an adapter"
			}
			report(imp, fmt.Sprintf("imports %s: %s", path, why))
		}
		if path == "math/rand/v2" {
			randName = "rand"
			if imp.Name != nil {
				randName = imp.Name.Name
			}
		}
	}
	ast.Inspect(f, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.GoStmt:
			report(n, "starts a goroutine")
		case *ast.SelectorExpr:
			if id, ok := n.X.(*ast.Ident); ok && randName != "" && id.Name == randName && !seededRand[n.Sel.Name] {
				report(n, fmt.Sprintf("uses the global generator %s.%s", id.Name, n.Sel.Name))
			}
		}
		return true
	})
	return out
}

func TestCorePackagesAreDeterministic(t *testing.T) {
	root := filepath.Join("..", "..")
	fset := token.NewFileSet()
	checked := 0
	for _, pkg := range corePackages {
		files, err := filepath.Glob(filepath.Join(root, pkg, "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range files {
			if strings.HasSuffix(name, "_test.go") {
				continue
			}
			f, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
			if err != nil {
				t.Fatal(err)
			}
			for _, v := range violations(fset, f) {
				t.Error(v)
			}
			checked++
		}
	}
	if checked < len(corePackages) {
		t.Fatalf("checked only %d files in %d packages", checked, len(corePackages))
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("the check did not run from the module: %v", err)
	}
}

// TestLintCatchesViolations makes sure the check is not empty.
func TestLintCatchesViolations(t *testing.T) {
	src := `package bad

import (
	"math/rand/v2"
	"sync"
	"time"
)

var mu sync.Mutex

func f() {
	go f()
	_ = time.Now()
	_ = rand.IntN(3)
	_ = rand.New(rand.NewPCG(1, 2))
}
`
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "bad.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	got := violations(fset, f)
	for _, want := range []string{"imports sync", "imports time", "starts a goroutine", "global generator rand.IntN"} {
		found := false
		for _, v := range got {
			found = found || strings.Contains(v, want)
		}
		if !found {
			t.Errorf("violation %q not reported; got %v", want, got)
		}
	}
	if len(got) != 4 {
		t.Errorf("got %d violations, want 4: %v", len(got), got)
	}
}
