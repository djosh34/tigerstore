// r333-callgraph computes a conservative, source-level RTA graph, NOT a gosim graph.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"

	"golang.org/x/tools/go/callgraph"
	"golang.org/x/tools/go/callgraph/rta"
	"golang.org/x/tools/go/callgraph/vta"
	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/go/ssa"
	"golang.org/x/tools/go/ssa/ssautil"
)

type Row struct {
	Function                string
	Audit                   bool
	Path                    []string
	ReplacedPackages        []string
	Body                    bool
	FrontierPath            []string
	OnlyViaNativeOrReplaced bool
}

func pkg(f *ssa.Function) string {
	if f.Pkg != nil {
		return f.Pkg.Pkg.Path()
	}
	if f.Object() != nil && f.Object().Pkg() != nil {
		return f.Object().Pkg().Path()
	}
	return ""
}
func main() {
	dir := flag.String("dir", ".", "tigerstore checkout")
	audit := flag.String("audit", "", "repro-audit.log")
	tags := flag.String("tags", "planted", "build tags")
	out := flag.String("out", "graph.json", "output")
	refine := flag.Bool("vta", false, "refine RTA with VTA (modulo reflection/unsafe)")
	hooksFile := flag.String("hooks", "", "gosim internal/translate/hooks_go123.go for projected frontier")
	flag.Parse()
	missing := map[string]bool{}
	if *audit != "" {
		f, e := os.Open(*audit)
		must(e)
		defer f.Close()
		s := bufio.NewScanner(f)
		re := regexp.MustCompile(`pkg=([^ ]+) name=([^ ]+)`)
		for s.Scan() {
			m := re.FindStringSubmatch(s.Text())
			if len(m) > 0 {
				missing[m[1]+"."+m[2]] = true
			}
		}
		must(s.Err())
	}
	cfg := &packages.Config{Dir: *dir, Mode: packages.LoadAllSyntax, Tests: true, BuildFlags: []string{"-tags=" + *tags}}
	ps, e := packages.Load(cfg, "./internal/sim", "./internal/node")
	must(e)
	if packages.PrintErrors(ps) > 0 {
		os.Exit(1)
	}
	prog, sp := ssautil.AllPackages(ps, ssa.InstantiateGenerics)
	prog.Build()
	var roots []*ssa.Function
	names := map[string]bool{"TestSeeds": true, "TestMembershipFaults": true, "TestPlanted": true, "TestConcurrentCleanerSimulation": true}
	seenPkg := map[*ssa.Package]bool{}
	var addInit func(*ssa.Package)
	addInit = func(p *ssa.Package) {
		if p == nil || seenPkg[p] {
			return
		}
		seenPkg[p] = true
		roots = append(roots, p.Func("init"))
		for _, i := range p.Pkg.Imports() {
			addInit(prog.Package(i))
		}
	}
	for _, p := range sp {
		if p == nil {
			continue
		}
		for n := range names {
			if n == "TestPlanted" && p.Pkg.Path() != "github.com/djosh34/tigerstore/internal/sim" {
				continue
			}
			if f := p.Func(n); f != nil {
				roots = append(roots, f)
				addInit(p)
			}
		}
	}
	rr := roots[:0]
	seen := map[*ssa.Function]bool{}
	for _, f := range roots {
		if f != nil && !seen[f] {
			seen[f] = true
			rr = append(rr, f)
		}
	}
	roots = rr
	sort.Slice(roots, func(i, j int) bool { return roots[i].String() < roots[j].String() })
	fmt.Fprintf(os.Stderr, "roots=%d packages=%d\n", len(roots), len(prog.AllPackages()))
	// Model synctest.Test's callback: runtime linkname Run has no SSA body.
	// Include only lexical call sites in these test packages; edges retain their callers.
	type callback struct {
		caller, callee *ssa.Function
		site           ssa.CallInstruction
	}
	var callbacks []callback
	for f := range ssautil.AllFunctions(prog) {
		if !(strings.Contains(f.String(), "github.com/djosh34/tigerstore/internal/sim.runWith") || strings.Contains(f.String(), "github.com/djosh34/tigerstore/internal/node.TestConcurrentCleanerSimulation")) {
			continue
		}
		for _, b := range f.Blocks {
			for _, i := range b.Instrs {
				c, ok := i.(ssa.CallInstruction)
				if !ok {
					continue
				}
				callee := c.Common().StaticCallee()
				if callee == nil || pkg(callee) != "testing/synctest" || callee.Name() != "Test" {
					continue
				}
				for _, a := range c.Common().Args {
					switch a := a.(type) {
					case *ssa.MakeClosure:
						target := a.Fn.(*ssa.Function)
						callbacks = append(callbacks, callback{f, target, c})
						roots = append(roots, target)
					case *ssa.Function:
						callbacks = append(callbacks, callback{f, a, c})
						roots = append(roots, a)
					}
				}
			}
		}
	}
	res := rta.Analyze(roots, true)
	cg := res.CallGraph
	if *refine {
		fs := map[*ssa.Function]bool{}
		for f := range res.Reachable {
			fs[f] = true
		}
		cg = vta.CallGraph(fs, cg)
	}
	// Model direct test Run/Cleanup arguments across replacement testing package.
	for f := range ssautil.AllFunctions(prog) {
		if !strings.HasPrefix(pkg(f), "github.com/djosh34/tigerstore/") {
			continue
		}
		for _, b := range f.Blocks {
			for _, i := range b.Instrs {
				c, ok := i.(ssa.CallInstruction)
				if !ok {
					continue
				}
				target := c.Common().StaticCallee()
				if target == nil || pkg(target) != "testing" || (target.Name() != "Run" && target.Name() != "Cleanup") {
					continue
				}
				for _, a := range c.Common().Args {
					switch a := a.(type) {
					case *ssa.MakeClosure:
						callbacks = append(callbacks, callback{f, a.Fn.(*ssa.Function), c})
					case *ssa.Function:
						callbacks = append(callbacks, callback{f, a, c})
					}
				}
			}
		}
	}
	// Remove synthetic callback roots; retain their real incoming call-site edges.
	for _, c := range callbacks {
		callgraph.AddEdge(cg.CreateNode(c.caller), c.site, cg.CreateNode(c.callee))
	}
	roots = rr
	// Multi-source BFS. Test roots precede init roots, but paths from init are retained.
	sort.SliceStable(roots, func(i, j int) bool { return (roots[i].Name() != "init") && (roots[j].Name() == "init") })
	prev := map[*callgraph.Node]*callgraph.Node{}
	var q []*callgraph.Node
	for _, f := range roots {
		n := cg.Nodes[f]
		if n == nil {
			continue
		}
		if _, ok := prev[n]; !ok {
			prev[n] = nil
			q = append(q, n)
		}
	}
	for i := 0; i < len(q); i++ {
		n := q[i]
		edges := append([]*callgraph.Edge(nil), n.Out...)
		sort.Slice(edges, func(i, j int) bool { return edges[i].Callee.Func.String() < edges[j].Callee.Func.String() })
		for _, e := range edges {
			if _, ok := prev[e.Callee]; !ok {
				prev[e.Callee] = n
				q = append(q, e.Callee)
			}
		}
	}
	// Projected frontier: not proof of the translated graph. Stop at version-123
	// hooks and packages kept native/swapped, with modeled testing callback edges.
	hooks := map[string]bool{}
	if *hooksFile != "" {
		b, e := os.ReadFile(*hooksFile)
		must(e)
		re := regexp.MustCompile(`\{Pkg: "([^"]+)", Selector: "([^"]+)"\}:`)
		for _, m := range re.FindAllStringSubmatch(string(b), -1) {
			hooks[m[1]+"."+m[2]] = true
		}
	}
	skipped := map[string]bool{"runtime": true, "errors": true, "reflect": true, "strconv": true, "embed": true, "math": true, "math/big": true, "unsafe": true, "runtime/coverage": true, "runtime/metrics": true, "runtime/pprof": true, "vendor/golang.org/x/sys/cpu": true, "unique": true, "testing": true, "testing/internal/testdeps": true, "internal/reflectlite": true}
	fp := map[*callgraph.Node]*callgraph.Node{}
	var fq []*callgraph.Node
	for _, f := range roots {
		n := cg.Nodes[f]
		if n == nil || skipped[pkg(f)] {
			continue
		}
		if _, ok := fp[n]; !ok {
			fp[n] = nil
			fq = append(fq, n)
		}
	}
	for i := 0; i < len(fq); i++ {
		n := fq[i]
		if skipped[pkg(n.Func)] || hooks[pkg(n.Func)+"."+n.Func.Name()] {
			continue
		}
		for _, e := range n.Out {
			if skipped[pkg(e.Callee.Func)] {
				continue
			}
			if _, ok := fp[e.Callee]; !ok {
				fp[e.Callee] = n
				fq = append(fq, e.Callee)
			}
		}
	}
	interesting := func(p string) bool {
		return p == "internal/runtime/atomic" || p == "internal/sync" || p == "sync" || p == "time" || p == "weak" || p == "unique" || p == "internal/synctest" || p == "crypto/rand" || p == "internal/cpu" || p == "encoding/json/jsontext" || p == "runtime/debug" || strings.HasPrefix(p, "crypto/internal/fips140") || p == "crypto/internal/sysrand" || strings.HasPrefix(p, "github.com/djosh34/tigerstore/internal/osfs")
	}
	var rows []Row
	for _, n := range q {
		f := n.Func
		p := pkg(f)
		a := f.Signature.Recv() == nil && missing[p+"."+f.Name()]
		if !a && !interesting(p) {
			continue
		}
		r := Row{Function: f.String(), Audit: a, Body: len(f.Blocks) > 0}
		if _, ok := fp[n]; ok {
			for x := n; x != nil; x = fp[x] {
				r.FrontierPath = append(r.FrontierPath, x.Func.String())
			}
			for i, j := 0, len(r.FrontierPath)-1; i < j; i, j = i+1, j-1 {
				r.FrontierPath[i], r.FrontierPath[j] = r.FrontierPath[j], r.FrontierPath[i]
			}
		} else {
			r.OnlyViaNativeOrReplaced = true
		}
		bounds := map[string]bool{}
		for x := n; x != nil; x = prev[x] {
			r.Path = append(r.Path, x.Func.String())
			p := pkg(x.Func)
			if p == "reflect" || p == "internal/reflectlite" || p == "testing" {
				bounds[p] = true
			}
		}
		for i, j := 0, len(r.Path)-1; i < j; i, j = i+1, j-1 {
			r.Path[i], r.Path[j] = r.Path[j], r.Path[i]
		}
		for p := range bounds {
			r.ReplacedPackages = append(r.ReplacedPackages, p)
		}
		sort.Strings(r.ReplacedPackages)
		rows = append(rows, r)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Function < rows[j].Function })
	f, e := os.Create(*out)
	must(e)
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	must(enc.Encode(rows))
	fmt.Fprintf(os.Stderr, "reachable=%d selected=%d auditReachable=%d\n", len(q), len(rows), func() int {
		n := 0
		for _, r := range rows {
			if r.Audit {
				n++
			}
		}
		return n
	}())
}
func must(e error) {
	if e != nil {
		panic(e)
	}
}
