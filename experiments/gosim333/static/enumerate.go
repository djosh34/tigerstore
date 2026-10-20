// enumerate inventories the selected package closure, not runtime reachability.
package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/build"
	"go/parser"
	"go/printer"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type Pkg struct {
	ImportPath, Dir            string
	Standard                   bool
	GoFiles, CgoFiles, Imports []string
	ImportMap                  map[string]string
}
type Row struct {
	Version, Arch, Pkg, Name, Kind, Target, Status, File string
	Line                                                 int
	Signature                                            string
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}
func str(e ast.Expr) string {
	if s, ok := e.(*ast.BasicLit); ok {
		v, _ := strconv.Unquote(s.Value)
		return v
	}
	return ""
}
func selectors(e ast.Expr) (string, string) {
	c, ok := e.(*ast.CompositeLit)
	if !ok {
		return "", ""
	}
	var p, s string
	for _, x := range c.Elts {
		if kv, ok := x.(*ast.KeyValueExpr); ok {
			switch kv.Key.(*ast.Ident).Name {
			case "Pkg":
				p = str(kv.Value)
			case "Selector":
				s = str(kv.Value)
			}
		}
	}
	return p, s
}
func table(file, name string) map[string]bool {
	out := map[string]bool{}
	f, e := parser.ParseFile(token.NewFileSet(), file, nil, 0)
	check(e)
	ast.Inspect(f, func(n ast.Node) bool {
		v, ok := n.(*ast.ValueSpec)
		if !ok {
			return true
		}
		for i, id := range v.Names {
			if id.Name == name && i < len(v.Values) {
				c := v.Values[i].(*ast.CompositeLit)
				for _, x := range c.Elts {
					kv := x.(*ast.KeyValueExpr)
					if k := str(kv.Key); k != "" {
						out[k] = true
					}
				}
			}
		}
		return true
	})
	return out
}
func hooks(file string) map[string]bool {
	out := map[string]bool{}
	f, e := parser.ParseFile(token.NewFileSet(), file, nil, 0)
	check(e)
	ast.Inspect(f, func(n ast.Node) bool {
		kv, ok := n.(*ast.KeyValueExpr)
		if ok {
			p, s := selectors(kv.Key)
			if p != "" && s != "" {
				out[p+"."+s] = true
			}
		}
		return true
	})
	return out
}
func base(p string) string { return strings.Split(p, " [")[0] }
func main() {
	deps := flag.String("deps", "", "go list -deps -test -json output")
	gs := flag.String("gosim", "", "gosim checkout")
	old := flag.String("oldroot", "", "Go 1.23.12 source GOROOT")
	arch := flag.String("arch", "arm64", "")
	flag.Parse()
	skip := table(filepath.Join(*gs, "internal/translate/main.go"), "skippedPackagesGo123")
	keep := table(filepath.Join(*gs, "internal/translate/main.go"), "keepAsmPackagesGo123")
	// Computed constant keys in skippedPackagesGo123 refer only to gosimruntime.
	skip["github.com/jellevandenhooff/gosim/gosimruntime"] = true
	hs := hooks(filepath.Join(*gs, "internal/translate/hooks_go123.go"))
	for k := range hooks(filepath.Join(*gs, "internal/translate/hooks_go123_"+*arch+"_gen.go")) {
		hs[k] = true
	}
	// acceptedgo123Linknames is a distinct map in the common file; it is NOT a hook.
	delete(hs, "hash/maphash.runtime_memhash")
	f, e := os.Open(*deps)
	check(e)
	defer f.Close()
	dec := json.NewDecoder(f)
	pkgs := map[string]Pkg{}
	files := map[string]map[string]bool{}
	for {
		var p Pkg
		e := dec.Decode(&p)
		if e == io.EOF {
			break
		}
		check(e)
		k := base(p.ImportPath)
		p.ImportPath = k
		pkgs[k] = p
		if files[k] == nil {
			files[k] = map[string]bool{}
		}
		for _, n := range append(p.GoFiles, p.CgoFiles...) {
			if !filepath.IsAbs(n) {
				n = filepath.Join(p.Dir, n)
			}
			files[k][n] = true
		}
	}
	active := map[string]bool{}
	var visit func(string)
	visit = func(k string) {
		k = base(k)
		if skip[k] || active[k] {
			return
		}
		p, ok := pkgs[k]
		if !ok {
			return
		}
		active[k] = true
		for _, d := range p.Imports {
			if m, ok := p.ImportMap[d]; ok {
				d = m
			}
			visit(d)
		}
	}
	// Roots include test variants merged above. Imports of merged variants are a superset.
	for k := range pkgs {
		if strings.HasPrefix(k, "github.com/djosh34/tigerstore/internal/sim") || strings.HasPrefix(k, "github.com/djosh34/tigerstore/internal/node") {
			visit(k)
		}
	}
	translated := func(k string) bool {
		return active[k] || k == "reflect" || k == "internal/reflectlite" || k == "testing" || k == "net"
	}
	var rows []Row
	scan := func(version, pkg, path string) {
		fs := token.NewFileSet()
		f, e := parser.ParseFile(fs, path, nil, parser.ParseComments)
		check(e)
		funcs := map[string]*ast.FuncDecl{}
		links := map[string][]*ast.Comment{}
		for _, d := range f.Decls {
			if d, ok := d.(*ast.FuncDecl); ok && d.Recv == nil {
				funcs[d.Name.Name] = d
			}
		}
		for _, g := range f.Comments {
			for _, c := range g.List {
				if strings.HasPrefix(c.Text, "//go:linkname ") {
					p := strings.Fields(c.Text)
					if len(p) >= 2 {
						links[p[1]] = append(links[p[1]], c)
					}
				}
			}
		}
		status := func(name, kind, target string, body bool) string {
			if !active[pkg] {
				return "skipped"
			}
			if hs[pkg+"."+name] {
				return "hook"
			}
			if kind == "linkname" {
				if target == "" {
					if body {
						return "export-linkname"
					}
					return "unknown-linkname"
				}
				i := strings.Index(target[strings.LastIndex(target, "/")+1:], ".")
				if i >= 0 {
					i += strings.LastIndex(target, "/") + 1
					tp, tn := target[:i], target[i+1:]
					if translated(tp) {
						return "translated-linkname"
					}
					if pkg == "hash/maphash" && name == "runtime_memhash" && tp == "runtime" && tn == "memhash" {
						return "accepted-linkname"
					}
					if strings.HasPrefix(tp, "gosimnotranslate/") {
						return "notranslate-linkname"
					}
				}
				return "unknown-linkname"
			}
			if keep[pkg] {
				return "keep-asm"
			}
			if name == "newUnixFile" {
				return "newUnixFile-exception"
			}
			return "missing-body"
		}
		signature := func(d *ast.FuncDecl) string {
			if d == nil {
				return ""
			}
			var b bytes.Buffer
			check(printer.Fprint(&b, fs, d.Type))
			return b.String()
		}
		for _, decl := range f.Decls {
			d, ok := decl.(*ast.FuncDecl)
			if !ok || d.Body != nil {
				continue
			}
			name := d.Name.Name
			s := status(name, "bodyless", "", false)
			if len(links[name]) > 0 && !hs[pkg+"."+name] && active[pkg] {
				s = "has-linkname"
			}
			pos := fs.Position(d.Pos())
			rows = append(rows, Row{version, *arch, pkg, name, "bodyless", "", s, pos.Filename, pos.Line, signature(d)})
		}
		for name, cs := range links {
			for _, c := range cs {
				p := strings.Fields(c.Text)
				target := ""
				if len(p) >= 3 {
					target = p[2]
				}
				d := funcs[name]
				body := d != nil && d.Body != nil
				pos := fs.Position(c.Pos())
				s := status(name, "linkname", target, body)
				if d == nil && active[pkg] {
					s = "dangling-or-variable-linkname-removed"
				}
				rows = append(rows, Row{version, *arch, pkg, name, "linkname", target, s, pos.Filename, pos.Line, signature(d)})
			}
		}
	}
	for k, ff := range files {
		for f := range ff {
			scan("1.27.2", k, f)
		}
	}
	if *old != "" {
		ctx := build.Default
		ctx.GOROOT = *old
		ctx.GOOS = "linux"
		ctx.GOARCH = *arch
		ctx.CgoEnabled = false
		ctx.BuildTags = []string{"sim"}
		ctx.ReleaseTags = nil
		for i := 1; i <= 23; i++ {
			ctx.ReleaseTags = append(ctx.ReleaseTags, fmt.Sprintf("go1.%d", i))
		}
		ctx.ToolTags = []string{"goexperiment.regabiwrappers", "goexperiment.regabiargs", "goexperiment.coverageredesign"}
		if *arch == "amd64" {
			ctx.ToolTags = append(ctx.ToolTags, "amd64.v1")
		}
		if *arch == "arm64" {
			ctx.ToolTags = append(ctx.ToolTags, "arm64.v8.0")
		}
		for k, p := range pkgs {
			if !p.Standard {
				continue
			}
			dir := filepath.Join(*old, "src", k)
			if _, e := os.Stat(dir); os.IsNotExist(e) {
				continue
			}
			bp, e := ctx.ImportDir(dir, 0)
			if e != nil {
				if _, ok := e.(*build.NoGoError); ok {
					continue
				}
				panic(fmt.Sprintf("%s: %v", dir, e))
			}
			for _, n := range append(bp.GoFiles, bp.CgoFiles...) {
				scan("1.23.12", k, filepath.Join(dir, n))
			}
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		return fmt.Sprintf("%s %s %s %s %s %09d", a.Version, a.Pkg, a.Name, a.Kind, a.File, a.Line) < fmt.Sprintf("%s %s %s %s %s %09d", b.Version, b.Pkg, b.Name, b.Kind, b.File, b.Line)
	})
	w := csv.NewWriter(os.Stdout)
	w.Comma = '\t'
	check(w.Write([]string{"version", "arch", "package", "name", "kind", "target", "status", "file", "line", "signature"}))
	for _, r := range rows {
		check(w.Write([]string{r.Version, r.Arch, r.Pkg, r.Name, r.Kind, r.Target, r.Status, r.File, strconv.Itoa(r.Line), r.Signature}))
	}
	w.Flush()
	check(w.Error())
}
