package translate

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"log/slog"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/dave/dst"
	"github.com/dave/dst/dstutil"
)

var rewriteSelectorRegexp = regexp.MustCompile(`[/.]`)

// RewriteSelector rewrites internal/bytealg.Equal to InternalBytealg_Equal
func RewriteSelector(pkg, selector string) string {
	pkgParts := rewriteSelectorRegexp.Split(pkg, -1)
	var rewritten strings.Builder
	for _, part := range pkgParts {
		rewritten.WriteString(strings.ToUpper(part[0:1]))
		rewritten.WriteString(part[1:])
	}
	rewritten.WriteString("_")
	rewritten.WriteString(selector)
	return rewritten.String()
}

func (t *packageTranslator) rewriteImport(c *dstutil.Cursor) {
	node := c.Node()

	imp, ok := node.(*dst.ImportSpec)
	if !ok {
		return
	}

	val, err := strconv.Unquote(imp.Path.Value)
	if err != nil {
		return
	}

	if imp.Name != nil && imp.Name.Name == "." {
		imp.Name = nil
	}

	if replaced, ok := t.replacedPkgs[val]; ok {
		imp.Path.Value = strconv.Quote(replaced)
	}
}

func (t *packageTranslator) rewriteIdentImpl(ident *dst.Ident) {
	lookupPath := ident.Path
	if lookupPath == "" {
		lookupPath = t.pkgPath
	}

	// special case new plan
	/*
		if info, ok := t.globalInfo.ByPackage[lookupPath]; ok {
			if newName, ok := info.Rewritten[ident.Name]; ok {
				if _, ok := t.astMap.Nodes[ident]; !ok {
					// XXX: hack job to skip newly created idents
					return
				}

				ident.Name = newName
				if ident.Path != "" {
					ident.Path = t.replacedPkgs[ident.Path] // XXX: how do we know this?
				}
				return
			}
		}
	*/

	// a test function used as a value, as in t.Run(name, TestFoo), has the
	// new name of its declaration (rewriteTestFunc)
	if ident.Path == "" && t.testFuncs[ident.Name] {
		if astIdent, ok := t.astMap.Nodes[ident].(*ast.Ident); ok {
			if fn, ok := t.typesInfo.Uses[astIdent].(*types.Func); ok && fn.Parent() == fn.Pkg().Scope() {
				ident.Name = t.globalInfo.ByPackage[t.pkgPath].Rewritten[ident.Name]
				return
			}
		}
	}

	// rewrite specific selectors
	selector := packageSelector{Pkg: ident.Path, Selector: ident.Name}
	replacement, ok := replacements[selector]
	if ok && replacement.Pkg == translatedHooksPackage && t.pkgPath == hooksGo127Package {
		// The hooks call the originals they replace elsewhere.
		ok = false
	}
	if !ok {
		// rewrite entire packages
		if replaced, ok := t.replacedPkgs[ident.Path]; ok {
			if ident.Path == "testing" {
				if _, ok := t.astMap.Nodes[ident]; !ok {
					// XXX: hack job to skip newly created idents
					return
				}
			}

			ident.Path = replaced
			ident.Obj = nil
			return
		}
		return
	}

	pkg := replacement.Pkg
	if pkg == "" {
		panic(replacement.Pkg + " " + replacement.Selector)
	}
	// XXX
	// pkg = t.replacedPkgs[pkg]
	// if pkg == "" {
	// panic(replacement.Pkg + " " + replacement.Selector)
	// }

	ident.Name = replacement.Selector
	ident.Path = pkg
	ident.Obj = nil
}

func (t *packageTranslator) rewriteIdent(c *dstutil.Cursor) {
	if ident, ok := c.Node().(*dst.Ident); ok {
		t.rewriteIdentImpl(ident)
	}
}

func (t *packageTranslator) rewriteStdlibEmptyAndLinkname(c *dstutil.Cursor) {
	node := c.Node()

	decl, ok := node.(*dst.FuncDecl)
	if !ok {
		return
	}

	// Hooks replace package-level functions. A method with the same name as a
	// hooked function, such as internal/synctest's Bubble.Run, keeps its body.
	var linkTo packageSelector
	hasHook := false
	if decl.Recv == nil {
		linkTo, hasHook = t.hooks[packageSelector{Pkg: t.pkgPath, Selector: decl.Name.Name}]
	}

	// XXX: this misses some linkname stuff
	// XXX: this wrongly assumes a linkname is going to be part of a FuncDecl's Decs; they can be anywhere in the code

	hasLinkname := false
	// unimplemented is set when the function has no body and its linkname
	// points at a body gosim does not translate.
	unimplemented := false
	for i, val := range decl.Decs.Start {
		if val == "//go:uintptrkeepalive" {
			decl.Decs.Start[i] = "//go:uintptrescapes" // uintptrkeepalive is only allowed in standard library...
		}

		if strings.HasPrefix(val, "//go:linkname") {
			hasLinkname = true

			if hasHook {
				decl.Decs.Start[i] = "//"
				hasLinkname = false
				continue
			}

			parts := strings.Split(val, " ")

			if len(parts) == 2 && decl.Body == nil && nativeLinknamePackages[t.pkgPath] {
				// Call the function the runtime pushes to the original
				// package.
				decl.Decs.Start[i] = fmt.Sprintf("//go:linkname %s %s.%s", decl.Name.Name, t.pkgPath, decl.Name.Name)
			} else if len(parts) == 2 && decl.Body == nil && !pushedLinknames[packageSelector{Pkg: t.pkgPath, Selector: decl.Name.Name}] {
				// The body is pushed from a package gosim does not
				// translate, such as runtime.
				decl.Decs.Start[i] = "// gosim unimplemented: " + val
				unimplemented = true
			}

			if len(parts) == 3 {
				dest := parts[2]
				idx1 := strings.LastIndex(dest, "/") + 1
				idx2 := strings.Index(dest[idx1:], ".")
				if idx2 != -1 {
					idx := idx1 + idx2
					pkg, name := dest[:idx], dest[idx+1:]
					if decl.Body == nil && pkg == t.pkgPath && name == decl.Name.Name {
						// A linkname to itself: the body lives in a package
						// gosim does not translate, such as runtime.
						decl.Decs.Start[i] = "// gosim unimplemented: " + val
						unimplemented = true
					} else if newPkg, ok := t.replacedPkgs[pkg]; ok {
						slog.Debug("rewriting linkname", "pkg", t.pkgPath, "name", decl.Name.Name)

						parts[2] = fmt.Sprintf("%s.%s", newPkg, name)
						decl.Decs.Start[i] = strings.Join(parts, " ")
					} else if t.acceptedLinknames[packageSelector{Pkg: t.pkgPath, Selector: decl.Name.Name}] == (packageSelector{Pkg: pkg, Selector: name}) {
						slog.Debug("keeping linkname", "pkg", t.pkgPath, "name", decl.Name.Name, "targetPkg", pkg, "targetName", name)
					} else if strings.HasPrefix(pkg, "gosimnotranslate/") {
						parts[2] = fmt.Sprintf("%s.%s", strings.TrimPrefix(pkg, "gosimnotranslate/"), name)
						decl.Decs.Start[i] = strings.Join(parts, " ")
					} else if decl.Body == nil {
						decl.Decs.Start[i] = "// gosim unimplemented: " + val
						unimplemented = true
					} else {
						// The function pushes its body into a package gosim
						// does not translate. The translated copy must not
						// replace that package's function.
						slog.Debug("removing linkname", "pkg", t.pkgPath, "name", decl.Name.Name, "targetPkg", pkg, "targetName", name)
						decl.Decs.Start[i] = "// gosim removed: " + val
						hasLinkname = false
					}
				}
			}
		}
	}

	if unimplemented {
		t.stub(decl)
		return
	}

	if decl.Body != nil && hasHook {
		// take over!
		decl.Body = nil
	}

	// XXX: hack
	if decl.Body == nil && !hasLinkname {
		for i, val := range decl.Decs.Start {
			if val == "//go:noescape" {
				decl.Decs.Start[i] = "//"
			}
		}

		if hasHook {
			selector := RewriteSelector(t.pkgPath, decl.Name.Name)

			pkg := linkTo.Pkg
			if replaced, ok := t.replacedPkgs[pkg]; ok {
				// XXX: replace machinePackage properly
				pkg = replaced
			}

			decl.Decs.Start = append(decl.Decs.Start, fmt.Sprintf("//go:linkname %s %s.%s",
				decl.Name.Name, pkg, selector))
			t.needsUnsafe = true

		} else if decl.Name.Name == "newUnixFile" {
			// need a go:linkname that is not required in the standard library...
			decl.Decs.Start = append(decl.Decs.Start, fmt.Sprintf("//go:linkname %s",
				decl.Name.Name))
			t.needsUnsafe = true

		} else if t.keepAsmPkgs[t.pkgPath] {
			slog.Debug("keeping asm header", "pkg", t.pkgPath, "name", decl.Name.Name)
		} else {
			// An assembly function, or a function implemented in runtime.
			t.stub(decl)
		}
	}

	// HACK: insert //go:nocheckptr for Mmap calls in the syscall packages.  The
	// simulation package implements Mmap with a backing slice allocated on the
	// heap, and checkptr is not happy with the unsafe conversion from uintptr
	// to unsafe.Pointer.
	// I believe checkptr can be safely overriden because the simulation keeps
	// the backing slice alive until Munmap is called.
	if decl.Name.Name == "Mmap" && (t.pkgPath == "syscall" || t.pkgPath == "golang.org/x/sys/unix") {
		decl.Decs.Start = append(decl.Decs.Start, "//go:nocheckptr")
	}
}

// stub gives decl a body that fails through gosimruntime.Unimplemented.
func (t *packageTranslator) stub(decl *dst.FuncDecl) {
	for i, val := range decl.Decs.Start {
		if val == "//go:noescape" {
			// Only allowed on functions without a body.
			decl.Decs.Start[i] = "//"
		}
	}

	name := strconv.Quote(t.pkgPath + "." + decl.Name.Name)
	body := []dst.Stmt{&dst.ExprStmt{X: &dst.CallExpr{
		Fun:  t.newRuntimeSelector("Unimplemented"),
		Args: []dst.Expr{&dst.BasicLit{Kind: token.STRING, Value: name}},
	}}}
	if decl.Type.Results != nil && len(decl.Type.Results.List) > 0 {
		// Unimplemented never returns.
		body = append(body, &dst.ExprStmt{X: &dst.CallExpr{
			Fun:  dst.NewIdent("panic"),
			Args: []dst.Expr{&dst.BasicLit{Kind: token.STRING, Value: strconv.Quote("unreachable")}},
		}})
	}
	decl.Body = &dst.BlockStmt{List: body}
}

func removeLinknames(commentLines []string) {
	for i, line := range commentLines {
		if strings.HasPrefix(line, "//go:linkname") {
			commentLines[i] = "// gosim removed: " + line
		}
	}
}

func (t *packageTranslator) rewriteDanglingLinknames(c *dstutil.Cursor) {
	node := c.Node()

	if funcDecl, ok := node.(*dst.FuncDecl); ok {
		endIdx := 0
		for i, line := range slices.Backward(funcDecl.Decs.Start) {
			if line == "\n" {
				endIdx = i
				break
			}
		}
		removeLinknames(funcDecl.Decs.Start[:endIdx])
		removeLinknames(funcDecl.Decs.End)
	}

	if genDecl, ok := node.(*dst.GenDecl); ok {
		for _, spec := range genDecl.Specs {
			removeLinknames(spec.Decorations().Start)
			removeLinknames(spec.Decorations().End)
		}
	}
}

func (t *packageTranslator) rewriteNotifyListHack(c *dstutil.Cursor) {
	if decl, ok := c.Node().(*dst.GenDecl); ok {
		if decl.Tok == token.TYPE {
			for _, spec := range decl.Specs {
				if spec, ok := spec.(*dst.TypeSpec); ok {
					if t.pkgPath == "sync" && spec.Name.Name == "notifyList" {
						spec.Assign = true
						spec.Type = t.newRuntimeSelector("NotifyList")
					}
				}
			}
		}
	}
}
