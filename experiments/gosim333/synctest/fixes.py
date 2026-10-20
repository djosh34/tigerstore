def add_hooks(pkg, names):
 entries = ''.join(f'\t{{Pkg: "{pkg}", Selector: "{n}"}}: {{Pkg: hooksGo123Package}},\n' for n in names)
 replace('internal/translate/hooks_go123.go', 'var hooksGo123 = map[packageSelector]packageSelector{\n', 'var hooksGo123 = map[packageSelector]packageSelector{\n'+entries)

# Fix 0 (compile prerequisite, not a runtime blocker): upstream hook dispatch
# keys only (package, declaration name), not receiver. internal/synctest now has
# both Run and (*Bubble).Run (Go 1.27.2 src/internal/synctest/synctest.go:16,109).
# Do not replace the method body with a second function-Run hook linkname.
replace('internal/translate/rewrites.go',
 'func (t *packageTranslator) rewriteStdlibEmptyAndLinkname(c *dstutil.Cursor) {\n',
 'func (t *packageTranslator) rewriteStdlibEmptyAndLinkname(c *dstutil.Cursor) {\n if d, ok := c.Node().(*dst.FuncDecl); ok && t.pkgPath == "internal/synctest" && d.Recv != nil { return }\n')

# Blocker 1: getIndicator in ECDSA CA generation; implement its paired setter
# too, following the same per-goroutine runtime state (source cited in Go file).
add_hooks('crypto/internal/fips140', ['getIndicator', 'setIndicator'])
shutil.copy(here/'gosim/fips333.go', src/'internal/hooks/go123/fips333.go')

# Blocker 3: weak registration reached from ECDSA fips140cache.Get. This is an
# explicit no-GC approximation; it does NOT claim to implement weak semantics.
add_hooks('weak', ['runtime_registerWeakPointer', 'runtime_makeStrongFromWeak'])
shutil.copy(here/'gosim/weak333.go', src/'internal/hooks/go123/weak333.go')
