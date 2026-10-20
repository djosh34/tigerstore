# Fix 0 (compile prerequisite, not a runtime blocker): upstream hook dispatch
# keys only (package, declaration name), not receiver. internal/synctest now has
# both Run and (*Bubble).Run (Go 1.27.2 src/internal/synctest/synctest.go:16,109).
# Do not replace the method body with a second function-Run hook linkname.
replace('internal/translate/rewrites.go',
 'func (t *packageTranslator) rewriteStdlibEmptyAndLinkname(c *dstutil.Cursor) {\n',
 'func (t *packageTranslator) rewriteStdlibEmptyAndLinkname(c *dstutil.Cursor) {\n if d, ok := c.Node().(*dst.FuncDecl); ok && t.pkgPath == "internal/synctest" && d.Recv != nil { return }\n')
