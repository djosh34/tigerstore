package translate

import (
	"fmt"
	"go/types"
	"maps"
	"os"
	"slices"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

// hookShapeExceptions lists hooks whose implementation uses a different type
// with the same memory layout as the hooked function.
var hookShapeExceptions = map[packageSelector]bool{
	// sync.notifyList is rewritten to gosimruntime.NotifyList, which the hooks
	// wrap in their own NotifyList.
	{Pkg: "sync", Selector: "runtime_notifyListAdd"}:       true,
	{Pkg: "sync", Selector: "runtime_notifyListWait"}:      true,
	{Pkg: "sync", Selector: "runtime_notifyListNotifyAll"}: true,
	{Pkg: "sync", Selector: "runtime_notifyListNotifyOne"}: true,
	// time's timers are gosim timers with time.Timer's first fields.
	{Pkg: "time", Selector: "newTimer"}:   true,
	{Pkg: "time", Selector: "stopTimer"}:  true,
	{Pkg: "time", Selector: "resetTimer"}: true,
}

// TestHookSignatures checks that every hooked function exists, that its hook
// exists, and that both have the same signature. Hooks are bound with
// go:linkname, which does not check signatures, so a mismatch after a Go
// upgrade would corrupt memory instead of failing to compile.
func TestHookSignatures(t *testing.T) {
	// exists records the hooked functions found on any architecture. Some
	// hooks exist only on one.
	exists := make(map[packageSelector]bool)
	all := maps.Clone(hooksGo127)
	for _, goarch := range []string{"amd64", "arm64"} {
		hooks := maps.Clone(hooksGo127)
		maps.Copy(hooks, hooksGensyscallGo127ByArch[goarch])
		maps.Copy(all, hooksGensyscallGo127ByArch[goarch])

		patterns := []string{hooksGo127Package}
		for hooked := range hooks {
			patterns = append(patterns, hooked.Pkg)
		}
		slices.Sort(patterns)
		patterns = slices.Compact(patterns)

		pkgs, err := packages.Load(&packages.Config{
			Mode:       packages.NeedName | packages.NeedTypes | packages.NeedSyntax | packages.NeedTypesInfo,
			Env:        append(os.Environ(), "GOOS=linux", "GOARCH="+goarch, "CGO_ENABLED=0"),
			BuildFlags: []string{"-tags", "sim,purego"},
		}, patterns...)
		if err != nil {
			t.Fatal(err)
		}
		byPath := make(map[string]*types.Package)
		for _, pkg := range pkgs {
			if len(pkg.Errors) > 0 {
				t.Fatalf("%s: loading %s: %v", goarch, pkg.PkgPath, pkg.Errors)
			}
			byPath[pkg.PkgPath] = pkg.Types
		}

		for hooked, impl := range hooks {
			if byPath[hooked.Pkg] == nil {
				continue
			}
			hookedObj := byPath[hooked.Pkg].Scope().Lookup(hooked.Selector)
			if hookedObj == nil {
				continue
			}
			exists[hooked] = true
			name := RewriteSelector(hooked.Pkg, hooked.Selector)
			implObj := byPath[impl.Pkg].Scope().Lookup(name)
			if implObj == nil {
				t.Errorf("%s: hook %s for %s.%s does not exist", goarch, name, hooked.Pkg, hooked.Selector)
				continue
			}
			if hookShapeExceptions[hooked] {
				continue
			}
			want, got := signatureShape(hookedObj.Type().(*types.Signature)), signatureShape(implObj.Type().(*types.Signature))
			if want != got {
				t.Errorf("%s: hook %s for %s.%s has signature\n\t%s\nwant\n\t%s", goarch, name, hooked.Pkg, hooked.Selector, got, want)
			}
		}
	}
	for hooked := range all {
		if !exists[hooked] {
			t.Errorf("hooked function %s.%s does not exist", hooked.Pkg, hooked.Selector)
		}
	}
}

// typeShape describes how a type is passed in a call: pointer-like types are
// all "ptr" and integers are described by their size. A hook may use its own
// types as long as they have the same shape.
func typeShape(typ types.Type) string {
	switch typ := typ.Underlying().(type) {
	case *types.Basic:
		switch {
		case typ.Kind() == types.UnsafePointer || typ.Kind() == types.Uintptr:
			return "ptr"
		case typ.Info()&types.IsInteger != 0:
			return fmt.Sprintf("int%d", sizes.Sizeof(typ)*8)
		default:
			return typ.Name()
		}
	case *types.Pointer, *types.Map, *types.Chan, *types.Signature:
		return "ptr"
	case *types.Slice:
		return "slice"
	case *types.Array:
		return fmt.Sprintf("[%d]%s", typ.Len(), typeShape(typ.Elem()))
	case *types.Interface:
		return "interface"
	case *types.TypeParam:
		return "typeparam"
	case *types.Struct:
		var fields []string
		for i := range typ.NumFields() {
			fields = append(fields, typeShape(typ.Field(i).Type()))
		}
		return "struct{" + strings.Join(fields, "; ") + "}"
	default:
		return typ.String()
	}
}

// signatureShape describes a function signature by the shapes of its
// parameters and results.
func signatureShape(sig *types.Signature) string {
	tuple := func(tuple *types.Tuple) string {
		var parts []string
		for i := range tuple.Len() {
			parts = append(parts, typeShape(tuple.At(i).Type()))
		}
		return strings.Join(parts, ", ")
	}
	variadic := ""
	if sig.Variadic() {
		variadic = "..."
	}
	return "func(" + tuple(sig.Params()) + variadic + ") (" + tuple(sig.Results()) + ")"
}

var sizes = types.SizesFor("gc", "arm64")
