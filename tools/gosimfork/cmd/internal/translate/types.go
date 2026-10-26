package translate

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"log"
	"slices"
	"strconv"

	"github.com/dave/dst"
	"golang.org/x/tools/go/ast/astutil"
	"golang.org/x/tools/go/packages"
)

// makeTypeExpr converts a type-checker types.Type into an AST expression
// describing that type. For example, it converts a *types.Slice of a
// *types.Named "foo.Id" into a *dst.ArrayType of a *dst.Ident "foo.Id".
//
// In most cases its preferable to reuse AST expressions instead of building new
// ones from types, but sometimes translated codes needs a type when no
// convenient AST expression exists. See the callers of makeTypeExpr.
func (t *packageTranslator) makeTypeExpr(typ types.Type) dst.Expr {
	if typ == nil {
		return nil
	}

	// XXX:
	// - more type params
	// - anonymous structs
	// - anonymous interfaces
	// - ???

	switch typ := typ.(type) {
	case *types.Alias:
		// helpp, copy pasted from Named below

		var path string
		if pkg := typ.Obj().Pkg(); pkg != nil {
			// eg. builtin error has no path
			path = pkg.Path()
			if path == t.pkgPath {
				path = ""
			}
		}

		id := &dst.Ident{Name: typ.Obj().Name(), Path: path}
		t.rewriteIdentImpl(id)
		if id.Path == "testing" {
			// rewriteIdentImpl skips new idents of package testing
			id.Path = t.replacedPkgs["testing"]
		}

		if id.Path != "" && id.Path != t.pkgPath && !token.IsExported(id.Name) {
			// bad news bears here...
			if slices.Index(PublicExportHacks[path], id.Name) == -1 {
				log.Fatalf("need public export hack for %s.%s", path, id.Name)
			}
			id.Name = "GosimPublicExportHack" + id.Name
		}

		res := dst.Expr(id)
		if typ.TypeArgs() != nil {
			var args []dst.Expr
			for i := 0; i < typ.TypeArgs().Len(); i++ {
				args = append(args, t.makeTypeExpr(typ.TypeArgs().At(i)))
			}
			res = &dst.IndexListExpr{
				X:       res,
				Indices: args,
			}
		}

		return res

	case *types.Named:
		var path string
		if pkg := typ.Obj().Pkg(); pkg != nil {
			// eg. builtin error has no path
			path = pkg.Path()
			if path == t.pkgPath {
				path = ""
			}
		}

		id := &dst.Ident{Name: typ.Obj().Name(), Path: path}
		t.rewriteIdentImpl(id)
		if id.Path == "testing" {
			// rewriteIdentImpl skips new idents of package testing
			id.Path = t.replacedPkgs["testing"]
		}

		if id.Path != "" && id.Path != t.pkgPath && !token.IsExported(id.Name) {
			// bad news bears here...
			if slices.Index(PublicExportHacks[path], id.Name) == -1 {
				log.Fatalf("need public export hack for %s.%s", path, id.Name)
			}
			id.Name = "GosimPublicExportHack" + id.Name
		}

		res := dst.Expr(id)
		if typ.TypeArgs() != nil {
			var args []dst.Expr
			for i := 0; i < typ.TypeArgs().Len(); i++ {
				args = append(args, t.makeTypeExpr(typ.TypeArgs().At(i)))
			}
			res = &dst.IndexListExpr{
				X:       res,
				Indices: args,
			}
		}

		return res

	case *types.Basic:
		path := ""
		if token.IsExported(typ.Name()) {
			// XXX: hack for unsafe.Pointer
			path = "unsafe"
		}
		return &dst.Ident{Name: typ.Name(), Path: path}
	case *types.Pointer:
		return &dst.StarExpr{X: t.makeTypeExpr(typ.Elem())}
	case *types.Slice:
		return &dst.ArrayType{Elt: t.makeTypeExpr(typ.Elem())}
	case *types.Map:
		return &dst.IndexListExpr{
			X: t.newRuntimeSelector("Map"),
			Indices: []dst.Expr{
				t.makeTypeExpr(typ.Key()),
				t.makeTypeExpr(typ.Elem()),
			},
		}
		// return &dst.MapType{Key: t.makeType(typ.Key()), Value: t.makeType(typ.Elem())}
	case *types.Chan:
		return &dst.IndexListExpr{
			X: t.newRuntimeSelector("Chan"),
			Indices: []dst.Expr{
				t.makeTypeExpr(typ.Elem()),
			},
		}
		/*
			dir := map[types.ChanDir]dst.ChanDir{
				types.SendRecv: dst.SEND | dst.RECV,
				types.SendOnly: dst.SEND,
				types.RecvOnly: dst.RECV,
			}[typ.Dir()]
			// XXX: use netDetgoSelector instead??
			return &dst.ChanType{Dir: dir, Value: t.makeType(typ.Elem())}
		*/
	case *types.Array:
		return &dst.ArrayType{Len: &dst.BasicLit{Kind: token.INT, Value: fmt.Sprint(typ.Len())}, Elt: t.makeTypeExpr(typ.Elem())}
	case *types.Interface:
		if typ.Empty() {
			return &dst.InterfaceType{Methods: &dst.FieldList{
				Opening: true,
				Closing: true,
			}}
			// XXX: any might overlap HELP
			// return &dst.Ident{Name: "any"}
		}
		var methods []*dst.Field
		for i := 0; i < typ.NumExplicitMethods(); i++ {
			m := typ.ExplicitMethod(i)
			methods = append(methods, &dst.Field{
				Names: []*dst.Ident{dst.NewIdent(m.Name())}, // XXX: wrong?
				Type:  t.makeTypeExpr(m.Type()),
			})
		}
		return &dst.InterfaceType{
			Methods: &dst.FieldList{
				List: methods,
			},
		}

	case *types.Struct:
		if typ.NumFields() == 0 {
			return &dst.StructType{
				Fields: &dst.FieldList{
					Opening: true,
					Closing: true,
				},
			}
		}
		var fields []*dst.Field
		for i := 0; i < typ.NumFields(); i++ {
			field := typ.Field(i)

			var names []*dst.Ident
			if !field.Anonymous() {
				names = []*dst.Ident{dst.NewIdent(field.Name())}
			}
			var tag *dst.BasicLit
			if tagValue := typ.Tag(i); tagValue != "" {
				tag = &dst.BasicLit{Kind: token.STRING, Value: strconv.Quote(typ.Tag(i))}
			}
			fields = append(fields, &dst.Field{
				Names: names,
				Type:  t.makeTypeExpr(field.Type()),
				Tag:   tag,
			})
		}
		return &dst.StructType{
			Fields: &dst.FieldList{
				List: fields,
			},
		}
	case *types.Signature:
		if typ.RecvTypeParams() != nil {
			panic(typ)
		}
		if typ.TypeParams() != nil {
			panic(typ)
		}

		var params, results []*dst.Field

		for i := 0; i < typ.Params().Len(); i++ {
			param := typ.Params().At(i)
			paramType := t.makeTypeExpr(param.Type())
			if typ.Variadic() && i == typ.Params().Len()-1 {
				paramType = &dst.Ellipsis{
					Elt: paramType.(*dst.ArrayType).Elt, // the signature for ... has an array type?
				}
			}
			params = append(params, &dst.Field{
				Names: []*dst.Ident{dst.NewIdent(param.Name())},
				Type:  paramType,
			})
		}

		for i := 0; i < typ.Results().Len(); i++ {
			param := typ.Results().At(i)
			results = append(results, &dst.Field{
				Names: []*dst.Ident{dst.NewIdent(param.Name())},
				Type:  t.makeTypeExpr(param.Type()),
			})
		}

		return &dst.FuncType{
			Func: true,
			Params: &dst.FieldList{
				List: params,
			},
			Results: &dst.FieldList{
				List: results,
			},
		}
	case *types.TypeParam:
		// XXX help
		return dst.NewIdent(typ.Obj().Name())

	default:
		panic(typ)
	}
}

// buildImplicitConversions computes the type of expressions that are converted
// according to the assignability (https://go.dev/ref/spec#Assignability) rule
// in the go spec.
//
// An implicit conversion happens on assignments like:
//
//	types Values map[string]string
//	var foo Values /* (the type) */ = map[string]string{} /* (the value) */
//
// Here, the spec allows assigning a value of unnamed type map[string]string{}
// to the variable foo of type Values. This is an implicit conversion of a
// map[string]string into a Values. For gosim this causes a problem because the
// translated type of map[string]string is a named type Map[string, string]
// which cannot be converted to a Values without an explicit conversion.
//
// buildImplicitConversions returns a map from ast.Expr of values that are
// implicitly converted to the type they are converted to. In this example, it
// would map the expression of the literal map[string]string{} to the Values
// type.
//
// Issue https://github.com/golang/go/issues/47151 tracks adding this
// functionality to the go/types package.
func buildImplicitConversions(pkg *packages.Package) map[ast.Expr]types.Type {
	builder := &implicitConversionsBuilder{
		conversions: make(map[ast.Expr]types.Type),
		typesInfo:   pkg.TypesInfo,
		sigstack:    nil,
		cursig:      nil,
	}
	for _, file := range pkg.Syntax {
		astutil.Apply(file, builder.before, builder.after)
	}
	return builder.conversions
}

type implicitConversionsBuilder struct {
	// conversions is the computed conversions map
	conversions map[ast.Expr]types.Type

	typesInfo *types.Info

	// cursig and sigstack track the signature of the functions the astutil
	// is visiting, used to compute the expected types for return statements.
	// before pushes onto the stack, after pops.
	cursig   *types.Signature
	sigstack []*types.Signature
}

func (b *implicitConversionsBuilder) before(c *astutil.Cursor) bool {
	switch node := c.Node().(type) {
	case *ast.FuncDecl:
		if b.typesInfo.Defs[node.Name] == nil {
			log.Println(node.Name)
		}
		sig := b.typesInfo.Defs[node.Name].Type().(*types.Signature)
		b.sigstack = append(b.sigstack, sig)
		b.cursig = sig

	case *ast.FuncLit:
		sig := b.typesInfo.Types[node].Type.(*types.Signature)
		b.sigstack = append(b.sigstack, sig)
		b.cursig = sig

	case *ast.CompositeLit:
		if struc, ok := b.typesInfo.Types[node].Type.Underlying().(*types.Struct); ok {
			for idx, elt := range node.Elts {
				if node, ok := elt.(*ast.KeyValueExpr); ok {
					y := b.typesInfo.Types[node.Value]

					name := node.Key.(*ast.Ident).Name

					for i := 0; i < struc.NumFields(); i++ {
						field := struc.Field(i)
						if name == field.Name() {
							x := field.Type()
							if !types.Identical(y.Type, x) {
								b.conversions[node.Value] = x
							}
						}
					}
				} else if idx < struc.NumFields() {
					x := struc.Field(idx).Type()
					y := b.typesInfo.Types[elt]
					if !types.Identical(y.Type, x) {
						b.conversions[elt] = x
					}
				}
			}
			break
		}

		if mp, ok := b.typesInfo.Types[node].Type.Underlying().(*types.Map); ok {
			for _, elt := range node.Elts {
				if node, ok := elt.(*ast.KeyValueExpr); ok {
					x := mp.Elem()
					y := b.typesInfo.Types[node.Value]
					if !types.Identical(y.Type, x) {
						b.conversions[node.Value] = x
					}

					x = mp.Key()
					y = b.typesInfo.Types[node.Key]
					if !types.Identical(y.Type, x) {
						b.conversions[node.Key] = x
					}
				}
			}
			break
		}

		var elem types.Type
		if arr, ok := b.typesInfo.Types[node].Type.Underlying().(*types.Array); ok {
			elem = arr.Elem()
		}
		if slice, ok := b.typesInfo.Types[node].Type.Underlying().(*types.Slice); ok {
			elem = slice.Elem()
		}
		if elem != nil {
			for _, elt := range node.Elts {
				x := elem
				y := b.typesInfo.Types[elt]
				if !types.Identical(y.Type, x) {
					b.conversions[elt] = x
				}
			}
		}

	case *ast.CallExpr:
		tv := b.typesInfo.Types[node.Fun]

		if tv.IsType() {
			// cast
			x := b.typesInfo.Types[node.Fun].Type
			y := b.typesInfo.Types[node.Args[0]]
			if y.IsNil() {
				if x == nil {
					panic("help")
				}
				b.conversions[node.Args[0]] = x
			}
		} else if sig, ok := tv.Type.Underlying().(*types.Signature); ok {
			// this breaks for a type Foo func() type-cast?!?!!??!
			for i, y := range node.Args {
				var x types.Type
				if i >= sig.Params().Len()-1 && sig.Variadic() {
					slice, ok := sig.Params().At(sig.Params().Len() - 1).Type().(*types.Slice)
					if !ok {
						continue
						// XXX: this happens with a string and a splat maybe???
					}
					if node.Ellipsis != token.NoPos {
						x = slice
					} else {
						x = slice.Elem()
					}
				} else {
					// log.Println(sig, i, sig.Params(), node.Args)
					x = sig.Params().At(i).Type()
				}

				yType := b.typesInfo.Types[y]
				if !types.Identical(yType.Type, x) {
					if x == nil {
						panic("help")
					}
					b.conversions[y] = x
				}
			}
		} else {
			// XXX?
		}

	case *ast.AssignStmt:
		// xxx: this can happen if we have a call
		if len(node.Lhs) != len(node.Rhs) {
			break
		}

		for i := range node.Lhs {
			x := b.typesInfo.Types[node.Lhs[i]]
			y := b.typesInfo.Types[node.Rhs[i]]
			if !types.Identical(x.Type, y.Type) {
				if x.Type == nil {
					continue
					// happens for :=, var ... =
					log.Println(node.Lhs, node.Rhs[i])
					panic("help")
				}
				b.conversions[node.Rhs[i]] = x.Type
			}
		}

	case *ast.SendStmt:
		// xxx: this can happen if we have a call

		chType := b.typesInfo.Types[node.Chan]

		if chType, ok := chType.Type.Underlying().(*types.Chan); ok {
			x := chType.Elem()
			y := b.typesInfo.Types[node.Value].Type

			if !types.Identical(x, y) {
				b.conversions[node.Value] = x
			}
		}

	case *ast.BinaryExpr:
		x := b.typesInfo.Types[node.X]
		y := b.typesInfo.Types[node.Y]
		if !types.Identical(x.Type, y.Type) {
			if x.IsNil() {
				if y.Type == nil {
					panic("help")
				}
				b.conversions[node.X] = y.Type
			} else {
				if x.Type == nil {
					panic("help")
				}
				b.conversions[node.Y] = x.Type
			}
		}

	case *ast.ReturnStmt:
		// XXX: gotta check len because of returning multiple
		if len(node.Results) > 0 && len(node.Results) == b.cursig.Results().Len() {
			for i, expr := range node.Results {
				x := b.typesInfo.Types[expr]
				y := b.cursig.Results().At(i).Type()
				if !types.Identical(x.Type, y) {
					if y == nil {
						panic("help")
					}
					b.conversions[expr] = y
				}
			}
		}
	}
	return true
}

func (b *implicitConversionsBuilder) after(c *astutil.Cursor) bool {
	switch c.Node().(type) {
	case *ast.FuncDecl:
		b.sigstack = b.sigstack[:len(b.sigstack)-1]
	case *ast.FuncLit:
		b.sigstack = b.sigstack[:len(b.sigstack)-1]
	}
	if len(b.sigstack) == 0 {
		b.cursig = nil
	} else {
		b.cursig = b.sigstack[len(b.sigstack)-1]
	}
	return true
}
