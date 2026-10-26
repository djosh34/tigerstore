package translate

import (
	"cmp"
	"fmt"
	"go/types"
	"slices"

	"github.com/dave/dst"
	"github.com/dave/dst/dstutil"
)

func (t *packageTranslator) rewriteGo(c *dstutil.Cursor) {
	// go stmts
	if goStmt, ok := c.Node().(*dst.GoStmt); ok {
		var fun dst.Expr

		funcType, ok := t.getType(goStmt.Call.Fun)
		if !ok {
			panic("help")
		}

		simple := funcType.(*types.Signature).Params().Len() == 0 && funcType.(*types.Signature).Results().Len() == 0

		if simple {
			// go func() { ... } ()
			fun = t.apply(goStmt.Call.Fun).(dst.Expr)
		} else {
			sig := funcType.(*types.Signature)
			args := sig.Params().Len()
			variable := sig.Variadic()
			ret := sig.Results().Len()

			bs := bindspec{args: args, variable: variable, ret: ret}
			t.collect.bindspecs[bs] = struct{}{}

			fun = &dst.CallExpr{
				Fun: &dst.CallExpr{
					Fun: &dst.Ident{
						Name: bs.Name(),
					},
					Args: []dst.Expr{t.apply(goStmt.Call.Fun).(dst.Expr)},
				},
				Args: goStmt.Call.Args,
			}
		}

		c.Replace(&dst.ExprStmt{
			X: &dst.CallExpr{
				Fun:  t.newRuntimeSelector("Go"),
				Args: []dst.Expr{fun},
			},
		})
	}
}

// bindspec describes programming-style bind functions used to rewrite go
// statements.
//
// Given a function `func foo(a int) error { ... }` with invocation `go foo(10)`
// the generated bind function and invocation are:
//
//	func Bind1_1[T1, T2 any](f func(v1 T1) T2) func(v1 T1) func() {
//		return func(v1 T1) func() {
//			return func() {
//				f(v1)
//			}
//		}
//	}
//	gosimruntime.Go(Bind1_1(foo)(10))
//
// This trick is necessary because Go() only accepts func() arguments. Each
// converted package includes all the Bind used in that that package. bindspec
// is used for the tracking of necessary functions and their generation.
type bindspec struct {
	args     int  // number of arguments
	variable bool // is the last argumennt variable?
	ret      int  // number of return values
}

func (bs bindspec) Name() string {
	dots := ""
	if bs.variable {
		dots = "var"
	}

	return fmt.Sprintf("Bind%d%s_%d", bs.args, dots, bs.ret)
}

func makeBindFunc(bs bindspec) *dst.FuncDecl {
	typeargs := &dst.Field{
		Type: dst.NewIdent("any"),
	}
	for i := 1; i <= bs.args+bs.ret; i++ {
		typeargs.Names = append(typeargs.Names, dst.NewIdent(fmt.Sprintf("T%d", i)))
	}

	funcargs := func() *dst.FieldList {
		var funcargs []*dst.Field
		for i := 1; i <= bs.args; i++ {
			var typ dst.Expr = dst.NewIdent(fmt.Sprintf("T%d", i))
			if bs.variable && i == bs.args {
				typ = &dst.Ellipsis{Elt: typ}
			}
			funcargs = append(funcargs, &dst.Field{
				Names: []*dst.Ident{dst.NewIdent(fmt.Sprintf("v%d", i))},
				Type:  typ,
			})
		}
		return &dst.FieldList{
			List: funcargs,
		}
	}

	var callargs []dst.Expr
	for i := 1; i <= bs.args; i++ {
		callargs = append(callargs, dst.NewIdent(fmt.Sprintf("v%d", i)))
	}

	funcret := func() *dst.FieldList {
		var funcret []*dst.Field
		for i := 1; i <= bs.ret; i++ {
			var typ dst.Expr = dst.NewIdent(fmt.Sprintf("T%d", bs.args+i))
			funcret = append(funcret, &dst.Field{
				Type: typ,
			})
		}
		return &dst.FieldList{
			List: funcret,
		}
	}

	bindret := &dst.FuncType{
		Params: funcargs(),
		Results: &dst.FieldList{
			List: []*dst.Field{
				{
					Type: &dst.FuncType{},
				},
			},
		},
	}

	return &dst.FuncDecl{
		Name: dst.NewIdent(bs.Name()),
		Type: &dst.FuncType{
			TypeParams: &dst.FieldList{
				List: []*dst.Field{
					typeargs,
				},
			},
			Params: &dst.FieldList{
				List: []*dst.Field{
					{
						Names: []*dst.Ident{
							dst.NewIdent("f"),
						},
						Type: &dst.FuncType{
							Params:  funcargs(),
							Results: funcret(),
						},
					},
				},
			},
			Results: &dst.FieldList{
				List: []*dst.Field{
					{
						Type: dst.Clone(bindret).(*dst.FuncType),
					},
				},
			},
		},
		Body: &dst.BlockStmt{
			List: []dst.Stmt{
				&dst.ReturnStmt{
					Results: []dst.Expr{
						&dst.FuncLit{
							Type: dst.Clone(bindret).(*dst.FuncType),
							Body: &dst.BlockStmt{
								List: []dst.Stmt{
									&dst.ReturnStmt{
										Results: []dst.Expr{
											&dst.FuncLit{
												Type: &dst.FuncType{},
												Body: &dst.BlockStmt{
													List: []dst.Stmt{
														&dst.ExprStmt{
															X: &dst.CallExpr{
																Fun:      dst.NewIdent("f"),
																Args:     callargs,
																Ellipsis: bs.variable,
															},
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func makeBindFuncs(bindspecs map[bindspec]struct{}) []dst.Decl {
	var bss []bindspec
	for bs := range bindspecs {
		bss = append(bss, bs)
	}
	slices.SortFunc(bss, func(a, b bindspec) int {
		if d := cmp.Compare(a.args, b.args); d != 0 {
			return d
		}
		if a.variable != b.variable {
			if a.variable {
				return +1
			} else {
				return -1
			}
		}
		return cmp.Compare(a.ret, b.ret)
	})

	var decls []dst.Decl
	for _, bs := range bss {
		decls = append(decls, makeBindFunc(bs))
	}
	return decls
}
