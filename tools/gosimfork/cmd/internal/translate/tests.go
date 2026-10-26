package translate

import (
	"cmp"
	"go/token"
	"slices"
	"strconv"

	"github.com/dave/dst"
	"github.com/dave/dst/dstutil"
)

func (t *packageTranslator) rewriteTestFunc(c *dstutil.Cursor) {
	file, ok := c.Node().(*dst.File)
	if !ok {
		return
	}

	var newDecls []dst.Decl

	for _, decl := range file.Decls {
		funcDecl, ok := decl.(*dst.FuncDecl)
		if !ok {
			newDecls = append(newDecls, decl)
			continue
		}
		if funcDecl.Recv != nil || !t.testFuncs[funcDecl.Name.Name] {
			newDecls = append(newDecls, decl)
			continue
		}

		oldName := funcDecl.Name.Name
		newName := t.globalInfo.ByPackage[t.pkgPath].Rewritten[oldName] // "Impl" + oldName

		funcDecl.Name.Name = newName
		newDecls = append(newDecls, funcDecl)
		/*
			newDecls = append(newDecls, &dst.FuncDecl{
				Name: dst.NewIdent(oldName),
				Type: &dst.FuncType{
					Params: &dst.FieldList{
						List: []*dst.Field{
							{
								Names: []*dst.Ident{
									dst.NewIdent("t"),
								},
								Type: &dst.StarExpr{
									X: &dst.Ident{
										Path: "testing",
										Name: "T",
									},
								},
							},
						},
					},
				},
				Body: &dst.BlockStmt{
					List: []dst.Stmt{
						&dst.ExprStmt{
							X: &dst.CallExpr{
								Fun: &dst.Ident{Path: t.replacedPkgs[testingPackage], Name: "RunTest"},
								Args: []dst.Expr{
									dst.NewIdent("t"),
									dst.NewIdent(newName),
								},
							},
						},
					},
				},
			})
		*/
	}

	file.Decls = newDecls
}

func (t *packageTranslator) makeMetaTestFile(pkgName string, tests []packageSelector, runtime string) (*dst.File, error) {
	var elts []dst.Expr
	slices.SortFunc(tests, func(a, b packageSelector) int {
		if d := cmp.Compare(a.Pkg, b.Pkg); d != 0 {
			return d
		}
		return cmp.Compare(a.Selector, b.Selector)
	})

	for _, name := range tests {
		elts = append(elts, &dst.CompositeLit{
			Elts: []dst.Expr{
				&dst.KeyValueExpr{
					Key:   dst.NewIdent("Name"), // XXX: what happens if two tests have the same name in foo and foo_test?
					Value: &dst.BasicLit{Kind: token.STRING, Value: strconv.Quote(name.Selector)},
				},
				&dst.KeyValueExpr{
					Key: dst.NewIdent("Test"),
					// XXX: include name
					// XXX: include original pkg
					Value: &dst.Ident{
						Path: t.replacedPkgs[name.Pkg],
						Name: "Impl" + name.Selector,
					},
				},
			},
			Decs: dst.CompositeLitDecorations{
				NodeDecs: dst.NodeDecs{
					Before: dst.NewLine,
					After:  dst.NewLine,
				},
			},
		})
	}

	dstFile := &dst.File{
		Name: dst.NewIdent(pkgName + "_test"),
		Decls: []dst.Decl{
			/*
				&dst.GenDecl{
					Tok:   token.VAR,
					Specs: specs,
				},
			*/
			&dst.FuncDecl{
				Name: dst.NewIdent("init"),
				Type: &dst.FuncType{
					Params: &dst.FieldList{},
				},
				Body: &dst.BlockStmt{
					List: []dst.Stmt{
						&dst.ExprStmt{
							X: &dst.CallExpr{
								Fun: &dst.Ident{Path: t.replacedPkgs[gosimruntimePackage], Name: "SetAllTests"},
								Args: []dst.Expr{
									&dst.CompositeLit{
										Type: &dst.ArrayType{
											Elt: &dst.Ident{Path: t.replacedPkgs[gosimruntimePackage], Name: "Test"},
										},
										Elts: elts,
									},
								},
							},
							Decs: dst.ExprStmtDecorations{
								NodeDecs: dst.NodeDecs{
									After: dst.NewLine,
								},
							},
						},
					},
				},
			},
			&dst.FuncDecl{
				Name: dst.NewIdent("TestMain"),
				Type: &dst.FuncType{
					Params: &dst.FieldList{
						List: []*dst.Field{
							{
								Names: []*dst.Ident{
									dst.NewIdent("m"),
								},
								Type: &dst.StarExpr{
									X: &dst.Ident{Path: "testing", Name: "M"},
								},
							},
						},
					},
				},
				Body: &dst.BlockStmt{
					List: []dst.Stmt{
						&dst.ExprStmt{
							X: &dst.CallExpr{
								Fun: t.newRuntimeSelector("TestMain"),
								Args: []dst.Expr{
									&dst.CallExpr{
										Fun: &dst.Ident{
											Path: t.replacedPkgs[runtime],
											Name: "Runtime",
										},
									},
								},
							},
							Decs: dst.ExprStmtDecorations{
								NodeDecs: dst.NodeDecs{
									After: dst.NewLine,
								},
							},
						},
						/*
							&dst.ExprStmt{
								X: &dst.CallExpr{
									Fun: &dst.Ident{Path: "os", Name: "Exit"},
									Args: []dst.Expr{
										&dst.CallExpr{
											Fun: &dst.SelectorExpr{
												X:   dst.NewIdent("m"),
												Sel: dst.NewIdent("Run"),
											},
										},
									},
								},
								Decs: dst.ExprStmtDecorations{
									NodeDecs: dst.NodeDecs{
										After: dst.NewLine,
									},
								},
							},
						*/
					},
				},
			},
		},
	}

	return dstFile, nil
}
