package translate

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"log"
	"strconv"
	"strings"

	"github.com/dave/dst"
	"github.com/dave/dst/dstutil"
)

func isRecvExpr(node dst.Node) (*dst.UnaryExpr, bool) {
	expr, ok := node.(dst.Expr)
	if !ok {
		return nil, false
	}
	expr = dstutil.Unparen(expr)
	if recvExpr, ok := expr.(*dst.UnaryExpr); ok && recvExpr.Op == token.ARROW {
		return recvExpr, true
	}
	return nil, false
}

type ChanType struct {
	Type  *types.Chan
	Named types.Type
}

func isTypChanType(typ types.Type) (ChanType, bool) {
	if chanTyp, ok := typ.Underlying().(*types.Chan); ok {
		return ChanType{Type: chanTyp, Named: typ}, true
	}

	if param, ok := typ.(*types.TypeParam); ok {
		iface := param.Constraint().Underlying().(*types.Interface)
		if iface.NumEmbeddeds() == 1 {
			if union, ok := iface.EmbeddedType(0).(*types.Union); ok {
				if union.Len() == 1 {
					if chanType, ok := union.Term(0).Type().(*types.Chan); ok {
						return ChanType{Type: chanType, Named: typ}, true
					}
				}
			}
		}
	}

	return ChanType{}, false
}

func (t *packageTranslator) isChanType(expr dst.Expr) (ChanType, bool) {
	if astExpr, ok := t.astMap.Nodes[expr].(ast.Expr); ok {
		if convertedType, ok := t.implicitConversions[astExpr]; ok {
			if typ, ok := isTypChanType(convertedType); ok {
				return typ, ok
			}
		}
	}

	if typ, ok := t.getType(expr); ok {
		return isTypChanType(typ)
	}

	return ChanType{}, false
}

func (t *packageTranslator) maybeConvertChanType(x dst.Expr, typ ChanType) dst.Expr {
	if typ.Named == typ.Type {
		return x
	}

	return &dst.CallExpr{
		Fun: t.makeTypeExpr(typ.Named),
		Args: []dst.Expr{
			x,
		},
	}
}

// chanForRewrite checks if dst.Expr has a chan type and prepares it for
// rewriting
func (t *packageTranslator) chanForRewrite(x dst.Expr) (dst.Expr, bool) {
	typ, ok := t.isChanType(x)
	if !ok {
		return nil, false
	}
	x = t.apply(x).(dst.Expr)
	if typ.Named != typ.Type {
		x = &dst.CallExpr{Fun: t.newRuntimeSelector("ExtractChan"), Args: []dst.Expr{x}}
	}
	return x, true
}

func (t *packageTranslator) rewriteChanLen(c *dstutil.Cursor) {
	if call, ok := c.Node().(*dst.CallExpr); ok && t.isNamedBuiltIn(call.Fun, "len") {
		if ch, ok := t.chanForRewrite(call.Args[0]); ok {
			call.Fun = &dst.SelectorExpr{
				X:   ch,
				Sel: dst.NewIdent("Len"),
			}
			call.Args = nil
		}
	}
}

func (t *packageTranslator) rewriteChanCap(c *dstutil.Cursor) {
	if call, ok := c.Node().(*dst.CallExpr); ok && t.isNamedBuiltIn(call.Fun, "cap") {
		if ch, ok := t.chanForRewrite(call.Args[0]); ok {
			call.Fun = &dst.SelectorExpr{
				X:   ch,
				Sel: dst.NewIdent("Cap"),
			}
			call.Args = nil
		}
	}
}

func (t *packageTranslator) rewriteChanType(c *dstutil.Cursor) {
	// chan type
	if chanType, ok := c.Node().(*dst.ChanType); ok {
		// XXX: this ignores the arrow...
		c.Replace(&dst.IndexListExpr{
			X: t.newRuntimeSelector("Chan"),
			Indices: []dst.Expr{
				t.apply(chanType.Value).(dst.Expr),
			},
		})
	}
}

func (t *packageTranslator) rewriteChanRange(c *dstutil.Cursor) {
	if rangeStmt, ok := c.Node().(*dst.RangeStmt); ok {
		if ch, ok := t.chanForRewrite(rangeStmt.X); ok {
			rangeStmt.X = &dst.CallExpr{
				Fun: &dst.SelectorExpr{
					X:   ch,
					Sel: dst.NewIdent("Range"),
				},
			}
		}
	}
}

func (t *packageTranslator) rewriteChanRecvSimpleExpr(c *dstutil.Cursor) {
	// read from chan in simple form
	// <-ch
	if recvExpr, ok := isRecvExpr(c.Node()); ok {
		// we catch (val, ok) cases earlier
		ch, _ := t.chanForRewrite(recvExpr.X)

		c.Replace(&dst.CallExpr{
			Fun: &dst.SelectorExpr{
				X:   ch,
				Sel: dst.NewIdent("Recv"),
			},
		})
	}
}

func (t *packageTranslator) rewriteChanRecvOk(c *dstutil.Cursor) {
	// read from chan in val, ok = <-ch

	rhs, ok := isDualAssign(c)
	if !ok {
		return
	}

	if recvExpr, ok := isRecvExpr(*rhs); ok {
		ch, _ := t.chanForRewrite(recvExpr.X)
		*rhs = &dst.CallExpr{
			Fun: &dst.SelectorExpr{
				X:   ch,
				Sel: dst.NewIdent("RecvOk"),
			},
		}
	}
}

func (t *packageTranslator) rewriteChanClose(c *dstutil.Cursor) {
	// close ch
	if callExpr, ok := c.Node().(*dst.CallExpr); ok && t.isNamedBuiltIn(callExpr.Fun, "close") {
		ch, _ := t.chanForRewrite(callExpr.Args[0])
		r := &dst.CallExpr{
			Decs: callExpr.Decs,
			Fun: &dst.SelectorExpr{
				X:   ch,
				Sel: dst.NewIdent("Close"),
			},
		}
		c.Replace(r)
	}
}

func (t *packageTranslator) rewriteChanSend(c *dstutil.Cursor) {
	// write to chan
	// ch <-
	if sendStmt, ok := c.Node().(*dst.SendStmt); ok {
		ch, _ := t.chanForRewrite(sendStmt.Chan)
		c.Replace(&dst.ExprStmt{
			X: &dst.CallExpr{
				Fun: &dst.SelectorExpr{
					X:   ch,
					Sel: dst.NewIdent("Send"),
				},
				Args: []dst.Expr{
					t.apply(sendStmt.Value).(dst.Expr),
				},
			},
		})
	}
}

func (t *packageTranslator) rewriteChanNil(c *dstutil.Cursor) {
	if ident, ok := c.Node().(*dst.Ident); ok && ident.Name == "nil" {
		if chanType, ok := t.isChanType(ident); ok {
			c.Replace(t.maybeConvertChanType(&dst.CallExpr{
				Fun: &dst.IndexListExpr{
					X: t.newRuntimeSelector("NilChan"),
					Indices: []dst.Expr{
						t.makeTypeExpr(chanType.Type.Elem()),
					},
				},
			}, chanType))
		}
	}
}

func (t *packageTranslator) rewriteChanImplicitConversion(c *dstutil.Cursor) {
	if expr, ok := c.Node().(dst.Expr); ok {
		astExpr, _ := t.astMap.Nodes[expr].(ast.Expr)
		if typ, ok := t.implicitConversions[astExpr]; ok {
			if _, ok := isTypChanType(typ); ok {
				c.Replace(&dst.CallExpr{
					Fun:  t.makeTypeExpr(typ),
					Args: []dst.Expr{expr},
				})
			}
		}
	}
}

func (t *packageTranslator) rewriteMakeChan(c *dstutil.Cursor) {
	// make(chan)
	if makeExpr, ok := c.Node().(*dst.CallExpr); ok && t.isNamedBuiltIn(makeExpr.Fun, "make") {
		if typ, ok := t.isChanType(makeExpr); ok {
			var lenArg dst.Expr = &dst.BasicLit{Kind: token.INT, Value: "0"}
			if len(makeExpr.Args) >= 2 {
				lenArg = t.apply(makeExpr.Args[1]).(dst.Expr)
			}

			c.Replace(t.maybeConvertChanType(&dst.CallExpr{
				Fun: &dst.IndexListExpr{
					X: t.newRuntimeSelector("NewChan"),
					Indices: []dst.Expr{
						t.makeTypeExpr(typ.Type.Elem()),
					},
				},
				Args: []dst.Expr{
					lenArg,
				},
			}, typ))
		}
	}
}

func (t *packageTranslator) rewriteSelectStmt(c *dstutil.Cursor) {
	// select stmt
	selectStmt, ok := c.Node().(*dst.SelectStmt)
	if !ok {
		return
	}

	handlers := []dst.Expr{}
	var clauses []dst.Stmt

	copyStmt := &dst.AssignStmt{
		Lhs: []dst.Expr{},
		Tok: token.DEFINE,
		Rhs: []dst.Expr{},
	}

	suffix := t.suffix()
	selectIdx := "idx" + suffix
	selectVal := "val" + suffix
	valUsed := false
	selectOk := "ok" + suffix
	okUsed := false

	hasDefault := false

	counter := 0

	for _, clause := range selectStmt.Body.List {
		clause := clause.(*dst.CommClause)

		isDefault := false
		var handler dst.Expr

		if sendStmt, ok := clause.Comm.(*dst.SendStmt); ok {
			ch, _ := t.chanForRewrite(sendStmt.Chan)
			handler = &dst.CallExpr{
				Fun:  &dst.SelectorExpr{X: ch, Sel: dst.NewIdent("SendSelector")},
				Args: []dst.Expr{sendStmt.Value},
			}
		} else if exprStmt, ok := clause.Comm.(*dst.ExprStmt); ok {
			recvExpr, ok := isRecvExpr(exprStmt.X)
			if !ok {
				log.Fatal("bad recv expr")
			}
			ch, _ := t.chanForRewrite(recvExpr.X)
			handler = &dst.CallExpr{
				Fun: &dst.SelectorExpr{X: ch, Sel: dst.NewIdent("RecvSelector")},
			}
		} else if assignStmt, ok := clause.Comm.(*dst.AssignStmt); ok {
			if len(assignStmt.Rhs) != 1 {
				log.Fatal("bad recv assign stmt")
			}
			recvExpr, ok := isRecvExpr(assignStmt.Rhs[0])
			if !ok {
				log.Fatal("bad recv expr")
			}

			typ, _ := t.isChanType(recvExpr.X)

			lhs := assignStmt.Lhs
			rhs := []dst.Expr{
				&dst.CallExpr{
					Fun: &dst.IndexExpr{
						X:     t.newRuntimeSelector("ChanCast"),
						Index: t.makeTypeExpr(typ.Type.Elem()),
					},
					Args: []dst.Expr{dst.NewIdent(selectVal)},
				},
			}
			valUsed = true
			if len(lhs) > 1 {
				rhs = append(rhs, dst.NewIdent(selectOk))
				okUsed = true
			}

			clause.Body = append([]dst.Stmt{
				&dst.AssignStmt{
					Lhs: lhs,
					Tok: assignStmt.Tok,
					Rhs: rhs,
					Decs: dst.AssignStmtDecorations{
						NodeDecs: dst.NodeDecs{
							End: clause.Decs.Comm,
						},
					},
				},
			}, clause.Body...)

			ch, _ := t.chanForRewrite(recvExpr.X)
			handler = &dst.CallExpr{
				Fun: &dst.SelectorExpr{X: ch, Sel: dst.NewIdent("RecvSelector")},
			}
		} else if clause.Comm == nil {
			hasDefault = true
			isDefault = true
		} else {
			panic("help")
		}

		var caseList []dst.Expr
		if !isDefault {
			caseList = []dst.Expr{&dst.BasicLit{Kind: token.INT, Value: fmt.Sprint(counter)}}
			counter++
			handlers = append(handlers, handler)
		}
		clauses = append(clauses, &dst.CaseClause{
			List: caseList,
			Body: clause.Body,
			Decs: dst.CaseClauseDecorations{
				NodeDecs: clause.Decs.NodeDecs,
				Case:     clause.Decs.Case,
				Colon:    clause.Decs.Colon,
			},
		})
	}

	if !hasDefault {
		// output a default case to help the compiler understand we will never pass over this select
		clauses = append(clauses, &dst.CaseClause{
			// stick this case at the original closing bracket to help comments find their way
			List: nil, // default
			Body: []dst.Stmt{
				&dst.ExprStmt{
					X: &dst.CallExpr{
						Fun: dst.NewIdent("panic"),
						Args: []dst.Expr{
							&dst.BasicLit{
								Kind:  token.STRING,
								Value: strconv.Quote("unreachable select"),
							},
						},
					},
				},
			},
		})
	} else {
		handlers = append(handlers, &dst.CallExpr{
			Fun: t.newRuntimeSelector("DefaultSelector"),
		})
	}

	if len(copyStmt.Lhs) > 0 {
		c.InsertBefore(copyStmt)
	}

	if !valUsed {
		selectVal = "_"
	}
	if !okUsed {
		selectOk = "_"
	}

	var call dst.Expr
	if hasDefault && counter == 1 {
		// special case
		call = handlers[0]
		sel := call.(*dst.CallExpr).Fun.(*dst.SelectorExpr).Sel
		sel.Name = "Select" + strings.TrimSuffix(sel.Name, "Selector") + "OrDefault"
	} else {
		call = &dst.CallExpr{
			Fun:  t.newRuntimeSelector("Select"),
			Args: handlers,
		}
	}

	c.Replace(&dst.SwitchStmt{
		Decs: dst.SwitchStmtDecorations{
			NodeDecs: selectStmt.Decs.NodeDecs,
			Switch:   selectStmt.Decs.Select,
		},
		Init: &dst.AssignStmt{
			Lhs: []dst.Expr{
				dst.NewIdent(selectIdx),
				dst.NewIdent(selectVal),
				dst.NewIdent(selectOk),
			},
			Tok: token.DEFINE,
			Rhs: []dst.Expr{call},
		},
		Tag: dst.NewIdent(selectIdx),
		Body: &dst.BlockStmt{
			List: clauses,
		},
	})
}
