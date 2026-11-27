//go:build !wlcost

package wl

// A compile-time false guard removes even counter argument evaluation.
const treeWorkEnabled = false

// treeWork has no state or runtime cost in production. The compiler
// removes the guarded calls; no callback or nil branch is compiled.
// Keep it first in tree so the zero-size field adds no trailing padding.
type treeWork struct{}

func (*treeWork) countWork(int) {}

func (*treeWork) observeWork(func(int)) bool { return false }
