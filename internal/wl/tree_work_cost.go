//go:build wlcost

package wl

const treeWorkEnabled = true

// Build with -tags wlcost to measure binary-search steps and slice
// entries copied, shifted or cleared (children and chunk headers).
// Neither this callback nor its branch exists in production builds.
type treeWork struct {
	testWork func(int)
}

func (w *treeWork) countWork(n int) {
	if w.testWork != nil {
		w.testWork(n)
	}
}

func (w *treeWork) observeWork(fn func(int)) bool {
	w.testWork = fn
	return true
}
