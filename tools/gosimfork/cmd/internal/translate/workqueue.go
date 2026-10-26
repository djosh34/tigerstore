package translate

import (
	"sync"
)

type depGraph struct {
	nodes map[string]struct{}
	deps  map[string]map[string]struct{}
}

func newDepGraph() *depGraph {
	return &depGraph{
		nodes: make(map[string]struct{}),
		deps:  make(map[string]map[string]struct{}),
	}
}

func (d *depGraph) addNode(e string) {
	d.nodes[e] = struct{}{}
	d.deps[e] = make(map[string]struct{})
}

func (d *depGraph) addDep(from, to string) {
	d.deps[from][to] = struct{}{}
}

type workQueue struct {
	afterMe map[string][]string
	pending map[string]int
	deps    map[string][]string
	queue   []string
}

func newWorkQueue(g *depGraph) *workQueue {
	wq := &workQueue{
		afterMe: make(map[string][]string),
		pending: make(map[string]int),
		deps:    make(map[string][]string),
	}
	for node := range g.nodes {
		wq.pending[node] = 0
	}
	for from, deps := range g.deps {
		for to := range deps {
			wq.pending[from]++
			wq.afterMe[to] = append(wq.afterMe[to], from)
			wq.deps[from] = append(wq.deps[from], to)
		}
	}

	return wq
}

func (wq *workQueue) next() (string, bool) {
	if len(wq.queue) == 0 {
		return "", false
	}
	e := wq.queue[0]
	wq.queue = wq.queue[1:]
	return e, true
}

func (wq *workQueue) done() bool {
	return len(wq.pending) == 0 && len(wq.queue) == 0
}

func (wq *workQueue) finish(e string) {
	for _, next := range wq.afterMe[e] {
		wq.pending[next]--
		if wq.pending[next] == 0 {
			delete(wq.pending, next)
			wq.queue = append(wq.queue, next)
		}
	}
}

func (wq *workQueue) workInParallel(numWorkers int, worker func(string)) {
	workCh := make(chan string, len(wq.pending))

	var mu sync.Mutex

	submit := func() {
		for {
			work, ok := wq.next()
			if !ok {
				break
			}
			workCh <- work
			if wq.done() {
				close(workCh)
			}
		}
	}

	for next, cnt := range wq.pending {
		if cnt == 0 {
			delete(wq.pending, next)
			wq.queue = append(wq.queue, next)
		}
	}
	submit()

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for work := range workCh {
				worker(work)
				mu.Lock()
				wq.finish(work)
				submit()
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
}

func collectPreviousResults[V any](g *depGraph, cur string, prev map[string]V) map[string]V {
	results := make(map[string]V)
	var collect func(string)
	collect = func(e string) {
		if _, ok := results[e]; ok {
			return
		}
		results[e] = prev[e]
		for dep := range g.deps[e] {
			collect(dep)
		}
	}
	collect(cur)
	return results
}

func buildInParallel[V any](g *depGraph, numWorkers int, results map[string]V, work func(e string, inputs map[string]V) V) {
	var mu sync.Mutex
	newWorkQueue(g).workInParallel(numWorkers, func(e string) {
		mu.Lock()
		// short-circuit
		if _, ok := results[e]; ok {
			mu.Unlock()
			return
		}
		prev := collectPreviousResults(g, e, results)
		mu.Unlock()
		here := work(e, prev)
		mu.Lock()
		results[e] = here
		mu.Unlock()
	})
}
