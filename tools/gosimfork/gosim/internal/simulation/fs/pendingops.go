package fs

import (
	"cmp"
	"fmt"
	"math/rand"
	"slices"
	"sync"
)

//go:generate go run golang.org/x/tools/cmd/stringer -type=depKind,depKeyKind

// A depKey is used to find fsOp dependencies.

type depKeyKind byte

const (
	depKeyNone depKeyKind = iota
	depKeyAlloc
	depKeyWriteFile
	depKeyWriteDir
	depKeyResizeFile // XXX: combine with alloc?
	depKeyWriteDirEntry
)

// A depKey is used to find fsOp dependencies.
//
// Plain struct to reduce allocations.
type depKey struct {
	kind  depKeyKind
	inode int32
	entry string // for dir entry write, "" means nothing
}

func (k *depKey) String() string {
	return fmt.Sprintf("kind:%s inode:%d entry:%s", k.kind, k.inode, k.entry)
}

type depKind byte

const (
	depStoreAs depKind = iota
	depStoreAsAndDependsOn
	depDependsOn
)

func (k depKind) isStoreAs() bool {
	return k != depDependsOn
}

func (k depKind) isDependsOn() bool {
	return k != depStoreAs
}

type pendingOp struct {
	first *pendingDep
	pos   int
	fsOp  fsOp
}

var opPool = &sync.Pool{
	New: func() any {
		return &pendingOp{}
	},
}

var depPool = &sync.Pool{
	New: func() any {
		return &pendingDep{}
	},
}

var (
	flagOpEnqueued = &pendingOp{}
	flagOpWalked   = &pendingOp{}
)

type pendingDep struct {
	key  depKey
	kind depKind
	op   *pendingOp

	prev, next *pendingDep // in dep order

	sibling *pendingDep // from dep
}

// pendingOps tracks a filesystem operations that are not yet flushed to disk.
// Dependencies between are organized using dependency keys (depKeys): Any
// operation can be "stored as" one or more dependency keys, and "depend on" on
// one or more keys. An operation depends on all operations stored under keys it
// depends.
//
// For example, a file inode allocation is stored with `depKey{kind:
// depKeyAlloc, inode: inode}` with `depStoreAs`. A later write to that file
// depends on that allocation using `depKey{kind: depKeyAlloc, inode: inode}`
// with `depDependsOn`.
//
// These operations and their dependencies are tracked in a graph. Every
// operation is held in *pendingOp. Each *pendingOp has one or more *pendingDep,
// stored in a linked list under pendingOp.first and pendingDep.sibling. Each
// pendingDep is stored in a linked list for its correspending depKey in
// pendingDep.prev and pendingDep.next, with the last pendingDep for each key
// stored in pendingOps.lastDepForKey.
//
// To flush an operation to disk, that operation and all its dependencies must
// be removed from the graph: For each operation we enqueue all store-as
// predecessors of its depends-on dependencies. Then we do that recursively
// using a BFS.
//
// This processing is really a walk over the pendingDep graph, but the queue is
// tracked with pendingOp nodes. To make sure each pendingOp is enqueued and
// processed exactly once, the status of a pendingDep is tracked in
// pendingDep.op with flagOpEnqueued and flagOpWalked. A dependency is marked
// flagOpEnqueued when its corresponding pendingOp has been enqueued, while a
// dep is marked flagOpWalked when it and all the deps before it (dep.prev,
// dep.prev.prev, etc.) have been enqueued.
//
// A dep and its ops are removed from the graph when the op is flushed to disk.
type pendingOps struct {
	nextPos       int
	lastDepForKey map[depKey]*pendingDep // used for graph
	buffer        []*pendingOp           // used for flush inodes
}

func newPendingOps() *pendingOps {
	return &pendingOps{
		nextPos:       1, // must be > 0 for iterAllCrashes
		lastDepForKey: make(map[depKey]*pendingDep),
	}
}

func (p *pendingOps) addDep(op *pendingOp, key depKey, kind depKind) {
	prev := p.lastDepForKey[key]

	if prev == nil && kind == depDependsOn {
		// fast path for dependencies that have already been flushed, eg. a
		// write to an already allocated and resized file
		return
	}
	if prev != nil && prev.op == op && prev.kind == kind {
		// fast path for eg. rename in dir
		return
	}

	dep := depPool.Get().(*pendingDep)
	dep.key = key // XXX: make unnecessary?
	dep.kind = kind
	dep.op = op

	dep.prev = prev
	dep.next = nil
	if dep.prev != nil {
		dep.prev.next = dep
	}
	p.lastDepForKey[key] = dep

	dep.sibling = op.first
	op.first = dep

	// log.Printf("adding dep %s %s", key.String(), kind)
}

func (p *pendingOps) addOpInternal() *pendingOp {
	op := opPool.Get().(*pendingOp)

	op.pos = p.nextPos
	p.nextPos++

	return op
}

func (p *pendingOps) maybeEnqueueOpFromDep(dep *pendingDep, out []*pendingOp) []*pendingOp {
	if dep.op == flagOpEnqueued || dep.op == flagOpWalked {
		// dep is already enqueued
		return out
	}
	out = append(out, dep.op)
	// mark parent as nil in nodes so we know op is enqueued. nodes will be
	// removed in flushOp
	for cur := dep.op.first; cur != nil; cur = cur.sibling {
		cur.op = flagOpEnqueued
	}
	return out
}

func (p *pendingOps) enqueuePrevDeps(dep *pendingDep, out []*pendingOp) []*pendingOp {
	for cur := dep; cur != nil && cur.op != flagOpWalked; cur = cur.prev {
		if cur.kind.isStoreAs() {
			out = p.maybeEnqueueOpFromDep(cur, out)
		} else {
			// XXX: how do we unhook this guy???????????????????????????????????
			// do we just leave it??? but then its prev and next are wrong!!!!!
			// (actually i think they are right, just other depends-on deps at
			// the start of the chain)
		}
		cur.op = flagOpWalked
	}
	return out
}

func (p *pendingOps) processOp(op *pendingOp, out []*pendingOp) []*pendingOp {
	cur := op.first
	op.first = nil
	for cur != nil {
		if cur.kind.isDependsOn() {
			out = p.enqueuePrevDeps(cur, out)
		}

		// unhook and free node
		if cur.prev != nil {
			cur.prev.next = cur.next
		}
		if cur.next != nil {
			cur.next.prev = cur.prev
		} else {
			// XXX: do this with a sentinel instead and we can skip the key field (and map lookup)?
			if cur.prev == nil {
				delete(p.lastDepForKey, cur.key)
			} else {
				p.lastDepForKey[cur.key] = cur.prev
			}
		}

		sibling := cur.sibling
		cur.prev, cur.next, cur.sibling, cur.op = nil, nil, nil, nil
		depPool.Put(cur)
		cur = sibling
	}
	return out
}

func (p *pendingOps) processQueue(out []*pendingOp) []*pendingOp {
	for idx := 0; idx < len(out); idx++ {
		cur := out[idx]
		out = p.processOp(cur, out)
	}
	slices.SortFunc(out, byOpPos)
	return out
}

func (p *pendingOps) addOp(fsOp fsOp) {
	op := p.addOpInternal()
	op.fsOp = fsOp
	fsOp.addDeps(p, op)
}

func (p *pendingOps) flushInode(inode int, out []*pendingOp) []*pendingOp {
	// file
	out = p.enqueuePrevDeps(p.lastDepForKey[depKey{kind: depKeyAlloc, inode: int32(inode)}], out)
	out = p.enqueuePrevDeps(p.lastDepForKey[depKey{kind: depKeyResizeFile, inode: int32(inode)}], out)
	out = p.enqueuePrevDeps(p.lastDepForKey[depKey{kind: depKeyWriteFile, inode: int32(inode)}], out)
	// dir
	out = p.enqueuePrevDeps(p.lastDepForKey[depKey{kind: depKeyWriteDir, inode: int32(inode)}], out)

	out = p.processQueue(out)

	return out
}

func (p *pendingOps) releaseBuffer(ops []*pendingOp) {
	for i, op := range ops {
		ops[i] = nil
		op.fsOp = nil
		/*
			if op.first != nil || op.fsOp != nil {
				panic("bad pool")
			}
		*/
		opPool.Put(op)
	}
	p.buffer = ops[:0]
}

func byOpPos(a, b *pendingOp) int {
	return cmp.Compare(a.pos, b.pos)
}

func (p *pendingOps) allOps() []*pendingOp {
	// collect all nodes
	var allOps []*pendingOp
	for _, head := range p.lastDepForKey {
		cur := head
		for cur != nil {
			// use pos as a little marker
			if cur.op.pos > 0 {
				cur.op.pos = -cur.op.pos
				allOps = append(allOps, cur.op)
				// log.Println(cur.parent)
			}
			cur = cur.prev
		}
	}

	// undo pos marking
	for _, op := range allOps {
		op.pos = -op.pos
	}

	// sort by pos
	slices.SortFunc(allOps, byOpPos)

	return allOps
}

func (p *pendingOps) iterAllCrashes(yield func([]fsOp) bool) {
	// XXX: if anything gets modified while iterAllCrashes runs we have a
	// problem. do a clone?

	allOps := p.allOps()

	// walk subsets:
	// - if we ever skip an op, mark all its store dep keys unavailable
	// - only allow adding a new op if key is available

	unavailable := make(map[depKey]int)
	var picked []fsOp

	var search func(idx int) bool
	search = func(idx int) bool {
		if idx == len(allOps) {
			return yield(picked)
		}

		op := allOps[idx]

		// consider picking
		cur := op.first
		ok := true
		for cur != nil {
			if cur.kind.isDependsOn() && unavailable[cur.key] > 0 {
				ok = false
				break
			}
			cur = cur.sibling
		}

		// do pick
		if ok {
			// mark picked
			picked = append(picked, op.fsOp)
			if !search(idx + 1) {
				return false
			}
			// unmark picked
			picked = picked[:len(picked)-1]
		}

		// don't pick
		// mark not picked
		cur = op.first
		for cur != nil {
			if cur.kind.isStoreAs() {
				unavailable[cur.key]++
			}
			cur = cur.sibling
		}
		if !search(idx + 1) {
			return false
		}
		// unmark not picked
		cur = op.first
		for cur != nil {
			if cur.kind.isStoreAs() {
				unavailable[cur.key]--
			}
			cur = cur.sibling
		}
		return true
	}

	search(0)
}

func (p *pendingOps) randomCrashSubset() []fsOp {
	allOps := p.allOps()

	// similar to iterAllCrashes
	// - if we ever skip an op, mark all its store dep keys unavailable
	// - only allow adding a new op if key is available

	unavailable := make(map[depKey]bool)
	var picked []fsOp

	for _, op := range allOps {
		// consider picking
		cur := op.first
		ok := true
		for cur != nil {
			if cur.kind.isDependsOn() && unavailable[cur.key] {
				ok = false
				break
			}
			cur = cur.sibling
		}

		// consider pick
		// XXX: this could really benefit from fuzzing guideance
		pick := ok && rand.Int31n(3) >= 1

		if pick {
			// do pick
			picked = append(picked, op.fsOp)
		} else {
			// mark unavailable
			cur = op.first
			for cur != nil {
				if cur.kind.isStoreAs() {
					unavailable[cur.key] = true
				}
				cur = cur.sibling
			}
		}
	}

	return picked
}
