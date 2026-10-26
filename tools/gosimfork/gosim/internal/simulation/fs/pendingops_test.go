package fs

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestPendingOps(t *testing.T) {
	type testStep struct {
		flushDeps []int // flush these deps
		flushOps  []int // expect these ops

		addOp                  int // add this op
		addDependsOn           []int
		addStoreAs             []int
		addStoreAsAndDependsOn []int
	}

	testCases := []struct {
		name  string
		steps []testStep
	}{
		{
			name: "basic",
			steps: []testStep{
				{addOp: 1, addStoreAs: []int{1}},
				{flushDeps: []int{1}, flushOps: []int{1}},
				{flushDeps: []int{1}, flushOps: []int{}},
			},
		},
		{
			name: "chain at once",
			steps: []testStep{
				{addOp: 1, addStoreAs: []int{1}},
				{addOp: 2, addStoreAs: []int{2}, addDependsOn: []int{1}},
				{addOp: 3, addStoreAs: []int{3}, addDependsOn: []int{2}},
				{addOp: 4, addStoreAs: []int{4}, addDependsOn: []int{3}},
				{flushDeps: []int{4}, flushOps: []int{1, 2, 3, 4}},
				{flushDeps: []int{2}, flushOps: []int{}},
				{flushDeps: []int{4}, flushOps: []int{}},
			},
		},
		{
			name: "chain in parts",
			steps: []testStep{
				{addOp: 1, addStoreAs: []int{1}},
				{addOp: 2, addStoreAs: []int{2}, addDependsOn: []int{1}},
				{addOp: 3, addStoreAs: []int{3}, addDependsOn: []int{2}},
				{addOp: 4, addStoreAs: []int{4}, addDependsOn: []int{3}},
				{flushDeps: []int{2}, flushOps: []int{1, 2}},
				{flushDeps: []int{4}, flushOps: []int{3, 4}},
				{flushDeps: []int{2}, flushOps: []int{}},
				{flushDeps: []int{4}, flushOps: []int{}},
			},
		},
		{
			name: "chain at once with more after",
			steps: []testStep{
				{addOp: 1, addStoreAs: []int{1}},
				{addOp: 2, addStoreAs: []int{2}, addDependsOn: []int{1}},
				{addOp: 3, addStoreAs: []int{3}, addDependsOn: []int{2}},
				{addOp: 4, addStoreAs: []int{4}, addDependsOn: []int{3}},
				{addOp: 5, addStoreAs: []int{1}},
				{addOp: 6, addStoreAs: []int{1}},
				{flushDeps: []int{4}, flushOps: []int{1, 2, 3, 4}},
				{flushDeps: []int{2}, flushOps: []int{}},
				{flushDeps: []int{1}, flushOps: []int{5, 6}},
			},
		},
		{
			name: "no duplicates in output",
			steps: []testStep{
				{addOp: 1, addStoreAsAndDependsOn: []int{1, 2}},
				{addOp: 2, addStoreAsAndDependsOn: []int{1, 2}},
				{addOp: 3, addStoreAsAndDependsOn: []int{1, 2}},
				{addOp: 4, addStoreAsAndDependsOn: []int{1, 2}},
				{flushDeps: []int{2}, flushOps: []int{1, 2, 3, 4}},
				{flushDeps: []int{1}, flushOps: []int{}},
			},
		},
		{
			name: "fast path",
			steps: []testStep{
				{addOp: 1, addDependsOn: []int{1}},
				{addOp: 2, addStoreAs: []int{1}},
				{addOp: 3, addDependsOn: []int{1}},
				{addOp: 4, addDependsOn: []int{1}, addStoreAs: []int{2}},
				{flushDeps: []int{2}, flushOps: []int{2, 4}},
				{flushDeps: []int{1}, flushOps: []int{}},
			},
		},
		{
			name: "double deps",
			steps: []testStep{
				{addOp: 1, addStoreAs: []int{1}, addDependsOn: []int{1}},
				{addOp: 2, addStoreAs: []int{2}, addDependsOn: []int{1, 1}},
				{addOp: 3, addStoreAsAndDependsOn: []int{2, 3, 3}},
				{addOp: 4, addStoreAsAndDependsOn: []int{3, 4}},
				{flushDeps: []int{4}, flushOps: []int{1, 2, 3, 4}},
				{flushDeps: []int{1}, flushOps: []int{}},
			},
		},
		{
			name: "tricky release",
			steps: []testStep{
				{addOp: 1, addStoreAsAndDependsOn: []int{1}},
				{addOp: 2, addStoreAsAndDependsOn: []int{1}},
				{addOp: 3, addStoreAsAndDependsOn: []int{1, 2}},
				{addOp: 4, addStoreAsAndDependsOn: []int{1}},
				{addOp: 5, addStoreAsAndDependsOn: []int{1}},
				{flushDeps: []int{2}, flushOps: []int{1, 2, 3}},
				{flushDeps: []int{1}, flushOps: []int{4, 5}},
			},
		},
		{
			name: "less tricky release",
			steps: []testStep{
				{addOp: 1, addStoreAsAndDependsOn: []int{1}},
				{addOp: 3, addStoreAsAndDependsOn: []int{1, 2}},
				{addOp: 4, addStoreAsAndDependsOn: []int{1}},
				{addOp: 5, addStoreAsAndDependsOn: []int{1}},
				{flushDeps: []int{2}, flushOps: []int{1, 3}},
				{flushDeps: []int{1}, flushOps: []int{4, 5}},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ops := newPendingOps()

			for i, step := range testCase.steps {
				if step.addOp != 0 {
					op := ops.addOpInternal()
					op.fsOp = &allocOp{inode: step.addOp}
					for _, dep := range step.addDependsOn {
						ops.addDep(op, depKey{inode: int32(dep)}, depDependsOn)
					}
					for _, dep := range step.addStoreAs {
						ops.addDep(op, depKey{inode: int32(dep)}, depStoreAs)
					}
					for _, dep := range step.addStoreAsAndDependsOn {
						ops.addDep(op, depKey{inode: int32(dep)}, depStoreAsAndDependsOn)
					}

				}

				if step.flushDeps != nil {
					out := ops.buffer
					for _, dep := range step.flushDeps {
						out = ops.enqueuePrevDeps(ops.lastDepForKey[depKey{inode: int32(dep)}], out)
					}
					out = ops.processQueue(out)

					flushOps := []int{}
					for _, op := range out {
						flushOps = append(flushOps, op.fsOp.(*allocOp).inode)
					}
					ops.releaseBuffer(out)

					if diff := cmp.Diff(step.flushOps, flushOps); diff != "" {
						t.Errorf("step %d: flush deps %v: diff: %s", i+1, step.flushDeps, diff)
					}
				}
			}
		})
	}
}
