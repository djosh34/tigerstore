package gosimruntime_test

import (
	"testing"

	"pgregory.net/rapid"

	"github.com/jellevandenhooff/gosim/gosimruntime"
)

func TestMapIterDeleteDuplicatesHappen(t *testing.T) {
	success := false
	for !success {
		m := make(map[int]struct{})
		m[0] = struct{}{}
		m[1] = struct{}{}
		count := 0
		for k := range m {
			if k == 1 {
				count++
			}
			delete(m, 0)
			delete(m, 1)
			m[1] = struct{}{}
		}
		if count > 0 {
			success = true
			break
		}
	}
}

func TestMapIterClearDuplicatesHappen(t *testing.T) {
	success := false
	for !success {
		m := make(map[int]struct{})
		m[0] = struct{}{}
		m[1] = struct{}{}
		count := 0
		for k := range m {
			if k == 1 {
				count++
			}
			clear(m)
			m[1] = struct{}{}
		}
		if count > 0 {
			success = true
			break
		}
	}
}

func TestCheckMap(t *testing.T) {
	t.Cleanup(gosimruntime.CleanupMockFastrand)
	rapid.Check(t, checkMap)
}

func checkMap(t *rapid.T) {
	gosimruntime.SetupMockFastrand()

	m := gosimruntime.NewMap[string, string]()
	model := make(map[string]string)

	actions := make(map[string]func(t *rapid.T))

	var iter gosimruntime.MapIter[string, string]
	var iterRequired, iterSeen map[string]struct{}
	haveIter := false

	actions["get"] = func(t *rapid.T) {
		k := rapid.String().Draw(t, "k")
		v, ok := m.GetOk(k)
		expectedV, expectedOk := model[k]
		if ok != expectedOk {
			t.Errorf("got ok %v, expected %v", ok, expectedOk)
		}
		if v != expectedV {
			t.Errorf("got v %q, expected %q", v, expectedV)
		}
	}

	actions["clear"] = func(t *rapid.T) {
		m.Clear()
		clear(model)
		if haveIter {
			clear(iterRequired)
			// after clearing a duplicate is legal, see TestMapIterClearDuplicatesHappen.
			clear(iterSeen)
		}
	}

	actions["set"] = func(t *rapid.T) {
		k := rapid.String().Draw(t, "k")
		v := rapid.String().Draw(t, "v")
		m.Set(k, v)
		model[k] = v
	}

	actions["delete"] = func(t *rapid.T) {
		k := rapid.String().Draw(t, "k")
		m.Delete(k)
		delete(model, k)
		if haveIter {
			delete(iterRequired, k)
			// after deleting a duplicate is legal, see TestMapIterDeleteDuplicatesHappen.
			delete(iterSeen, k)
		}
	}

	actions["start-iter"] = func(t *rapid.T) {
		if haveIter {
			t.Skip()
		}
		iter = m.Iter()
		iterRequired = make(map[string]struct{})
		for k := range model {
			iterRequired[k] = struct{}{}
		}
		iterSeen = make(map[string]struct{})
		haveIter = true
	}

	actions["step-iter"] = func(t *rapid.T) {
		if !haveIter {
			t.Skip()
		}
		// might be too high but not a problem
		steps := rapid.IntRange(1, 1+len(model)).Draw(t, "steps")
		for i := 0; i < steps; i++ {
			k, v, ok := iter.Next()
			if !ok {
				t.Log("got iter finished")
				if len(iterRequired) > 0 {
					t.Errorf("unexpected iter stop, still missing %v", iterRequired)
				}
				haveIter = false
				break
			}
			t.Logf("got iter key %q value %q", k, v)
			if _, ok := model[k]; !ok {
				t.Errorf("unexpected iter key %q", k)
			}
			if expectedV := model[k]; expectedV != v {
				t.Errorf("iter value mismatch for key %q: got %q, expected %q", k, v, expectedV)
			}
			if _, ok := iterSeen[k]; ok {
				t.Errorf("duplicate iter key %q", k)
			}
			iterSeen[k] = struct{}{}
			delete(iterRequired, k)
		}
	}

	actions[""] = func(t *rapid.T) {
		if got, expected := m.Len(), len(model); got != expected {
			t.Errorf("length mismatch: got %d, expected %d", got, expected)
		}
		iter := m.Iter()
		for {
			k, v, ok := iter.Next()
			if !ok {
				break
			}
			expectedV, expectedOk := model[k]
			if !expectedOk {
				t.Errorf("contents mismatch: got unexpected key %q with value %q", k, v)
			} else if v != expectedV {
				t.Errorf("contents mismatch: key %q got value %q, expected value %q", k, v, expectedV)
			}
		}
	}

	t.Repeat(actions)
}
