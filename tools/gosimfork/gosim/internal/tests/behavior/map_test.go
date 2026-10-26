package behavior_test

import (
	"cmp"
	"maps"
	"reflect"
	"slices"
	"testing"
)

func TestMapSmallGetOK(t *testing.T) {
	m := make(map[int]int)
	m[1] = 3
	m[2] = 5
	m[3] = 8

	if value, ok := m[2]; !ok || value != 5 {
		t.Error("bad get")
	}
}

func TestMapGetOK(t *testing.T) {
	m := make(map[int]int)
	m[1] = 3
	m[2] = 5
	m[3] = 8
	m[9] = 11
	m[16] = 7

	if value, ok := m[2]; !ok || value != 5 {
		t.Error("bad get")
	}
	if value, ok := m[16]; !ok || value != 7 {
		t.Error("bad get")
	}
}

func TestMapRMW(t *testing.T) {
	m := make(map[int]int)
	m[1] += 5

	m[1]++

	if value, ok := m[1]; !ok || value != 6 {
		t.Error("bad get")
	}
}

func TestMapSmallLen(t *testing.T) {
	m := make(map[int]int)
	m[1] = 3
	m[2] = 5
	m[3] = 8
	delete(m, 2)

	if len(m) != 2 {
		t.Error("bad len")
	}
}

func TestMapLen(t *testing.T) {
	m := make(map[int]int)
	m[1] = 3
	m[2] = 5
	m[3] = 8
	m[9] = 11
	m[16] = 7
	m[32] = -1
	delete(m, 2)

	if len(m) != 5 {
		t.Error("bad len")
	}

	delete(m, 16)
	if len(m) != 4 {
		t.Error("bad len")
	}
}

func TestMapGetNil(t *testing.T) {
	var m map[int]int

	if m[10] != 0 {
		t.Error("bad get")
	}

	if _, ok := m[10]; ok {
		t.Error("bad get")
	}
}

func TestMapLenNil(t *testing.T) {
	var m map[int]int

	if len(m) != 0 {
		t.Error("bad len")
	}
}

func TestMapIterNil(t *testing.T) {
	var m map[int]int

	for k, v := range m {
		t.Log(k, v)
	}
}

func TestMapDeleteNil(t *testing.T) {
	var m map[int]int

	delete(m, 0)
}

// Lifted from golang.org/x/exp/maps:

// Keys returns the keys of the map m.
// The keys will be in an indeterminate order.
func Keys[M ~map[K]V, K comparable, V any](m M) []K {
	r := make([]K, 0, len(m))
	for k := range m {
		r = append(r, k)
	}
	return r
}

// Values returns the values of the map m.
// The values will be in an indeterminate order.
func Values[M ~map[K]V, K comparable, V any](m M) []V {
	r := make([]V, 0, len(m))
	for _, v := range m {
		r = append(r, v)
	}
	return r
}

func TestMapsValues(t *testing.T) {
	m := make(map[int]int)
	m[1] = 3
	m[2] = 5
	m[3] = 8
	delete(m, 2)

	values := Values(m)
	slices.Sort(values)

	if !reflect.DeepEqual(values, []int{3, 8}) {
		t.Error("bad values")
	}
}

func TestMapsKeys(t *testing.T) {
	m := make(map[int]int)
	m[1] = 3
	m[2] = 5
	m[3] = 8
	delete(m, 2)

	keys := Keys(m)
	slices.Sort(keys)

	if !reflect.DeepEqual(keys, []int{1, 3}) {
		t.Error("bad keys")
	}
}

func TestMapsClone(t *testing.T) {
	m := make(map[int]int)
	m[1] = 3
	m[2] = 5
	m[3] = 8
	delete(m, 2)

	clone := maps.Clone(m)

	for k, v := range m {
		if clone[k] != v {
			t.Error("bad value", k, v, clone[k])
		}
	}

	for k, v := range clone {
		if m[k] != v {
			t.Error("bad value", k, v, m[k])
		}
	}

	if v, ok := m[3]; !ok || v != 8 {
		t.Error("bad get")
	}

	if v, ok := clone[3]; !ok || v != 8 {
		t.Error("bad get")
	}

	delete(m, 3)

	if _, ok := m[3]; ok {
		t.Error("bad get")
	}

	if v, ok := clone[3]; !ok || v != 8 {
		t.Error("bad get")
	}
}

func TestMapSmallGetMissing(t *testing.T) {
	m := make(map[int]int)
	m[1] = 3
	m[2] = 5
	m[3] = 8

	if _, ok := m[5]; ok {
		t.Error("bad get")
	}
}

func TestMapGetMissing(t *testing.T) {
	m := make(map[int]int)
	m[1] = 3
	m[2] = 5
	m[3] = 8
	m[11] = 2
	m[17] = 9
	m[19] = 1

	if _, ok := m[5]; ok {
		t.Error("bad get")
	}
}

func TestMapSmallGetMissingThenSet(t *testing.T) {
	m := make(map[int]int)
	m[1] = 3
	m[2] = 5
	m[3] = 8

	if _, ok := m[5]; ok {
		t.Error("bad get")
	}

	m[5] = 10

	if value, ok := m[5]; !ok || value != 10 {
		t.Error("bad get")
	}
}

func TestMapGetMissingThenSet(t *testing.T) {
	m := make(map[int]int)
	m[1] = 3
	m[2] = 5
	m[3] = 8
	m[11] = 2
	m[17] = 9
	m[19] = 1

	if _, ok := m[5]; ok {
		t.Error("bad get")
	}

	m[5] = 10

	if value, ok := m[5]; !ok || value != 10 {
		t.Error("bad get")
	}
}

func TestMapSmallDelete(t *testing.T) {
	m := make(map[int]int)
	m[1] = 3
	m[2] = 5
	m[3] = 8

	if value, ok := m[2]; !ok || value != 5 {
		t.Error("bad get")
	}

	delete(m, 2)

	if _, ok := m[2]; ok {
		t.Error("bad get")
	}
}

func TestMapDelete(t *testing.T) {
	m := make(map[int]int)
	m[1] = 3
	m[2] = 5
	m[3] = 8
	m[11] = 2
	m[17] = 9
	m[19] = 1

	if value, ok := m[2]; !ok || value != 5 {
		t.Error("bad get")
	}

	delete(m, 2)

	if _, ok := m[2]; ok {
		t.Error("bad get")
	}
}

func TestMapSmallDeleteMissing(t *testing.T) {
	m := make(map[int]int)
	m[1] = 3
	m[2] = 5
	m[3] = 8

	if _, ok := m[5]; ok {
		t.Error("bad get")
	}

	delete(m, 5)

	if _, ok := m[5]; ok {
		t.Error("bad get")
	}
}

func TestMapDeleteMissing(t *testing.T) {
	m := make(map[int]int)
	m[1] = 3
	m[2] = 5
	m[3] = 8
	m[11] = 2
	m[17] = 9
	m[19] = 1

	if _, ok := m[5]; ok {
		t.Error("bad get")
	}

	delete(m, 5)

	if _, ok := m[5]; ok {
		t.Error("bad get")
	}
}

type pair struct{ key, value int }

func TestMapSmallSimpleIter(t *testing.T) {
	m := make(map[int]int)
	m[1] = 3
	m[2] = 5
	m[3] = 8

	expected := []pair{
		{1, 3},
		{2, 5},
		{3, 8},
	}

	var seen []pair
	for k, v := range m {
		seen = append(seen, pair{key: k, value: v})
	}

	slices.SortFunc(seen, func(a pair, b pair) int {
		return cmp.Compare(a.key, b.key)
	})
	if !reflect.DeepEqual(seen, expected) {
		t.Error("bad", seen, expected)
	}
}

func TestMapSimpleIter(t *testing.T) {
	m := make(map[int]int)
	m[1] = 3
	m[2] = 5
	m[3] = 8
	m[11] = 9
	m[13] = 7

	expected := []pair{
		{1, 3},
		{2, 5},
		{3, 8},
		{11, 9},
		{13, 7},
	}

	var seen []pair
	for k, v := range m {
		seen = append(seen, pair{key: k, value: v})
	}

	slices.SortFunc(seen, func(a pair, b pair) int {
		return cmp.Compare(a.key, b.key)
	})
	if !reflect.DeepEqual(seen, expected) {
		t.Error("bad", seen, expected)
	}
}

func TestMapSmallRandomIter(t *testing.T) {
	m := make(map[int]int)
	m[1] = 3
	m[2] = 5
	m[3] = 8

	var firstSeen []pair
	for k, v := range m {
		firstSeen = append(firstSeen, pair{key: k, value: v})
	}

	success := false
	for i := 0; i < 20; i++ {
		var seen []pair
		for k, v := range m {
			seen = append(seen, pair{key: k, value: v})
		}

		if !reflect.DeepEqual(seen, firstSeen) {
			success = true
		}
	}

	if !success {
		t.Error("bad")
	}
}

func TestMapRandomIter(t *testing.T) {
	m := make(map[int]int)
	m[1] = 3
	m[2] = 5
	m[3] = 8
	m[11] = 9
	m[13] = 7

	var firstSeen []pair
	for k, v := range m {
		firstSeen = append(firstSeen, pair{key: k, value: v})
	}

	success := false
	for i := 0; i < 20; i++ {
		var seen []pair
		for k, v := range m {
			seen = append(seen, pair{key: k, value: v})
		}

		if !reflect.DeepEqual(seen, firstSeen) {
			success = true
		}
	}

	if !success {
		t.Error("bad")
	}
}

func TestMapLiteral(t *testing.T) {
	m := map[int]int{
		1: 3,
		2: 5,
		3: 8,
	}

	expected := []pair{
		{1, 3},
		{2, 5},
		{3, 8},
	}

	var seen []pair
	for k, v := range m {
		seen = append(seen, pair{key: k, value: v})
	}
	slices.SortFunc(seen, func(a pair, b pair) int {
		return cmp.Compare(a.key, b.key)
	})

	if !reflect.DeepEqual(seen, expected) {
		t.Error("bad", seen, expected)
	}
}

func TestMapSmallIterDelete(t *testing.T) {
	m := make(map[int]int)
	m[1] = 3
	m[2] = 5
	m[3] = 8
	m[4] = 10

	expected := []pair{
		{1, 3},
		{4, 10},
	}

	var seen []pair
	for k, v := range m {
		if k == 2 || k == 3 {
			expected = append(expected, pair{k, v})
		}

		delete(m, 2)
		delete(m, 3)

		seen = append(seen, pair{key: k, value: v})
	}

	slices.SortFunc(seen, func(a pair, b pair) int {
		return cmp.Compare(a.key, b.key)
	})
	slices.SortFunc(expected, func(a pair, b pair) int {
		return cmp.Compare(a.key, b.key)
	})

	if !reflect.DeepEqual(seen, expected) {
		t.Error("bad", seen, expected)
	}
}

func TestMapIterDelete(t *testing.T) {
	m := make(map[int]int)
	m[1] = 3
	m[2] = 5
	m[3] = 8
	m[4] = 10
	m[5] = 11
	m[6] = 13

	expected := []pair{
		{1, 3},
		{4, 10},
		{5, 11},
		{6, 13},
	}

	var seen []pair
	for k, v := range m {
		if k == 2 || k == 3 {
			expected = append(expected, pair{k, v})
		}

		delete(m, 2)
		delete(m, 3)

		seen = append(seen, pair{key: k, value: v})
	}

	slices.SortFunc(seen, func(a pair, b pair) int {
		return cmp.Compare(a.key, b.key)
	})
	slices.SortFunc(expected, func(a pair, b pair) int {
		return cmp.Compare(a.key, b.key)
	})

	if !reflect.DeepEqual(seen, expected) {
		t.Error("bad", seen, expected)
	}
}

func TestMapSmallIterAdd(t *testing.T) {
	m := make(map[int]int)
	m[1] = 3
	m[2] = 5
	m[3] = 8

	expected := []pair{
		{1, 3},
		{2, 5},
		{3, 8},
	}

	var seen []pair
	for k, v := range m {
		if k == 4 {
			expected = append(expected, pair{k, v})
		}

		m[4] = 10

		seen = append(seen, pair{key: k, value: v})
	}

	slices.SortFunc(seen, func(a pair, b pair) int {
		return cmp.Compare(a.key, b.key)
	})
	slices.SortFunc(expected, func(a pair, b pair) int {
		return cmp.Compare(a.key, b.key)
	})

	if !reflect.DeepEqual(seen, expected) {
		t.Error("bad", seen, expected)
	}
}

func TestMapIterAdd(t *testing.T) {
	m := make(map[int]int)
	m[1] = 3
	m[2] = 5
	m[3] = 8
	m[5] = 11
	m[6] = 13

	expected := []pair{
		{1, 3},
		{2, 5},
		{3, 8},
		{5, 11},
		{6, 13},
	}

	var seen []pair
	for k, v := range m {
		if k == 4 {
			expected = append(expected, pair{k, v})
		}

		m[4] = 10

		seen = append(seen, pair{key: k, value: v})
	}

	slices.SortFunc(seen, func(a pair, b pair) int {
		return cmp.Compare(a.key, b.key)
	})
	slices.SortFunc(expected, func(a pair, b pair) int {
		return cmp.Compare(a.key, b.key)
	})

	if !reflect.DeepEqual(seen, expected) {
		t.Error("bad", seen, expected)
	}
}

func TestMapGrowingIterAdd(t *testing.T) {
	// grow from small to not small during iteration

	m := make(map[int]int)
	m[1] = 3
	m[2] = 5
	m[3] = 8

	expected := []pair{
		{1, 3},
		{2, 5},
		{3, 8},
	}

	var seen []pair
	for k, v := range m {
		if k >= 4 && k <= 7 {
			expected = append(expected, pair{k, k + 6})
		}

		m[4] = 10
		m[5] = 11
		m[6] = 12
		m[7] = 13

		seen = append(seen, pair{key: k, value: v})
	}

	slices.SortFunc(seen, func(a pair, b pair) int {
		return cmp.Compare(a.key, b.key)
	})
	slices.SortFunc(expected, func(a pair, b pair) int {
		return cmp.Compare(a.key, b.key)
	})

	if !reflect.DeepEqual(seen, expected) {
		t.Error("bad", seen, expected)
	}
}

func TestMapIterDeleteAdd(t *testing.T) {
	m := make(map[int]int)
	m[1] = 3
	m[2] = 5
	m[3] = 8
	m[4] = 10

	expected := []pair{
		{1, 3},
		{4, 10},
	}

	var seen []pair
	for k, v := range m {
		if k == 2 || k == 3 || k == 5 {
			expected = append(expected, pair{k, v})
		}

		delete(m, 2)
		delete(m, 3)
		m[5] = 13

		seen = append(seen, pair{key: k, value: v})
	}

	slices.SortFunc(seen, func(a pair, b pair) int {
		return cmp.Compare(a.key, b.key)
	})
	slices.SortFunc(expected, func(a pair, b pair) int {
		return cmp.Compare(a.key, b.key)
	})

	if !reflect.DeepEqual(seen, expected) {
		t.Error("bad", seen, expected)
	}
}

func TestMapIterDeleteRestore(t *testing.T) {
	m := make(map[int]int)
	m[1] = 3
	m[2] = 5
	m[3] = 8

	expected := []pair{}

	var seen []pair
	for k, v := range m {
		delete(m, k)
		m[k] = v

		expected = append(expected, pair{key: k, value: v})
		seen = append(seen, pair{key: k, value: v})
	}

	slices.SortFunc(seen, func(a pair, b pair) int {
		return cmp.Compare(a.key, b.key)
	})
	slices.SortFunc(expected, func(a pair, b pair) int {
		return cmp.Compare(a.key, b.key)
	})

	if !reflect.DeepEqual(seen, expected) {
		t.Error("bad", seen, expected)
	}
}

func TestMapSmallClear(t *testing.T) {
	m := make(map[int]int)
	m[1] = 3
	m[2] = 5
	m[3] = 8

	clear(m)
	if len(m) != 0 {
		t.Error("bad len")
	}
	if _, ok := m[2]; ok {
		t.Error("bad get")
	}

	m[8] = 10
	if value, ok := m[8]; !ok || value != 10 {
		t.Error("bad get")
	}
}

func TestMapClear(t *testing.T) {
	m := make(map[int]int)
	m[1] = 3
	m[2] = 5
	m[3] = 8
	m[9] = 11
	m[16] = 7

	clear(m)
	if len(m) != 0 {
		t.Error("bad len")
	}
	if _, ok := m[2]; ok {
		t.Error("bad get")
	}

	m[8] = 10
	if value, ok := m[8]; !ok || value != 10 {
		t.Error("bad get")
	}
}

func TestMapLiteralRange(t *testing.T) {
	var seen []pair

	for k, v := range map[int]int{
		1: 3,
		2: 5,
		3: 8,
	} {
		seen = append(seen, pair{key: k, value: v})
	}

	slices.SortFunc(seen, func(a pair, b pair) int {
		return cmp.Compare(a.key, b.key)
	})

	expected := []pair{
		{key: 1, value: 3},
		{key: 2, value: 5},
		{key: 3, value: 8},
	}

	if !reflect.DeepEqual(seen, expected) {
		t.Error("bad", seen, expected)
	}
}

func TestMapAliasConversions(t *testing.T) {
	type Alias map[int]int

	x := []Alias{
		map[int]int{1: 3},
		map[int]int{2: 5},
	}

	y := [...]Alias{
		map[int]int{3: 8},
	}

	var seen []pair
	for _, m := range x {
		for k, v := range m {
			seen = append(seen, pair{key: k, value: v})
		}
	}
	for _, m := range y {
		for k, v := range m {
			seen = append(seen, pair{key: k, value: v})
		}
	}

	slices.SortFunc(seen, func(a pair, b pair) int {
		return cmp.Compare(a.key, b.key)
	})

	expected := []pair{
		{key: 1, value: 3},
		{key: 2, value: 5},
		{key: 3, value: 8},
	}

	if !reflect.DeepEqual(seen, expected) {
		t.Error("bad", seen, expected)
	}
}

func TestMapMultiAssign(t *testing.T) {
	m := make(map[int]string)

	var foo string
	m[1], m[2], m[3], foo = "4", "5", "6", "7"

	if m[1] != "4" {
		t.Errorf("expected 4, got %s", m[1])
	}
	if m[2] != "5" {
		t.Errorf("expected 5, got %s", m[2])
	}
	if m[3] != "6" {
		t.Errorf("expected 6, got %s", m[3])
	}
	if foo != "7" {
		t.Errorf("expected 7, got %s", foo)
	}
}
