package behavior_test

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestReflectFormatMap(t *testing.T) {
	formatted := fmt.Sprint(map[string]int{"foo": 1, "bar": 2})
	if formatted != "map[bar:2 foo:1]" {
		t.Error(formatted)
	}
}

func TestReflectFormatMapInterface(t *testing.T) {
	type Foo struct {
		Bar int
	}

	x := Foo{Bar: 3}
	y := Foo{Bar: 5}

	formatted1 := fmt.Sprint(map[any]int{"foo": 1, "bar": 2, x: 3, y: 4, &x: 10, &y: 11})
	formatted2 := fmt.Sprint(map[any]int{"foo": 1, "bar": 2, x: 3, y: 4, &x: 10, &y: 11})
	// TODO: somehow allow this log
	// do not log, it is not deterministic...
	// t.Log(formatted1)
	if formatted1 != formatted2 {
		t.Error(formatted1)
	}
}

func TestReflectMapValue(t *testing.T) {
	m := make(map[int]string)

	value := reflect.ValueOf(m)
	if value.Kind() != reflect.Map {
		t.Error(value.Kind())
	}

	m[3] = "hello"
	res := value.MapIndex(reflect.ValueOf(3))
	if res.Kind() != reflect.String || res.Interface() != "hello" {
		t.Error(res)
	}

	value.SetMapIndex(reflect.ValueOf(5), reflect.ValueOf("world"))
	if m[5] != "world" {
		t.Error(m[5])
	}

	mapKeys := value.MapKeys()
	if len(mapKeys) != 2 {
		t.Error(mapKeys)
	}

	gotMapKeys := make(map[int]bool)
	for _, key := range mapKeys {
		gotMapKeys[key.Interface().(int)] = true
	}
	if len(gotMapKeys) != 2 || !gotMapKeys[3] || !gotMapKeys[5] {
		t.Error(gotMapKeys)
	}

	gotIter := make(map[int]string)
	iter := value.MapRange()
	for iter.Next() {
		gotIter[iter.Key().Interface().(int)] = iter.Value().Interface().(string)
	}
	if len(gotIter) != 2 || gotIter[3] != "hello" || gotIter[5] != "world" {
		t.Error(gotIter)
	}

	// XXX: panics for bad behavior
	// XXX: wrapping of items in interface (eg. map[string]any items have Kind reflect.Interface)
}

func TestReflectMapType(t *testing.T) {
	m := make(map[int]string)

	typ := reflect.TypeOf(m)
	if typ.Kind() != reflect.Map {
		t.Error(typ.Kind())
	}

	typ2 := reflect.TypeOf(map[int]string{2: "foo"})
	if typ != typ2 {
		t.Error(typ, typ2)
	}

	if typ.Key() != reflect.TypeOf(int(0)) {
		t.Error(typ.Key().Kind())
	}

	if typ.Elem() != reflect.TypeOf(string("")) {
		t.Error(typ.Elem().Kind())
	}

	// XXX: typ.String()
}

func TestReflectJSON(t *testing.T) {
	type Foo struct {
		A int
		B string `json:"buz"`
		C map[string]any
	}
	type Bar struct {
		D map[string]string
	}

	x := Foo{
		A: 5,
		B: "hello",
		C: map[string]any{
			"x": "y",
			"bar": Bar{
				D: map[string]string{
					"x": "y",
					"z": "w",
				},
			},
		},
	}

	bytes, err := json.Marshal(x)
	if err != nil {
		t.Error(err)
	}

	if string(bytes) != `{"A":5,"buz":"hello","C":{"bar":{"D":{"x":"y","z":"w"}},"x":"y"}}` {
		t.Error(string(bytes))
	}

	var out any
	if err := json.Unmarshal(bytes, &out); err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(out, map[string]any{
		"A":   5.0,
		"buz": "hello",
		"C": map[string]any{
			"x": "y",
			"bar": map[string]any{
				"D": map[string]any{
					"x": "y",
					"z": "w",
				},
			},
		},
	}) {
		t.Error(out)
	}

	newBytes, err := json.Marshal(out)
	if err != nil {
		t.Error(err)
	}

	if string(newBytes) != `{"A":5,"C":{"bar":{"D":{"x":"y","z":"w"}},"x":"y"},"buz":"hello"}` {
		t.Error(string(newBytes))
	}
}

func TestReflectDeepEqual(t *testing.T) {
	cases := []struct {
		A, B  any
		Equal bool
	}{
		{
			A:     "foo",
			B:     "bar",
			Equal: false,
		},
		{
			A:     "foo",
			B:     "foo",
			Equal: true,
		},
		{
			A:     map[string]int{"foo": 3, "bar": 4},
			B:     map[string]int{"foo": 4, "bar": 3},
			Equal: false,
		},
		{
			A:     map[string]int{"foo": 3, "bar": 4},
			B:     map[string]int{"foo": 3, "bar": 4},
			Equal: true,
		},
	}

	for _, c := range cases {
		if reflect.DeepEqual(c.A, c.B) != c.Equal {
			t.Error(c.A, c.B, c.Equal)
		}
	}
}

func TestReflectGoCmp(t *testing.T) {
	cases := []struct {
		A, B        any
		MustContain []string
	}{
		{
			A:           "foo",
			B:           "bar",
			MustContain: []string{"foo", "bar"},
		},
	}

	for _, c := range cases {
		diff := cmp.Diff(c.A, c.B)
		ok := true
		for _, part := range c.MustContain {
			if !strings.Contains(diff, part) {
				ok = false
			}
		}
		if !ok {
			t.Error(c.A, c.B, c.MustContain, strconv.Quote(diff))
		}
	}
}
