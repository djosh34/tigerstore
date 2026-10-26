//go:build !sim

package behavior_test

import (
	"maps"
	"strings"
	"testing"

	"github.com/jellevandenhooff/gosim/metatesting"
)

func TestMetaRandDifferentBySeed(t *testing.T) {
	mt := metatesting.ForCurrentPackage(t)
	tests, err := mt.ListTests()
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range tests {
		if !strings.HasPrefix(test, "TestRand") {
			continue
		}

		values := make(map[string]int)
		for _, seed := range []int64{1, 2, 3, 4, 5} {
			run, err := mt.Run(t, &metatesting.RunConfig{
				Test: test,
				Seed: seed,
			})
			if err != nil {
				t.Fatal(err)
			}
			value := metatesting.MustFindLogValue(metatesting.ParseLog(run.LogOutput), "rand", "val").(string)
			values[value]++
		}
		t.Logf("values=%v", values)
		if len(values) != 5 {
			t.Error("expected different seeds")
		}
	}
}

func TestMetaRandDeterministic(t *testing.T) {
	mt := metatesting.ForCurrentPackage(t)
	tests, err := mt.ListTests()
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range tests {
		if !strings.HasPrefix(test, "TestRand") {
			continue
		}

		t.Run(test, func(t *testing.T) {
			values := make(map[string]int)
			for _, seed := range []int64{1, 2, 3, 4, 5} {
				innerValues := make(map[string]int)
				for range 2 {
					run, err := mt.Run(t, &metatesting.RunConfig{
						Test: test,
						Seed: seed,
					})
					if err != nil {
						t.Fatal(err)
					}
					value := metatesting.MustFindLogValue(metatesting.ParseLog(run.LogOutput), "rand", "val").(string)
					innerValues[value]++
				}
				if len(innerValues) != 1 {
					t.Error("expected determinism")
				}
				maps.Copy(values, innerValues)
			}
			t.Logf("values=%v", values)
			if len(values) != 5 {
				t.Error("expected different seeds")
			}
		})
	}
}
