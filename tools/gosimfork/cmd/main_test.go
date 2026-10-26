package main

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestGroup(t *testing.T) {
	grouped := batchPackagesWithDifferentNames([]string{
		"hello",
		"github.com/bar",
		"github.com/foo/baz",
		"github.com/foo/bar",
		"github.com/foo/hello",
		"github.com/ok",
		"goodbye/hello",
	})

	if diff := cmp.Diff(grouped, [][]string{
		{
			"hello",
			"github.com/bar",
			"github.com/foo/baz",
			"github.com/ok",
		},
		{
			"github.com/foo/bar",
			"github.com/foo/hello",
		},
		{
			"goodbye/hello",
		},
	}); diff != "" {
		t.Error(diff)
	}
}
