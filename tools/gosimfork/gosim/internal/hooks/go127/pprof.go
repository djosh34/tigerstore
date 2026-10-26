package go127

import (
	"context"
	"runtime/pprof"
	"slices"
	"strings"
)

// The functions of runtime/pprof that take a context.Context stand in for the
// originals, which run outside the simulation and take the native
// context.Context. The labels live in the context, as in runtime/pprof, but
// no profiler reads them: profiles are not part of the simulation.

type pprofLabelsKey struct{}

// pprofLabels returns the labels in ctx, sorted by key.
func pprofLabels(ctx context.Context) [][2]string {
	labels, _ := ctx.Value(pprofLabelsKey{}).([][2]string)
	return labels
}

func PprofWithLabels(ctx context.Context, labels pprof.LabelSet) context.Context {
	merged := slices.Clone(pprofLabels(ctx))
	for _, pair := range pprofLabelPairs(labels) {
		i, found := slices.BinarySearchFunc(merged, pair[0], func(p [2]string, key string) int {
			return strings.Compare(p[0], key)
		})
		if found {
			merged[i] = pair
		} else {
			merged = slices.Insert(merged, i, pair)
		}
	}
	return context.WithValue(ctx, pprofLabelsKey{}, merged)
}

func PprofLabel(ctx context.Context, key string) (string, bool) {
	for _, pair := range pprofLabels(ctx) {
		if pair[0] == key {
			return pair[1], true
		}
	}
	return "", false
}

func PprofForLabels(ctx context.Context, f func(key, value string) bool) {
	for _, pair := range pprofLabels(ctx) {
		if !f(pair[0], pair[1]) {
			return
		}
	}
}

// PprofSetGoroutineLabels does nothing: no profiler reads a goroutine's
// labels.
func PprofSetGoroutineLabels(ctx context.Context) {}

func PprofDo(ctx context.Context, labels pprof.LabelSet, f func(context.Context)) {
	f(PprofWithLabels(ctx, labels))
}
