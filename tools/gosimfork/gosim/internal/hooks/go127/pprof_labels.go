package go127

import (
	"context" //gosim:notranslate
	"runtime/pprof"
)

// pprofLabelPairs returns the key and value pairs in labels, sorted by key.
// runtime/pprof runs outside the simulation and takes the native
// context.Context, so this file uses the native context package.
func pprofLabelPairs(labels pprof.LabelSet) [][2]string {
	var pairs [][2]string
	pprof.ForLabels(pprof.WithLabels(context.Background(), labels), func(key, value string) bool {
		pairs = append(pairs, [2]string{key, value})
		return true
	})
	return pairs
}
