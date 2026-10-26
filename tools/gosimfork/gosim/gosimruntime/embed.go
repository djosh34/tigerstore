package gosimruntime

import (
	"bytes"
	"embed"
)

// The //go:embed variables of the translated packages, each with its
// embedded value. Translated packages register them from init.
var (
	embeddedBytes   []embedded[[]byte]
	embeddedStrings []embedded[string]
	embeddedFSes    []embedded[embed.FS]
)

type embedded[T any] struct {
	variable *T
	value    T
}

// RegisterEmbeddedBytes records a //go:embed []byte variable, so that every
// run starts with its embedded contents, also when an earlier run changed
// them.
func RegisterEmbeddedBytes(variable *[]byte) {
	embeddedBytes = append(embeddedBytes, embedded[[]byte]{variable: variable, value: bytes.Clone(*variable)})
}

// RegisterEmbeddedString records a //go:embed string variable, so that every
// run starts with its embedded value.
func RegisterEmbeddedString(variable *string) {
	embeddedStrings = append(embeddedStrings, embedded[string]{variable: variable, value: *variable})
}

// RegisterEmbeddedFS records a //go:embed embed.FS variable, so that every
// run starts with its embedded files.
func RegisterEmbeddedFS(variable *embed.FS) {
	embeddedFSes = append(embeddedFSes, embedded[embed.FS]{variable: variable, value: *variable})
}

// resetEmbedded gives every //go:embed variable its embedded value again.
// The machines of a run share the variables.
//
// Marked go:norace: the goroutines of the run before used the variables, and
// nothing orders their accesses before these writes for the race detector.
//
//go:norace
func resetEmbedded() {
	for _, e := range embeddedBytes {
		// A loop, not copy: the runtime's copy tells the race detector.
		b := make([]byte, len(e.value))
		for i, c := range e.value {
			b[i] = c
		}
		*e.variable = b
	}
	for _, e := range embeddedStrings {
		*e.variable = e.value
	}
	for _, e := range embeddedFSes {
		*e.variable = e.value
	}
}
