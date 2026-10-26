package gosimruntime

import (
	"bytes"
	mathrand "math/rand"
)

// WriteArtifact saves a copy of data under name in the result of the current
// run. Writing a name again replaces its data.
func WriteArtifact(name string, data []byte) {
	data = bytes.Clone(data)
	getg().upcall(func() {
		gs := gs.get()
		if gs.artifacts == nil {
			gs.artifacts = make(map[string][]byte)
		}
		gs.artifacts[name] = data
	})
}

// Reseed sets the scheduler's random state from seed, so that what the
// simulation does next depends on seed and not on what ran before in it
// (tigerstore research #449).
func Reseed(seed int64) {
	getg().upcall(func() {
		gs.get().fastrander.state = mathrand.New(mathrand.NewSource(seed)).Uint64()
	})
}
