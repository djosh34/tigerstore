package gosimruntime

import "bytes"

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
