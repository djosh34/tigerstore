package gosimruntime

// SetupMockFastrand mocks gs and fastrander to test the map implementation.
func SetupMockFastrand() {
	gs.set(&scheduler{
		fastrander: fastrander{state: 0},
	})
}

// CleanupMockFastrand cleans up the mock gs.
func CleanupMockFastrand() {
	gs.set(nil)
}
