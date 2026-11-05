package mvcc

// Exp473Size returns the keys and versions in the index (research #473,
// exp branch only).
func (s *Store) Exp473Size() (keys, versions int) {
	s.keys.Ascend(func(k *keyIndex) bool {
		keys++
		versions += len(k.vers)
		return true
	})
	return keys, versions
}
