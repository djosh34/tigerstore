package wl

// watchFillSize is selection-local scalar scratch, not a source/event plan.
// Its boundaries match writeEvents/watchFillWire, including each repeated
// header and the fragment flag omitted from the final message.
type watchFillSize struct {
	header, part, count, total int
}

func (s *watchFillSize) add(bytes int) {
	if s.count > 0 && s.part+bytes >= maxRequestBytes {
		s.total += 5 + s.part
		s.part, s.count = s.header+2, 0
	}
	s.part += bytes
	s.count++
}

func (s *watchFillSize) wire() int { return s.total + 5 + s.part - 2 }
