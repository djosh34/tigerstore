package k8strace

// WatchCache reports, by call, the calls of kube-apiserver's watch cache:
// reads of many keys of a whole watched resource, as the cache lists it,
// and progress requests. A replay of many copies of the trace, one bigger
// cluster, still has one watch cache per resource, so only copy 0 makes
// them (cmd/loadgen). A read of one key of a resource, such as the
// revision a consistent LIST waits on, is no cache call.
func (t *Trace) WatchCache() []bool {
	whole := map[string]bool{"\x00": true} // watched prefixes and their ends
	for _, ws := range t.Watches {
		for _, w := range ws.Creates {
			if w.End >= 0 {
				p := t.Keys[w.Key]
				whole[p] = true
				whole[prefixEnd(p)] = true
			}
		}
	}
	out := make([]bool, len(t.Calls))
	for i := range t.Calls {
		c := &t.Calls[i]
		//exhaustive:ignore // no other kind comes from the watch cache
		switch c.Kind {
		case CallProgress:
			out[i] = true
		case CallRange, CallRangeStream:
			out[i] = c.Op.End >= 0 && whole[t.Keys[c.Op.Key]] && c.Op.Limit != 1
		}
	}
	return out
}

// prefixEnd is the end of the range of keys that start with p.
func prefixEnd(p string) string {
	end := []byte(p)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return string(end[:i+1])
		}
	}
	return "\x00"
}
