package wl

import "math"

// Size the existing chunk protocol without changing sort/selection or
// retaining another candidate array. The normal logical admission bound
// remains separate from this exact destination capacity.
func (n *node) residentStreamWire(r *rangeReq, kvs [][]byte, count int64, more bool, header int64, term uint64) int {
	hdr := sizeBytesField(1, n.hdrSizeTerm(header, term))
	if r.countOnly {
		return 5 + sizeBytesField(1, hdr+sizeVarintField(4, uint64(count)))
	}
	total := r.limit
	if total <= 0 {
		total = math.MaxInt64
	}
	left := int64(0)
	if more {
		left = count - int64(len(kvs))
	}
	chunk := min(int64(initialStreamChunk), total)
	sent := int64(0)
	wire := 0
	for {
		rest := int64(len(kvs)) - sent + left
		take := min(chunk, int64(len(kvs))-sent)
		chunkMore := rest > chunk
		recs := 0
		for _, kv := range kvs[sent : sent+take] {
			recs += sizeBytesField(2, len(kv))
		}
		sent += take
		last := !chunkMore || sent == total
		body := recs
		if last {
			body += hdr + sizeVarintField(4, uint64(sent+max(rest-take, 0)*b2i(chunkMore)))
			if chunkMore {
				body += 2
			}
		}
		wire += 5 + sizeBytesField(1, body)
		if last {
			return wire
		}
		adaptive := sizeBytesField(1, sizeVarintField(3, uint64(header))) + 2 + sizeVarintField(4, uint64(min(rest, chunk+1))) + recs
		chunk = min(nextChunk(chunk, adaptive, maxRequestBytes), total-sent)
	}
}
