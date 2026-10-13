package main

import (
	"fmt"
	"math/rand/v2"
)

// keyPrefix is the resource prefix every generated key lives under, the way
// kube-apiserver stores pods.
const keyPrefix = "/registry/pods/"

// nameAlphabet is the alphabet Kubernetes uses for generated name suffixes.
const nameAlphabet = "bcdfghjklmnpqrstvwxz2456789"

// keys holds n fixed-length keys shaped like Kubernetes pod keys:
// /registry/pods/<namespace>/<deployment>-<hash>-<suffix>. The name comes
// from a bijective mix of the index, so keys are unique and arrive in random
// order, as generated names do.
type keys struct {
	n    int
	size int
	flat []byte
}

func makeKeys(n int) keys {
	const nsCount = 100
	ks := keys{n: n}
	for i := range n {
		x := uint64(i) * 0x9e3779b97f4a7c15 // odd multiplier: a bijection on uint64
		var name [15]byte
		for j := range name {
			name[j] = nameAlphabet[x%uint64(len(nameAlphabet))]
			x /= uint64(len(nameAlphabet))
		}
		k := fmt.Sprintf("%sbench-%03d/web-%s-%s", keyPrefix, i%nsCount, name[:10], name[10:])
		if ks.size == 0 {
			ks.size = len(k)
		}
		ks.flat = append(ks.flat, k...)
	}
	return ks
}

func (ks keys) key(i int) []byte { return ks.flat[i*ks.size : (i+1)*ks.size] }

// valuePoolSize is the random data values are cut from. Each request takes a
// different window, so values do not compress and no two requests in a row
// carry the same bytes.
const valuePoolSize = 16 << 20

type values struct {
	size int
	pool []byte
	off  int
}

func makeValues(size int, seed uint64) *values {
	r := rand.NewChaCha8([32]byte{byte(seed), byte(seed >> 8), byte(seed >> 16), byte(seed >> 24)})
	pool := make([]byte, valuePoolSize+size)
	// Reading from a ChaCha8 cannot fail.
	_, _ = r.Read(pool)
	return &values{size: size, pool: pool}
}

// next returns the next value: the next disjoint window of a 16 MiB pool
// of random bytes. Adjacent values share no data, but a connection's
// values repeat after 16 MiB and connections walk the same pool, so a
// compression benchmark needs values drawn afresh. Each connection has
// its own cursor, so next needs no lock.
func (v *values) next() []byte {
	b := v.pool[v.off : v.off+v.size]
	v.off += v.size
	if v.off+v.size > len(v.pool) {
		v.off = 0
	}
	return b
}
