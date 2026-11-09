package crypto576

import (
	"crypto/aes"
	"crypto/cipher"
	"fmt"
	"testing"
)

var sink byte

func BenchmarkAESGCM(b *testing.B) {
	for _, bits := range []int{128, 256} {
		for _, size := range []int{1024, 16384, 65536} {
			for _, open := range []bool{false, true} {
				b.Run(fmt.Sprintf("bits%d/size%d/open%v", bits, size, open), func(b *testing.B) {
					block, err := aes.NewCipher(make([]byte, bits/8))
					if err != nil {
						b.Fatal(err)
					}
					a, err := cipher.NewGCM(block)
					if err != nil {
						b.Fatal(err)
					}
					nonce := make([]byte, a.NonceSize())
					src := make([]byte, size)
					dst := make([]byte, 0, size+a.Overhead())
					sealed := a.Seal(nil, nonce, src, nil)
					b.SetBytes(int64(size))
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if open {
							dst, err = a.Open(dst[:0], nonce, sealed, nil)
							if err != nil {
								b.Fatal(err)
							}
						} else {
							dst = a.Seal(dst[:0], nonce, src, nil)
						}
					}
					sink = dst[0]
				})
			}
		}
	}
}
func BenchmarkCopy(b *testing.B) {
	for _, size := range []int{1024, 16384, 65536, 262144} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			src := make([]byte, size)
			dst := make([]byte, size)
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				copy(dst, src)
			}
			sink = dst[0]
		})
	}
}
