package ceilinghw

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"fmt"
	"hash/crc32"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"golang.org/x/crypto/chacha20poly1305"
	"google.golang.org/protobuf/proto"
)

var sink []byte
var sum32 uint32
var sum256 [32]byte

func BenchmarkPrimitives(b *testing.B) {
	for _, n := range []int{1024, 16384} {
		for _, algorithm := range []string{"AES128GCM", "AES256GCM", "ChaCha20Poly1305"} {
			var a cipher.AEAD
			if algorithm == "ChaCha20Poly1305" {
				a, _ = chacha20poly1305.New(make([]byte, 32))
			} else {
				k := 16
				if algorithm == "AES256GCM" {
					k = 32
				}
				c, _ := aes.NewCipher(make([]byte, k))
				a, _ = cipher.NewGCM(c)
			}
			src := make([]byte, n)
			nonce := make([]byte, a.NonceSize())
			aad := make([]byte, 5)
			dst := make([]byte, 0, n+a.Overhead())
			sealed := a.Seal(nil, nonce, src, aad)
			b.Run(fmt.Sprintf("%s/Seal/%d", algorithm, n), func(b *testing.B) {
				b.SetBytes(int64(n))
				b.ReportAllocs()
				for b.Loop() {
					sink = a.Seal(dst[:0], nonce, src, aad)
				}
			})
			b.Run(fmt.Sprintf("%s/Open/%d", algorithm, n), func(b *testing.B) {
				b.SetBytes(int64(n))
				b.ReportAllocs()
				for b.Loop() {
					var err error
					sink, err = a.Open(dst[:0], nonce, sealed, aad)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
	src := make([]byte, 1024)
	table := crc32.MakeTable(crc32.Castagnoli)
	b.Run("SHA256/1024", func(b *testing.B) {
		b.SetBytes(1024)
		for b.Loop() {
			sum256 = sha256.Sum256(src)
		}
	})
	b.Run("CRC32C/1024", func(b *testing.B) {
		b.SetBytes(1024)
		for b.Loop() {
			sum32 = crc32.Checksum(src, table)
		}
	})
	req := &pb.PutRequest{Key: []byte("/registry/pods/default/bench-00000000000000000000"), Value: src}
	wire, err := proto.Marshal(req)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("PutMarshal/1024", func(b *testing.B) {
		b.SetBytes(int64(len(wire)))
		b.ReportAllocs()
		for b.Loop() {
			sink, err = proto.Marshal(req)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("PutUnmarshal/1024", func(b *testing.B) {
		b.SetBytes(int64(len(wire)))
		b.ReportAllocs()
		for b.Loop() {
			var out pb.PutRequest
			if err := proto.Unmarshal(wire, &out); err != nil {
				b.Fatal(err)
			}
		}
	})
	for _, n := range []int{1024, 65536} {
		src := make([]byte, n)
		dst := make([]byte, n)
		b.Run(fmt.Sprintf("Memcpy/%d", n), func(b *testing.B) {
			b.SetBytes(int64(n))
			for b.Loop() {
				copy(dst, src)
			}
			sink = dst
		})
	}
}
