package main

import (
	"crypto/aes"
	"crypto/cipher"
	"fmt"
	"github.com/djosh34/tigerstore/internal/k8ssize"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/protobuf/proto"
	"math/rand/v2"
	"testing"
)

func BenchmarkResearch484(b *testing.B) {
	for _, n := range []int{64, 1460, 16384, 33365} {
		b.Run(fmt.Sprintf("AESGCMSeal/%d", n), func(b *testing.B) {
			block, _ := aes.NewCipher(make([]byte, 16))
			a, _ := cipher.NewGCM(block)
			src := make([]byte, n)
			dst := make([]byte, n+16)
			nonce := make([]byte, 12)
			b.SetBytes(int64(n))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				a.Seal(dst[:0], nonce, src, nil)
			}
		})
		b.Run(fmt.Sprintf("AESGCMOpen/%d", n), func(b *testing.B) {
			block, _ := aes.NewCipher(make([]byte, 16))
			a, _ := cipher.NewGCM(block)
			nonce := make([]byte, 12)
			enc := a.Seal(nil, nonce, make([]byte, n), nil)
			dst := make([]byte, n)
			b.SetBytes(int64(n))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := a.Open(dst[:0], nonce, enc, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
	for _, n := range []int{1, 500} {
		r := &pb.RangeResponse{Header: &pb.ResponseHeader{Revision: 100000, RaftTerm: 2, ClusterId: 12345678, MemberId: 12345678}, Count: int64(n)}
		for i := range n {
			r.Kvs = append(r.Kvs, &mvccpb.KeyValue{Key: mixedName("events", 0, 0, i), Value: make([]byte, 1460), CreateRevision: 10000, ModRevision: 100000, Version: 3})
		}
		raw, _ := proto.Marshal(r)
		b.Run(fmt.Sprintf("ProtoMarshalRange/%d", n), func(b *testing.B) {
			dst := make([]byte, 0, len(raw))
			b.SetBytes(int64(len(raw)))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := (proto.MarshalOptions{}).MarshalAppend(dst[:0], r); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("ProtoUnmarshalRange/%d", n), func(b *testing.B) {
			b.SetBytes(int64(len(raw)))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				v := &pb.RangeResponse{}
				if err := proto.Unmarshal(raw, v); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
	b.Run("DrawResource", func(b *testing.B) {
		r := rand.New(rand.NewPCG(1, 2))
		for b.Loop() {
			k8ssize.Draw(r, "")
		}
	})
	b.Run("DrawEventsSize", func(b *testing.B) {
		r := rand.New(rand.NewPCG(1, 2))
		for b.Loop() {
			k8ssize.Draw(r, "events")
		}
	})
	b.Run("BuildUpdate", func(b *testing.B) {
		k := &mixedKey{key: mixedName("events", 0, 0, 0), mod: 100000}
		v := make([]byte, 1460)
		b.ReportAllocs()
		for b.Loop() {
			m := mixedTxn(k, mixedUpdate, v)
			if _, err := newTemplate("/etcdserverpb.KV/Txn", m, true); err != nil {
				b.Fatal(err)
			}
		}
	})
}
