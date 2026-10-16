package server

import (
	"math"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/mvcc"
)

// initialChunk is the key limit of a RangeStream's first chunk, as in
// etcd v3.7.2.
const initialChunk = 10

// rangeStream serves KV.RangeStream as etcd v3.7.2 does. It answers in
// chunks of keys, each a Range from the key after the last one sent,
// all at the revision of the first. Only the last chunk carries the
// header, count and more; merged, the chunks equal the Range answer. The
// header revision is the store's revision at the first read, also for a
// read at a past revision. The first read is linearizable unless the
// request is serializable; the later ones read the pinned revision, so
// they need no ReadIndex. A compaction past that revision fails the
// stream midway, as in etcd.
func (s *Server) rangeStream(st *stream) error {
	r := &pb.RangeRequest{}
	if err := st.RecvOne(r); err != nil {
		return err
	}
	if err := mvcc.CheckRangeStream(r); err != nil {
		return err
	}
	span := st.startSpan("range_streaming",
		attribute.String("range_begin", string(r.GetKey())),
		attribute.String("range_end", string(r.GetRangeEnd())),
		attribute.Int64("rev", r.GetRevision()),
		attribute.Int64("limit", r.GetLimit()),
		attribute.Bool("count_only", r.GetCountOnly()),
	)
	defer span.End()
	if r.CountOnly {
		resp, err := s.countRead(st, r)
		if err != nil {
			return err
		}
		return st.Send(&pb.RangeStreamResponse{RangeResponse: &pb.RangeResponse{Header: resp.Header, Count: resp.Count}})
	}
	total := r.Limit
	if total == 0 {
		total = math.MaxInt64
	}
	r.Limit = min(initialChunk, total)
	var header *pb.ResponseHeader
	count := int64(0)
	for {
		cp, err := s.n.RangeChunk(st.ctx, st.c.callerOf(st), r)
		if err != nil {
			cp.Hold.Release()
			return toStatus(err)
		}
		resp := cp.Resp.(*pb.RangeResponse)
		if header == nil {
			header = resp.Header
			if r.Revision == 0 {
				r.Revision = header.Revision
			}
			// The later chunks read the pinned revision.
			r.Serializable = true
		}
		count += int64(len(resp.Kvs))
		var next []byte
		if resp.More {
			// A copy: the key may share memory with the store.
			last := resp.Kvs[len(resp.Kvs)-1].Key
			next = append(append(make([]byte, 0, len(last)+1), last...), 0)
		}
		// The next limit follows the size of this chunk as etcd's range
		// answers it inside the server: the store revision as its only
		// header field, and the keys the chunk read saw as its count. It
		// is taken before Send, so that resp does not stay alive while a
		// slow client takes the chunk.
		size := proto.Size(&pb.RangeResponse{Header: &pb.ResponseHeader{Revision: resp.Header.Revision}, Kvs: resp.Kvs, More: resp.More, Count: resp.Count})
		out := &pb.RangeResponse{Kvs: resp.Kvs}
		done := !resp.More || count == total
		if done {
			out.Header, out.More, out.Count = header, resp.More, count
			if resp.More {
				rest, err := s.countRead(st, &pb.RangeRequest{Key: next, RangeEnd: r.RangeEnd, Revision: r.Revision, CountOnly: true, Serializable: true})
				if err != nil {
					cp.Hold.Release()
					return err
				}
				out.Count += rest.Count
			}
		}
		err = st.SendHeld(&pb.RangeStreamResponse{RangeResponse: out}, cp.Hold)
		cp.Hold.Release()
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		r.Key = next
		r.Limit = min(nextChunk(r.Limit, size, s.maxRequestBytes), total-count)
	}
}

// countRead runs a count-only Range of a RangeStream on the node, with the
// total count.
func (s *Server) countRead(st *stream, r *pb.RangeRequest) (*pb.RangeResponse, error) {
	resp, err := s.n.DoAs(st.ctx, st.c.callerOf(st), r)
	if err != nil {
		return nil, toStatus(err)
	}
	return resp.(*pb.RangeResponse), nil
}

// nextChunk picks the next chunk's key limit so that a chunk lands near
// target bytes, as etcd's adjustChunkLimit does: double below half the
// target, halve above twice it, never below one.
func nextChunk(limit int64, size, target int) int64 {
	switch {
	case size < target/2:
		limit *= 2
	case size > target*2:
		limit /= 2
	}
	return max(limit, 1)
}
