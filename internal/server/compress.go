package server

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// gRPC compression follows grpc-go's server, which etcd v3.7.2 runs with
// its gzip compressor registered: a call with grpc-encoding gzip may send
// compressed messages, each with flag 1, and gets every response message
// that is not empty compressed with gzip too, under grpc-encoding gzip.
// Another encoding than gzip or identity fails the call before its
// handler runs.

const (
	gzipName = "gzip"
	identity = "identity"
)

// checkFlag is grpc-go's checkRecvPayload for a message's flag byte.
func (st *stream) checkFlag(flag byte) error {
	switch flag {
	case 0:
		return nil
	case 1:
		if st.encoding == "" || st.encoding == identity {
			return status.Error(codes.Internal, "grpc: compressed flag set with identity or empty encoding")
		}
		return nil
	}
	return status.Errorf(codes.Internal, "grpc: received unexpected payload format %d", flag)
}

// gunzip decompresses a message, at most limit bytes of it.
func gunzip(b []byte, limit int) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "grpc: failed to decompress the received message: %v", err)
	}
	out, err := io.ReadAll(io.LimitReader(r, int64(limit)+1))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "grpc: failed to decompress the received message: %v", err)
	}
	if len(out) > limit {
		return nil, status.Errorf(codes.ResourceExhausted, "grpc: received message after decompression larger than max (%d vs. %d)", len(out), limit)
	}
	return out, nil
}

// compress compresses a message with its prefix for a gzip call: it
// returns msg itself for any other call and for an empty message.
func (st *stream) compress(msg []byte) ([]byte, error) {
	if st.encoding != gzipName || len(msg) <= 5 {
		return msg, nil
	}
	var b bytes.Buffer
	b.Write([]byte{1, 0, 0, 0, 0})
	w := gzip.NewWriter(&b)
	if _, err := w.Write(msg[5:]); err != nil {
		return nil, status.Errorf(codes.Internal, "grpc: error while compressing: %v", err)
	}
	if err := w.Close(); err != nil {
		return nil, status.Errorf(codes.Internal, "grpc: error while compressing: %v", err)
	}
	out := b.Bytes()
	binary.BigEndian.PutUint32(out[1:5], uint32(len(out)-5))
	return out, nil
}
