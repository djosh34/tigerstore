package peer

import (
	"context"
	"crypto/tls"
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/djosh34/tigerstore/internal/testcert"
)

// TestCall checks that Call reaches the other node's Answer with the
// caller's ID and brings back its answer, or its error with the gRPC
// code and message.
func TestCall(t *testing.T) {
	ca, err := testcert.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	certs := map[uint64]tls.Certificate{}
	for id := uint64(1); id <= 2; id++ {
		if certs[id], _, _, err = ca.Issue("node", int64(id+1)); err != nil {
			t.Fatal(err)
		}
	}
	addrs, lns := listeners(t, 1, 2)
	a := start(t, 1, lns[1], addrs, certs[1], ca.Pool)
	b := startWith(t, 2, lns[2], addrs, certs[2], ca.Pool, func(cfg *Config) {
		cfg.Answer = func(from uint64, req []byte) ([]byte, error) {
			if string(req) == "fail" {
				return nil, status.Error(codes.OutOfRange, "no such revision")
			}
			return append([]byte{byte(from)}, req...), nil
		}
	})
	defer func() {
		if err := errors.Join(a.t.Close(), b.t.Close()); err != nil {
			t.Error(err)
		}
	}()
	got, err := a.t.Call(context.Background(), 2, []byte("hello"))
	if err != nil || string(got) != "\x01hello" {
		t.Fatalf("call: %q, %v", got, err)
	}
	_, err = a.t.Call(context.Background(), 2, []byte("fail"))
	if !errors.Is(err, status.Error(codes.OutOfRange, "no such revision")) {
		t.Fatalf("failing call: %v", err)
	}
	if _, err := b.t.Call(context.Background(), 1, nil); status.Code(err) != codes.Unknown {
		t.Fatalf("call to a node that answers none: %v", err)
	}
}
