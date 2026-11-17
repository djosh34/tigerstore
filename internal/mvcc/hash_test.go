package mvcc

import (
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

func del(t *testing.T, s *Store, k string) {
	t.Helper()
	if _, err := s.DeleteRange(&pb.DeleteRangeRequest{Key: []byte(k)}); err != nil {
		t.Fatal(err)
	}
}
