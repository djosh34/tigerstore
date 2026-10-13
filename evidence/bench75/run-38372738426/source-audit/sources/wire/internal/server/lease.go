package server

import (
	"errors"
	"io"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// keepAlive serves Lease.LeaseKeepAlive: it renews leases one request at
// a time. A missing lease gets TTL 0, not an error, as in etcd.
func (s *Server) keepAlive(st *stream) error {
	for {
		r := &pb.LeaseKeepAliveRequest{}
		err := st.Recv(r)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		resp, err := s.n.Do(st.ctx, r)
		if err != nil {
			return toStatus(err)
		}
		if err := st.Send(resp); err != nil {
			return err
		}
	}
}
