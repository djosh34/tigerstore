package server

import (
	"errors"
	"io"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.opentelemetry.io/otel/attribute"
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
			s.streamFailed(st, "receive", "lease-keepalive", err)
			return err
		}
		span := st.startSpan("lease_renew", attribute.Int64("id", r.ID))
		resp, err := s.n.DoAs(st.ctx, st.c.callerOf(st), r)
		span.End()
		if err != nil {
			return toStatus(err)
		}
		if err := st.Send(resp); err != nil {
			s.streamFailed(st, "send", "lease-keepalive", err)
			return err
		}
	}
}
