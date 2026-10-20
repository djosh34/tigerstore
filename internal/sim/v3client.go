package sim

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/server"
)

// Besides the clients that enter at Node.Do, Config.V3Clients clients use
// etcd's Go client, clientv3, as Kubernetes does: each talks to the real
// gRPC server of one voting node over the simulator's network, through
// the links between the clients' host and the nodes, so their faults
// apply. Their calls are records of the history like the others'.

// clientAddr is the client address of node i of the first three.
func clientAddr(i int) string { return hosts[i] + ":2379" }

// serve starts p's gRPC server on its node's client address. The lock is
// held.
func (s *Sim) serve(p *proc) error {
	n := p.n
	srv, err := server.New(p.node, server.Config{
		TLS:       &tls.Config{Certificates: []tls.Certificate{s.net.cert}},
		NoGateway: true,
		Logf:      func(format string, args ...any) { s.logf(n, format, args...) },
	})
	if err != nil {
		return err
	}
	p.srv = srv
	ln := s.net.listen(s, p, []string{clientAddr(n.idx)})
	s.procs.Go(func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, net.ErrClosed) {
			s.mu.Lock()
			s.fail("node %d: serving clients: %w", n.id, err)
			s.mu.Unlock()
		}
	})
	return nil
}

// newV3 makes client c's clientv3 client, for the node it calls.
func (c *client) newV3() error {
	s := c.sim
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"https://" + clientAddr(c.node)},
		TLS:       &tls.Config{RootCAs: s.net.ca},
		Logger:    zap.NewNop(),
		DialOptions: []grpc.DialOption{
			grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
				return s.net.dial(ctx, s, nil, addr)
			}),
			// gRPC's own backoff between dials grows to 2 minutes, which
			// would hold a client of one node that was down a few times
			// past the end of the run: calls wait for a connection.
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff:           backoff.Config{BaseDelay: 100 * time.Millisecond, Multiplier: 1.6, Jitter: 0.2, MaxDelay: time.Second},
				MinConnectTimeout: time.Duration(dialTimeout),
			}),
		},
	})
	c.v3 = cli
	return err
}

// callV3 makes the call of req with clientv3 and returns its answer as
// the response of the etcd API, and an error as the server sent it.
func (c *client) callV3(req proto.Message) (proto.Message, error) {
	s := c.sim
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(s.cfg.ClientTimeout))
	defer cancel()
	resp, err := c.doV3(ctx, req)
	return resp, v3Error(err)
}

func (c *client) doV3(ctx context.Context, req proto.Message) (proto.Message, error) {
	cli := c.v3
	switch r := req.(type) {
	case *pb.PutRequest:
		resp, err := cli.Put(ctx, string(r.Key), string(r.Value), putOpts(r)...)
		return (*pb.PutResponse)(resp), err
	case *pb.RangeRequest:
		resp, err := cli.Get(ctx, string(r.Key), rangeOpts(r)...)
		return (*pb.RangeResponse)(resp), err
	case *pb.DeleteRangeRequest:
		resp, err := cli.Delete(ctx, string(r.Key), deleteOpts(r)...)
		return (*pb.DeleteRangeResponse)(resp), err
	case *pb.TxnRequest:
		cmps := make([]clientv3.Cmp, len(r.Compare))
		for i, cmp := range r.Compare {
			cmps[i] = clientv3.FromCompare(cmp)
		}
		then, err := v3Ops(r.Success)
		if err != nil {
			return nil, err
		}
		els, err := v3Ops(r.Failure)
		if err != nil {
			return nil, err
		}
		resp, err := cli.Txn(ctx).If(cmps...).Then(then...).Else(els...).Commit()
		return (*pb.TxnResponse)(resp), err
	case *pb.LeaseGrantRequest:
		resp, err := cli.Grant(ctx, r.TTL)
		if err != nil {
			return nil, err
		}
		return &pb.LeaseGrantResponse{Header: resp.ResponseHeader, ID: int64(resp.ID), TTL: resp.TTL, Error: resp.Error}, nil
	case *pb.LeaseRevokeRequest:
		resp, err := cli.Revoke(ctx, clientv3.LeaseID(r.ID))
		return (*pb.LeaseRevokeResponse)(resp), err
	case *pb.LeaseKeepAliveRequest:
		resp, err := cli.KeepAliveOnce(ctx, clientv3.LeaseID(r.ID))
		if err != nil {
			return nil, err
		}
		return &pb.LeaseKeepAliveResponse{Header: resp.ResponseHeader, ID: int64(resp.ID), TTL: resp.TTL}, nil
	case *pb.CompactionRequest:
		var opts []clientv3.CompactOption
		if r.Physical {
			opts = append(opts, clientv3.WithCompactPhysical())
		}
		resp, err := cli.Compact(ctx, r.Revision, opts...)
		return (*pb.CompactionResponse)(resp), err
	}
	return nil, fmt.Errorf("%w: %T", errNoV3Call, req)
}

// errNoV3Call is the error of a request the clientv3 clients do not make.
var errNoV3Call = errors.New("sim: no clientv3 call for the request")

func putOpts(r *pb.PutRequest) []clientv3.OpOption {
	var opts []clientv3.OpOption
	if r.Lease != 0 {
		opts = append(opts, clientv3.WithLease(clientv3.LeaseID(r.Lease)))
	}
	if r.PrevKv {
		opts = append(opts, clientv3.WithPrevKV())
	}
	return opts
}

func rangeOpts(r *pb.RangeRequest) []clientv3.OpOption {
	var opts []clientv3.OpOption
	if len(r.RangeEnd) > 0 {
		opts = append(opts, clientv3.WithRange(string(r.RangeEnd)))
	}
	if r.Limit != 0 {
		opts = append(opts, clientv3.WithLimit(r.Limit))
	}
	if r.Revision != 0 {
		opts = append(opts, clientv3.WithRev(r.Revision))
	}
	if r.Serializable {
		opts = append(opts, clientv3.WithSerializable())
	}
	return opts
}

func deleteOpts(r *pb.DeleteRangeRequest) []clientv3.OpOption {
	var opts []clientv3.OpOption
	if len(r.RangeEnd) > 0 {
		opts = append(opts, clientv3.WithRange(string(r.RangeEnd)))
	}
	if r.PrevKv {
		opts = append(opts, clientv3.WithPrevKV())
	}
	return opts
}

// v3Ops are the clientv3 operations of a transaction branch.
func v3Ops(ops []*pb.RequestOp) ([]clientv3.Op, error) {
	out := make([]clientv3.Op, len(ops))
	for i, op := range ops {
		switch r := op.Request.(type) {
		case *pb.RequestOp_RequestPut:
			out[i] = clientv3.OpPut(string(r.RequestPut.Key), string(r.RequestPut.Value), putOpts(r.RequestPut)...)
		case *pb.RequestOp_RequestRange:
			out[i] = clientv3.OpGet(string(r.RequestRange.Key), rangeOpts(r.RequestRange)...)
		case *pb.RequestOp_RequestDeleteRange:
			out[i] = clientv3.OpDelete(string(r.RequestDeleteRange.Key), deleteOpts(r.RequestDeleteRange)...)
		default:
			return nil, fmt.Errorf("%w: %T in a transaction", errNoV3Call, op.Request)
		}
	}
	return out, nil
}

// v3Errors are the errors clientv3 turns the server's errors into, with
// the server's, which the rest of the simulator knows.
var v3Errors = []struct{ v3, server error }{
	{rpctypes.ErrNoLeader, rpctypes.ErrGRPCNoLeader},
	{rpctypes.ErrLeaderChanged, rpctypes.ErrGRPCLeaderChanged},
	{rpctypes.ErrKeyNotFound, rpctypes.ErrGRPCKeyNotFound},
	{rpctypes.ErrLeaseNotFound, rpctypes.ErrGRPCLeaseNotFound},
	{rpctypes.ErrFutureRev, rpctypes.ErrGRPCFutureRev},
	{rpctypes.ErrCompacted, rpctypes.ErrGRPCCompacted},
	{rpctypes.ErrDuplicateKey, rpctypes.ErrGRPCDuplicateKey},
	{rpctypes.ErrEmptyKey, rpctypes.ErrGRPCEmptyKey},
	{rpctypes.ErrNoSpace, rpctypes.ErrGRPCNoSpace},
	{rpctypes.ErrStopped, rpctypes.ErrGRPCStopped},
}

// v3Error is err as the server sent it: the server's error for one that
// clientv3 turned into its own, errClientTimeout for the call's own
// deadline, and any other error as it is.
func v3Error(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return errClientTimeout
	}
	for _, e := range v3Errors {
		if errors.Is(err, e.v3) {
			return e.server
		}
	}
	return err
}
