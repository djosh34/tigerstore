package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"slices"
	"strings"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
)

// ParsePeerURLs checks and sorts peer URLs as etcd's types.NewURLs does.
func ParsePeerURLs(in []string) ([]string, error) {
	if len(in) == 0 {
		return nil, errors.New("no valid URLs given")
	}
	out := make([]string, len(in))
	for i, s := range in {
		s = strings.TrimSpace(s)
		u, err := url.Parse(s)
		if err != nil {
			return nil, err
		}
		switch u.Scheme {
		case "http", "https":
			if _, _, err := net.SplitHostPort(u.Host); err != nil {
				return nil, fmt.Errorf(`URL address does not have the form "host:port": %s`, s)
			}
			if u.Path != "" {
				return nil, fmt.Errorf("URL must not contain a path: %s", s)
			}
		case "unix", "unixs":
		default:
			return nil, fmt.Errorf("URL scheme must be http, https, unix, or unixs: %s", s)
		}
		out[i] = u.String()
	}
	slices.Sort(out)
	return out, nil
}

// memberAdd answers Cluster.MemberAdd: as etcd's handler, it checks and
// sorts the peer URLs before the node takes the change.
func memberAdd(ctx context.Context, s *Server, _ core.Caller, b []byte) (proto.Message, error) {
	var r pb.MemberAddRequest
	if err := unmarshal(b, &r); err != nil {
		return nil, err
	}
	urls, err := ParsePeerURLs(r.PeerURLs)
	if err != nil {
		return nil, rpctypes.ErrGRPCMemberBadURLs
	}
	r.PeerURLs = urls
	resp, err := s.n.Do(ctx, &r)
	if err != nil {
		return nil, toStatus(err)
	}
	return resp, nil
}

// memberPromote answers Cluster.MemberPromote, which a follower hands
// the leader (node.Promote).
func memberPromote(ctx context.Context, s *Server, _ core.Caller, b []byte) (proto.Message, error) {
	var r pb.MemberPromoteRequest
	if err := unmarshal(b, &r); err != nil {
		return nil, err
	}
	resp, err := s.n.Promote(ctx, &r)
	if err != nil {
		return nil, toStatus(err)
	}
	return resp, nil
}
