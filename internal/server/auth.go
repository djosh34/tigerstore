package server

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"math/big"

	"go.etcd.io/etcd/api/v3/authpb"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
)

// The Auth service follows etcd v3.7.2's (server/etcdserver/v3_server.go):
// every request goes through the log as an InternalRaftRequest, which the
// node checks and applies (core/auth.go). Passwords are hashed and
// checked here, with bcrypt, on the call's goroutine, never on the
// committer: UserAdd and UserChangePassword carry the hash into the log,
// and Authenticate checks the password against the hash it reads first.

// authCall serves an Auth method whose request goes into the log as is,
// in the field set picks.
func authCall[T any, P interface {
	*T
	proto.Message
}](set func(*pb.InternalRaftRequest, P)) func(ctx context.Context, s *Server, caller core.Caller, b []byte) (proto.Message, error) {
	return func(ctx context.Context, s *Server, caller core.Caller, b []byte) (proto.Message, error) {
		req := P(new(T))
		if err := unmarshal(b, req); err != nil {
			return nil, err
		}
		r := &pb.InternalRaftRequest{}
		set(r, req)
		resp, err := s.n.DoAs(ctx, caller, r)
		if err != nil {
			return nil, toStatus(err)
		}
		return resp, nil
	}
}

// authMethods are the Auth service's methods.
var authMethods = map[string]*method{
	"/etcdserverpb.Auth/AuthEnable":         {unary: authCall(func(r *pb.InternalRaftRequest, q *pb.AuthEnableRequest) { r.AuthEnable = q })},
	"/etcdserverpb.Auth/AuthDisable":        {unary: authCall(func(r *pb.InternalRaftRequest, q *pb.AuthDisableRequest) { r.AuthDisable = q })},
	"/etcdserverpb.Auth/AuthStatus":         {unary: authCall(func(r *pb.InternalRaftRequest, q *pb.AuthStatusRequest) { r.AuthStatus = q })},
	"/etcdserverpb.Auth/Authenticate":       {unary: authenticate},
	"/etcdserverpb.Auth/UserAdd":            {unary: userAdd},
	"/etcdserverpb.Auth/UserGet":            {unary: authCall(func(r *pb.InternalRaftRequest, q *pb.AuthUserGetRequest) { r.AuthUserGet = q })},
	"/etcdserverpb.Auth/UserList":           {unary: authCall(func(r *pb.InternalRaftRequest, q *pb.AuthUserListRequest) { r.AuthUserList = q })},
	"/etcdserverpb.Auth/UserDelete":         {unary: authCall(func(r *pb.InternalRaftRequest, q *pb.AuthUserDeleteRequest) { r.AuthUserDelete = q })},
	"/etcdserverpb.Auth/UserChangePassword": {unary: userChangePassword},
	"/etcdserverpb.Auth/UserGrantRole":      {unary: authCall(func(r *pb.InternalRaftRequest, q *pb.AuthUserGrantRoleRequest) { r.AuthUserGrantRole = q })},
	"/etcdserverpb.Auth/UserRevokeRole":     {unary: authCall(func(r *pb.InternalRaftRequest, q *pb.AuthUserRevokeRoleRequest) { r.AuthUserRevokeRole = q })},
	"/etcdserverpb.Auth/RoleAdd":            {unary: authCall(func(r *pb.InternalRaftRequest, q *pb.AuthRoleAddRequest) { r.AuthRoleAdd = q })},
	"/etcdserverpb.Auth/RoleGet":            {unary: authCall(func(r *pb.InternalRaftRequest, q *pb.AuthRoleGetRequest) { r.AuthRoleGet = q })},
	"/etcdserverpb.Auth/RoleList":           {unary: authCall(func(r *pb.InternalRaftRequest, q *pb.AuthRoleListRequest) { r.AuthRoleList = q })},
	"/etcdserverpb.Auth/RoleDelete":         {unary: authCall(func(r *pb.InternalRaftRequest, q *pb.AuthRoleDeleteRequest) { r.AuthRoleDelete = q })},
	"/etcdserverpb.Auth/RoleGrantPermission": {unary: authCall(func(r *pb.InternalRaftRequest, q *pb.AuthRoleGrantPermissionRequest) {
		r.AuthRoleGrantPermission = q
	})},
	"/etcdserverpb.Auth/RoleRevokePermission": {unary: authCall(func(r *pb.InternalRaftRequest, q *pb.AuthRoleRevokePermissionRequest) {
		r.AuthRoleRevokePermission = q
	})},
}

// hash is the base64 of a password's bcrypt hash, as etcd puts it into
// the log.
func (s *Server) hash(password string) (string, error) {
	h, err := bcrypt.GenerateFromPassword([]byte(password), s.bcryptCost())
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(h), nil
}

// bcryptCost is etcd's --bcrypt-cost: bcrypt's default cost unless set.
func (s *Server) bcryptCost() int {
	if c := s.cfg.BcryptCost; c >= bcrypt.MinCost && c <= bcrypt.MaxCost {
		return c
	}
	return bcrypt.DefaultCost
}

func userAdd(ctx context.Context, s *Server, caller core.Caller, b []byte) (proto.Message, error) {
	var r pb.AuthUserAddRequest
	if err := unmarshal(b, &r); err != nil {
		return nil, err
	}
	if r.Options == nil || !r.Options.NoPassword {
		h, err := s.hash(r.Password)
		if err != nil {
			return nil, toStatus(err)
		}
		r.HashedPassword, r.Password = h, ""
	}
	resp, err := s.n.DoAs(ctx, caller, &pb.InternalRaftRequest{AuthUserAdd: &r})
	if err != nil {
		return nil, toStatus(err)
	}
	return resp, nil
}

func userChangePassword(ctx context.Context, s *Server, caller core.Caller, b []byte) (proto.Message, error) {
	var r pb.AuthUserChangePasswordRequest
	if err := unmarshal(b, &r); err != nil {
		return nil, err
	}
	if r.Password != "" {
		h, err := s.hash(r.Password)
		if err != nil {
			return nil, toStatus(err)
		}
		r.HashedPassword, r.Password = h, ""
	}
	resp, err := s.n.DoAs(ctx, caller, &pb.InternalRaftRequest{AuthUserChangePassword: &r})
	if err != nil {
		return nil, toStatus(err)
	}
	return resp, nil
}

// authenticate is etcd's Authenticate: it reads the user's hash with a
// linearizable read, checks the password, and has the log assign a
// simple token with a prefix it picks, or signs a JWT once the log took
// the Authenticate. If auth changed in between, it starts again.
func authenticate(ctx context.Context, s *Server, caller core.Caller, b []byte) (proto.Message, error) {
	var r pb.AuthenticateRequest
	if err := unmarshal(b, &r); err != nil {
		return nil, err
	}
	for {
		rep, err := s.n.DoReply(ctx, caller, &pb.AuthenticateRequest{Name: r.Name})
		if err != nil {
			return nil, toStatus(err)
		}
		u, ok := rep.Resp.(*authpb.User)
		if !ok {
			return nil, status.Errorf(codes.Internal, "the credentials came as %T", rep.Resp)
		}
		if bcrypt.CompareHashAndPassword(u.Password, []byte(r.Password)) != nil {
			return nil, rpctypes.ErrGRPCAuthFailed
		}
		var prefix string
		if s.cfg.JWT == nil {
			if prefix, err = tokenPrefix(); err != nil {
				return nil, toStatus(err)
			}
		}
		applied, err := s.n.DoReply(ctx, caller, &pb.InternalRaftRequest{Authenticate: &pb.InternalAuthenticateRequest{Name: r.Name, SimpleToken: prefix}})
		if err != nil {
			return nil, toStatus(err)
		}
		if applied.Rev != rep.Rev {
			continue
		}
		if s.cfg.JWT == nil {
			return applied.Resp, nil
		}
		// etcd signs when the entry applies, with the auth revision then.
		resp, ok := applied.Resp.(*pb.AuthenticateResponse)
		if !ok {
			return nil, status.Errorf(codes.Internal, "Authenticate answered %T", applied.Resp)
		}
		if resp.Token, err = s.cfg.JWT.sign(r.Name, uint64(applied.Rev)); err != nil {
			return nil, toStatus(err)
		}
		return resp, nil
	}
}

// tokenPrefix is a simple token's random prefix: 16 letters, as etcd's
// genTokenPrefix picks.
func tokenPrefix() (string, error) {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, 16)
	for i := range b {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(letters))))
		if err != nil {
			return "", err
		}
		b[i] = letters[n.Int64()]
	}
	return string(b), nil
}
