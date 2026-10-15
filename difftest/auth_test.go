//go:build difftest

package difftest

import (
	"context"
	"regexp"
	"testing"

	"go.etcd.io/etcd/api/v3/authpb"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/metadata"
)

// TestAuth runs on a fresh server, because it turns auth on
// (scripts/difftest.sh). The client's certificate names the user
// "client", which the server takes as the caller when a request has no
// token.
func TestAuth(t *testing.T) {
	runCases(t, "etcd-v3.7.2-auth.json", []testCase{{"auth", authCase}})
}

// TestAuthJWT runs the same case on a fresh server with JWT tokens
// (--auth-token jwt,...), which no change of a user ends, but which every
// change of auth makes old.
func TestAuthJWT(t *testing.T) {
	runCases(t, "etcd-v3.7.2-auth-jwt.json", []testCase{{"auth", authCase}})
}

// simpleToken is the form of etcd's simple tokens: 16 letters, a dot and
// the index of the Authenticate entry; jwtToken that of a JWT.
var (
	simpleToken = regexp.MustCompile(`^[a-zA-Z]{16}\.[0-9]+$`)
	jwtToken    = regexp.MustCompile(`^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$`)
)

// as makes the next steps go with token, or without one when it is
// empty.
func (r *run) as(base context.Context, token string) {
	r.ctx = base
	if token != "" {
		r.ctx = metadata.AppendToOutgoingContext(base, rpctypes.TokenFieldNameGRPC, token)
	}
}

// authDo records an Auth call's error, and its answer as res makes it.
func authDo[T any](r *run, do string, call func(ctx context.Context) (T, error), res func(T) any) {
	r.t.Helper()
	resp, err := call(r.ctx)
	var out any
	if err == nil && res != nil {
		out = res(resp)
	}
	r.record(do, out, err)
}

// login authenticates and returns the token; the step records the
// token's form.
func (r *run) login(do, name, password string) string {
	r.t.Helper()
	auth := pb.NewAuthClient(r.c.conn)
	resp, err := auth.Authenticate(r.ctx, &pb.AuthenticateRequest{Name: name, Password: password})
	var res any
	if err == nil {
		form := "other"
		switch {
		case simpleToken.MatchString(resp.Token):
			form = "simple"
		case jwtToken.MatchString(resp.Token):
			form = "jwt"
		}
		res = struct {
			Form string `json:"form"`
		}{form}
	}
	r.record(do, res, err)
	return resp.GetToken()
}

func authCase(r *run) {
	auth := pb.NewAuthClient(r.c.conn)
	base := r.ctx
	status := func(do string) {
		authDo(r, do, func(ctx context.Context) (*pb.AuthStatusResponse, error) {
			return auth.AuthStatus(ctx, &pb.AuthStatusRequest{})
		}, func(s *pb.AuthStatusResponse) any {
			return struct {
				Enabled  bool   `json:"enabled"`
				Revision uint64 `json:"revision"`
			}{s.Enabled, s.AuthRevision}
		})
	}
	enable := func(do string) {
		authDo(r, do, func(ctx context.Context) (*pb.AuthEnableResponse, error) {
			return auth.AuthEnable(ctx, &pb.AuthEnableRequest{})
		}, nil)
	}
	userAdd := func(do, name, password string, noPassword bool) {
		authDo(r, do, func(ctx context.Context) (*pb.AuthUserAddResponse, error) {
			return auth.UserAdd(ctx, &pb.AuthUserAddRequest{Name: name, Password: password, Options: &authpb.UserAddOptions{NoPassword: noPassword}})
		}, nil)
	}
	roleAdd := func(do, name string) {
		authDo(r, do, func(ctx context.Context) (*pb.AuthRoleAddResponse, error) {
			return auth.RoleAdd(ctx, &pb.AuthRoleAddRequest{Name: name})
		}, nil)
	}
	grantRole := func(do, user, role string) {
		authDo(r, do, func(ctx context.Context) (*pb.AuthUserGrantRoleResponse, error) {
			return auth.UserGrantRole(ctx, &pb.AuthUserGrantRoleRequest{User: user, Role: role})
		}, nil)
	}
	grantPerm := func(do, role string, typ authpb.Permission_Type, key, end []byte) {
		authDo(r, do, func(ctx context.Context) (*pb.AuthRoleGrantPermissionResponse, error) {
			return auth.RoleGrantPermission(ctx, &pb.AuthRoleGrantPermissionRequest{Name: role, Perm: &authpb.Permission{PermType: typ, Key: key, RangeEnd: end}})
		}, nil)
	}
	userGet := func(do, name string) {
		authDo(r, do, func(ctx context.Context) (*pb.AuthUserGetResponse, error) {
			return auth.UserGet(ctx, &pb.AuthUserGetRequest{Name: name})
		}, func(u *pb.AuthUserGetResponse) any { return u.Roles })
	}
	userList := func(do string) {
		authDo(r, do, func(ctx context.Context) (*pb.AuthUserListResponse, error) {
			return auth.UserList(ctx, &pb.AuthUserListRequest{})
		}, func(u *pb.AuthUserListResponse) any { return u.Users })
	}
	roleGet := func(do, name string) {
		authDo(r, do, func(ctx context.Context) (*pb.AuthRoleGetResponse, error) {
			return auth.RoleGet(ctx, &pb.AuthRoleGetRequest{Role: name})
		}, func(g *pb.AuthRoleGetResponse) any {
			type perm struct {
				Type string `json:"type"`
				Key  string `json:"key"`
				End  string `json:"end,omitempty"`
			}
			var perms []perm
			for _, p := range g.Perm {
				perms = append(perms, perm{p.PermType.String(), string(p.Key), string(p.RangeEnd)})
			}
			return perms
		})
	}
	roleList := func(do string) {
		authDo(r, do, func(ctx context.Context) (*pb.AuthRoleListResponse, error) {
			return auth.RoleList(ctx, &pb.AuthRoleListRequest{})
		}, func(l *pb.AuthRoleListResponse) any { return l.Roles })
	}

	a, b, c, d := r.key("a"), r.key("b"), r.key("c"), r.key("d")
	p := r.key("p/")
	status("status of a new server")
	enable("enable without root")
	userAdd("add root", "root", "rootpw", false)
	userAdd("add root again", "root", "rootpw", false)
	userAdd("add a user without a name", "", "pw", false)
	enable("enable while root has no root role")
	grantRole("grant a role that does not exist", "root", "nope")
	grantRole("grant root the root role", "root", "root")
	roleList("role list before the root role is added")
	roleAdd("add the root role", "root")
	roleAdd("add an empty role", "")
	roleAdd("add role rw", "rw")
	roleAdd("add role rw again", "rw")
	grantPerm("grant rw readwrite on [a, c)", "rw", authpb.Permission_READWRITE, a, c)
	grantPerm("grant rw read on c", "rw", authpb.Permission_READ, c, nil)
	grantPerm("grant rw write on d", "rw", authpb.Permission_WRITE, d, nil)
	grantPerm("grant a permission with a bad range", "rw", authpb.Permission_READ, c, a)
	grantPerm("grant a permission to a missing role", "nope", authpb.Permission_READ, c, nil)
	roleAdd("add role reader", "reader")
	grantPerm("grant reader read on the prefix p/", "reader", authpb.Permission_READ, p, append(r.key("p"), '0'))
	userAdd("add alice", "alice", "alicepw", false)
	userAdd("add the client's user without a password", "client", "", true)
	grantRole("grant alice rw", "alice", "rw")
	grantRole("grant alice rw again", "alice", "rw")
	grantRole("grant client reader", "client", "reader")
	r.put("put while auth is off", &pb.PutRequest{Key: append(p, 'x'), Value: []byte("1")})
	status("status before enabling")
	enable("enable")
	enable("enable again")
	status("status after enabling")

	// The certificate names the user "client".
	r.get("client reads the prefix", &pb.RangeRequest{Key: p, RangeEnd: append(r.key("p"), '0')})
	r.put("client writes the prefix", &pb.PutRequest{Key: append(p, 'y'), Value: []byte("1")})
	r.get("client reads a", &pb.RangeRequest{Key: a})
	userGet("client gets itself", "client")
	userList("client lists the users")
	r.login("authenticate a user without a password", "client", "")
	r.login("authenticate with a wrong password", "alice", "wrong")
	r.login("authenticate a missing user", "nobody", "pw")
	alice := r.login("authenticate alice", "alice", "alicepw")

	r.as(base, "bogus.1")
	r.get("read with a bogus token", &pb.RangeRequest{Key: a})

	r.as(base, alice)
	r.put("alice puts a", &pb.PutRequest{Key: a, Value: []byte("1")})
	r.put("alice puts b with prev_kv", &pb.PutRequest{Key: b, Value: []byte("1"), PrevKv: true})
	r.put("alice puts c", &pb.PutRequest{Key: c, Value: []byte("1")})
	r.put("alice puts d", &pb.PutRequest{Key: d, Value: []byte("1")})
	r.put("alice puts d with prev_kv", &pb.PutRequest{Key: d, Value: []byte("2"), PrevKv: true})
	r.get("alice reads [a, c)", &pb.RangeRequest{Key: a, RangeEnd: c})
	r.get("alice reads [a, c] as one range", &pb.RangeRequest{Key: a, RangeEnd: append(c, 0)})
	r.get("alice reads [a, d)", &pb.RangeRequest{Key: a, RangeEnd: d})
	r.get("alice reads d", &pb.RangeRequest{Key: d})
	r.get("alice reads everything", &pb.RangeRequest{Key: []byte{0}, RangeEnd: []byte{0}})
	r.del("alice deletes b", &pb.DeleteRangeRequest{Key: b})
	r.del("alice deletes [a, d)", &pb.DeleteRangeRequest{Key: a, RangeEnd: d})
	r.txn("alice's txn within her keys", &pb.TxnRequest{
		Compare: []*pb.Compare{{Key: a, Target: pb.Compare_VERSION, Result: pb.Compare_GREATER}},
		Success: []*pb.RequestOp{putOp(b, "2")},
		Failure: []*pb.RequestOp{{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: c}}}},
	})
	r.txn("alice's txn comparing c", &pb.TxnRequest{Compare: []*pb.Compare{{Key: c, Target: pb.Compare_VERSION}}})
	r.txn("alice's txn comparing d", &pb.TxnRequest{Compare: []*pb.Compare{{Key: d, Target: pb.Compare_VERSION}}})
	r.txn("alice's txn with a put on c in the branch not taken", &pb.TxnRequest{Failure: []*pb.RequestOp{putOp(c, "x")}})
	r.compact("alice compacts", r.abs(1), false)
	r.watch("alice watches a", &pb.WatchCreateRequest{Key: a, StartRevision: r.abs(1)})
	r.watch("alice watches d", &pb.WatchCreateRequest{Key: d, StartRevision: r.abs(1)})
	lease := r.grant("alice grants a lease", 0, 600)
	r.put("alice puts a with the lease", &pb.PutRequest{Key: a, Value: []byte("2"), Lease: lease})
	authDo(r, "alice's lease time to live", func(ctx context.Context) (*pb.LeaseTimeToLiveResponse, error) {
		return r.c.lease.LeaseTimeToLive(ctx, &pb.LeaseTimeToLiveRequest{ID: lease, Keys: true})
	}, func(l *pb.LeaseTimeToLiveResponse) any { return len(l.Keys) })
	authDo(r, "alice's status", func(ctx context.Context) (*pb.StatusResponse, error) {
		return r.c.maint.Status(ctx, &pb.StatusRequest{})
	}, func(*pb.StatusResponse) any { return true })
	authDo(r, "alice's hash", func(ctx context.Context) (*pb.HashKVResponse, error) {
		return r.c.maint.HashKV(ctx, &pb.HashKVRequest{})
	}, func(*pb.HashKVResponse) any { return true })
	r.alarm("alice lists the alarms", &pb.AlarmRequest{})
	userGet("alice gets herself", "alice")
	userGet("alice gets root", "root")
	roleGet("alice gets her role", "rw")
	roleGet("alice gets root's role", "root")
	userList("alice lists the users")
	enable("alice enables")
	authDo(r, "alice disables", func(ctx context.Context) (*pb.AuthDisableResponse, error) {
		return auth.AuthDisable(ctx, &pb.AuthDisableRequest{})
	}, nil)

	r.as(base, "")
	root := r.login("authenticate root", "root", "rootpw")
	r.as(base, root)
	r.get("root reads everything under the case", &pb.RangeRequest{Key: r.key(""), RangeEnd: r.key("\xff"), CountOnly: true})
	userList("root lists the users")
	roleList("root lists the roles")
	roleGet("root gets the root role", "root")
	roleGet("root gets rw", "rw")
	userGet("root gets alice", "alice")
	authDo(r, "root revokes rw's write on d", func(ctx context.Context) (*pb.AuthRoleRevokePermissionResponse, error) {
		return auth.RoleRevokePermission(ctx, &pb.AuthRoleRevokePermissionRequest{Role: "rw", Key: d})
	}, nil)
	authDo(r, "root revokes it again", func(ctx context.Context) (*pb.AuthRoleRevokePermissionResponse, error) {
		return auth.RoleRevokePermission(ctx, &pb.AuthRoleRevokePermissionRequest{Role: "rw", Key: d})
	}, nil)
	roleGet("rw after the revoke", "rw")
	authDo(r, "root revokes root from root", func(ctx context.Context) (*pb.AuthUserRevokeRoleResponse, error) {
		return auth.UserRevokeRole(ctx, &pb.AuthUserRevokeRoleRequest{Name: "root", Role: "root"})
	}, nil)
	authDo(r, "root deletes root", func(ctx context.Context) (*pb.AuthUserDeleteResponse, error) {
		return auth.UserDelete(ctx, &pb.AuthUserDeleteRequest{Name: "root"})
	}, nil)
	authDo(r, "root deletes the root role", func(ctx context.Context) (*pb.AuthRoleDeleteResponse, error) {
		return auth.RoleDelete(ctx, &pb.AuthRoleDeleteRequest{Role: "root"})
	}, nil)
	authDo(r, "root changes alice's password", func(ctx context.Context) (*pb.AuthUserChangePasswordResponse, error) {
		return auth.UserChangePassword(ctx, &pb.AuthUserChangePasswordRequest{Name: "alice", Password: "newpw"})
	}, nil)
	authDo(r, "root gives client a password", func(ctx context.Context) (*pb.AuthUserChangePasswordResponse, error) {
		return auth.UserChangePassword(ctx, &pb.AuthUserChangePasswordRequest{Name: "client", Password: "pw"})
	}, nil)
	r.as(base, alice)
	r.get("alice's old token after the new password", &pb.RangeRequest{Key: a})
	r.as(base, "")
	r.login("authenticate alice with the old password", "alice", "alicepw")
	alice = r.login("authenticate alice with the new password", "alice", "newpw")
	r.as(base, alice)
	r.put("alice puts d after the revoke", &pb.PutRequest{Key: d, Value: []byte("3")})
	r.as(base, root)
	authDo(r, "root revokes rw from alice", func(ctx context.Context) (*pb.AuthUserRevokeRoleResponse, error) {
		return auth.UserRevokeRole(ctx, &pb.AuthUserRevokeRoleRequest{Name: "alice", Role: "rw"})
	}, nil)
	authDo(r, "root revokes it again", func(ctx context.Context) (*pb.AuthUserRevokeRoleResponse, error) {
		return auth.UserRevokeRole(ctx, &pb.AuthUserRevokeRoleRequest{Name: "alice", Role: "rw"})
	}, nil)
	r.as(base, alice)
	r.put("alice puts a without a role", &pb.PutRequest{Key: a, Value: []byte("3")})
	r.as(base, root)
	authDo(r, "root deletes rw", func(ctx context.Context) (*pb.AuthRoleDeleteResponse, error) {
		return auth.RoleDelete(ctx, &pb.AuthRoleDeleteRequest{Role: "rw"})
	}, nil)
	authDo(r, "root deletes alice", func(ctx context.Context) (*pb.AuthUserDeleteResponse, error) {
		return auth.UserDelete(ctx, &pb.AuthUserDeleteRequest{Name: "alice"})
	}, nil)
	authDo(r, "root deletes alice again", func(ctx context.Context) (*pb.AuthUserDeleteResponse, error) {
		return auth.UserDelete(ctx, &pb.AuthUserDeleteRequest{Name: "alice"})
	}, nil)
	r.as(base, alice)
	r.get("a deleted user's token", &pb.RangeRequest{Key: a})
	r.as(base, root)
	status("status before disabling")
	authDo(r, "root disables", func(ctx context.Context) (*pb.AuthDisableResponse, error) {
		return auth.AuthDisable(ctx, &pb.AuthDisableRequest{})
	}, nil)
	r.as(base, "")
	status("status after disabling")
	r.put("put after disabling", &pb.PutRequest{Key: a, Value: []byte("4")})
	userList("user list after disabling")
	r.login("authenticate after disabling", "root", "rootpw")
}
