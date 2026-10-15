package server_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.etcd.io/etcd/api/v3/authpb"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/metadata"

	"github.com/djosh34/tigerstore/internal/server"
)

// TestAuthOverGRPC sets auth up as etcdctl does and checks what the server
// adds to the node's checks: passwords hashed and checked with bcrypt, a
// token in the "token" or "authorization" metadata, and a simple token
// that one node handed out and another took before it applied the entry
// that made it.
func TestAuthOverGRPC(t *testing.T) {
	c := newCluster(t)
	cert, _, _, err := c.ca.Issue("client", 77)
	if err != nil {
		t.Fatal(err)
	}
	setUpAuth(t, c, cert)
	ctx := context.Background()

	// The certificate's name, "client", is no user.
	if _, err := c.client(1, cert).Put(ctx, &pb.PutRequest{Key: []byte("a1"), Value: []byte("1")}); !errors.Is(err, rpctypes.ErrGRPCPermissionDenied) {
		t.Fatalf("put as the certificate's user: %v", err)
	}
	if _, err := pb.NewAuthClient(c.dial(2, cert)).Authenticate(ctx, &pb.AuthenticateRequest{Name: "alice", Password: "wrong"}); !errors.Is(err, rpctypes.ErrGRPCAuthFailed) {
		t.Fatalf("a wrong password: %v", err)
	}
	resp, err := pb.NewAuthClient(c.dial(2, cert)).Authenticate(ctx, &pb.AuthenticateRequest{Name: "alice", Password: "alicepw"})
	if err != nil {
		t.Fatal(err)
	}
	for id := uint64(1); id <= 3; id++ {
		as := metadata.AppendToOutgoingContext(ctx, rpctypes.TokenFieldNameGRPC, resp.Token)
		if id == 1 {
			as = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+resp.Token)
		}
		kv := c.client(id, cert)
		if _, err := kv.Put(as, &pb.PutRequest{Key: []byte("a1"), Value: []byte("1")}); err != nil {
			t.Fatalf("alice's put on node %d: %v", id, err)
		}
		if _, err := kv.Put(as, &pb.PutRequest{Key: []byte("z"), Value: []byte("1")}); !errors.Is(err, rpctypes.ErrGRPCPermissionDenied) {
			t.Fatalf("alice's put outside her role on node %d: %v", id, err)
		}
		if _, err := kv.Range(as, &pb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("b")}); err != nil {
			t.Fatalf("alice's range on node %d: %v", id, err)
		}
	}

	w, err := pb.NewWatchClient(c.dial(3, cert)).Watch(metadata.AppendToOutgoingContext(ctx, rpctypes.TokenFieldNameGRPC, resp.Token))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Send(&pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{CreateRequest: &pb.WatchCreateRequest{Key: []byte("z")}}}); err != nil {
		t.Fatal(err)
	}
	wr, err := w.Recv()
	if err != nil || !wr.Canceled || wr.CancelReason != rpctypes.ErrGRPCPermissionDenied.Error() {
		t.Fatalf("alice's watch outside her role: %v, %v", wr, err)
	}

	auth := pb.NewAuthClient(c.dial(1, cert))
	if _, err := auth.UserChangePassword(metadata.AppendToOutgoingContext(ctx, rpctypes.TokenFieldNameGRPC, resp.Token), &pb.AuthUserChangePasswordRequest{Name: "alice", Password: "x"}); !errors.Is(err, rpctypes.ErrGRPCPermissionDenied) {
		t.Fatalf("alice changes her password: %v", err)
	}
	if _, err := pb.NewAuthClient(c.dial(3, cert)).Authenticate(ctx, &pb.AuthenticateRequest{Name: "root", Password: "rootpw"}); err != nil {
		t.Fatal(err)
	}
}

// setUpAuth adds root with the root role and alice with readwrite on [a,
// b), and enables auth.
func setUpAuth(t *testing.T, c *cluster, cert tls.Certificate) {
	auth := pb.NewAuthClient(c.dial(1, cert))
	retry(t, func(ctx context.Context) (*pb.AuthUserAddResponse, error) {
		return auth.UserAdd(ctx, &pb.AuthUserAddRequest{Name: "root", Password: "rootpw"})
	})
	ctx := context.Background()
	for _, step := range []func() error{
		func() error {
			_, err := auth.RoleAdd(ctx, &pb.AuthRoleAddRequest{Name: "root"})
			return err
		},
		func() error {
			_, err := auth.UserGrantRole(ctx, &pb.AuthUserGrantRoleRequest{User: "root", Role: "root"})
			return err
		},
		func() error {
			_, err := auth.UserAdd(ctx, &pb.AuthUserAddRequest{Name: "alice", Password: "alicepw"})
			return err
		},
		func() error {
			_, err := auth.RoleAdd(ctx, &pb.AuthRoleAddRequest{Name: "rw"})
			return err
		},
		func() error {
			_, err := auth.RoleGrantPermission(ctx, &pb.AuthRoleGrantPermissionRequest{Name: "rw", Perm: &authpb.Permission{
				PermType: authpb.Permission_READWRITE, Key: []byte("a"), RangeEnd: []byte("b"),
			}})
			return err
		},
		func() error {
			_, err := auth.UserGrantRole(ctx, &pb.AuthUserGrantRoleRequest{User: "alice", Role: "rw"})
			return err
		},
		func() error {
			_, err := auth.AuthEnable(ctx, &pb.AuthEnableRequest{})
			return err
		},
	} {
		if err := step(); err != nil {
			t.Fatal(err)
		}
	}
}

// TestJWTOverGRPC runs auth with JWT tokens: a token signed on one node
// works on every node, any auth change, such as a new password, makes it
// old, as in etcd, and a token that does not verify is invalid.
func TestJWTOverGRPC(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "jwt.pem")
	if err := os.WriteFile(file, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	jwt, err := server.ParseAuthToken("jwt,sign-method=ES256,priv-key=" + file)
	if err != nil {
		t.Fatal(err)
	}
	c := newClusterWith(t, func(c *cluster) { c.jwt = jwt })
	cert, _, _, err := c.ca.Issue("client", 78)
	if err != nil {
		t.Fatal(err)
	}
	setUpAuth(t, c, cert)
	ctx := context.Background()
	login := func(id uint64, user, password string) context.Context {
		t.Helper()
		resp, err := pb.NewAuthClient(c.dial(id, cert)).Authenticate(ctx, &pb.AuthenticateRequest{Name: user, Password: password})
		if err != nil {
			t.Fatal(err)
		}
		if strings.Count(resp.Token, ".") != 2 {
			t.Fatalf("not a JWT: %q", resp.Token)
		}
		return metadata.AppendToOutgoingContext(ctx, rpctypes.TokenFieldNameGRPC, resp.Token)
	}
	alice := login(2, "alice", "alicepw")
	for id := uint64(1); id <= 3; id++ {
		if _, err := c.client(id, cert).Put(alice, &pb.PutRequest{Key: []byte("a1"), Value: []byte("1")}); err != nil {
			t.Fatalf("alice's put on node %d: %v", id, err)
		}
	}
	root := login(3, "root", "rootpw")
	auth := pb.NewAuthClient(c.dial(1, cert))
	if _, err := auth.UserChangePassword(root, &pb.AuthUserChangePasswordRequest{Name: "alice", Password: "new"}); err != nil {
		t.Fatal(err)
	}
	// The new password counts the auth revision up, so the token is old.
	if _, err := c.client(1, cert).Put(alice, &pb.PutRequest{Key: []byte("a1"), Value: []byte("2")}); !errors.Is(err, rpctypes.ErrGRPCAuthOldRevision) {
		t.Fatalf("alice's token after an auth change: %v", err)
	}
	alice = login(1, "alice", "new")
	if _, err := c.client(3, cert).Range(alice, &pb.RangeRequest{Key: []byte("a1")}); err != nil {
		t.Fatalf("alice's new token: %v", err)
	}
	forged := metadata.AppendToOutgoingContext(ctx, rpctypes.TokenFieldNameGRPC, "eyJhbGciOiJub25lIn0.eyJ1c2VybmFtZSI6InJvb3QiLCJyZXZpc2lvbiI6MTAwfQ.")
	if _, err := c.client(2, cert).Range(forged, &pb.RangeRequest{Key: []byte("a1")}); !errors.Is(err, rpctypes.ErrGRPCInvalidAuthToken) {
		t.Fatalf("an unsigned token: %v", err)
	}
}
