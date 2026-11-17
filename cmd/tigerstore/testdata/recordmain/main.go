// Command recordmain records the fixture of TestTheUpgradeKeepsWhatThePreviousReleaseHeld
// (cmd/tigerstore/migrate_test.go): it starts one node of the previous
// release, runs puts, overwrites, deletes, Txns, leases and a compaction
// through it, reads every key at every revision from the compacted one to
// the head, turns auth on, and saves a snapshot. It writes v010.snapshot
// and v010.json, what the previous release answered, into the directory
// it is given.
//
//	go run ./cmd/tigerstore/testdata/recordmain <previous release's binary> cmd/tigerstore/testdata
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// fixture is what the previous release answered.
type fixture struct {
	Rev, Compacted int64
	// Reads are every key at each revision from Compacted to Rev.
	Reads map[int64][]kv
	// Leases are the leases alive at the end, with their granted TTLs.
	Leases map[int64]int64
	// AuthRevision is the auth store's revision after auth was turned on.
	AuthRevision uint64
	Users        map[string][]string
	Roles        map[string][]perm
}

type kv struct {
	Key, Value                    string
	Create, Mod, Version, LeaseID int64
}

type perm struct {
	Type     int32
	Key, End string
}

func main() {
	if len(os.Args) != 3 {
		log.Fatal("usage: recordmain <previous release's binary> <out dir>")
	}
	if err := run(os.Args[1], os.Args[2]); err != nil {
		log.Fatal(err)
	}
}

func run(binary, out string) (err error) {
	dir, err := os.MkdirTemp("", "recordmain-")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, os.RemoveAll(dir)) }()
	client, peer := freePort(), freePort()
	cmd := exec.Command(binary, "--name", "n1", "--data-dir", filepath.Join(dir, "n1"),
		"--listen-client-urls", "http://"+client, "--advertise-client-urls", "http://"+client,
		"--listen-peer-urls", "https://"+peer, "--initial-advertise-peer-urls", "https://"+peer,
		"--initial-cluster", "n1=https://"+peer, "--initial-cluster-token", "recordmain", "--peer-auto-tls")
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
	if err := cmd.Start(); err != nil {
		return err
	}
	defer func() { err = errors.Join(err, cmd.Process.Kill(), ignoreKilled(cmd.Wait())) }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	c, err := clientv3.New(clientv3.Config{Endpoints: []string{client}, DialTimeout: 10 * time.Second})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, c.Close()) }()
	for {
		if _, err := c.Put(ctx, "/ready", "1"); err == nil {
			break
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		time.Sleep(100 * time.Millisecond)
	}
	f, err := workload(ctx, c)
	if err != nil {
		return err
	}
	if f.AuthRevision, err = enableAuth(ctx, c); err != nil {
		return err
	}
	root, err := clientv3.New(clientv3.Config{Endpoints: []string{client}, DialTimeout: 10 * time.Second, Username: "root", Password: "rootpw"})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, root.Close()) }()
	if err := readAuth(ctx, root, f); err != nil {
		return err
	}
	if err := saveSnapshot(ctx, root, filepath.Join(out, "v010.snapshot")); err != nil {
		return err
	}
	b, err := json.MarshalIndent(f, "", "\t")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(out, "v010.json"), append(b, '\n'), 0o600)
}

// workload changes the store in every way the upgrade carries over, and
// reads it back at every revision from the compacted one on.
func workload(ctx context.Context, c *clientv3.Client) (*fixture, error) {
	put := func(k, v string, opts ...clientv3.OpOption) error {
		_, err := c.Put(ctx, k, v, opts...)
		return err
	}
	var errs []error
	for i := range 10 {
		errs = append(errs, put(fmt.Sprintf("/a/k%d", i), fmt.Sprintf("v%d", i)))
	}
	for i := range 3 {
		errs = append(errs, put(fmt.Sprintf("/a/k%d", i), fmt.Sprintf("v%d-again", i)))
	}
	_, err := c.Delete(ctx, "/a/k3")
	errs = append(errs, err)
	_, err = c.Delete(ctx, "/a/k7", clientv3.WithRange("/a/k9\x00"))
	errs = append(errs, err)
	// A Txn whose compare holds, one whose compare fails, and one that
	// puts a key and deletes another in one revision.
	_, err = c.Txn(ctx).If(clientv3.Compare(clientv3.Value("/a/k1"), "=", "v1-again")).
		Then(clientv3.OpPut("/a/k1", "v1-txn"), clientv3.OpPut("/t/x", "x")).Else(clientv3.OpGet("/a/k1")).Commit()
	errs = append(errs, err)
	_, err = c.Txn(ctx).If(clientv3.Compare(clientv3.Version("/a/k2"), "=", 99)).
		Then(clientv3.OpPut("/t/never", "n")).Else(clientv3.OpPut("/t/else", "e")).Commit()
	errs = append(errs, err)
	_, err = c.Txn(ctx).Then(clientv3.OpPut("/t/y", "y"), clientv3.OpDelete("/t/x")).Commit()
	errs = append(errs, err)
	// Leases: one kept with two keys, one revoked with its key.
	kept, err := c.Grant(ctx, 600)
	errs = append(errs, err)
	gone, err := c.Grant(ctx, 600)
	errs = append(errs, err)
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	errs = append(errs, put("/l/x", "x", clientv3.WithLease(kept.ID)), put("/l/y", "y", clientv3.WithLease(kept.ID)),
		put("/l/z", "z", clientv3.WithLease(gone.ID)))
	_, err = c.Revoke(ctx, gone.ID)
	errs = append(errs, err)
	r, err := c.Get(ctx, "/ready")
	errs = append(errs, err)
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	compacted := r.Header.Revision - 6
	if _, err := c.Compact(ctx, compacted); err != nil {
		return nil, err
	}
	for i := range 4 {
		errs = append(errs, put(fmt.Sprintf("/b/k%d", i), fmt.Sprintf("b%d", i)))
	}
	errs = append(errs, put("/a/k0", "v0-after"), put("/auth/data", "secret"))
	_, err = c.Delete(ctx, "/b/k1")
	errs = append(errs, err)
	r, err = c.Get(ctx, "/ready")
	errs = append(errs, err)
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	f := &fixture{Rev: r.Header.Revision, Compacted: compacted, Reads: map[int64][]kv{}, Leases: map[int64]int64{}}
	for rev := compacted; rev <= f.Rev; rev++ {
		g, err := c.Get(ctx, "\x00", clientv3.WithFromKey(), clientv3.WithRev(rev))
		if err != nil {
			return nil, err
		}
		kvs := []kv{}
		for _, x := range g.Kvs {
			kvs = append(kvs, kv{string(x.Key), string(x.Value), x.CreateRevision, x.ModRevision, x.Version, x.Lease})
		}
		f.Reads[rev] = kvs
	}
	ls, err := c.Leases(ctx)
	if err != nil {
		return nil, err
	}
	for _, l := range ls.Leases {
		ttl, err := c.TimeToLive(ctx, l.ID)
		if err != nil {
			return nil, err
		}
		f.Leases[int64(l.ID)] = ttl.GrantedTTL
	}
	return f, nil
}

// enableAuth adds root and alice, who may read /auth/ only, turns auth
// on and returns the auth revision.
func enableAuth(ctx context.Context, c *clientv3.Client) (uint64, error) {
	steps := []func() error{
		func() error { _, err := c.UserAdd(ctx, "root", "rootpw"); return err },
		func() error { _, err := c.RoleAdd(ctx, "root"); return err },
		func() error { _, err := c.UserGrantRole(ctx, "root", "root"); return err },
		func() error { _, err := c.UserAdd(ctx, "alice", "alicepw"); return err },
		func() error { _, err := c.RoleAdd(ctx, "reader"); return err },
		func() error {
			_, err := c.RoleGrantPermission(ctx, "reader", "/auth/", clientv3.GetPrefixRangeEnd("/auth/"), clientv3.PermissionType(clientv3.PermRead))
			return err
		},
		func() error { _, err := c.UserGrantRole(ctx, "alice", "reader"); return err },
		func() error { _, err := c.AuthEnable(ctx); return err },
	}
	for _, step := range steps {
		if err := step(); err != nil {
			return 0, err
		}
	}
	root, err := clientv3.New(clientv3.Config{Endpoints: c.Endpoints(), DialTimeout: 10 * time.Second, Username: "root", Password: "rootpw"})
	if err != nil {
		return 0, err
	}
	defer func() { _ = root.Close() }()
	s, err := root.AuthStatus(ctx)
	if err != nil {
		return 0, err
	}
	return s.AuthRevision, nil
}

// readAuth records the users with their roles and the roles with their
// permissions.
func readAuth(ctx context.Context, c *clientv3.Client, f *fixture) error {
	f.Users, f.Roles = map[string][]string{}, map[string][]perm{}
	us, err := c.UserList(ctx)
	if err != nil {
		return err
	}
	for _, u := range us.Users {
		g, err := c.UserGet(ctx, u)
		if err != nil {
			return err
		}
		f.Users[u] = g.Roles
	}
	rs, err := c.RoleList(ctx)
	if err != nil {
		return err
	}
	for _, r := range rs.Roles {
		g, err := c.RoleGet(ctx, r)
		if err != nil {
			return err
		}
		ps := []perm{}
		for _, p := range g.Perm {
			ps = append(ps, perm{int32(p.PermType), string(p.Key), string(p.RangeEnd)})
		}
		f.Roles[r] = ps
	}
	return nil
}

func saveSnapshot(ctx context.Context, c *clientv3.Client, path string) error {
	rc, err := c.Snapshot(ctx)
	if err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return errors.Join(err, rc.Close())
	}
	_, err = io.Copy(f, rc)
	return errors.Join(err, rc.Close(), f.Close())
}

func freePort() string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	return ln.Addr().String()
}

func ignoreKilled(err error) error {
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return nil
	}
	return err
}
