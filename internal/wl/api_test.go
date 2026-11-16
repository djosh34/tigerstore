package wl

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// The parts of etcd's API a cluster serves that the other tests leave
// out: HashKV, MoveLeader, alarms, the list of leases, the users and roles
// of the auth store, and a Txn's compares by target, each answered as etcd
// answers it.
func TestClusterServesTheRestOfTheEtcdAPI(t *testing.T) {
	tc := newTestCluster(t)
	c := tc.client(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	t.Run("HashKV agrees across the nodes and keeps a past revision's", func(t *testing.T) {
		if _, err := c.Put(ctx, "/hash/a", "1"); err != nil {
			t.Fatal(err)
		}
		p, err := c.Put(ctx, "/hash/b", "2")
		if err != nil {
			t.Fatal(err)
		}
		rev := p.Header.Revision
		var hashes []uint32
		for _, ep := range tc.endpoints {
			// A follower may not have applied rev yet: a linearizable read
			// on it waits until it has.
			if _, err := tc.clientOf(t, ep).Get(ctx, "/hash/a"); err != nil {
				t.Fatal(err)
			}
			h, err := c.HashKV(ctx, ep, rev)
			if err != nil {
				t.Fatal(err)
			}
			if h.HashRevision != rev {
				t.Fatalf("HashKV at %d on %s hashed revision %d", rev, ep, h.HashRevision)
			}
			hashes = append(hashes, h.Hash)
		}
		if hashes[1] != hashes[0] || hashes[2] != hashes[0] {
			t.Fatalf("the nodes' hashes at revision %d differ: %x", rev, hashes)
		}
		last, err := c.Put(ctx, "/hash/a", "3")
		if err != nil {
			t.Fatal(err)
		}
		again, err := c.HashKV(ctx, tc.endpoints[0], rev)
		if err != nil {
			t.Fatal(err)
		}
		now, err := c.HashKV(ctx, tc.endpoints[0], 0)
		if err != nil {
			t.Fatal(err)
		}
		if again.Hash != hashes[0] || now.Hash == hashes[0] || now.HashRevision != last.Header.Revision {
			t.Fatalf("after a put: %x at %d (was %x), %x at the newest revision %d", again.Hash, rev, hashes[0], now.Hash, now.HashRevision)
		}
	})

	t.Run("MoveLeader hands the leadership to another member", func(t *testing.T) {
		leader, err := c.Status(ctx, tc.endpoints[0])
		if err != nil {
			t.Fatal(err)
		}
		var at string
		for i, ep := range tc.endpoints {
			if tc.ids[i] == leader.Leader {
				at = ep
			}
		}
		to := tc.ids[0]
		if to == leader.Leader {
			to = tc.ids[1]
		}
		if _, err := tc.clientOf(t, at).MoveLeader(ctx, to); err != nil {
			t.Fatal(err)
		}
		for deadline := time.Now().Add(10 * time.Second); ; {
			s, err := c.Status(ctx, tc.endpoints[0])
			if err == nil && s.Leader == to {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("the leader is still not %d: %v %v", to, s, err)
			}
			time.Sleep(50 * time.Millisecond)
		}
		// A transferee that is no member is refused.
		if _, err := c.MoveLeader(ctx, 0x7777); err == nil {
			t.Fatal("MoveLeader to no member succeeded")
		}
	})

	t.Run("no alarm is raised, and disarming one that is not changes nothing", func(t *testing.T) {
		l, err := c.AlarmList(ctx)
		if err != nil || len(l.Alarms) != 0 {
			t.Fatalf("alarms: %v %v", l, err)
		}
		d, err := c.AlarmDisarm(ctx, &clientv3.AlarmMember{MemberID: tc.ids[0], Alarm: pb.AlarmType_NOSPACE})
		if err != nil || len(d.Alarms) != 0 {
			t.Fatalf("disarm: %v %v", d, err)
		}
	})

	t.Run("Leases lists the leases granted", func(t *testing.T) {
		// One node: it lists its own leases, as etcd's does, which a
		// follower that has not applied a revoke yet still holds (#716).
		c := tc.clientOf(t, tc.endpoints[0])
		a, err := c.Grant(ctx, 60)
		if err != nil {
			t.Fatal(err)
		}
		b, err := c.Grant(ctx, 60)
		if err != nil {
			t.Fatal(err)
		}
		l, err := c.Leases(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var ids []clientv3.LeaseID
		for _, s := range l.Leases {
			ids = append(ids, s.ID)
		}
		if !slices.Contains(ids, a.ID) || !slices.Contains(ids, b.ID) {
			t.Fatalf("Leases lists %v, not %d and %d", ids, a.ID, b.ID)
		}
		if _, err := c.Revoke(ctx, a.ID); err != nil {
			t.Fatal(err)
		}
		if l, err = c.Leases(ctx); err != nil {
			t.Fatal(err)
		}
		for _, s := range l.Leases {
			if s.ID == a.ID {
				t.Fatalf("a revoked lease is still listed: %v", l.Leases)
			}
		}
	})

	t.Run("users and roles", func(t *testing.T) {
		if _, err := c.UserAdd(ctx, "u", "p"); err != nil {
			t.Fatal(err)
		}
		if _, err := c.UserAdd(ctx, "u", "p"); !errors.Is(err, rpctypes.ErrUserAlreadyExist) {
			t.Fatalf("adding a user twice: %v", err)
		}
		if _, err := c.RoleAdd(ctx, "r"); err != nil {
			t.Fatal(err)
		}
		if _, err := c.RoleGrantPermission(ctx, "r", "/a", "/b", clientv3.PermissionType(clientv3.PermReadWrite)); err != nil {
			t.Fatal(err)
		}
		if _, err := c.UserGrantRole(ctx, "u", "r"); err != nil {
			t.Fatal(err)
		}
		if _, err := c.UserGrantRole(ctx, "u", "none"); !errors.Is(err, rpctypes.ErrRoleNotFound) {
			t.Fatalf("granting a missing role: %v", err)
		}
		u, err := c.UserGet(ctx, "u")
		if err != nil || !slices.Equal(u.Roles, []string{"r"}) {
			t.Fatalf("user u: %v %v", u, err)
		}
		r, err := c.RoleGet(ctx, "r")
		if err != nil || len(r.Perm) != 1 || string(r.Perm[0].Key) != "/a" || string(r.Perm[0].RangeEnd) != "/b" {
			t.Fatalf("role r: %v %v", r, err)
		}
		users, err := c.UserList(ctx)
		if err != nil || !slices.Equal(users.Users, []string{"u"}) {
			t.Fatalf("users: %v %v", users, err)
		}
		roles, err := c.RoleList(ctx)
		if err != nil || !slices.Equal(roles.Roles, []string{"r"}) {
			t.Fatalf("roles: %v %v", roles, err)
		}
		if _, err := c.UserChangePassword(ctx, "u", "q"); err != nil {
			t.Fatal(err)
		}
		if _, err := c.UserRevokeRole(ctx, "u", "r"); err != nil {
			t.Fatal(err)
		}
		if _, err := c.RoleRevokePermission(ctx, "r", "/a", "/b"); err != nil {
			t.Fatal(err)
		}
		if r, err = c.RoleGet(ctx, "r"); err != nil || len(r.Perm) != 0 {
			t.Fatalf("role r after the revoke: %v %v", r, err)
		}
		if _, err := c.UserDelete(ctx, "u"); err != nil {
			t.Fatal(err)
		}
		if _, err := c.RoleDelete(ctx, "r"); err != nil {
			t.Fatal(err)
		}
		if _, err := c.UserGet(ctx, "u"); !errors.Is(err, rpctypes.ErrUserNotFound) {
			t.Fatalf("a deleted user: %v", err)
		}
		if _, err := c.RoleGet(ctx, "r"); !errors.Is(err, rpctypes.ErrRoleNotFound) {
			t.Fatalf("a deleted role: %v", err)
		}
	})

	t.Run("a txn compares each target", func(t *testing.T) {
		l, err := c.Grant(ctx, 60)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Put(ctx, "/cmp/a", "x"); err != nil {
			t.Fatal(err)
		}
		p, err := c.Put(ctx, "/cmp/a", "y", clientv3.WithLease(l.ID))
		if err != nil {
			t.Fatal(err)
		}
		mod := p.Header.Revision
		g, err := c.Get(ctx, "/cmp/a")
		if err != nil {
			t.Fatal(err)
		}
		created := g.Kvs[0].CreateRevision
		for _, cs := range []struct {
			cmp  clientv3.Cmp
			want bool
		}{
			{clientv3.Compare(clientv3.Version("/cmp/a"), "=", 2), true},
			{clientv3.Compare(clientv3.Version("/cmp/a"), ">", 2), false},
			{clientv3.Compare(clientv3.Version("/cmp/none"), "=", 0), true},
			{clientv3.Compare(clientv3.CreateRevision("/cmp/a"), "=", created), true},
			{clientv3.Compare(clientv3.CreateRevision("/cmp/a"), "<", created), false},
			{clientv3.Compare(clientv3.ModRevision("/cmp/a"), "=", mod), true},
			{clientv3.Compare(clientv3.ModRevision("/cmp/a"), "!=", mod), false},
			{clientv3.Compare(clientv3.Value("/cmp/a"), "=", "y"), true},
			{clientv3.Compare(clientv3.Value("/cmp/a"), ">", "x"), true},
			{clientv3.Compare(clientv3.Value("/cmp/a"), "<", "x"), false},
			{clientv3.Compare(clientv3.LeaseValue("/cmp/a"), "=", l.ID), true},
			{clientv3.Compare(clientv3.LeaseValue("/cmp/a"), "=", 0), false},
			// Over a range, every key must match.
			{clientv3.Compare(clientv3.Version("/cmp/"), ">", 0).WithPrefix(), true},
			{clientv3.Compare(clientv3.Value("/cmp/"), "=", "x").WithPrefix(), false},
		} {
			r, err := c.Txn(ctx).If(cs.cmp).Then(clientv3.OpGet("/cmp/a")).Else(clientv3.OpGet("/cmp/none")).Commit()
			if err != nil {
				t.Fatal(err)
			}
			if r.Succeeded != cs.want {
				t.Errorf("compare %v: succeeded %v, want %v", cs.cmp, r.Succeeded, cs.want)
			}
			// The branch taken ran: its get finds the key only on success.
			if got := len(r.Responses[0].GetResponseRange().Kvs) == 1; got != cs.want {
				t.Errorf("compare %v: the branch's get found the key %v", cs.cmp, got)
			}
		}
	})

	t.Run("a watch's options: prev_kv, a filter, a progress request and its cancel", func(t *testing.T) {
		wctx, wcancel := context.WithCancel(ctx)
		defer wcancel()
		if _, err := c.Put(ctx, "/w/a", "old"); err != nil {
			t.Fatal(err)
		}
		wc := c.Watch(wctx, "/w/", clientv3.WithPrefix(), clientv3.WithPrevKV(), clientv3.WithFilterPut(), clientv3.WithCreatedNotify())
		if r := <-wc; !r.Created {
			t.Fatalf("the first response is not the watch's creation: %+v", r)
		}
		if err := c.RequestProgress(wctx); err != nil {
			t.Fatal(err)
		}
		if r := <-wc; !r.IsProgressNotify() {
			t.Fatalf("a progress request got %+v", r)
		}
		if _, err := c.Put(ctx, "/w/a", "new"); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Delete(ctx, "/w/a"); err != nil {
			t.Fatal(err)
		}
		// The put is filtered out; the delete comes with the value before it.
		r := <-wc
		if len(r.Events) != 1 || r.Events[0].Type != clientv3.EventTypeDelete || r.Events[0].PrevKv == nil || string(r.Events[0].PrevKv.Value) != "new" {
			t.Fatalf("the events: %+v", r.Events)
		}
		wcancel()
		for range wc {
		}
	})

	t.Run("the health checks, /debug/vars and /metrics over HTTP/1.1", func(t *testing.T) {
		hc := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{
			Certificates: []tls.Certificate{tc.cert}, RootCAs: tc.ca.Pool, MinVersion: tls.VersionTLS13,
		}}}
		get := func(method, path string) (int, string) {
			t.Helper()
			req, err := http.NewRequestWithContext(ctx, method, "https://"+tc.endpoints[0]+path, nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := hc.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			b, err := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if err != nil {
				t.Fatal(err)
			}
			return resp.StatusCode, string(b)
		}
		for path, want := range map[string]string{
			"/livez":                    "ok\n",
			"/readyz":                   "ok\n",
			"/readyz/linearizable_read": "ok\n",
			"/livez?verbose":            "[+]serializable_read ok\nok\n",
			"/readyz?verbose&exclude=non_learner&exclude=none": "[+]data_corruption ok\n[+]serializable_read ok\n[+]linearizable_read ok\nok\n",
		} {
			if code, body := get(http.MethodGet, path); code != http.StatusOK || body != want {
				t.Errorf("%s: %d %q, want 200 %q", path, code, body, want)
			}
		}
		if code, _ := get(http.MethodGet, "/readyz/none"); code != http.StatusNotFound {
			t.Errorf("a check that does not exist: %d, want 404", code)
		}
		if code, _ := get(http.MethodPost, "/health"); code != http.StatusMethodNotAllowed {
			t.Errorf("POST /health: %d, want 405", code)
		}
		if code, body := get(http.MethodGet, "/debug/vars"); code != http.StatusOK || !strings.Contains(body, `"memstats"`) {
			t.Errorf("/debug/vars: %d, %.200s", code, body)
		}
		code, body := get(http.MethodGet, "/metrics")
		if code != http.StatusOK || !strings.Contains(body, "etcd_server_has_leader 1") || !strings.Contains(body, `etcd_server_healthchecks_total{name="linearizable_read",status="success",type="readyz"}`) {
			t.Errorf("/metrics: %d, without the leader gauge or the checks' counts:\n%s", code, body)
		}
	})
}
