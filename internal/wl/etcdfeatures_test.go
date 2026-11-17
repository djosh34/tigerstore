package wl

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/status"
)

// The space quota, as etcd's: a put past --quota-backend-bytes is refused
// with "database space exceeded" and the node raises NOSPACE; while the
// alarm is up, puts, Txns that put and lease grants are refused and reads
// and deletes go on; once the store is compacted small again and the
// alarm disarmed, puts are taken again.
func TestTheSpaceQuotaRaisesNOSPACEAndRefusesWrites(t *testing.T) {
	tc := newTestClusterWith(t, func(tc *testCluster) {
		tc.config = func(c *Config) { c.QuotaBackendBytes = 64 << 10 }
	})
	c := tc.client(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	value := strings.Repeat("v", 4<<10)
	var err error
	put := 0
	for ; put < 64; put++ {
		if _, err = c.Put(ctx, fmt.Sprintf("/q/%02d", put), value); err != nil {
			break
		}
	}
	if !errors.Is(err, rpctypes.ErrNoSpace) {
		t.Fatalf("after %d puts of 4 KiB under a 64 KiB quota: %v", put, err)
	}
	// The node raises NOSPACE through the log once it refused the put,
	// as etcd's does.
	var alarms *clientv3.AlarmResponse
	for {
		alarms, err = c.AlarmList(ctx)
		if err == nil && slices.ContainsFunc(alarms.Alarms, func(a *pb.AlarmMember) bool { return a.Alarm == pb.AlarmType_NOSPACE }) {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("alarms after the quota: %v %v", alarms, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := c.Put(ctx, "/q/small", "x"); !errors.Is(err, rpctypes.ErrNoSpace) {
		t.Fatalf("a put while NOSPACE is up: %v", err)
	}
	if _, err := c.Txn(ctx).Then(clientv3.OpPut("/q/small", "x")).Commit(); !errors.Is(err, rpctypes.ErrNoSpace) {
		t.Fatalf("a Txn that puts while NOSPACE is up: %v", err)
	}
	if _, err := c.Grant(ctx, 60); !errors.Is(err, rpctypes.ErrNoSpace) {
		t.Fatalf("a lease grant while NOSPACE is up: %v", err)
	}
	if _, err := c.Txn(ctx).Then(clientv3.OpGet("/q/00")).Commit(); err != nil {
		t.Fatalf("a Txn that reads while NOSPACE is up: %v", err)
	}
	r, err := c.Get(ctx, "/q/", clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil || r.Count != int64(put) {
		t.Fatalf("a read while NOSPACE is up: %v %v", r, err)
	}
	d, err := c.Delete(ctx, "/q/", clientv3.WithPrefix())
	if err != nil || d.Deleted != int64(put) {
		t.Fatalf("a delete while NOSPACE is up: %v %v", d, err)
	}
	if _, err := c.Compact(ctx, d.Header.Revision, clientv3.WithCompactPhysical()); err != nil {
		t.Fatal(err)
	}
	// Each node raises its own NOSPACE as it refuses a put, some after
	// the list above: disarm until none is left.
	for len(alarms.Alarms) > 0 {
		for _, a := range alarms.Alarms {
			if _, err := c.AlarmDisarm(ctx, &clientv3.AlarmMember{MemberID: a.MemberID, Alarm: a.Alarm}); err != nil {
				t.Fatal(err)
			}
		}
		if alarms, err = c.AlarmList(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.Put(ctx, "/q/after", value); err != nil {
		t.Fatalf("a put once the store is small and the alarm disarmed: %v", err)
	}
}

// Periodic auto compaction, as etcd's: with a retention of a second the
// leader compacts at the revision it noted a second before, so a read of
// the first revisions soon gets "compacted".
func TestPeriodicAutoCompactionCompactsWhatItsRetentionLeaves(t *testing.T) {
	tc := newTestClusterWith(t, func(tc *testCluster) {
		tc.config = func(c *Config) { c.AutoCompactPeriod = time.Second }
	})
	c := tc.client(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	first, err := c.Put(ctx, "/ac/k", "0")
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; ; i++ {
		if _, err := c.Put(ctx, "/ac/k", strconv.Itoa(i)); err != nil {
			t.Fatal(err)
		}
		_, err := c.Get(ctx, "/ac/k", clientv3.WithRev(first.Header.Revision))
		if errors.Is(err, rpctypes.ErrCompacted) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if ctx.Err() != nil {
			t.Fatal("nothing was compacted in a minute with a retention of a second")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, err := c.Get(ctx, "/ac/k"); err != nil {
		t.Fatalf("the head after an auto compaction: %v", err)
	}
}

// Downgrades, as etcd v3.7.2's: VALIDATE and ENABLE take only 3.6, ENABLE
// lowers the storage version, a second one and a VALIDATE are refused
// while it is on, and CANCEL brings the version back.
func TestADowngradeLowersTheVersionUntilCanceled(t *testing.T) {
	tc := newTestCluster(t)
	c := tc.client(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	storage := func() string {
		t.Helper()
		s, err := c.Status(ctx, tc.endpoints[0])
		if err != nil {
			t.Fatal(err)
		}
		return s.StorageVersion
	}
	r, err := c.Downgrade(ctx, clientv3.DowngradeValidate, "3.6")
	if err != nil || r.Version != "3.7" {
		t.Fatalf("validating 3.6: %v %v", r, err)
	}
	for _, v := range []string{"3.5", "4.6", "3.7"} {
		if _, err := c.Downgrade(ctx, clientv3.DowngradeValidate, v); !errors.Is(err, rpctypes.ErrInvalidDowngradeTargetVersion) {
			t.Fatalf("validating %s: %v", v, err)
		}
	}
	if _, err := c.Downgrade(ctx, clientv3.DowngradeValidate, "three"); !errors.Is(err, rpctypes.ErrWrongDowngradeVersionFormat) {
		t.Fatalf("validating a target that is not a version: %v", err)
	}
	if v := storage(); v != "3.7.0" {
		t.Fatalf("the storage version before a downgrade is %q", v)
	}
	if _, err := c.Downgrade(ctx, clientv3.DowngradeEnable, "3.6.0"); err != nil {
		t.Fatal(err)
	}
	if v := storage(); v != "3.6.0" {
		t.Fatalf("the storage version during a downgrade is %q, want 3.6.0", v)
	}
	if _, err := c.Downgrade(ctx, clientv3.DowngradeEnable, "3.6"); !errors.Is(err, rpctypes.ErrDowngradeInProcess) {
		t.Fatalf("a second downgrade: %v", err)
	}
	if _, err := c.Downgrade(ctx, clientv3.DowngradeValidate, "3.6"); !errors.Is(err, rpctypes.ErrDowngradeInProcess) {
		t.Fatalf("validating during a downgrade: %v", err)
	}
	if _, err := c.Downgrade(ctx, clientv3.DowngradeCancel, ""); err != nil {
		t.Fatal(err)
	}
	if v := storage(); v != "3.7.0" {
		t.Fatalf("the storage version after the cancel is %q, want 3.7.0", v)
	}
}

// With auth on, each call is checked against the caller's roles, as
// etcd's: a user who may read /auth/ reads there and nowhere else, and
// may not write, delete or run a Txn that writes; root may do all.
func TestAuthChecksEachCallAgainstTheCallersRoles(t *testing.T) {
	tc := newTestCluster(t)
	// One node: a call is checked against the auth store the node it
	// reaches has applied.
	c := tc.clientOf(t, tc.endpoints[0])
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for _, k := range []string{"/auth/a", "/other/a"} {
		if _, err := c.Put(ctx, k, "v"); err != nil {
			t.Fatal(err)
		}
	}
	// A lease whose key alice may not write: its revoke deletes it.
	lease, err := c.Grant(ctx, 600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Put(ctx, "/other/leased", "v", clientv3.WithLease(lease.ID)); err != nil {
		t.Fatal(err)
	}
	steps := []func() error{
		func() error { _, err := c.UserAdd(ctx, "root", "rootpw"); return err },
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
			t.Fatal(err)
		}
	}
	login := func(user, password string) *clientv3.Client {
		t.Helper()
		cl, err := clientv3.New(clientv3.Config{Endpoints: tc.endpoints[:1], TLS: tc.clientTLS(), Username: user, Password: password, DialTimeout: 5 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = cl.Close() })
		return cl
	}
	alice, root := login("alice", "alicepw"), login("root", "rootpw")
	if _, err := alice.Get(ctx, "/auth/a"); err != nil {
		t.Fatalf("alice reads /auth/: %v", err)
	}
	denied := map[string]func() error{
		"reads /other/": func() error { _, err := alice.Get(ctx, "/other/a"); return err },
		"reads a range past /auth/": func() error {
			_, err := alice.Get(ctx, "/auth/", clientv3.WithRange("/other/z"))
			return err
		},
		"puts in /auth/":    func() error { _, err := alice.Put(ctx, "/auth/b", "v"); return err },
		"deletes in /auth/": func() error { _, err := alice.Delete(ctx, "/auth/a"); return err },
		"runs a Txn that writes": func() error {
			_, err := alice.Txn(ctx).If(clientv3.Compare(clientv3.Value("/auth/a"), "=", "v")).Then(clientv3.OpPut("/auth/a", "w")).Commit()
			return err
		},
		"runs a Txn that compares /other/": func() error {
			_, err := alice.Txn(ctx).If(clientv3.Compare(clientv3.Value("/other/a"), "=", "v")).Then(clientv3.OpGet("/auth/a")).Commit()
			return err
		},
		"adds a user": func() error { _, err := alice.UserAdd(ctx, "bob", "p"); return err },
		"revokes a lease whose keys she may not write": func() error {
			_, err := alice.Revoke(ctx, lease.ID)
			return err
		},
	}
	for what, call := range denied {
		if err := call(); !errors.Is(err, rpctypes.ErrPermissionDenied) {
			t.Errorf("alice %s: %v", what, err)
		}
	}
	if _, err := alice.Txn(ctx).If(clientv3.Compare(clientv3.Value("/auth/a"), "=", "v")).Then(clientv3.OpGet("/auth/a")).Commit(); err != nil {
		t.Errorf("alice runs a Txn that reads /auth/: %v", err)
	}
	if _, err := root.Put(ctx, "/other/b", "v"); err != nil {
		t.Errorf("root puts: %v", err)
	}
	if _, err := root.Revoke(ctx, lease.ID); err != nil {
		t.Errorf("root revokes the lease: %v", err)
	}
	// With no token, the client's certificate names its user, as in
	// etcd: "node", who has no role.
	if _, err := c.Get(ctx, "/auth/a"); !errors.Is(err, rpctypes.ErrPermissionDenied) {
		t.Errorf("a client whose certificate names a user with no role: %v", err)
	}
	if _, err := root.AuthDisable(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get(ctx, "/other/a"); err != nil {
		t.Errorf("that client once auth is off: %v", err)
	}
}

// A range's options, as etcd's executeRange takes them: sort orders and
// targets, revision filters, keys only, count only, and a limit with
// more and the whole range's count.
func TestARangesOptionsShapeItsAnswer(t *testing.T) {
	tc := newTestCluster(t)
	c := tc.client(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	// /r/a created first, /r/c second, /r/b third; /r/a put again last.
	for _, p := range [][2]string{{"/r/a", "3"}, {"/r/c", "1"}, {"/r/b", "2"}, {"/r/a", "4"}} {
		if _, err := c.Put(ctx, p[0], p[1]); err != nil {
			t.Fatal(err)
		}
	}
	keys := func(opts ...clientv3.OpOption) []string {
		t.Helper()
		r, err := c.Get(ctx, "/r/", append([]clientv3.OpOption{clientv3.WithPrefix()}, opts...)...)
		if err != nil {
			t.Fatal(err)
		}
		var ks []string
		for _, kv := range r.Kvs {
			ks = append(ks, string(kv.Key))
		}
		return ks
	}
	for _, c := range []struct {
		what string
		opts []clientv3.OpOption
		want []string
	}{
		{"by key", nil, []string{"/r/a", "/r/b", "/r/c"}},
		{"by key, descending", []clientv3.OpOption{clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend)}, []string{"/r/c", "/r/b", "/r/a"}},
		{"by value", []clientv3.OpOption{clientv3.WithSort(clientv3.SortByValue, clientv3.SortAscend)}, []string{"/r/c", "/r/b", "/r/a"}},
		{"by create revision", []clientv3.OpOption{clientv3.WithSort(clientv3.SortByCreateRevision, clientv3.SortAscend)}, []string{"/r/a", "/r/c", "/r/b"}},
		{"by mod revision, descending", []clientv3.OpOption{clientv3.WithSort(clientv3.SortByModRevision, clientv3.SortDescend)}, []string{"/r/a", "/r/b", "/r/c"}},
		{"by version, descending", []clientv3.OpOption{clientv3.WithSort(clientv3.SortByVersion, clientv3.SortDescend), clientv3.WithLimit(1)}, []string{"/r/a"}},
		{"keys only", []clientv3.OpOption{clientv3.WithKeysOnly()}, []string{"/r/a", "/r/b", "/r/c"}},
	} {
		if got := keys(c.opts...); !slices.Equal(got, c.want) {
			t.Errorf("a range sorted %s: %v, want %v", c.what, got, c.want)
		}
	}
	all, err := c.Get(ctx, "/r/", clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	aMod := all.Kvs[0].ModRevision
	if got := keys(clientv3.WithMinModRev(aMod)); !slices.Equal(got, []string{"/r/a"}) {
		t.Errorf("a range from /r/a's mod revision: %v", got)
	}
	if got := keys(clientv3.WithMaxCreateRev(all.Kvs[2].CreateRevision)); !slices.Equal(got, []string{"/r/a", "/r/c"}) {
		t.Errorf("a range up to /r/c's create revision: %v", got)
	}
	if r, err := c.Get(ctx, "/r/", clientv3.WithPrefix(), clientv3.WithKeysOnly()); err != nil || len(r.Kvs[0].Value) != 0 {
		t.Errorf("a keys-only range has values: %v %v", r, err)
	}
	if r, err := c.Get(ctx, "/r/", clientv3.WithPrefix(), clientv3.WithCountOnly()); err != nil || r.Count != 3 || len(r.Kvs) != 0 {
		t.Errorf("a count-only range: %v %v", r, err)
	}
	if r, err := c.Get(ctx, "/r/", clientv3.WithPrefix(), clientv3.WithLimit(2)); err != nil || len(r.Kvs) != 2 || !r.More || r.Count != 3 {
		t.Errorf("a range limited to 2 of 3: %v %v", r, err)
	}
	if r, err := c.Get(ctx, "/r/a", clientv3.WithRev(aMod-1), clientv3.WithSerializable()); err != nil || string(r.Kvs[0].Value) != "3" {
		t.Errorf("a serializable read before /r/a's last put: %v %v", r, err)
	}
}

// RangeStream, as etcd v3.7.2's: the range in chunks of 10 keys first and
// then more, the last with the header, the whole range's count and more;
// a limit cuts the keys sent, not the count; a sort or a revision filter
// is refused, as etcd refuses them.
func TestARangeStreamSendsTheRangeInChunks(t *testing.T) {
	tc := newTestCluster(t)
	c := tc.client(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for i := range 100 {
		if _, err := c.Put(ctx, fmt.Sprintf("/s/%03d", i), "v"); err != nil {
			t.Fatal(err)
		}
	}
	kv := pb.NewKVClient(c.ActiveConnection())
	stream := func(r *pb.RangeRequest) (chunks []int, last *pb.RangeResponse, err error) {
		t.Helper()
		st, err := kv.RangeStream(ctx, r)
		if err != nil {
			return nil, nil, err
		}
		for {
			resp, err := st.Recv()
			if errors.Is(err, io.EOF) {
				return chunks, last, nil
			}
			if err != nil {
				return chunks, last, err
			}
			chunks = append(chunks, len(resp.RangeResponse.Kvs))
			last = resp.RangeResponse
		}
	}
	chunks, last, err := stream(&pb.RangeRequest{Key: []byte("/s/"), RangeEnd: []byte("/s0")})
	if err != nil {
		t.Fatal(err)
	}
	sum := 0
	for _, k := range chunks {
		sum += k
	}
	if sum != 100 || chunks[0] != 10 || len(chunks) < 2 || last.Count != 100 || last.More || last.Header == nil {
		t.Fatalf("a RangeStream of 100 keys: chunks %v, last %v", chunks, last)
	}
	chunks, last, err = stream(&pb.RangeRequest{Key: []byte("/s/"), RangeEnd: []byte("/s0"), Limit: 25})
	if err != nil {
		t.Fatal(err)
	}
	sum = 0
	for _, k := range chunks {
		sum += k
	}
	if sum != 25 || last.Count != 100 || !last.More {
		t.Fatalf("a RangeStream limited to 25: chunks %v, last %v", chunks, last)
	}
	if _, last, err = stream(&pb.RangeRequest{Key: []byte("/s/"), RangeEnd: []byte("/s0"), CountOnly: true}); err != nil || last.Count != 100 || len(last.Kvs) != 0 {
		t.Fatalf("a count-only RangeStream: %v %v", last, err)
	}
	for what, r := range map[string]*pb.RangeRequest{
		"sorted by value":   {Key: []byte("/s/"), RangeEnd: []byte("/s0"), SortTarget: pb.RangeRequest_VALUE, SortOrder: pb.RangeRequest_ASCEND},
		"filtered by a mod": {Key: []byte("/s/"), RangeEnd: []byte("/s0"), MinModRevision: 5},
	} {
		if _, _, err := stream(r); status.Code(err) != codes.Unimplemented {
			t.Errorf("a RangeStream %s: %v", what, err)
		}
	}
}

// A watch from a revision before the compacted one is canceled with the
// compacted revision, as etcd's; one from the compacted revision gets the
// changes since.
func TestAWatchFromACompactedRevisionIsCanceled(t *testing.T) {
	tc := newTestCluster(t)
	c := tc.client(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	first, err := c.Put(ctx, "/w/k", "0")
	if err != nil {
		t.Fatal(err)
	}
	last := first
	for i := 1; i < 5; i++ {
		if last, err = c.Put(ctx, "/w/k", strconv.Itoa(i)); err != nil {
			t.Fatal(err)
		}
	}
	compacted := last.Header.Revision - 1
	if _, err := c.Compact(ctx, compacted); err != nil {
		t.Fatal(err)
	}
	resp := <-c.Watch(ctx, "/w/", clientv3.WithPrefix(), clientv3.WithRev(first.Header.Revision))
	if !resp.Canceled || resp.CompactRevision != compacted || !errors.Is(resp.Err(), rpctypes.ErrCompacted) {
		t.Fatalf("a watch from before the compacted revision: canceled %v, compact revision %d, %v", resp.Canceled, resp.CompactRevision, resp.Err())
	}
	resp = <-c.Watch(ctx, "/w/", clientv3.WithPrefix(), clientv3.WithRev(compacted))
	if resp.Err() != nil || len(resp.Events) == 0 || resp.Events[0].Kv.ModRevision != compacted {
		t.Fatalf("a watch from the compacted revision: %v %v", resp.Events, resp.Err())
	}
}

// A request compressed with gzip, as a gRPC client may send it, is taken
// as the same request uncompressed.
func TestAGzipRequestIsTaken(t *testing.T) {
	tc := newTestCluster(t)
	c := tc.client(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	kv := pb.NewKVClient(c.ActiveConnection())
	value := strings.Repeat("compressible ", 1000)
	if _, err := kv.Put(ctx, &pb.PutRequest{Key: []byte("/gz/k"), Value: []byte(value)}, grpc.UseCompressor(gzip.Name)); err != nil {
		t.Fatal(err)
	}
	r, err := kv.Range(ctx, &pb.RangeRequest{Key: []byte("/gz/k")}, grpc.UseCompressor(gzip.Name))
	if err != nil || len(r.Kvs) != 1 || string(r.Kvs[0].Value) != value {
		t.Fatalf("a range of the gzip put, itself compressed: %v", err)
	}
}

// etcd's HTTP side beside gRPC: pprof under /debug/pprof/ with
// --enable-pprof, CORS headers for an allowed origin only, OPTIONS
// answered at once, and a --listen-metrics-urls listener that serves
// /metrics, /health, /livez and /readyz, GET only. An alarm fails /health
// unless the request excludes it.
func TestTheHTTPEndpointsServeAsEtcds(t *testing.T) {
	var metricsAddrs []string
	tc := newTestClusterWith(t, func(tc *testCluster) {
		tc.config = func(c *Config) {
			c.Pprof = true
			c.CORS = map[string]struct{}{"https://ok.example": {}}
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			metricsAddrs = append(metricsAddrs, ln.Addr().String())
			c.MetricsListeners = []net.Listener{ln}
		}
	})
	c := tc.client(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	hc := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{
		Certificates: []tls.Certificate{tc.cert}, RootCAs: tc.ca.Pool, MinVersion: tls.VersionTLS13,
	}}}
	do := func(method, url string, header map[string]string) (*http.Response, string) {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, method, url, nil)
		if err != nil {
			t.Fatal(err)
		}
		for k, v := range header {
			req.Header.Set(k, v)
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
		return resp, string(b)
	}
	client := "https://" + tc.endpoints[0]
	for _, path := range []string{"/debug/pprof/", "/debug/pprof/cmdline", "/debug/pprof/symbol"} {
		if resp, _ := do(http.MethodGet, client+path, nil); resp.StatusCode != http.StatusOK {
			t.Errorf("GET %s: %s", path, resp.Status)
		}
	}
	resp, _ := do(http.MethodOptions, client+"/version", map[string]string{"Origin": "https://ok.example"})
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Access-Control-Allow-Origin") != "https://ok.example" {
		t.Errorf("OPTIONS from an allowed origin: %s, %v", resp.Status, resp.Header)
	}
	resp, _ = do(http.MethodGet, client+"/version", map[string]string{"Origin": "https://other.example"})
	if resp.Header.Get("Access-Control-Allow-Origin") != "" {
		t.Errorf("a request from another origin got CORS headers: %v", resp.Header)
	}
	metrics := "http://" + metricsAddrs[0]
	for path, want := range map[string]string{"/metrics": "etcd_server_has_leader", "/health": `"health":"true"`, "/livez": "ok", "/readyz": "ok"} {
		if resp, body := do(http.MethodGet, metrics+path, nil); resp.StatusCode != http.StatusOK || !strings.Contains(body, want) {
			t.Errorf("GET %s on the metrics listener: %s %q", path, resp.Status, body)
		}
	}
	if resp, _ := do(http.MethodPost, metrics+"/health", nil); resp.StatusCode != http.StatusMethodNotAllowed || resp.Header.Get("Allow") != http.MethodGet {
		t.Errorf("POST /health: %s, Allow %q", resp.Status, resp.Header.Get("Allow"))
	}
	if resp, _ := do(http.MethodGet, metrics+"/nothing", nil); resp.StatusCode != http.StatusNotFound {
		t.Errorf("GET /nothing: %s", resp.Status)
	}
	// An alarm fails /health, unless excluded.
	m := pb.NewMaintenanceClient(c.ActiveConnection())
	if _, err := m.Alarm(ctx, &pb.AlarmRequest{Action: pb.AlarmRequest_ACTIVATE, MemberID: tc.ids[0], Alarm: pb.AlarmType_NOSPACE}); err != nil {
		t.Fatal(err)
	}
	for {
		resp, body := do(http.MethodGet, metrics+"/health", nil)
		if resp.StatusCode == http.StatusServiceUnavailable && strings.Contains(body, "ALARM NOSPACE") {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("/health with NOSPACE up: %s %q", resp.Status, body)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if resp, body := do(http.MethodGet, metrics+"/health?exclude=NOSPACE", nil); resp.StatusCode != http.StatusOK {
		t.Errorf("/health excluding NOSPACE: %s %q", resp.Status, body)
	}
	if _, err := c.AlarmDisarm(ctx, &clientv3.AlarmMember{MemberID: tc.ids[0], Alarm: pb.AlarmType_NOSPACE}); err != nil {
		t.Fatal(err)
	}
}

// A watch create that etcd refuses comes back created and canceled at
// once, with watch ID -1 and etcd's reason: a range whose end is before
// its key, or a watch ID the stream already uses.
func TestAWatchEtcdRefusesIsCanceledAtOnce(t *testing.T) {
	tc := newTestCluster(t)
	c := tc.client(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	ws, err := pb.NewWatchClient(c.ActiveConnection()).Watch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	create := func(r *pb.WatchCreateRequest) *pb.WatchResponse {
		t.Helper()
		if err := ws.Send(&pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{CreateRequest: r}}); err != nil {
			t.Fatal(err)
		}
		resp, err := ws.Recv()
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	if r := create(&pb.WatchCreateRequest{Key: []byte("/z"), RangeEnd: []byte("/a")}); !r.Created || !r.Canceled || r.WatchId != -1 || r.CancelReason != "mvcc: watcher range is empty" {
		t.Errorf("a watch of an empty range: %v", r)
	}
	if r := create(&pb.WatchCreateRequest{Key: []byte("/k"), WatchId: 5}); !r.Created || r.Canceled || r.WatchId != 5 {
		t.Fatalf("a watch with ID 5: %v", r)
	}
	if r := create(&pb.WatchCreateRequest{Key: []byte("/k2"), WatchId: 5}); !r.Canceled || r.WatchId != -1 || r.CancelReason != "mvcc: duplicate watch ID provided on the WatchStream" {
		t.Errorf("a second watch with ID 5: %v", r)
	}
}

// A call of a method no service serves gets grpc-go's answer:
// Unimplemented, naming the method or the service.
func TestAnUnknownMethodIsUnimplemented(t *testing.T) {
	tc := newTestCluster(t)
	c := tc.client(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for method, want := range map[string]string{
		"/etcdserverpb.KV/Nothing": "unknown method Nothing for service etcdserverpb.KV",
		"/no.Service/Call":         "unknown service no.Service",
	} {
		err := c.ActiveConnection().Invoke(ctx, method, &pb.RangeRequest{}, &pb.RangeResponse{})
		if status.Code(err) != codes.Unimplemented || status.Convert(err).Message() != want {
			t.Errorf("%s: %v, want Unimplemented %q", method, err, want)
		}
	}
}

// A watch that asks for fragments gets its events past the request limit
// split across responses, as etcd's: here values of 1 MiB with the values
// before them, about 2 MiB an event; the client puts them back together,
// every value whole.
func TestAWatchsLargeEventsComeInFragments(t *testing.T) {
	tc := newTestCluster(t)
	c := tc.client(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	values := []string{strings.Repeat("a", 1<<20), strings.Repeat("b", 1<<20), strings.Repeat("c", 1<<20)}
	var first int64
	for i, v := range values {
		r, err := c.Put(ctx, "/frag/k", v)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			first = r.Header.Revision
		}
	}
	got := 0
	for resp := range c.Watch(ctx, "/frag/k", clientv3.WithRev(first), clientv3.WithPrevKV(), clientv3.WithFragment()) {
		if err := resp.Err(); err != nil {
			t.Fatal(err)
		}
		for _, ev := range resp.Events {
			if string(ev.Kv.Value) != values[got] {
				t.Fatalf("event %d has %d bytes, not value %d whole", got, len(ev.Kv.Value), got)
			}
			if got > 0 && string(ev.PrevKv.Value) != values[got-1] {
				t.Fatalf("event %d's value before has %d bytes, not value %d whole", got, len(ev.PrevKv.Value), got-1)
			}
			got++
		}
		if got == len(values) {
			break
		}
	}
	if got != len(values) {
		t.Fatalf("the watch got %d events, want %d", got, len(values))
	}
}
