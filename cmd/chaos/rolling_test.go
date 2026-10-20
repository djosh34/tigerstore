package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/internal/committed"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// These tests use two builds of this source, not historical releases.
// The old build supports entry format 1 and peer range 2-2; the new one
// supports entry formats 1-2 and peer range 2-3. The workflow changes only
// those constants. No test hook runs inside the server.
func TestRollingUpgrade(t *testing.T) {
	for _, seed := range []uint64{148, 174} {
		t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
			r := startRolling(t, seed)
			r.stage("initial", 1)
			r.restart(0, r.newBin)
			r.stage("first upgrade", 1)
			r.restart(1, r.newBin)
			r.stage("second upgrade", 1)

			// Keep the exact announcement bytes of node 2's upgraded run.
			// Reproposing it later models a delayed MsgProp without TLS
			// byte corruption or a new announcement from the rollback run.
			r.stop(1)
			stale := r.announcement(1, 2)
			r.start(1, r.oldBin)
			r.stage("rollback", 1)
			r.stop(1)
			rolled := r.announcement(1, 1)
			if rolled.Run <= stale.Run {
				t.Fatalf("rollback run %d did not supersede run %d", rolled.Run, stale.Run)
			}
			r.start(1, r.oldBin)
			r.stage("rollback restart", 1)
			r.restart(2, r.newBin)
			r.stage("third node upgraded, rollback holds gate", 1)
			r.replay(stale)
			r.stage("delayed announcement after rollback", 1)

			// A successful send alone is not coverage: require the stale
			// bytes in the committed WAL after the rollback announcement.
			r.stop(1)
			n := r.wal(1)
			seen := false
			for _, a := range announcements(t, n) {
				if a.Index > rolled.Index && bytes.Equal(a.Batch, stale.Batch) {
					seen = true
					t.Logf("delayed announcement committed at %d, rollback at %d", a.Index, rolled.Index)
				}
			}
			if !seen {
				t.Fatal("delayed announcement was not committed after rollback")
			}
			for _, e := range n.Entries {
				if e.GetIndex() <= n.Commit && e.GetType() == raftpb.EntryNormal && len(e.Data) > 0 && e.Data[0] == 0 {
					t.Fatalf("entry %d has versioned data %x before the last node supports it", e.GetIndex(), e.Data)
				}
			}
			r.start(1, r.newBin)
			r.stage("last node upgraded", 2)
			r.stop(1)
			n = r.wal(1)
			format2 := false
			for _, e := range n.Entries {
				if e.GetIndex() <= n.Commit && e.GetType() == raftpb.EntryNormal && len(e.Data) > 1 && e.Data[0] == 0 && e.Data[1] == 2 {
					format2 = true
				}
			}
			if !format2 {
				t.Fatal("no committed format-2 entry after activation")
			}
			r.refuseOld(1)
			r.start(1, r.newBin)
			r.stage("new binary recovers after refused downgrade", 2)
		})
	}
}

// #174 also asks for the other order: upgrade the last node while the
// rollback node cannot announce. The remaining nodes must wait for its
// confirmation instead of treating its earlier support as consent.
func TestRollingRollbackBeforeAnnouncement(t *testing.T) {
	r := startRolling(t, 175)
	r.stage("initial", 1)
	r.restart(0, r.newBin)
	r.stage("first upgrade", 1)
	r.restart(1, r.newBin)
	r.stage("second upgrade", 1)
	r.stop(1)
	// The rollback binary is selected but still down. The old support
	// announcement remains in replicated state when node 3 upgrades.
	r.restart(2, r.newBin)
	// A leader election can outlast a short sleep. Keep the rollback
	// down for twenty election timeouts, or until activation is seen.
	v := 1
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		var err error
		v, _, err = r.metric(0)
		if err != nil {
			t.Fatal(err)
		}
		if v != 1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if v != 1 {
		t.Errorf("version %d activated while the rollback node was down; want 1 (#174)", v)
		r.refuseOld(1)
		r.start(1, r.newBin)
		r.stage("restore the new binary after unsafe activation", 2)
		return
	}
	r.start(1, r.oldBin)
	r.stage("rollback announces after last upgrade", 1)
}

// peerMin is the oldest peer protocol version both builds speak.
const peerMin = 2

type rollingRun struct {
	t                   *testing.T
	c                   *cluster
	traffic             *traffic
	dir, oldBin, newBin string
	stopped             bool
	ranges              [3]int
	// nodes is how the nodes stood before finish stopped them.
	nodes string
}

func startRolling(t *testing.T, seed uint64) *rollingRun {
	t.Helper()
	oldBin, newBin := os.Getenv("ROLLING_OLD"), os.Getenv("ROLLING_NEW")
	if oldBin == "" || newBin == "" {
		t.Skip("set ROLLING_OLD and ROLLING_NEW to the two workflow builds")
	}
	dir := os.Getenv("ROLLING_OUT")
	if dir == "" {
		dir = t.TempDir()
	}
	dir = filepath.Join(dir, fmt.Sprintf("seed-%d", seed))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	c, err := newCluster(dir, oldBin, seed, [3]string{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r := &rollingRun{t: t, c: c, dir: dir, oldBin: oldBin, newBin: newBin}
	t.Cleanup(func() {
		if err := c.closeProxies(); err != nil {
			t.Error(err)
		}
	})
	t.Cleanup(func() {
		for _, n := range c.nodes {
			if err := n.log.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	t.Cleanup(func() {
		if !r.stopped {
			if err := c.stop(); err != nil {
				t.Error(err)
			}
		}
	})
	// This runs after finish and before the stop, so a failure anywhere,
	// in the start too, says how each node stood, and how to run the test
	// again.
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		if r.nodes == "" && !r.stopped {
			r.nodes = c.report(context.Background())
		}
		t.Logf("the nodes when the test failed:\n%s", r.nodes)
		t.Logf("replay: ROLLING_OLD=<old binary> ROLLING_NEW=<new binary> go test -count=1 -v ./cmd/chaos -run '^%s$'", t.Name())
	})
	for i := range 3 {
		r.start(i, oldBin)
	}
	if err := c.ready(context.Background(), 30*time.Second); err != nil {
		t.Fatal(err)
	}
	cs, err := dialClients(c, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := closeClients(cs); err != nil {
			t.Error(err)
		}
	})
	r.traffic, err = startTraffic(workloadConfig{seed: seed, rate: 10, keys: 6, valueSize: 256, requestTimeout: 2 * time.Second, watchTimeout: 20 * time.Second, quiet: 8 * time.Second}, cs)
	if err != nil {
		t.Fatal(err)
	}
	// Run the checkers even when a version assertion fails. Their verdict
	// does not excuse a failed upgrade or an unexpected process exit.
	t.Cleanup(r.finish)
	// This runs before finish, which stops the nodes, so that a failure in
	// finish too has the report of how the nodes stood.
	t.Cleanup(func() {
		if !r.stopped {
			r.nodes = c.report(context.Background())
		}
	})
	return r
}

func (r *rollingRun) start(i int, bin string) {
	r.t.Helper()
	r.c.bin = bin
	r.ranges[i] = peerMin
	if bin == r.newBin {
		r.ranges[i] = peerMin + 1
	}
	if err := r.c.start(r.c.nodes[i]); err != nil {
		r.t.Fatal(err)
	}
	r.t.Logf("node%d started with %s", i+1, bin)
}

// stop ends node i with SIGTERM. Like etcd's, a leader stops its client
// port and hands its leadership to a peer before Raft stops (#232 rows 45
// and 46): once its process has exited, a peer leads already, without
// waiting an election timeout for the loss.
func (r *rollingRun) stop(i int) {
	r.t.Helper()
	lead, err := r.c.leader(context.Background(), 10*time.Second)
	if err != nil {
		r.t.Fatal(err)
	}
	ps, _, err := r.c.nodes[i].signal(syscall.SIGTERM)
	if err != nil {
		r.t.Fatal(err)
	}
	if !ps.Success() {
		r.t.Fatalf("node%d stopped with %v", i+1, ps)
	}
	if lead != i {
		return
	}
	var seen []string
	for j := range 3 {
		if j == i {
			continue
		}
		st, err := r.c.status(context.Background(), j)
		if err == nil && st.Leader == r.c.ids[j] {
			return
		}
		seen = append(seen, fmt.Sprintf("node%d: %v, %v", j+1, st, err))
	}
	r.t.Fatalf("node%d led and stopped, and no peer leads: %s", i+1, strings.Join(seen, "; "))
}

func (r *rollingRun) restart(i int, bin string) {
	r.t.Helper()
	r.stop(i)
	r.start(i, bin)
}

func (r *rollingRun) stage(name string, version int) {
	r.t.Helper()
	begin := r.traffic.r.now()
	for i := range 3 {
		r.version(i, version)
	}
	// Idle Raft senders need not notice a restarted peer until they next
	// write. Probe fresh connections rather than requiring six live
	// Raft links between every pair of followers.
	for from := range 3 {
		for to := range 3 {
			if from == to {
				continue
			}
			conn := r.connect(from, to, r.ranges[from])
			if err := conn.Close(); err != nil {
				r.t.Fatal(err)
			}
		}
	}
	time.Sleep(3 * time.Second)
	for i := range 3 {
		r.version(i, version)
	}
	r.traffic.r.mu.Lock()
	writes := 0
	for _, op := range r.traffic.r.h.Ops {
		if op.Call >= begin && op.Err == nil && op.Req.Mutates() {
			writes++
		}
	}
	r.traffic.r.mu.Unlock()
	if writes == 0 {
		r.t.Fatalf("%s: no successful writes during stage", name)
	}
	r.t.Logf("%s: active version %d, peer ranges %v negotiated, %d successful writes", name, version, r.ranges, writes)
}

func (r *rollingRun) version(i, want int) {
	r.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		v, body, err := r.metric(i)
		if err == nil {
			last = fmt.Sprintf("tigerstore_cluster_version %d", v)
			if v == want {
				if err := r.binaryRange(i, body); err != nil {
					last = err.Error()
				} else {
					if err := os.WriteFile(filepath.Join(r.dir, fmt.Sprintf("metrics-node%d.txt", i+1)), []byte(body), 0o644); err != nil {
						r.t.Fatal(err)
					}
					return
				}
			}
		} else {
			last = err.Error()
		}
		time.Sleep(100 * time.Millisecond)
	}
	r.t.Fatalf("node%d: want active version %d: %s", i+1, want, last)
}

func (r *rollingRun) binaryRange(i int, body string) error {
	want := []string{
		fmt.Sprintf("tigerstore_peer_protocol_min_version %d", peerMin),
		fmt.Sprintf("tigerstore_peer_protocol_max_version %d", r.ranges[i]),
		fmt.Sprintf("tigerstore_cluster_version_max %d", r.ranges[i]-peerMin+1),
	}
	for _, line := range want {
		if !strings.Contains(body, "\n"+line+"\n") {
			return fmt.Errorf("node%d metrics lack %s", i+1, line)
		}
	}
	return nil
}

func (r *rollingRun) metric(i int) (int, string, error) {
	transport := &http.Transport{TLSClientConfig: r.c.tls.Clone()}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: time.Second}
	url := "https://" + r.c.nodes[i].control.Endpoints()[0] + "/metrics"
	resp, err := client.Get(url)
	if err != nil {
		return 0, "", err
	}
	b, re := io.ReadAll(resp.Body)
	if err := errors.Join(re, resp.Body.Close()); err != nil {
		return 0, "", err
	}
	body := string(b)
	for line := range strings.SplitSeq(body, "\n") {
		if value, ok := strings.CutPrefix(line, "tigerstore_cluster_version "); ok {
			v, err := strconv.Atoi(value)
			return v, body, err
		}
	}
	return 0, body, fmt.Errorf("metrics HTTP %d has no active version", resp.StatusCode)
}

func (r *rollingRun) wal(i int) committed.Node {
	r.t.Helper()
	n, err := committed.Read(filepath.Join(r.dir, "data", fmt.Sprintf("node%d", i+1)))
	if err != nil {
		r.t.Fatal(err)
	}
	return n
}

// announced keeps a single record, including its original origin, boot,
// request ID and run field, encoded as a one-record format-1 batch.
type announced struct {
	Index, Origin, Run uint64
	Version            uint32
	Batch              []byte
}

func announcements(t *testing.T, n committed.Node) []announced {
	t.Helper()
	var out []announced
	for _, e := range n.Entries {
		if e.GetIndex() > n.Commit || e.GetType() != raftpb.EntryNormal || len(e.Data) == 0 {
			continue
		}
		data := e.Data
		if data[0] == 0 {
			if len(data) < 2 {
				t.Fatal("truncated entry format header")
			}
			data = data[2:]
		}
		uv := func() uint64 {
			x, k := binary.Uvarint(data)
			if k <= 0 {
				t.Fatal("malformed batch varint")
			}
			data = data[k:]
			return x
		}
		count := uv()
		for range count {
			start := data
			origin := uv()
			uv() // boot
			uv() // request ID
			if len(data) == 0 {
				t.Fatal("missing batch kind")
			}
			kind := data[0]
			data = data[1:]
			size := uv()
			if size > uint64(len(data)) {
				t.Fatal("truncated batch body")
			}
			body := data[:size]
			data = data[size:]
			if kind != 10 { // core.kindVersion
				continue
			}
			var m wrapperspb.UInt32Value
			if err := proto.Unmarshal(body, &m); err != nil {
				t.Fatal(err)
			}
			a := announced{Index: e.GetIndex(), Origin: origin, Version: m.Value, Batch: append([]byte{1}, start[:len(start)-len(data)]...)}
			u := m.ProtoReflect().GetUnknown()
			for len(u) > 0 {
				num, typ, k := protowire.ConsumeTag(u)
				if k < 0 {
					t.Fatal(protowire.ParseError(k))
				}
				u = u[k:]
				if num == 2 && typ == protowire.VarintType {
					x, k := protowire.ConsumeVarint(u)
					if k < 0 {
						t.Fatal(protowire.ParseError(k))
					}
					a.Run = x
				}
				k = protowire.ConsumeFieldValue(num, typ, u)
				if k < 0 {
					t.Fatal(protowire.ParseError(k))
				}
				u = u[k:]
			}
			out = append(out, a)
		}
	}
	return out
}

func (r *rollingRun) announcement(i int, version uint32) announced {
	r.t.Helper()
	var found announced
	for _, a := range announcements(r.t, r.wal(i)) {
		if a.Origin == r.c.ids[i] {
			found = a
		}
	}
	if found.Version != version || found.Run == 0 {
		r.t.Fatalf("node%d announcement %+v, want version %d and a run", i+1, found, version)
	}
	if err := save(filepath.Join(r.dir, fmt.Sprintf("announcement-node%d-run%d.json", i+1, found.Run)), found); err != nil {
		r.t.Fatal(err)
	}
	return found
}

func (r *rollingRun) connect(from, to, high int) *tls.Conn {
	r.t.Helper()
	cert, err := tls.LoadX509KeyPair(filepath.Join(r.dir, "certs", fmt.Sprintf("node%d.pem", from+1)), filepath.Join(r.dir, "certs", fmt.Sprintf("node%d-key.pem", from+1)))
	if err != nil {
		r.t.Fatal(err)
	}
	cfg := r.c.tls.Clone()
	cfg.Certificates = []tls.Certificate{cert}
	cfg.MinVersion = tls.VersionTLS13
	cfg.NextProtos = nil
	for v := high; v >= peerMin; v-- {
		cfg.NextProtos = append(cfg.NextProtos, fmt.Sprintf("tigerstore-peer/%d", v))
	}
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", r.c.peers[from][to].Proxy.Addr(), cfg)
	if err != nil {
		r.t.Fatal(err)
	}
	ok := false
	defer func() {
		if !ok {
			if err := conn.Close(); err != nil {
				r.t.Error(err)
			}
		}
	}()
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		r.t.Fatal(err)
	}
	// The probe is a known member, so it advertises no address.
	preface := append(binary.BigEndian.AppendUint64([]byte{'T', 'S', peerMin, byte(high)}, r.c.ids[from]), 0)
	if _, err := conn.Write(preface); err != nil {
		r.t.Fatal(err)
	}
	var reply [1]byte
	want := min(high, r.ranges[to])
	if _, err := io.ReadFull(conn, reply[:]); err != nil || int(reply[0]) != want {
		r.t.Fatalf("peer node%d to node%d: %v, reply %v, want %d", from+1, to+1, err, reply, want)
	}
	if got := conn.ConnectionState().NegotiatedProtocol; got != fmt.Sprintf("tigerstore-peer/%d", want) {
		r.t.Fatalf("ALPN %q disagrees with preface reply %d", got, want)
	}
	ok = true
	return conn
}

func (r *rollingRun) replay(a announced) {
	r.t.Helper()
	lead, err := r.c.leader(context.Background(), 10*time.Second)
	if err != nil {
		r.t.Fatal(err)
	}
	// Use a different member's inbound path even if the original sender
	// is now leader. The entry's origin and boot remain unchanged.
	from := (lead + 1) % 3
	conn := r.connect(from, lead, peerMin)
	defer func() {
		if err := conn.Close(); err != nil {
			r.t.Error(err)
		}
	}()
	m := &raftpb.Message{Type: raftpb.MsgProp.Enum(), From: new(r.c.ids[from]), To: new(r.c.ids[lead]), Entries: []*raftpb.Entry{{Type: raftpb.EntryNormal.Enum(), Data: a.Batch}}}
	b, err := proto.Marshal(m)
	if err != nil {
		r.t.Fatal(err)
	}
	frame := binary.BigEndian.AppendUint32(nil, uint32(len(b)))
	if _, err := conn.Write(append(frame, b...)); err != nil {
		r.t.Fatal(err)
	}
	r.t.Logf("replayed node %d run %d version %d to node%d", a.Origin, a.Run, a.Version, lead+1)
}

func (r *rollingRun) refuseOld(i int) {
	r.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, r.oldBin, r.c.nodes[i].args...)
	b, err := cmd.CombinedOutput()
	if e := os.WriteFile(filepath.Join(r.dir, "old-binary-refused.log"), b, 0o644); e != nil {
		r.t.Fatal(e)
	}
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 1 || ctx.Err() != nil || !strings.Contains(string(b), "this binary is too old for the cluster") || bytes.Contains(b, []byte("WARNING: DATA RACE")) {
		r.t.Fatalf("old binary refusal: %v: %s", err, b)
	}
	r.t.Logf("old binary refused with exit code %d", exit.ExitCode())
}

func (r *rollingRun) finish() {
	t := r.t
	r.traffic.stopClients()
	if err := r.traffic.finish(); err != nil {
		t.Error(err)
	}
	if err := r.c.stop(); err != nil {
		t.Error(err)
	}
	r.stopped = true
	for _, u := range r.c.unexpectedExits() {
		t.Error(u)
	}
	for i := range 3 {
		b, err := os.ReadFile(filepath.Join(r.dir, fmt.Sprintf("node%d.log", i+1)))
		if err != nil {
			t.Error(err)
		} else if bytes.Contains(b, []byte("WARNING: DATA RACE")) {
			t.Errorf("node%d reported a data race", i+1)
		}
	}
	var dirs []string
	for i := range 3 {
		dirs = append(dirs, filepath.Join(r.dir, "data", fmt.Sprintf("node%d", i+1)))
	}
	log, evidence, err := committedLog(dirs)
	if err != nil {
		t.Error(err)
	}
	h := &r.traffic.r.h
	h.Log = log
	for name, value := range map[string]any{"wal.json": evidence, "history.json": h, "watch-responses.json": r.traffic.r.diagnostics} {
		if err := save(filepath.Join(r.dir, name), value); err != nil {
			t.Error(err)
		}
	}
	if err := runChecks(h, time.Minute, r.dir, t.Logf); err != nil {
		t.Error(err)
	}
	if err := save(filepath.Join(r.dir, "verdict.json"), verdict{Passed: !t.Failed()}); err != nil {
		t.Error(err)
	}
}
