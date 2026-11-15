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
	"github.com/djosh34/tigerstore/internal/wl"
	"go.etcd.io/raft/v3/raftpb"
)

// These tests use two builds of this source; only TestReleaseUpgrade
// also runs with two releases. The old build reads and writes batch
// format 1, the new one formats 1-2 (internal/wl/format.go); the workflow
// changes only maxFormat. No test hook runs inside the server.
func TestRollingUpgrade(t *testing.T) {
	for _, seed := range []uint64{148, 174} {
		t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
			r := startRolling(t, seed)
			r.stage("initial", 1)
			r.restart(0, r.newBin)
			r.stage("first upgrade", 1)
			r.restart(1, r.newBin)
			r.stage("second upgrade", 1)

			// Keep the exact announcement of node 2's upgraded run.
			// Proposing it again later models a delayed proposal, without
			// a new announcement from the rollback run.
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
			r.stage("third node upgraded, rollback holds the format", 1)
			batch := r.replay(stale)
			r.stage("delayed announcement after rollback", 1)

			// A successful send alone is not coverage: require the stale
			// announcement in the committed log after the rollback's.
			r.stop(1)
			n := r.wal(1)
			seen := false
			for _, e := range committedEntries(n) {
				if e.GetIndex() > rolled.Index && bytes.Equal(e.Data, batch) {
					seen = true
					t.Logf("delayed announcement committed at %d, rollback at %d", e.GetIndex(), rolled.Index)
				}
			}
			if !seen {
				t.Fatal("delayed announcement was not committed after rollback")
			}
			if f, index := newestFormat(t, n); f != 1 {
				t.Fatalf("entry %d is a batch of format %d before the last node supports it", index, f)
			}
			r.start(1, r.newBin)
			r.stage("last node upgraded", 2)
			r.stop(1)
			if f, _ := newestFormat(t, r.wal(1)); f != 2 {
				t.Fatal("no committed batch of format 2 after the cluster took it")
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
	// down for twenty seconds, or until activation is seen.
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
		t.Errorf("format %d activated while the rollback node was down; want 1 (#174)", v)
		r.refuseOld(1)
		r.start(1, r.newBin)
		r.stage("restore the new binary after unsafe activation", 2)
		return
	}
	r.start(1, r.oldBin)
	r.stage("rollback announces after last upgrade", 1)
}

// TestReleaseUpgrade upgrades a cluster from the previous release's binary
// (ROLLING_OLD) to the release candidate (ROLLING_NEW) one node at a time
// under load, then downgrades one node; the release workflow runs it. Any
// two releases can do this, so it asks only that the nodes agree on the
// newest format all of them support. The downgraded node runs the old
// binary again, unless the cluster went past the old binary's newest
// format: then the old binary must refuse to start, and the node comes
// back on the candidate. The checkers run on the whole history.
func TestReleaseUpgrade(t *testing.T) {
	r := startRolling(t, 355)
	oldMax := r.maxVersion(0)
	r.settle("previous release", oldMax)
	r.restart(0, r.newBin)
	r.settle("node1 upgraded", oldMax)
	r.restart(1, r.newBin)
	r.settle("node2 upgraded", oldMax)
	r.restart(2, r.newBin)
	newMax := r.maxVersion(2)
	r.settle("node3 upgraded", newMax)
	if newMax > oldMax {
		r.stop(0)
		r.refuseOld(0)
		r.start(0, r.newBin)
	} else {
		r.restart(0, r.oldBin)
	}
	r.settle("node1 downgraded", newMax)
}

type rollingRun struct {
	t                   *testing.T
	c                   *cluster
	traffic             *traffic
	dir, oldBin, newBin string
	stopped             bool
	// formats is the newest format each node's binary reads.
	formats [3]int
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
	r.formats[i] = 1
	if bin == r.newBin {
		r.formats[i] = 2
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

// stage waits until every node uses format, with its binary's newest,
// and fails unless they still do 3 s later and writes succeed meanwhile.
func (r *rollingRun) stage(name string, format int) {
	r.t.Helper()
	begin := r.traffic.r.now()
	for i := range 3 {
		r.format(i, format)
	}
	time.Sleep(3 * time.Second)
	for i := range 3 {
		r.format(i, format)
	}
	writes := r.writesSince(begin)
	if writes == 0 {
		r.t.Fatalf("%s: no successful writes during stage", name)
	}
	r.t.Logf("%s: format %d, binaries' newest %v, %d successful writes", name, format, r.formats, writes)
}

// settle waits until every node has active version want, and fails
// unless they still have it 3 s later and writes succeed meanwhile.
func (r *rollingRun) settle(name string, want int) {
	r.t.Helper()
	begin := r.traffic.r.now()
	for i := range 3 {
		r.activeVersion(i, want)
	}
	time.Sleep(3 * time.Second)
	for i := range 3 {
		r.activeVersion(i, want)
	}
	writes := r.writesSince(begin)
	if writes == 0 {
		r.t.Fatalf("%s: no successful writes during stage", name)
	}
	r.t.Logf("%s: active version %d, %d successful writes", name, want, writes)
}

// activeVersion waits until node i has active version want.
func (r *rollingRun) activeVersion(i, want int) {
	r.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		v, _, err := r.metric(i)
		if err == nil && v == want {
			return
		}
		last = fmt.Sprintf("version %d, error %v", v, err)
		time.Sleep(100 * time.Millisecond)
	}
	r.t.Fatalf("node%d: want active version %d: %s", i+1, want, last)
}

// maxVersion is the newest version the binary of node i supports.
func (r *rollingRun) maxVersion(i int) int {
	r.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		_, body, err := r.metric(i)
		if err == nil {
			var v int
			if v, err = metricValue(body, "tigerstore_cluster_version_max"); err == nil {
				return v
			}
		}
		if time.Now().After(deadline) {
			r.t.Fatalf("node%d: newest supported version: %v", i+1, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// writesSince counts the writes that succeeded from begin on.
func (r *rollingRun) writesSince(begin int64) int {
	r.traffic.r.mu.Lock()
	defer r.traffic.r.mu.Unlock()
	writes := 0
	for _, op := range r.traffic.r.h.Ops {
		if op.Call >= begin && op.Err == nil && op.Req.Mutates() {
			writes++
		}
	}
	return writes
}

// format waits until node i uses format want and its metrics name its
// binary's newest.
func (r *rollingRun) format(i, want int) {
	r.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		v, body, err := r.metric(i)
		if err == nil {
			last = fmt.Sprintf("tigerstore_cluster_version %d", v)
			if v == want {
				if err := r.binaryFormat(i, body); err != nil {
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
	r.t.Fatalf("node%d: want format %d: %s", i+1, want, last)
}

func (r *rollingRun) binaryFormat(i int, body string) error {
	line := fmt.Sprintf("tigerstore_cluster_version_max %d", r.formats[i])
	if !strings.Contains(body, "\n"+line+"\n") {
		return fmt.Errorf("node%d metrics lack %s", i+1, line)
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
	v, err := metricValue(body, "tigerstore_cluster_version")
	if err != nil {
		return 0, body, fmt.Errorf("metrics HTTP %d: %w", resp.StatusCode, err)
	}
	return v, body, nil
}

// errNoMetric is a metric missing from a /metrics page.
var errNoMetric = errors.New("no such metric")

// metricValue is the value of the metric name, without labels, in body.
func metricValue(body, name string) (int, error) {
	for line := range strings.SplitSeq(body, "\n") {
		if value, ok := strings.CutPrefix(line, name+" "); ok {
			return strconv.Atoi(value)
		}
	}
	return 0, fmt.Errorf("%s: %w", name, errNoMetric)
}

func (r *rollingRun) wal(i int) committed.Node {
	r.t.Helper()
	n, err := committed.Read(filepath.Join(r.dir, "data", fmt.Sprintf("node%d", i+1)))
	if err != nil {
		r.t.Fatal(err)
	}
	return n
}

// committedEntries are the entries n committed.
func committedEntries(n committed.Node) []*raftpb.Entry {
	if n.Commit < n.First {
		return nil
	}
	return n.Entries[:min(n.Commit-n.First+1, uint64(len(n.Entries)))]
}

// newestFormat is the newest format of the batches n committed, and the
// index of the first batch of it.
func newestFormat(t *testing.T, n committed.Node) (int, uint64) {
	t.Helper()
	format, at := 0, uint64(0)
	for _, e := range committedEntries(n) {
		if e.GetType() != raftpb.EntryNormal || len(e.Data) == 0 {
			continue
		}
		b, err := wl.ParseBatch(e.Data)
		if err != nil {
			t.Fatalf("entry %d: %v", e.GetIndex(), err)
		}
		if int(b.Format) > format {
			format, at = int(b.Format), e.GetIndex()
		}
	}
	return format, at
}

// announced is an announcement a node committed, with its op as the log
// holds it.
type announced struct {
	Index uint64
	wl.Announcement
	Op wl.Op
}

func announcements(t *testing.T, n committed.Node) []announced {
	t.Helper()
	var out []announced
	for _, e := range committedEntries(n) {
		if e.GetType() != raftpb.EntryNormal || len(e.Data) == 0 {
			continue
		}
		b, err := wl.ParseBatch(e.Data)
		if err != nil {
			t.Fatalf("entry %d: %v", e.GetIndex(), err)
		}
		for _, o := range b.Ops {
			m, err := o.Message()
			if err != nil {
				t.Fatalf("entry %d: %v", e.GetIndex(), err)
			}
			if a, ok := m.(*wl.Announcement); ok {
				out = append(out, announced{Index: e.GetIndex(), Announcement: *a, Op: o})
			}
		}
	}
	return out
}

// announcement is node i's last committed announcement, which must be of
// format.
func (r *rollingRun) announcement(i int, format uint8) announced {
	r.t.Helper()
	var found announced
	for _, a := range announcements(r.t, r.wal(i)) {
		if a.ID == r.c.ids[i] {
			found = a
		}
	}
	if found.Format != format || found.Run == 0 {
		r.t.Fatalf("node%d announcement %+v, want format %d and a run", i+1, found, format)
	}
	if err := save(filepath.Join(r.dir, fmt.Sprintf("announcement-node%d-run%d.json", i+1, found.Run)), found); err != nil {
		r.t.Fatal(err)
	}
	return found
}

// replay proposes a again to the leader, in a batch of its own, through a
// peer connection of another member, and returns the batch. The batch
// has a's origin and a sequence of a's run that no batch of the run has,
// so no node takes another batch for it.
func (r *rollingRun) replay(a announced) []byte {
	r.t.Helper()
	lead, err := r.c.leader(context.Background(), 10*time.Second)
	if err != nil {
		r.t.Fatal(err)
	}
	batch := wl.AppendBatch(nil, wl.Batch{Origin: a.ID, Seq: a.Run << 32, Format: 1, Ops: []wl.Op{a.Op}})
	// Use a different member's inbound path even if the original sender
	// is now leader.
	from := (lead + 1) % 3
	conn := r.peerConn(from, lead)
	defer func() {
		if err := conn.Close(); err != nil {
			r.t.Error(err)
		}
	}()
	if _, err := conn.Write(proposalFrame(r.c.ids[from], batch)); err != nil {
		r.t.Fatal(err)
	}
	// The leader reads the frame before the connection closes.
	time.Sleep(time.Second)
	r.t.Logf("replayed node %d run %d format %d to node%d", a.ID, a.Run, a.Format, lead+1)
	return batch
}

// peerConn dials node to's peer port as node from, and says the peer
// hello: from's ID and the cluster's.
func (r *rollingRun) peerConn(from, to int) *tls.Conn {
	r.t.Helper()
	cert, err := tls.LoadX509KeyPair(filepath.Join(r.dir, "certs", fmt.Sprintf("node%d.pem", from+1)), filepath.Join(r.dir, "certs", fmt.Sprintf("node%d-key.pem", from+1)))
	if err != nil {
		r.t.Fatal(err)
	}
	cfg := r.c.tls.Clone()
	cfg.Certificates = []tls.Certificate{cert}
	cfg.MinVersion = tls.VersionTLS13
	cfg.NextProtos = nil
	st, err := r.c.status(context.Background(), to)
	if err != nil {
		r.t.Fatal(err)
	}
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", r.c.routers[to].Addr(), cfg)
	if err != nil {
		r.t.Fatal(err)
	}
	hello := binary.LittleEndian.AppendUint64(nil, r.c.ids[from])
	hello = binary.LittleEndian.AppendUint64(hello, st.Header.ClusterId)
	if _, err := conn.Write(hello); err != nil {
		r.t.Fatal(errors.Join(err, conn.Close()))
	}
	return conn
}

// proposalFrame is a peer frame of a MsgProp from node from with one
// entry of data, as internal/wl's sendPeer writes it: the length, the
// message's header (type, flags, from, term, log term, index, commit,
// reject hint, vote, entry count), each entry's term, index, type and
// length with its data, and the context's length.
func proposalFrame(from uint64, data []byte) []byte {
	m := []byte{byte(raftpb.MsgProp), 0}
	m = binary.LittleEndian.AppendUint64(m, from)
	m = append(m, make([]byte, 6*8)...)
	m = binary.LittleEndian.AppendUint32(m, 1)
	m = append(m, make([]byte, 16)...)
	m = append(m, byte(raftpb.EntryNormal))
	m = binary.LittleEndian.AppendUint32(m, uint32(len(data)))
	m = append(m, data...)
	m = binary.LittleEndian.AppendUint32(m, 0)
	return append(binary.LittleEndian.AppendUint32(nil, uint32(len(m))), m...)
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
