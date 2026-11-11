package peer

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/metrics"
	"github.com/djosh34/tigerstore/internal/testcert"
)

func TestPick(t *testing.T) {
	cases := []struct {
		name                   string
		dmin, dmax, amin, amax byte
		want                   byte
	}{
		{"equal", 1, 1, 1, 1, 1},
		{"equal ranges", 1, 3, 1, 3, 3},
		{"overlapping", 1, 3, 2, 4, 3},
		{"overlapping, acceptor lower", 2, 4, 1, 3, 3},
		{"one inside the other", 1, 4, 2, 3, 3},
		{"touching", 1, 2, 2, 3, 2},
		{"touching, acceptor lower", 2, 3, 1, 2, 2},
		{"disjoint", 1, 1, 2, 2, 0},
		{"disjoint, acceptor lower", 3, 4, 1, 2, 0},
		{"version 0 dialer", 0, 0, 1, 1, 0},
		{"reversed range", 3, 1, 1, 3, 0},
	}
	for _, c := range cases {
		if got := pick(c.dmin, c.dmax, c.amin, c.amax); got != c.want {
			t.Errorf("%s: pick(%d, %d, %d, %d) = %d, want %d", c.name, c.dmin, c.dmax, c.amin, c.amax, got, c.want)
		}
	}
	if got, want := alpns(1, 3), []string{"tigerstore-peer/3", "tigerstore-peer/2", "tigerstore-peer/1"}; !slices.Equal(got, want) {
		t.Errorf("alpns(1, 3) = %q, want %q", got, want)
	}
}

// logs collects a transport's log lines.
type logs struct {
	t     *testing.T
	mu    sync.Mutex
	lines []string
}

func (l *logs) logf(format string, args ...any) {
	l.t.Helper()
	l.t.Logf(format, args...)
	l.mu.Lock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
	l.mu.Unlock()
}

// count returns how many lines contain s.
func (l *logs) count(s string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, line := range l.lines {
		if strings.Contains(line, s) {
			n++
		}
	}
	return n
}

// startPair starts nodes 1 and 2 with the version ranges lo1-hi1 and
// lo2-hi2.
func startPair(t *testing.T, lo1, hi1, lo2, hi2 byte) (a, b *end, la, lb *logs) {
	t.Helper()
	ca, err := testcert.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	addrs, lns := listeners(t, 1, 2)
	la, lb = &logs{t: t}, &logs{t: t}
	ends := []*end{}
	for _, n := range []struct {
		id     uint64
		lo, hi byte
		l      *logs
	}{{1, lo1, hi1, la}, {2, lo2, hi2, lb}} {
		cert, _, _, err := ca.Issue("node", int64(n.id+1))
		if err != nil {
			t.Fatal(err)
		}
		e := startWith(t, n.id, lns[n.id], addrs, cert, ca.Pool, func(c *Config) {
			c.minVersion, c.maxVersion, c.Logf = n.lo, n.hi, n.l.logf
		})
		t.Cleanup(func() {
			if err := e.t.Close(); err != nil {
				t.Error(err)
			}
		})
		ends = append(ends, e)
	}
	return ends[0], ends[1], la, lb
}

func TestSharedVersionDelivers(t *testing.T) {
	cases := []struct {
		name               string
		lo1, hi1, lo2, hi2 byte
	}{
		{"overlapping", 1, 3, 2, 4},
		{"touching", 1, 2, 2, 3},
		{"touching, acceptor lower", 2, 3, 1, 2},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			a, b, _, _ := startPair(t, c.lo1, c.hi1, c.lo2, c.hi2)
			for i := uint64(1); i <= 10; i++ {
				a.t.Send(msg(i, "x"))
			}
			for i, m := range receive(t, b, 10) {
				if m.GetIndex() != uint64(i+1) {
					t.Fatalf("message %d = %v", i, m)
				}
			}
		})
	}
}

func TestDisjointVersionsDeliverNothing(t *testing.T) {
	a, b, la, lb := startPair(t, 1, 1, 2, 2)
	// Each sender redials about six times in two seconds.
	for range 40 {
		a.t.Send(msg(1, "x"))
		time.Sleep(50 * time.Millisecond)
	}
	select {
	case m := <-b.got:
		t.Fatalf("nodes with no shared version delivered %v", m)
	default:
	}
	if a.unreachable.Load() == 0 {
		t.Error("no unreachable report for a peer with no shared version")
	}
	// Each node logs its failed dial once and its failed accept once.
	for _, n := range []struct {
		name string
		l    *logs
		peer uint64
	}{{"node 1", la, 2}, {"node 2", lb, 1}} {
		dial := fmt.Sprintf("handshake with %d failed", n.peer)
		if got := n.l.count(dial); got != 1 {
			t.Errorf("%s logged %q %d times, want once", n.name, dial, got)
		}
		if got := n.l.count("handshake from 127.0.0.1 failed"); got != 1 {
			t.Errorf("%s logged its failed accept %d times, want once", n.name, got)
		}
	}
}

// TestRejectsBadPreface dials a node with a raw TLS client and checks
// that every preface but a good one gets the connection closed.
func TestRejectsBadPreface(t *testing.T) {
	ca, err := testcert.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	cert, _, _, err := ca.Issue("node", 2)
	if err != nil {
		t.Fatal(err)
	}
	addrs, lns := listeners(t, 1, 2)
	// Node 1 never runs, so node 2's dials to it are refused.
	if err := lns[1].Close(); err != nil {
		t.Fatal(err)
	}
	// Node 2 speaks 1-2, so a preface of 2-3 picks 2 while ALPN chose 1.
	// Its cluster is 7.
	b := startWith(t, 2, lns[2], addrs, cert, ca.Pool, func(c *Config) { c.minVersion, c.maxVersion, c.ClusterID = 1, 2, 7 })
	defer func() {
		if err := b.t.Close(); err != nil {
			t.Error(err)
		}
	}()
	preface := func(magic string, lo, hi byte, id, cluster uint64, addr string) []byte {
		p := binary.BigEndian.AppendUint64(append([]byte(magic), lo, hi), id)
		p = binary.BigEndian.AppendUint64(p, cluster)
		return append(append(p, byte(len(addr))), addr...)
	}
	old := binary.BigEndian.AppendUint64(nil, 1)
	cases := []struct {
		name   string
		protos []string
		p      []byte
		reply  []byte
	}{
		{"version 1", []string{"tigerstore-peer/1"}, preface("TS", 1, 1, 1, 7, ""), []byte{1}},
		{"a version-0 node, no ALPN", nil, old, nil},
		{"the old preface with ALPN", []string{"tigerstore-peer/1"}, append(old, make([]byte, 13)...), nil},
		{"bad magic", []string{"tigerstore-peer/1"}, preface("XX", 1, 1, 1, 7, ""), nil},
		{"unknown peer without an address", []string{"tigerstore-peer/1"}, preface("TS", 1, 1, 9, 7, ""), nil},
		{"unknown peer with a bad address", []string{"tigerstore-peer/1"}, preface("TS", 1, 1, 9, 7, "no port"), nil},
		{"preface range beyond ALPN", []string{"tigerstore-peer/1"}, preface("TS", 2, 3, 1, 7, ""), []byte{0}},
		{"no shared version in the preface", []string{"tigerstore-peer/1"}, preface("TS", 3, 4, 1, 7, ""), []byte{0}},
		{"a member of another cluster", []string{"tigerstore-peer/1"}, preface("TS", 1, 1, 1, 8, ""), []byte{clusterReply}},
		{"a node without an ID asks another cluster's", []string{"tigerstore-peer/1"}, preface("TS", 1, 1, callBit, 0, ""), []byte{1}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			conn, err := tls.Dial("tcp", addrs[2], &tls.Config{
				Certificates: []tls.Certificate{cert},
				RootCAs:      ca.Pool,
				ServerName:   "127.0.0.1",
				MinVersion:   tls.VersionTLS13,
				NextProtos:   c.protos,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close() //nolint:errcheck // closing a test connection
			if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
				t.Fatal(err)
			}
			if _, err := conn.Write(c.p); err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(io.LimitReader(conn, 1))
			if err != nil && !errors.Is(err, syscall.ECONNRESET) {
				t.Fatalf("read: %v", err)
			}
			if !slices.Equal(got, c.reply) {
				t.Fatalf("reply %v, want %v", got, c.reply)
			}
			if len(got) == 1 && got[0] != 0 {
				return
			}
			if n, err := conn.Read(make([]byte, 1)); err == nil {
				t.Fatalf("the connection stays open after the reply and gave %d more bytes", n)
			}
		})
	}
	select {
	case m := <-b.got:
		t.Fatalf("a bad preface delivered %v", m)
	default:
	}
}

func TestFetchAfterHandshake(t *testing.T) {
	ca, err := testcert.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	addrs, lns := listeners(t, 1, 2)
	var ends []*end
	for id := uint64(1); id <= 2; id++ {
		cert, _, _, err := ca.Issue("node", int64(id+1))
		if err != nil {
			t.Fatal(err)
		}
		e := startWith(t, id, lns[id], addrs, cert, ca.Pool, func(c *Config) {
			c.Serve = func(from, first, last uint64, w io.Writer) error {
				_, err := fmt.Fprintf(w, "for %d: %d-%d", from, first, last)
				return err
			}
		})
		t.Cleanup(func() {
			if err := e.t.Close(); err != nil {
				t.Error(err)
			}
		})
		ends = append(ends, e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r, err := ends[0].t.Fetch(ctx, 2, 5, 9)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	if cerr := r.Close(); cerr != nil {
		t.Error(cerr)
	}
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "for 1: 5-9" {
		t.Fatalf("fetched %q", got)
	}
}

// TestHeldFormsNeverReachAnOlderPeer runs a node of versions 3 to 4 against
// one of version 3: the writer drops a message of held entries, such as a
// named append queued while a connection of version 4 was up, and still
// sends the rest, a heartbeat whose own context starts like one included.
func TestHeldFormsNeverReachAnOlderPeer(t *testing.T) {
	a, b, _, _ := startPair(t, 3, 4, 3, 3)
	named := msg(1, "a key, not the entry")
	named.Context = []byte("TSH\x02\x00")
	beat := &raftpb.Message{
		Type: raftpb.MsgHeartbeat.Enum(), From: new(uint64(1)), To: new(uint64(2)),
		Context: []byte{'T', 'S', 'H', 0, 0, 0, 0, 0},
	}
	a.t.Send(named)
	a.t.Send(beat)
	a.t.Send(msg(2, "x"))
	for _, m := range receive(t, b, 2) {
		if core.HeldForm(m) || m.GetType() == raftpb.MsgApp && m.GetIndex() != 2 {
			t.Fatalf("node 2, of version 3, got %v", m)
		}
	}
	if a.t.HeldEntries(2) {
		t.Fatal("HeldEntries reports a connection of version 3 as carrying held entries")
	}
}

// TestHeldFormsDroppedAfterDowngrade connects nodes 1 and 2 at version 4,
// then starts node 2 again speaking only version 3, as a rolling
// downgrade does. Node 1 keeps sending named appends, as a core that still
// believes the old connection does, and plain ones: once it has dialed
// again at version 3, its writer drops every named append and delivers
// the rest.
func TestHeldFormsDroppedAfterDowngrade(t *testing.T) {
	ca, err := testcert.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	certs := map[uint64]tls.Certificate{}
	for id := uint64(1); id <= 2; id++ {
		if certs[id], _, _, err = ca.Issue("node", int64(id+1)); err != nil {
			t.Fatal(err)
		}
	}
	addrs, lns := listeners(t, 1, 2)
	a := start(t, 1, lns[1], addrs, certs[1], ca.Pool)
	t.Cleanup(func() {
		if err := a.t.Close(); err != nil {
			t.Error(err)
		}
	})
	b := start(t, 2, lns[2], addrs, certs[2], ca.Pool)
	a.t.Send(msg(1, "x"))
	receive(t, b, 1)
	if !a.t.HeldEntries(2) {
		t.Fatal("a connection of version 4 does not carry held entries")
	}
	if err := b.t.Close(); err != nil {
		t.Fatal(err)
	}
	b = startWith(t, 2, nil, addrs, certs[2], ca.Pool, func(c *Config) { c.minVersion, c.maxVersion = 3, 3 })
	t.Cleanup(func() {
		if err := b.t.Close(); err != nil {
			t.Error(err)
		}
	})
	deadline := time.After(10 * time.Second)
	for i := uint64(2); ; i += 2 {
		named := msg(i, "a key, not the entry")
		named.Context = []byte("TSH\x02\x00")
		a.t.Send(named)
		a.t.Send(msg(i+1, "x"))
		select {
		case m := <-b.got:
			if core.HeldForm(m) || m.GetIndex()%2 == 0 {
				t.Fatalf("node 2, started again at version 3, got %v", m)
			}
			if a.t.HeldEntries(2) {
				t.Fatal("HeldEntries reports the connection of version 3 as carrying held entries")
			}
			return
		case <-deadline:
			t.Fatal("node 2 got nothing in 10 s after it started again")
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// TestOldConnectionRefusesHeldForms reads, on a connection of version 3,
// a named append and then a heartbeat whose own context starts like one:
// the named append is refused and counted, and the heartbeat delivered.
func TestOldConnectionRefusesHeldForms(t *testing.T) {
	set, err := metrics.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	var got []*raftpb.Message
	tr := &TCP{cfg: Config{Metrics: set, Logf: t.Logf, Deliver: func(m *raftpb.Message) { got = append(got, m) }}}
	var frames []byte
	for _, m := range []*raftpb.Message{
		{Type: raftpb.MsgApp.Enum(), From: new(uint64(2)), To: new(uint64(1)), Context: []byte("TSH\x02\x00"), Entries: []*raftpb.Entry{{Data: []byte("a key")}}},
		{Type: raftpb.MsgHeartbeat.Enum(), From: new(uint64(2)), To: new(uint64(1)), Context: []byte{'T', 'S', 'H', 2, 0, 0, 0, 0}},
	} {
		b, err := proto.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		frames = binary.BigEndian.AppendUint32(frames, uint32(len(b)))
		frames = append(frames, b...)
	}
	cs := &counters{received: prometheus.NewCounter(prometheus.CounterOpts{Name: "received"}), heartbeatReceived: prometheus.NewGauge(prometheus.GaugeOpts{Name: "heartbeat"})}
	if err := tr.frames(bytes.NewReader(frames), 2, 3, cs); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].GetType() != raftpb.MsgHeartbeat {
		t.Fatalf("delivered %v, want only the heartbeat", got)
	}
	if n := testutil.ToFloat64(set.PeerHeldRefused); n != 1 {
		t.Fatalf("%g messages refused, want 1", n)
	}
}
