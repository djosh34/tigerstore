package wl

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
)

type directReaders struct {
	n       *node
	l       *loop
	st      *store
	log     *memLog
	clients []*client
	fds     []int
	msg     []byte
	peak    int64
}

func residentSlowReaders(t *testing.T, mib int64, slow, total int) *directReaders {
	t.Helper()
	c, _, st, log, l := fillRangeFixture(t)
	n := c.node
	l.epfd = -1
	st.setBudget(mib<<20, 0)
	n.sizes = sizeFor(mib << 20)
	n.connOut = n.sizes.connOut(3)
	for i := range 4 {
		putValue(t, st, log, fmt.Sprintf("/large-resident/%d", i), string(bytes.Repeat([]byte{byte('a' + i)}, (2<<20)-256)))
	}
	if !c.idle() {
		t.Fatal("fixture not idle before its first request")
	}
	c.giveBufs()
	r := &directReaders{n: n, l: l, st: st, log: log, clients: []*client{c}, msg: appendBytesField(appendBytesField(nil, 1, []byte("/large-resident/")), 2, []byte("/large-resident0"))}
	for i := 1; i < total; i++ {
		other := newClient(&handoff{fd: -1, wr: c.wr, rd: c.rd}, n)
		other.idx = i
		other.gen = 1
		n.socks = append(n.socks, other)
		n.gens = append(n.gens, 1)
		r.clients = append(r.clients, other)
		t.Cleanup(other.dropAll)
	}
	n.clients = total
	for _, cl := range r.clients {
		cl.plain = true
		r.fds = append(r.fds, roomSocket(t, cl))
	}
	for i := range slow {
		cl := r.clients[i]
		if n.cpool+cl.bufNeed() > n.sizes.clientInBufs() {
			t.Fatalf("input admission at client%d of%d: pool=%d need=%d limit=%d slotBytes=%d active=%d", i, total, n.cpool, cl.bufNeed(), n.sizes.clientInBufs(), slotBytes, n.active)
		}
		cl.takeBufs()
		s := cl.newStream(3, kRange)
		cl.flow.conn = 1024
		s.win = 1024
		serveRoomRange(t, cl, s, r.msg)
		r.check(t)
		l.flush()
		drainRoomSocket(t, r.fds[i])
		r.check(t)
		for step := 0; cl.flow.conn > 0 && step < 16; step++ {
			n.pumpBig()
			l.flush()
			drainRoomSocket(t, r.fds[i])
			r.check(t)
		}
		if cl.flow.conn != 0 || !cl.idle() {
			t.Fatalf("blocked direct reader: window=%d stages=%d owners=%d private=%t waiting=%d rec=%d holds=%d pending=%d", cl.flow.conn, cl.stages, cl.fillOwners, cl.hasPrivateFill(), cl.waiting(), cl.rec, len(cl.flow.held), cl.stagePending)
		}
		cl.giveBufs()
		r.check(t)
	}
	return r
}

func (r *directReaders) check(t *testing.T) {
	t.Helper()
	used := r.n.stageCredits.used + r.n.stageOther(0)
	r.peak = max(r.peak, used)
	if used > r.n.sizes.clientPool {
		t.Fatalf("direct piece exceeded P BEFORE/AFTER allocation: %d > %d", used, r.n.sizes.clientPool)
	}
	if total := r.st.outside() - r.st.mapped + r.n.sizes.worst(r.st.budget, 0); total > r.st.budget {
		t.Fatalf("store + worst node allocation ledger exceeds B: %d > %d", total, r.st.budget)
	}
	for _, b := range r.n.bigs {
		if cap(b.buf) != 0 {
			t.Fatal("direct path made an unfunded main byte buffer")
		}
	}
	if len(r.n.fills) != 0 || r.n.fillBook != nil || r.st.loads != 0 {
		t.Fatal("all-resident direct path built a source plan")
	}
}

func admittedDirectReaders(mib int64) int {
	z := sizeFor(mib << 20)
	return int((z.clientInBufs()-int64(clientInBuf+clientPtBuf+slotBytes))/int64(slotBytes)) + 1
}

func (r *directReaders) assertNextNotAdmitted(t *testing.T, index int) {
	t.Helper()
	c := r.clients[index]
	n := r.n
	if n.cpool+c.bufNeed() <= n.sizes.clientInBufs() {
		t.Fatal("cohort did not reach the unchanged ceiling")
	}
	if _, err := unix.Write(r.fds[index], []byte{0}); err != nil {
		t.Fatal(err)
	}
	r.l.input(index, false)
	if c.in != nil || !c.starved || len(n.waiting) != 1 || n.waiting[0] != c {
		t.Fatal("loop.input bypassed unchanged input admission")
	}
	r.l.serveWaiting()
	if c.in != nil || !c.starved || len(n.waiting) != 1 || n.waiting[0] != c {
		t.Fatal("serveWaiting bypassed unchanged input admission")
	}
	t.Logf("next reader queued by input/serveWaiting: pool=%d need=%d allowance=%d", n.cpool, c.bufNeed(), n.sizes.clientInBufs())
	r.check(t)
}

func TestDirectResidentLargePiecesStayWithinPAndB(t *testing.T) {
	for _, mib := range []int64{83, 137, 191} {
		for _, many := range []bool{false, true} {
			count := 3
			if many {
				count = admittedDirectReaders(mib)
			}
			t.Run(fmt.Sprintf("%dMiB/readers%d", mib, count), func(t *testing.T) {
				total := count
				if many {
					total++
				}
				r := residentSlowReaders(t, mib, count, total)
				if many {
					r.assertNextNotAdmitted(t, count)
				}
				if len(r.n.bigs) != count {
					t.Fatalf("not all %d slow replies admitted: bigs=%d waits=%d", count, len(r.n.bigs), len(r.n.roomWait))
				}
				t.Logf("B=%d P=%d readers=%d peak-P=%d live-stages=%d source-plans=%d", r.st.budget, r.n.sizes.clientPool, count, r.peak, len(r.n.fills), len(r.n.fills))
			})
		}
	}
}

func TestDirectResidentSlowReadersCannotStarveFastReader(t *testing.T) {
	for _, mib := range []int64{83, 137, 191} {
		t.Run(strconv.FormatInt(mib, 10), func(t *testing.T) {
			count := admittedDirectReaders(mib)
			r := residentSlowReaders(t, mib, count-1, count)
			c := r.clients[count-1]
			if r.n.cpool+c.bufNeed() > r.n.sizes.clientInBufs() {
				t.Fatal("fast reader was not among the admitted cohort")
			}
			c.takeBufs()
			s := c.newStream(3, kRange)
			c.flow.conn = 1 << 30
			s.win = 1 << 30
			start := time.Now()
			serveRoomRange(t, c, s, r.msg)
			var wire []byte
			for step := 0; c.get(3) != nil || c.waiting() > 0; step++ {
				if step > 4096 || time.Since(start) >= fetchShareReads {
					t.Fatal("fast resident reader starved behind slow P owners")
				}
				r.n.pumpBig()
				r.l.flush()
				readFillSocket(t, r.fds[count-1], &wire)
				r.check(t)
			}
			data := fillData(t, wire)
			var got pb.RangeResponse
			if len(data) < 5 || int(binary.BigEndian.Uint32(data[1:5])) != len(data)-5 {
				t.Fatal("fast reader did not receive one complete message")
			}
			if err := proto.Unmarshal(data[5:], &got); err != nil {
				t.Fatal(err)
			}
			if got.Count != 4 || len(got.Kvs) != 4 {
				t.Fatal("fast response lost values")
			}
			for i, kv := range got.Kvs {
				if !bytes.Equal(kv.Value, bytes.Repeat([]byte{byte('a' + i)}, (2<<20)-256)) {
					t.Fatal("fast response changed value")
				}
			}
			if len(r.n.bigs) != count-1 {
				t.Fatal("fast progress required completing a slow reader")
			}
			t.Logf("fast large reply with %d zero-window owners: %v (bound %v); peak-P=%d/%d", count-1, time.Since(start), fetchShareReads, r.peak, r.n.sizes.clientPool)
		})
	}
}

func TestDirectResidentRangeColdMidMessageKeepsExactWire(t *testing.T) {
	var want []byte
	for _, cold := range []bool{false, true} {
		c, s, st, log, l := fillRangeFixture(t)
		n := c.node
		n.connOut = 64 << 10
		c.plain = true
		l.epfd = -1
		for i := range 3 {
			putValue(t, st, log, fmt.Sprintf("/cold/%d", i), string(bytes.Repeat([]byte{byte('a' + i)}, 900<<10)))
		}
		fd := roomSocket(t, c)
		c.flow.conn = 1024
		s.win = 1024
		msg := appendBytesField(appendBytesField(nil, 1, []byte("/cold/")), 2, []byte("/cold0"))
		serveRoomRange(t, c, s, msg)
		if n.fillBook != nil || len(n.fills) != 0 || len(n.bigs) != 1 || n.bigs[0].resume.taken != -1 {
			t.Fatal("resident prefix built a source cursor")
		}
		var wire []byte
		l.flush()
		readFillSocket(t, fd, &wire)
		if len(fillData(t, wire)) != 1024 {
			t.Fatal("test did not publish a partial resident value")
		}
		if cold {
			for i := range 3 {
				putValue(t, st, log, fmt.Sprintf("/cold/%d", i), "overwritten")
			}
		}
		c.flow.conn = 1 << 30
		s.win = 1 << 30
		tail := driveLargeFills(t, n, l, c, fd, nil)
		wire = append(wire, tail...)
		data := fillData(t, wire)
		var got pb.RangeResponse
		if len(data) < 5 {
			t.Fatal("no message")
		}
		if err := proto.Unmarshal(data[5:], &got); err != nil {
			t.Fatal(err)
		}
		if !cold {
			want = bytes.Clone(data)
			if n.fillBook != nil {
				t.Fatal("all-resident completion allocated W/source plan")
			}
		} else if !bytes.Equal(data, want) {
			t.Fatal("cold continuation changed, replayed or discarded already-published bytes")
		}
		if got.Header.Revision != 4 || got.Count != 3 || len(got.Kvs) != 3 || n.stageCredits.used != 0 || st.heldReads() != 0 {
			t.Fatal("snapshot/ownership did not survive cold handoff")
		}
	}
}
