package sim

import (
	"errors"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// TestNoSpaceOperators raises NOSPACE for all three members directly:
// growth must disarm every alarm before a put succeeds, while the
// operator of a fault on one disk must disarm only that disk's alarm.
// A delete could succeed with the other alarms still up (#870's review).
func TestNoSpaceOperators(t *testing.T) {
	if simrun.Drive(t, 3802) {
		return
	}
	for _, c := range []struct {
		name string
		grow bool
		left int
	}{
		{"growth disarms every alarm", true, 0},
		{"fault disarms only its alarm", false, 2},
	} {
		t.Run(c.name, func(t *testing.T) {
			cfg := DefaultConfig(3802)
			cfg.Faults = Faults{DiskBudget: 32 << 10, DiskCapacity: 20 << 10}
			cfg.Duration, cfg.Heal = 60*Second, 20*Second
			s, err := newSim(cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(s.shutdown)
			for _, n := range s.nodes {
				if err := s.start(n); err != nil {
					t.Fatal(err)
				}
			}
			time.Sleep(3 * time.Second)
			s.mu.Lock()
			i := s.leaderIdx()
			s.mu.Unlock()
			if i < 0 {
				t.Fatal("no leader after 3 s")
			}
			leader := s.nodes[i]
			for _, n := range s.nodes {
				req := &pb.AlarmRequest{Action: pb.AlarmRequest_ACTIVATE, MemberID: n.id, Alarm: pb.AlarmType_NOSPACE}
				if _, err := s.call(leader, req, cfg.ClientTimeout); err != nil {
					t.Fatal(err)
				}
			}
			alarms := func(n *node) []*pb.AlarmMember {
				t.Helper()
				resp, err := s.call(n, &pb.AlarmRequest{Action: pb.AlarmRequest_GET}, cfg.ClientTimeout)
				if err != nil {
					t.Fatal(err)
				}
				return resp.(*pb.AlarmResponse).Alarms
			}
			if got := len(alarms(leader)); got != 3 {
				t.Fatalf("%d alarms raised, want 3", got)
			}
			put := &pb.PutRequest{Key: []byte("after"), Value: []byte("growth")}
			if _, err := s.call(leader, put, cfg.ClientTimeout); !errors.Is(err, rpctypes.ErrGRPCNoSpace) {
				t.Fatalf("a put with three alarms raised: %v, want NOSPACE", err)
			}
			s.mu.Lock()
			s.nodes[0].disarming = true
			before := s.now()
			s.mu.Unlock()
			if c.grow {
				s.growDisk(s.nodes[0])
			} else {
				s.disarmNoSpace(s.nodes[0])
			}
			s.mu.Lock()
			st, after := s.stats, s.now()
			s.mu.Unlock()
			if st.Disarms != 3-c.left || st.DisarmedAt <= before || st.DisarmedAt > after {
				t.Fatalf("%d disarms, last at %s, want %d between %s and %s", st.Disarms, seconds(st.DisarmedAt), 3-c.left, seconds(before), seconds(after))
			}
			if got := st.DiskGrowths == 1; got != c.grow {
				t.Fatalf("%d growths, want grown %v", st.DiskGrowths, c.grow)
			}
			// Let the followers apply the last disarm before asking each
			// for its local alarm list.
			time.Sleep(time.Second)
			for _, n := range s.nodes {
				as := alarms(n)
				if len(as) != c.left {
					t.Fatalf("node %d: alarms %v, want %d left", n.id, as, c.left)
				}
				for _, a := range as {
					if a.MemberID == s.nodes[0].id || a.Alarm != pb.AlarmType_NOSPACE {
						t.Fatalf("node %d: unexpected alarm left: %v", n.id, a)
					}
				}
			}
			_, err = s.call(leader, put, cfg.ClientTimeout)
			if c.grow && err != nil {
				t.Fatalf("a put after all alarms were disarmed: %v", err)
			}
			if !c.grow && !errors.Is(err, rpctypes.ErrGRPCNoSpace) {
				t.Fatalf("a put with two alarms still raised: %v, want NOSPACE", err)
			}
		})
	}
}
