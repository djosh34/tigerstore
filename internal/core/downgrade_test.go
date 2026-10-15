package core

import (
	"errors"
	"testing"

	"go.etcd.io/etcd/api/v3/authpb"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/simfs"
)

// downgrade sends a Downgrade and returns the version it answers.
func (s *single) downgrade(id uint64, action pb.DowngradeRequest_DowngradeAction, version string, want error) string {
	s.t.Helper()
	r := s.do(id, &pb.DowngradeRequest{Action: action, Version: version})
	if !errors.Is(r.Err, want) {
		s.t.Fatalf("%v %q: %v, want %v", action, version, r.Err, want)
	}
	if r.Err != nil {
		return ""
	}
	resp, ok := r.Resp.(*pb.DowngradeResponse)
	if !ok || resp.Header == nil {
		s.t.Fatalf("%v %q: %v", action, version, r.Resp)
	}
	return resp.Version
}

// monitor ticks the node through a run of etcd's version monitor.
func (s *single) monitor() {
	for range s.n.cfg.ticksOf(monitorSeconds) + 1 {
		s.n.Tick()
		s.run()
	}
}

// TestDowngrade follows etcd v3.7.2's Downgrade: the version formats it
// takes, the only target it allows, one downgrade at a time, the cluster
// and storage versions the leader's monitor moves to the target and
// back, CANCEL without a downgrade, and the state surviving a restart
// from a checkpoint.
func TestDowngrade(t *testing.T) {
	disk := simfs.New()
	s := startSingleWith(t, disk, 1, tinyCheckpoints).capable()
	V, E, C := pb.DowngradeRequest_VALIDATE, pb.DowngradeRequest_ENABLE, pb.DowngradeRequest_CANCEL
	for i, v := range []string{"", "x", "v3.6", "3", "3.6.x"} {
		s.downgrade(uint64(1+i), V, v, rpctypes.ErrGRPCWrongDowngradeVersionFormat)
	}
	s.downgrade(10, V, "3.5", rpctypes.ErrGRPCInvalidDowngradeTargetVersion)
	s.downgrade(11, E, "3.7", rpctypes.ErrGRPCInvalidDowngradeTargetVersion)
	for i, v := range []string{"3.6", "3.6.0", "3.6.9", "3.6.0-rc.1"} {
		if got := s.downgrade(uint64(12+i), V, v, nil); got != "3.7" {
			t.Fatalf("validate %q: version %q", v, got)
		}
	}
	if got := s.downgrade(20, C, "", nil); got != "3.7" {
		t.Fatalf("cancel without a downgrade: %q", got)
	}
	if r := s.do(21, &pb.DowngradeRequest{Action: 7}); !errors.Is(r.Err, errUnknownMethod) {
		t.Fatalf("unknown action: %v", r.Err)
	}
	if got := s.downgrade(22, E, "3.6", nil); got != "3.7" {
		t.Fatalf("enable: version %q", got)
	}
	s.downgrade(23, E, "3.6", rpctypes.ErrGRPCDowngradeInProcess)
	s.downgrade(24, V, "3.6", rpctypes.ErrGRPCDowngradeInProcess)
	if st := s.n.Status(); !st.DowngradeInfo.Enabled || st.DowngradeInfo.TargetVersion != "3.6.0" || st.StorageVersion != "3.7.0" {
		t.Fatalf("status: %v", st)
	}
	s.monitor()
	if v := s.n.EtcdVersion(); v != "3.6.0" {
		t.Fatalf("the cluster version during the downgrade: %q", v)
	}
	if st := s.n.Status(); st.StorageVersion != "3.6.0" {
		t.Fatalf("the storage version during the downgrade: %q", st.StorageVersion)
	}
	s.downgrade(30, V, "3.5", rpctypes.ErrGRPCDowngradeInProcess)

	old := s
	s = restartFromCheckpoint(t, s, disk, func(i int) proto.Message {
		if i%5 == 4 {
			return &pb.CompactionRequest{Revision: old.n.kv.Rev()}
		}
		return &pb.PutRequest{Key: []byte("a"), Value: []byte{byte(i)}}
	})
	if st := s.n.Status(); !st.DowngradeInfo.Enabled || st.DowngradeInfo.TargetVersion != "3.6.0" || s.n.EtcdVersion() != "3.6.0" {
		t.Fatalf("after the restart: %v, cluster version %q", st.DowngradeInfo, s.n.EtcdVersion())
	}
	s.capable()
	if got := s.downgrade(40, C, "", nil); got != "3.6" {
		t.Fatalf("cancel: version %q", got)
	}
	if st := s.n.Status(); st.DowngradeInfo.Enabled || st.DowngradeInfo.TargetVersion != "" {
		t.Fatalf("status after the cancel: %v", st.DowngradeInfo)
	}
	s.monitor()
	if v := s.n.EtcdVersion(); v != "3.7.0" {
		t.Fatalf("the cluster version after the cancel: %q", v)
	}
	if got := s.downgrade(41, V, "3.6", nil); got != "3.7" {
		t.Fatalf("validate after the cancel: %q", got)
	}
}

// TestHashCoversReplicatedState changes one part of the replicated state
// at a time that the index does not hold; each change must change Hash.
func TestHashCoversReplicatedState(t *testing.T) {
	base := NewState(1, nil).Hash()
	for name, change := range map[string]func(*State){
		"cluster version":   func(st *State) { st.etcdVersion = "3.6.0" },
		"downgrade enabled": func(st *State) { st.downgradeEnabled = true },
		"downgrade target":  func(st *State) { st.downgradeTarget = "3.6.0" },
		"empty role":        func(st *State) { st.auth.roles["r"] = &authpb.Role{Name: []byte("r")} },
	} {
		st := NewState(1, nil)
		change(st)
		if st.Hash() == base {
			t.Errorf("%s does not change the hash", name)
		}
	}
}
