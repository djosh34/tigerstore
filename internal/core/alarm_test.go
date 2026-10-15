package core

import (
	"errors"
	"slices"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/simfs"
)

// tinyCheckpoints makes a node cut checkpoints after a few KiB of log.
func tinyCheckpoints(cfg *Config) {
	cfg.WindowBytes, cfg.WindowMaxBytes = 512, 1024
	cfg.CheckpointBytes, cfg.CheckpointRatio, cfg.CheckpointChunk = 2048, 2, 64
}

// restartFromCheckpoint runs the writes fill makes until a durable
// checkpoint holds everything the node applied so far and the first
// segment is gone, then restarts the node from its disk with boot 2. The
// state it had must come back from the checkpoint, not from the log.
func restartFromCheckpoint(t *testing.T, s *single, disk *simfs.Disk, fill func(i int) proto.Message) *single {
	t.Helper()
	applied := s.n.Applied()
	for i := 0; s.n.DurableCheckpoint().Index <= applied || slices.Contains(disk.DurableNames(), "0000000000000001.wal"); i++ {
		if i == 1000 {
			t.Fatalf("no checkpoint became durable: %v applied %d now %d %v", s.n.DurableCheckpoint(), applied, s.n.Applied(), disk.DurableNames())
		}
		if r := s.do(uint64(1_000_000+i), fill(i)); r.Err != nil {
			t.Fatal(r.Err)
		}
	}
	want := s.n.Hash()
	if err := s.log.Close(); err != nil {
		t.Fatal(err)
	}
	s = startSingleWith(t, disk, 2, tinyCheckpoints)
	if s.n.Hash() != want {
		t.Fatal("the restarted node's state differs")
	}
	return s
}

// capable ticks the node until its announcement of its version applied,
// so that it writes the kinds of featureVersion.
func (s *single) capable() *single {
	s.t.Helper()
	for range 50 {
		if s.n.capable() {
			return s
		}
		s.n.Tick()
		s.run()
	}
	s.t.Fatal("the node never reached the version of its features")
	return nil
}

func alarmReq(action pb.AlarmRequest_AlarmAction, typ pb.AlarmType, member uint64) *pb.AlarmRequest {
	return &pb.AlarmRequest{Action: action, Alarm: typ, MemberID: member}
}

func (s *single) alarms(id uint64, r *pb.AlarmRequest) []*pb.AlarmMember {
	s.t.Helper()
	resp, ok := s.ok(id, r).(*pb.AlarmResponse)
	if !ok {
		s.t.Fatal("not an alarm response")
	}
	return resp.Alarms
}

func sameAlarms(got []*pb.AlarmMember, want ...*pb.AlarmMember) bool {
	return slices.EqualFunc(got, want, func(a, b *pb.AlarmMember) bool { return proto.Equal(a, b) })
}

// TestAlarms follows etcd v3.7.2: activating answers the alarm, again too;
// a list gives every alarm or those of one type; NOSPACE refuses puts,
// transactions with a put and lease grants; CORRUPT refuses every write
// but keepalives; deactivating lifts them; and the alarms survive a
// restart from a checkpoint.
func TestAlarms(t *testing.T) {
	disk := simfs.New()
	s := startSingleWith(t, disk, 1, tinyCheckpoints).capable()
	nospace7 := &pb.AlarmMember{MemberID: 7, Alarm: pb.AlarmType_NOSPACE}
	corrupt3 := &pb.AlarmMember{MemberID: 3, Alarm: pb.AlarmType_CORRUPT}
	g := s.ok(1, &pb.LeaseGrantRequest{ID: 5, TTL: 600})
	if g == nil {
		t.Fatal("no grant")
	}
	for i, r := range []*pb.AlarmRequest{
		alarmReq(pb.AlarmRequest_ACTIVATE, pb.AlarmType_NOSPACE, 7),
		alarmReq(pb.AlarmRequest_ACTIVATE, pb.AlarmType_NOSPACE, 7),
	} {
		if got := s.alarms(uint64(10+i), r); !sameAlarms(got, nospace7) {
			t.Fatalf("activate %d: %v", i, got)
		}
	}
	if got := s.alarms(12, alarmReq(pb.AlarmRequest_ACTIVATE, pb.AlarmType_NONE, 7)); len(got) != 0 {
		t.Fatalf("activating NONE: %v", got)
	}
	refusals := []struct {
		op   proto.Message
		want error
	}{
		{&pb.PutRequest{Key: []byte("a"), Value: []byte("1")}, rpctypes.ErrGRPCNoSpace},
		{&pb.TxnRequest{Failure: []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("a")}}}}}, rpctypes.ErrGRPCNoSpace},
		{&pb.LeaseGrantRequest{ID: 6, TTL: 60}, rpctypes.ErrGRPCNoSpace},
		{&pb.DeleteRangeRequest{Key: []byte("a")}, nil},
		{&pb.TxnRequest{Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestDeleteRange{RequestDeleteRange: &pb.DeleteRangeRequest{Key: []byte("a")}}}}}, nil},
		{&pb.LeaseKeepAliveRequest{ID: 5}, nil},
	}
	for i, c := range refusals {
		if r := s.do(uint64(20+i), c.op); !errors.Is(r.Err, c.want) {
			t.Fatalf("%T under NOSPACE: %v, want %v", c.op, r.Err, c.want)
		}
	}
	// A put in wire form goes the same way.
	s.n.Submit(Request{ID: 30, Put: Put{PutOp: mustParsePut(t, &pb.PutRequest{Key: []byte("w"), Value: []byte("1")}).PutOp, Wire: mustMarshal(t, &pb.PutRequest{Key: []byte("w"), Value: []byte("1")})}})
	for _, r := range s.run() {
		if r.ID == 30 && !errors.Is(r.Err, rpctypes.ErrGRPCNoSpace) {
			t.Fatalf("wire put under NOSPACE: %v", r.Err)
		}
	}

	s.alarms(40, alarmReq(pb.AlarmRequest_ACTIVATE, pb.AlarmType_CORRUPT, 3))
	if got := s.alarms(41, alarmReq(pb.AlarmRequest_GET, pb.AlarmType_NONE, 0)); !sameAlarms(got, nospace7, corrupt3) {
		t.Fatalf("list: %v", got)
	}
	if got := s.alarms(42, alarmReq(pb.AlarmRequest_GET, pb.AlarmType_CORRUPT, 0)); !sameAlarms(got, corrupt3) {
		t.Fatalf("list CORRUPT: %v", got)
	}
	if got := s.n.Status().Errors; !slices.Equal(got, []string{"memberID:7 alarm:NOSPACE", "memberID:3 alarm:CORRUPT"}) {
		t.Fatalf("Status errors: %q", got)
	}
	for i, op := range []proto.Message{
		&pb.PutRequest{Key: []byte("a"), Value: []byte("1")},
		&pb.DeleteRangeRequest{Key: []byte("a")},
		&pb.CompactionRequest{Revision: 1},
		&pb.LeaseRevokeRequest{ID: 5},
	} {
		if r := s.do(uint64(50+i), op); !errors.Is(r.Err, rpctypes.ErrGRPCCorrupt) {
			t.Fatalf("%T under CORRUPT: %v", op, r.Err)
		}
	}
	if r := s.do(60, &pb.RangeRequest{Key: []byte("a")}); r.Err != nil {
		t.Fatalf("range under CORRUPT: %v", r.Err)
	}

	// Under CORRUPT, only keepalives still write.
	s = restartFromCheckpoint(t, s, disk, func(int) proto.Message { return &pb.LeaseKeepAliveRequest{ID: 5} })
	if got := s.alarms(70, alarmReq(pb.AlarmRequest_GET, pb.AlarmType_NONE, 0)); !sameAlarms(got, nospace7, corrupt3) {
		t.Fatalf("list after the restart: %v", got)
	}
	if r := s.do(71, &pb.PutRequest{Key: []byte("a"), Value: []byte("1")}); !errors.Is(r.Err, rpctypes.ErrGRPCCorrupt) {
		t.Fatalf("put under CORRUPT after the restart: %v", r.Err)
	}
	if got := s.alarms(72, alarmReq(pb.AlarmRequest_DEACTIVATE, pb.AlarmType_CORRUPT, 3)); !sameAlarms(got, corrupt3) {
		t.Fatalf("deactivate: %v", got)
	}
	if got := s.alarms(73, alarmReq(pb.AlarmRequest_DEACTIVATE, pb.AlarmType_CORRUPT, 3)); len(got) != 0 {
		t.Fatalf("deactivate again: %v", got)
	}
	s.alarms(74, alarmReq(pb.AlarmRequest_DEACTIVATE, pb.AlarmType_NOSPACE, 7))
	if r := s.do(75, &pb.PutRequest{Key: []byte("a"), Value: []byte("1")}); r.Err != nil {
		t.Fatalf("put after deactivating: %v", r.Err)
	}
	if got := s.alarms(76, &pb.AlarmRequest{Action: 7}); len(got) != 0 {
		t.Fatalf("unknown action: %v", got)
	}
}

func mustMarshal(t *testing.T, m proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func mustParsePut(t *testing.T, r *pb.PutRequest) Put {
	t.Helper()
	p, err := ParsePut(mustMarshal(t, r))
	if err != nil {
		t.Fatal(err)
	}
	return p
}
