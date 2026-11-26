package sim

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// TestATightDiskHoldsItsCheckpointsAndRecovers runs DefaultConfig's load
// on disks too small for the store's second checkpoint, with no other
// fault: each node finds no room, holds its checkpoints with NOSPACE
// raised and stays up, its log ring filling behind the checkpoint it
// cannot take, until an operator grows the disks and disarms the alarm
// (Faults.GrowDisk). The run must then recover (checkGrown) and pass
// every checker (#769, #846's ruling, Opus's review of #846 on a ring
// that fills while checkpoints wait).
func TestATightDiskHoldsItsCheckpointsAndRecovers(t *testing.T) {
	if simrun.Race {
		t.Skip("the disks' room is counted under gosim")
	}
	simrun.Each(t, seedRange(3800, 2), func(t *testing.T, seed uint64) Stats {
		cfg := DefaultConfig(seed)
		cfg.Faults = Faults{DiskBudget: 32 << 10, DiskCapacity: 20 << 10, GrowDisk: true}
		// Time for the disk to fill, for the operator, and for the writes
		// taken again after the disarm.
		cfg.Duration, cfg.Heal = 40*Second, 20*Second
		res := run(t, cfg)
		checkers(t, res)
		st := res.Stats
		t.Logf("seed %d: %d ok, NoRoom %d, NoRoomOwn %d, NoSpace %d, DiskGrowths %d, Disarms %d, GrownCheckpoints %d, OutOfSpace %d, checkpoints %d, log cuts %d",
			seed, st.OK, st.NoRoom, st.NoRoomOwn, st.NoSpace, st.DiskGrowths, st.Disarms, st.GrownCheckpoints, st.OutOfSpace, st.Checkpoints, st.LogCuts)
		if st.NoRoom == 0 || st.OutOfSpace != 0 {
			t.Fatalf("seed %d: %d checkpoints held for room, %d nodes out of space: the disk must fill and the nodes stay up", seed, st.NoRoom, st.OutOfSpace)
		}
		checkGrown(t, seed, res, true)
		return st
	}, nil)
}

// checkGrown checks, of a run with Faults.GrowDisk that found no room for
// a checkpoint, that it recovered: a disarm and a write NOSPACE would
// refuse that succeeded after the last disarm. Only a lack of room with
// no disk fault active demands one growth and a checkpoint after it
// (#502's ruling): a fault alone needs no growth. NOSPACE's refusals count
// for the liveness rule as answers; the checkers check that every node
// caught up. With calm, the run had no other fault, and no call may time
// out while NOSPACE was up, from the first call it refused to the disarm:
// a node whose log ring wedged behind the checkpoints it could not take
// would leave calls unanswered (Opus's review of #846). A run whose disk
// did not fill passes.
func checkGrown(t *testing.T, seed uint64, res *Result, calm bool) {
	t.Helper()
	if err := grownRecovery(res, calm); err != nil {
		t.Fatalf("seed %d: %v", seed, err)
	}
}

// grownRecovery is checkGrown's verdict, separate so its cases can be
// built directly, without a seed's timing.
func grownRecovery(res *Result, calm bool) error {
	st := res.Stats
	if st.NoRoom == 0 {
		return nil
	}
	if st.NoRoomOwn > 0 && (st.DiskGrowths != 1 || st.GrownCheckpoints == 0) {
		return fmt.Errorf("%d checkpoints held for room with no disk fault, %d growths, %d checkpoints after: the operator must grow the disks, and a checkpoint come after",
			st.NoRoomOwn, st.DiskGrowths, st.GrownCheckpoints)
	}
	if st.Disarms == 0 {
		return fmt.Errorf("%d checkpoints held for room, no disarms: the operator must disarm", st.NoRoom)
	}
	if calm {
		from := int64(math.MaxInt64)
		for _, r := range res.History {
			if errors.Is(r.Err, rpctypes.ErrGRPCNoSpace) {
				from = min(from, r.Call)
			}
		}
		if from == math.MaxInt64 {
			return errors.New("no call was refused with NOSPACE")
		}
		for _, r := range res.History {
			if r.Call >= from && r.Return <= st.DisarmedAt && errors.Is(r.Err, errClientTimeout) {
				return fmt.Errorf("client %d's call at %s timed out while NOSPACE was up (%s to %s)", r.Client, seconds(r.Call), seconds(from), seconds(st.DisarmedAt))
			}
		}
	}
	if !slices.ContainsFunc(res.History, func(r *Record) bool { return r.Call > st.DisarmedAt && r.Err == nil && noSpaceWrite(r.req, r.Resp) }) {
		return fmt.Errorf("no write succeeded after the last disarm at %s", seconds(st.DisarmedAt))
	}
	return nil
}

// noSpaceWrite reports whether a call did work NOSPACE refuses: a put,
// a lease grant, or a put in a transaction's executed branch. Unlike
// core.refusedOp and core.txnCost, it looks only at the branches that ran,
// nested ones included: a read-only branch proves no recovery.
func noSpaceWrite(req, resp proto.Message) bool {
	switch req := req.(type) {
	case *pb.PutRequest, *pb.LeaseGrantRequest:
		return true
	case *pb.TxnRequest:
		resp, ok := resp.(*pb.TxnResponse)
		if !ok || resp == nil {
			return false
		}
		ops := req.Failure
		if resp.Succeeded {
			ops = req.Success
		}
		for i, op := range ops {
			if op.GetRequestPut() != nil {
				return true
			}
			if txn := op.GetRequestTxn(); txn != nil && i < len(resp.Responses) && noSpaceWrite(txn, resp.Responses[i].GetResponseTxn()) {
				return true
			}
		}
	}
	return false
}

// TestGrownRecovery separates a disk that filled on its own from a fault
// that left no room (#502): both need a disarm and a write after the last
// disarm, but only the former needs a growth and a checkpoint after it.
func TestGrownRecovery(t *testing.T) {
	write := func(call, ret int64, err error) *Record {
		return &Record{Call: call * Second, Return: ret * Second, Err: err, req: &pb.PutRequest{Key: []byte("k"), Value: []byte("v")}}
	}
	faults := Stats{NoRoom: 4, Disarms: 4, DisarmedAt: 20 * Second}
	mixed := faults
	mixed.NoRoomOwn = 1
	own := Stats{NoRoom: 1, NoRoomOwn: 1, Disarms: 1, DisarmedAt: 20 * Second}
	grown := mixed
	grown.DiskGrowths, grown.GrownCheckpoints = 1, 1
	noDisarm := faults
	noDisarm.Disarms = 0
	noCheckpoint := grown
	noCheckpoint.GrownCheckpoints = 0
	twoGrowths := grown
	twoGrowths.DiskGrowths = 2
	refused := &Record{Call: 10 * Second, Return: 11 * Second, Err: rpctypes.ErrGRPCNoSpace}
	after := write(21, 22, nil)
	for _, c := range []struct {
		name    string
		stats   Stats
		history []*Record
		calm    bool
		want    string
	}{
		{"no lack of room", Stats{}, nil, false, ""},
		{"only faults, recovered", faults, []*Record{after}, false, ""},
		{"only faults, no write after the last disarm", faults, []*Record{write(15, 16, nil)}, false, "no write succeeded after the last disarm"},
		{"write across the last disarm", faults, []*Record{write(19, 21, nil)}, false, "no write succeeded after the last disarm"},
		{"write at the last disarm", faults, []*Record{write(20, 21, nil)}, false, "no write succeeded after the last disarm"},
		{"failed write after the last disarm", faults, []*Record{write(21, 22, errClientTimeout)}, false, "no write succeeded after the last disarm"},
		{"read after the last disarm", faults, []*Record{{Call: 21 * Second, Return: 22 * Second, req: &pb.RangeRequest{}}}, false, "no write succeeded after the last disarm"},
		{"mixed, no growth", mixed, []*Record{after}, false, "the operator must grow the disks"},
		{"no fault, no growth", own, []*Record{after}, false, "the operator must grow the disks"},
		{"mixed, grown and recovered", grown, []*Record{after}, false, ""},
		{"mixed, grown but no write after the last disarm", grown, []*Record{write(15, 16, nil)}, false, "no write succeeded after the last disarm"},
		{"no disarm", noDisarm, []*Record{after}, false, "the operator must disarm"},
		{"no checkpoint after growth", noCheckpoint, []*Record{after}, false, "a checkpoint come after"},
		{"more than one growth", twoGrowths, []*Record{after}, false, "the operator must grow the disks"},
		{"calm, recovered", grown, []*Record{refused, after}, true, ""},
		{"calm, no NOSPACE refusal", grown, []*Record{after}, true, "no call was refused with NOSPACE"},
		{"calm, timeout while NOSPACE was up", grown, []*Record{refused, write(12, 13, errClientTimeout), after}, true, "timed out while NOSPACE was up"},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := grownRecovery(&Result{Stats: c.stats, History: c.history}, c.calm)
			if c.want == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("got %v, want %q", err, c.want)
			}
		})
	}
}

// TestGrownRecoveryNeedsSpace checks that recovery needs a write that
// NOSPACE would refuse, not a delete or compaction allowed under it, nor
// a transaction whose executed branch only read. A nested transaction's
// response selects its branch too.
func TestGrownRecoveryNeedsSpace(t *testing.T) {
	put := putOp(&pb.PutRequest{Key: []byte("k"), Value: []byte("v")})
	get := &pb.RequestOp{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: []byte("k")}}}
	del := &pb.RequestOp{Request: &pb.RequestOp_RequestDeleteRange{RequestDeleteRange: &pb.DeleteRangeRequest{Key: []byte("k")}}}
	txn := &pb.TxnRequest{Success: []*pb.RequestOp{put}, Failure: []*pb.RequestOp{get}}
	inverse := &pb.TxnRequest{Success: []*pb.RequestOp{get}, Failure: []*pb.RequestOp{put}}
	nested := &pb.TxnRequest{Success: []*pb.RequestOp{get, {Request: &pb.RequestOp_RequestTxn{RequestTxn: txn}}}}
	putResp := &pb.ResponseOp{Response: &pb.ResponseOp_ResponsePut{ResponsePut: &pb.PutResponse{}}}
	getResp := &pb.ResponseOp{Response: &pb.ResponseOp_ResponseRange{ResponseRange: &pb.RangeResponse{}}}
	delResp := &pb.ResponseOp{Response: &pb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: &pb.DeleteRangeResponse{}}}
	response := func(succeeded bool, ops ...*pb.ResponseOp) *pb.TxnResponse {
		return &pb.TxnResponse{Succeeded: succeeded, Responses: ops}
	}
	for _, c := range []struct {
		name string
		req  proto.Message
		resp proto.Message
		want bool
	}{
		{"put", put.GetRequestPut(), &pb.PutResponse{}, true},
		{"lease grant", &pb.LeaseGrantRequest{TTL: 10}, &pb.LeaseGrantResponse{}, true},
		{"only a delete", del.GetRequestDeleteRange(), &pb.DeleteRangeResponse{}, false},
		{"only a compaction", &pb.CompactionRequest{Revision: 1}, &pb.CompactionResponse{}, false},
		{"only a lease revoke", &pb.LeaseRevokeRequest{ID: 1}, &pb.LeaseRevokeResponse{}, false},
		{"delete-only transaction", &pb.TxnRequest{Success: []*pb.RequestOp{del}}, response(true, delResp), false},
		{"transaction read-only failure branch", txn, response(false, getResp), false},
		{"transaction read-only success branch", inverse, response(true, getResp), false},
		{"transaction put in success branch", txn, response(true, putResp), true},
		{"transaction put in failure branch", inverse, response(false, putResp), true},
		{"transaction without a response", txn, nil, false},
		{"nested transaction only read", nested, response(true, getResp, &pb.ResponseOp{Response: &pb.ResponseOp_ResponseTxn{ResponseTxn: response(false, getResp)}}), false},
		{"nested transaction put", nested, response(true, getResp, &pb.ResponseOp{Response: &pb.ResponseOp_ResponseTxn{ResponseTxn: response(true, putResp)}}), true},
		{"nested transaction without a response", nested, response(true, getResp), false},
	} {
		t.Run(c.name, func(t *testing.T) {
			res := &Result{
				Stats:   Stats{NoRoom: 4, Disarms: 4, DisarmedAt: 20 * Second},
				History: []*Record{{Call: 21 * Second, Return: 22 * Second, req: c.req, Resp: c.resp}},
			}
			err := grownRecovery(res, false)
			if got := err == nil; got != c.want {
				t.Fatalf("the rule gave %v, want recovered %v", err, c.want)
			}
			if err != nil && !strings.Contains(err.Error(), "no write succeeded after the last disarm") {
				t.Fatalf("the rule failed on another check: %v", err)
			}
		})
	}
}
