package sim

import (
	"errors"
	"fmt"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// errBulkEnded is a bulk load the end of the run cut short.
var errBulkEnded = errors.New("sim: the run ended during its bulk load")

// bulkClient is the client of the history whose calls load Config.BulkBytes.
const bulkClient = 1 << 20

// bulkValue is the size of each value a bulk load puts, and bulkPerTxn
// how many a Txn puts: 1 MiB a Txn, within etcd's request limit.
const (
	bulkValue  = 256 << 10
	bulkPerTxn = 4
)

// bulkLoad puts Config.BulkBytes of values under /bulk/ before the clients
// start and the faults begin, a Txn of bulkPerTxn at a time, as one client
// of the history: data past a node's memory budget (#465). Each Txn puts
// only keys that do not exist yet, and each attempt values of its own, so
// one that went again after a lost answer writes nothing twice, in the
// store or in the log.
func (s *Sim) bulkLoad() error {
	c := &client{id: bulkClient, sim: s}
	n := int(s.cfg.BulkBytes / bulkValue)
	// The load may take the run's fault window, or a second a value if
	// that is longer: under a small memory budget it puts about three a
	// second (TestAgedBulk: 372 of 610 in 120 s, at 137 MiB).
	limit := max(s.cfg.Duration, int64(n)*Second)
	for i := 0; i < n; i += bulkPerTxn {
		// The bulk client's calls follow one another: the next is called
		// after the last returned (history.Validate).
		if i > 0 && !s.sleep(Millisecond) {
			return errBulkEnded
		}
		for attempt := 0; ; attempt++ {
			// The watchdog starts after the load: a load that has not
			// ended by its limit ends the run here.
			if s.now() >= limit {
				return fmt.Errorf("bulk load: %d of %d values in after %s, the load's limit", i, n, seconds(s.now()))
			}
			// Each attempt puts values of its own: one whose answer was
			// lost may have committed, and the next, whose compare then
			// fails, must not put the same write in the log again.
			txn := &pb.TxnRequest{}
			for k := i; k < min(i+bulkPerTxn, n); k++ {
				key := fmt.Appendf(nil, "/bulk/%08d", k)
				value := fmt.Appendf(make([]byte, 0, bulkValue), "bulk-%d-a%d ", k, attempt)
				for len(value) < bulkValue {
					value = append(value, '.')
				}
				txn.Compare = append(txn.Compare, &pb.Compare{Key: key, Target: pb.Compare_CREATE, Result: pb.Compare_EQUAL, TargetUnion: &pb.Compare_CreateRevision{CreateRevision: 0}})
				txn.Success = append(txn.Success, putOp(&pb.PutRequest{Key: key, Value: value}))
			}
			s.mu.Lock()
			node := max(s.leaderIdx(), 0)
			r := &Record{Client: c.id, Node: node, Call: s.now(), req: txn}
			s.mu.Unlock()
			resp, err := s.call(s.nodes[node], txn, s.cfg.ClientTimeout)
			s.mu.Lock()
			c.done(r, resp, err)
			stopped := s.err != nil
			s.mu.Unlock()
			if stopped {
				return s.err
			}
			if err == nil {
				break
			}
			if !s.sleep(100 * Millisecond) {
				return errBulkEnded
			}
		}
	}
	s.mu.Lock()
	s.tracef("bulk load: %d values of %d bytes", n, bulkValue)
	s.mu.Unlock()
	return nil
}
