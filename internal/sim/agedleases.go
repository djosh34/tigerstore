package sim

import (
	"errors"
	"fmt"
	"sync"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"
)

// loadImageLeases grants Config.ImageLeases after the ordinary clients
// finish and the faults heal, with keys on a quarter of them. Its 32
// clients finish within half the TTL, so none expires before the image.
// Each call goes through the ordinary client history and all checkers.
// After the image's idle gap a new leader starts their TTLs over, as
// etcd's lessor does: the forks expire them together, not while down.
func (s *Sim) loadImageLeases() error {
	const clients = 32
	limit := s.now() + kubeLeaseTTL*Second/2
	var wg sync.WaitGroup
	errs := make(chan error, clients)
	for id := range clients {
		wg.Go(func() {
			// Keep these clients apart from the bulk client's history.
			c := &client{id: bulkClient + 1 + id, sim: s}
			call := func(req proto.Message) (proto.Message, error) {
				for s.now() < limit {
					// Calls of one client must not overlap, even at a tick.
					if !s.sleep(Millisecond) {
						return nil, errors.New("image leases: the run ended")
					}
					s.mu.Lock()
					node := max(s.leaderIdx(), 0)
					c.n++
					r := &Record{Client: c.id, Node: node, Call: s.now(), req: req}
					s.mu.Unlock()
					resp, err := s.call(s.nodes[node], req, Second)
					s.mu.Lock()
					c.done(r, resp, err)
					stopped := s.err
					s.mu.Unlock()
					if stopped != nil {
						return nil, stopped
					}
					if err == nil {
						return resp, nil
					}
					if !s.sleep(100 * Millisecond) {
						return nil, err
					}
				}
				return nil, fmt.Errorf("image leases: did not finish %d grants within half the TTL", s.cfg.ImageLeases)
			}
			for i := id; i < s.cfg.ImageLeases; i += clients {
				resp, err := call(&pb.LeaseGrantRequest{TTL: kubeLeaseTTL})
				if err != nil {
					errs <- err
					return
				}
				if i%4 == 0 {
					key := fmt.Appendf(nil, "/leases/%08d", i)
					_, err = call(&pb.TxnRequest{
						Compare: []*pb.Compare{createdNever(&kubeObject{key: key})},
						Success: []*pb.RequestOp{putOp(&pb.PutRequest{
							Key: key, Value: fmt.Appendf(nil, "image-lease-%d", i), Lease: resp.(*pb.LeaseGrantResponse).ID,
						})},
					})
					if err != nil {
						errs <- err
						return
					}
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	var err error
	for e := range errs {
		err = errors.Join(err, e)
	}
	return err
}
