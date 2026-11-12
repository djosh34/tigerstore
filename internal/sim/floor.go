package sim

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"sync"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
	tnode "github.com/djosh34/tigerstore/internal/node"
	"github.com/djosh34/tigerstore/internal/peer"
	"github.com/djosh34/tigerstore/internal/server"
	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/simrun"
	"github.com/djosh34/tigerstore/internal/testcert"
	"github.com/djosh34/tigerstore/internal/wal"
)

// The floor of the simulator's speed (#494, #500) is what the calls of a
// calm run (CalmConfig) cost through the real code path with nothing of
// the simulator around them: three real nodes in one process, each with
// its peer transport and its gRPC server, speaking TLS over in-memory
// connections (gRPC's bufconn), on in-memory simfs disks that never fail.
// The connections cost no system calls, as the simulator's do not, so
// the floor is the work of the calls themselves. The same clients make the
// same calls at the same think times: half of them enter at Node.Do, the
// others use clientv3. The same watches stay open. There is no simulated
// network, no fault, no history, no trace, no state hash and no check.
//
// Natively it runs on Go's scheduler and clock: that is the floor. Under
// gosim the same code runs on gosim's scheduler and clock, which shows
// what gosim alone costs.

// FloorResult is what a floor run did while its clients ran.
type FloorResult struct {
	// Answered counts the calls answered without error, and Failed the
	// others.
	Answered, Failed int
	// CPU is the process's CPU time over the load, on all of its threads,
	// and Wall how long the load took.
	CPU, Wall time.Duration
}

// CallsPerCoreSecond is the answered calls per second of CPU time.
func (r FloorResult) CallsPerCoreSecond() float64 { return float64(r.Answered) / r.CPU.Seconds() }

// floor is one floor run.
type floor struct {
	cfg Config
	// mu guards src's random source, the clients and the counts. src is
	// the Sim the clients draw their calls and think times from
	// (client.request): it holds only cfg and the random source.
	mu    sync.Mutex
	src   *Sim
	nodes []*floorNode
	ca    *testcert.CA
	// lns are the listeners of the nodes' peer and client addresses.
	lns map[string]*bufconn.Listener
	// stop is closed when the clients stop.
	stop chan struct{}
	// wg counts the clients and watches, serving the gRPC servers.
	wg, serving sync.WaitGroup
	res         FloorResult
	errs        []error
}

// floorNode is one node of the floor: its process and how to reach it.
type floorNode struct {
	node *tnode.Node
	t    *peer.TCP
	srv  *server.Server
	// ran gets what its Run returns.
	ran chan error
}

// errNoFloorCluster is the error of a floor run whose cluster did not
// answer within formLimit.
var errNoFloorCluster = errors.New("sim: the floor's cluster did not answer")

// Floor runs the floor of seed with its clients running for d.
func Floor(seed uint64, d time.Duration) (FloorResult, error) {
	cfg := CalmConfig(seed)
	f := &floor{cfg: cfg, src: &Sim{cfg: cfg, rng: rand.New(rand.NewPCG(seed, 0x666c6f6f72))}, stop: make(chan struct{})}
	var err error
	if f.ca, err = testcert.NewCA(); err != nil {
		return FloorResult{}, err
	}
	cert, _, _, err := f.ca.IssueFor("tigerstore-floor", 2, nil, hosts)
	if err != nil {
		return FloorResult{}, err
	}
	f.lns = map[string]*bufconn.Listener{}
	var urls []string
	for i := range 3 {
		f.lns[peerAddr(i)] = bufconn.Listen(floorBuffer)
		f.lns[clientAddr(i)] = bufconn.Listen(floorBuffer)
		urls = append(urls, peerURL(i))
	}
	defer func() {
		// A listener that a server or transport closed already returns
		// an error that is none here.
		for _, ln := range f.lns {
			_ = ln.Close()
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	leaseCheckpoints := f.src.rng.IntN(2) == 0
	for i := range 3 {
		if err := f.start(ctx, i, urls, leaseCheckpoints, cert); err != nil {
			return FloorResult{}, errors.Join(err, f.shutdown(cancel))
		}
	}
	if err := f.formed(ctx); err != nil {
		return FloorResult{}, errors.Join(err, f.shutdown(cancel))
	}
	cpu, err := simrun.CPUTime()
	if err != nil {
		return FloorResult{}, errors.Join(err, f.shutdown(cancel))
	}
	start := time.Now()
	f.load(ctx)
	time.Sleep(d)
	close(f.stop)
	f.wg.Wait()
	end, err := simrun.CPUTime()
	f.res.CPU, f.res.Wall = end-cpu, time.Since(start)
	err = errors.Join(err, f.shutdown(cancel))
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.res, errors.Join(append(f.errs, err)...)
}

// start starts node i of the floor's cluster, whose peer URLs are urls.
func (f *floor) start(ctx context.Context, i int, urls []string, leaseCheckpoints bool, cert tls.Certificate) error {
	id := uint64(i + 1)
	cfg := baseNodeConfig(id, urls, leaseCheckpoints)
	// The state hash after every entry is for the simulator's checks.
	cfg.Core.Record = false
	tn, err := tnode.NewOnFS(cfg, &memDisk{d: simfs.New()})
	if err != nil {
		return err
	}
	peers := map[uint64]string{}
	for j, u := range urls {
		if j != i {
			peers[uint64(j+1)] = hostPort(u)
		}
	}
	t, err := peer.Start(peer.Config{
		ID: id, Listeners: []net.Listener{f.lns[peerAddr(i)]}, Peers: peers, Addr: peerAddr(i),
		Certificate: cert, CA: f.ca.Pool,
		Loop: tn.Loop(), Deliver: tn.Receive, Unreachable: tn.Unreachable, Serve: tn.Serve, Removed: tn.Removed, Answer: tn.Answer,
		Dial: f.dial,
	})
	if err != nil {
		return err
	}
	srv, err := server.New(tn, server.Config{TLS: &tls.Config{Certificates: []tls.Certificate{cert}}, NoGateway: true})
	if err != nil {
		return errors.Join(err, t.Close())
	}
	n := &floorNode{node: tn, t: t, srv: srv, ran: make(chan error, 1)}
	f.nodes = append(f.nodes, n)
	f.serving.Go(func() {
		// Serve returns when the server stops.
		if err := srv.Serve(f.lns[clientAddr(i)]); err != nil && !errors.Is(err, net.ErrClosed) {
			f.fail(fmt.Errorf("node %d: serving clients: %w", id, err))
		}
	})
	go func() { n.ran <- tn.Run(ctx, t) }()
	return nil
}

// floorBuffer is the size of each direction of a floor connection's
// buffer.
const floorBuffer = 1 << 20

// dial connects to the listener at addr.
func (f *floor) dial(ctx context.Context, addr string) (net.Conn, error) {
	ln, ok := f.lns[addr]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errRefused, addr)
	}
	return ln.DialContext(ctx)
}

// formed waits until node 1 answers a linearizable read, which needs a
// leader, for at most formLimit.
func (f *floor) formed(ctx context.Context) error {
	deadline := time.Now().Add(time.Duration(formLimit))
	for time.Now().Before(deadline) {
		c, cancel := context.WithTimeout(ctx, time.Second)
		_, err := f.nodes[0].node.Do(c, &pb.RangeRequest{Key: key(0)})
		cancel()
		if err == nil {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return errNoFloorCluster
}

// shutdown stops the nodes and their servers, and returns what went
// wrong in them.
func (f *floor) shutdown(cancel context.CancelFunc) error {
	cancel()
	var errs []error
	for i, n := range f.nodes {
		if err := <-n.ran; err != nil && !errors.Is(err, context.Canceled) {
			errs = append(errs, fmt.Errorf("node %d: %w", i+1, err))
		}
		errs = append(errs, n.srv.Stop(), n.t.Close())
	}
	f.serving.Wait()
	return errors.Join(errs...)
}

// load starts the clients and the watches of a calm run.
func (f *floor) load(ctx context.Context) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.cfg.Clients + f.cfg.V3Clients {
		c := &client{id: i, sim: f.src, node: -1}
		if i >= f.cfg.Clients {
			c.node = i % 3
		}
		wait := f.src.rng.Int64N(100 * Millisecond)
		f.wg.Go(func() { f.call(ctx, c, wait) })
	}
	for _, sp := range watchSpecs(0) {
		next := int64(0)
		if f.src.rng.IntN(2) == 0 {
			next = 1
		}
		wait := f.src.rng.Int64N(Second)
		f.wg.Go(func() { f.watch(ctx, sp, next, wait) })
	}
}

// sleep waits d, and reports false when the clients stopped first.
func (f *floor) sleep(d int64) bool {
	t := time.NewTimer(time.Duration(max(d, 0)))
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-f.stop:
		return false
	}
}

// exp draws a time with an exponential distribution of mean mean. The
// lock is held.
func (f *floor) exp(mean int64) int64 { return int64(f.src.rng.ExpFloat64()*float64(mean)) + 1 }

// call runs client c: one call after the other, after a first wait.
func (f *floor) call(ctx context.Context, c *client, wait int64) {
	if c.node >= 0 {
		if err := c.dialV3(clientAddr(c.node), f.ca.Pool, f.dial); err != nil {
			f.fail(fmt.Errorf("client %d: %w", c.id, err))
			return
		}
		defer func() {
			if err := c.closeV3(); err != nil {
				f.fail(fmt.Errorf("client %d: closing clientv3: %w", c.id, err))
			}
		}()
	}
	for f.sleep(wait) {
		f.mu.Lock()
		c.n++
		i := c.node
		if c.v3 == nil {
			i = f.src.rng.IntN(3)
		}
		req := c.request()
		f.mu.Unlock()
		var resp proto.Message
		var err error
		if c.v3 != nil {
			resp, err = c.callV3(req)
		} else {
			cctx, cancel := context.WithTimeout(ctx, time.Duration(f.cfg.ClientTimeout))
			resp, err = f.nodes[i].node.Do(cctx, req)
			cancel()
		}
		f.mu.Lock()
		if err == nil {
			f.res.Answered++
			c.learn(resp)
		} else {
			f.res.Failed++
		}
		wait = c.think(err)
		f.mu.Unlock()
	}
}

// fail makes err an error of the run.
func (f *floor) fail(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errs = append(f.errs, err)
}

// watch keeps watch sp open from revision next, as a watch client of a
// run does (watchClient.run): on a random node, closing the stream now
// and then and resuming on another from the first revision not seen, with
// the same waits.
func (f *floor) watch(ctx context.Context, sp watchSpec, next, wait int64) {
	var dropAt time.Time
	for f.sleep(wait) {
		f.mu.Lock()
		n := f.nodes[f.src.rng.IntN(3)]
		if !time.Now().Before(dropAt) {
			dropAt = time.Now().Add(time.Duration(f.exp(3 * Second)))
		}
		// The wait after a stream the node refused.
		wait = 100*Millisecond + f.src.rng.Int64N(400*Millisecond)
		f.mu.Unlock()
		ws, err := n.node.OpenWatch(ctx, core.Caller{})
		if err != nil {
			continue
		}
		req := &pb.WatchCreateRequest{Key: []byte(sp.key), StartRevision: next, PrevKv: sp.prevKV, ProgressNotify: sp.progress}
		if sp.end != "" {
			req.RangeEnd = []byte(sp.end)
		}
		if err := ws.Send(ctx, &pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{CreateRequest: req}}); err != nil {
			ws.Close()
			continue
		}
		next, wait, dropAt = f.follow(ws, next, dropAt)
		ws.Close()
	}
}

// follow reads stream ws until it ends, the drop at dropAt comes or the
// clients stop, and returns the first revision not seen, how long to wait
// before the next stream and when to drop that one: up to 2 s and a new
// drop after a drop, no wait and the same drop after a stream that ended
// or a compaction that canceled the watch.
func (f *floor) follow(ws *tnode.WatchStream, next int64, dropAt time.Time) (int64, int64, time.Time) {
	t := time.NewTimer(time.Until(dropAt))
	defer t.Stop()
	for {
		select {
		case r := <-ws.C:
			ws.Taken(r)
			switch {
			case r.Canceled:
				// A compaction passed it; a new logical watch starts
				// from now.
				return 0, 0, dropAt
			case r.Created && next == 0:
				next = r.Header.Revision + 1
			case len(r.Events) > 0:
				next = r.Events[len(r.Events)-1].Kv.ModRevision + 1
			case !r.Created:
				next = max(next, r.Header.Revision+1)
			}
		case <-ws.Done():
			return next, 0, dropAt
		case <-t.C:
			f.mu.Lock()
			defer f.mu.Unlock()
			return next, f.src.rng.Int64N(2 * Second), time.Now().Add(time.Duration(f.exp(3 * Second)))
		case <-f.stop:
			return next, 0, dropAt
		}
	}
}

// memDisk is a simfs disk that never fails, with its calls serialized,
// as the nodes' goroutines share it.
type memDisk struct {
	mu sync.Mutex
	d  *simfs.Disk
}

func (d *memDisk) List() ([]string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.d.List()
}

func (d *memDisk) Usage() (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return int64(d.d.Bytes()), nil
}

func (d *memDisk) open(name string, open func(string) (wal.File, error)) (wal.File, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	f, err := open(name)
	if err != nil {
		return nil, err
	}
	return &memFile{d: d, f: f}, nil
}

func (d *memDisk) Create(name string) (wal.File, error) { return d.open(name, d.d.Create) }
func (d *memDisk) Open(name string) (wal.File, error)   { return d.open(name, d.d.Open) }

func (d *memDisk) Remove(name string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.d.Remove(name)
}

func (d *memDisk) Rename(a, b string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.d.Rename(a, b)
}

func (d *memDisk) SyncDir() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.d.SyncDir()
}

// memFile is an open file of a memDisk.
type memFile struct {
	d *memDisk
	f wal.File
}

func (f *memFile) ReadAll() ([]byte, error) {
	f.d.mu.Lock()
	defer f.d.mu.Unlock()
	return f.f.ReadAll()
}

func (f *memFile) ReadAt(p []byte, off int64) (int, error) {
	f.d.mu.Lock()
	defer f.d.mu.Unlock()
	return f.f.ReadAt(p, off)
}

func (f *memFile) Write(p []byte) (int, error) {
	f.d.mu.Lock()
	defer f.d.mu.Unlock()
	return f.f.Write(p)
}

func (f *memFile) WriteAt(p []byte, off int64) (int, error) {
	f.d.mu.Lock()
	defer f.d.mu.Unlock()
	return f.f.WriteAt(p, off)
}

func (f *memFile) Truncate(size int64) error {
	f.d.mu.Lock()
	defer f.d.mu.Unlock()
	return f.f.Truncate(size)
}

func (f *memFile) Sync() error {
	f.d.mu.Lock()
	defer f.d.mu.Unlock()
	return f.f.Sync()
}

func (f *memFile) DropCache() error {
	f.d.mu.Lock()
	defer f.d.mu.Unlock()
	return f.f.DropCache()
}

func (f *memFile) Close() error {
	f.d.mu.Lock()
	defer f.d.mu.Unlock()
	return f.f.Close()
}
