// Throwaway direct-gRPC reliability comparison. No etcd client retries or endpoint failover.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var deadline = time.Date(2026, 10, 13, 22, 0, 0, 0, time.UTC)

type recorder struct {
	mu    sync.Mutex
	enc   *json.Encoder
	start time.Time
}

func (r *recorder) log(fields map[string]any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	fields["at"] = time.Now().UTC().Format(time.RFC3339Nano)
	fields["elapsed_ms"] = time.Since(r.start).Milliseconds()
	if err := r.enc.Encode(fields); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}
func ctxFor(d time.Duration, require bool) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	if require {
		ctx = metadata.AppendToOutgoingContext(ctx, rpctypes.MetadataRequireLeaderKey, rpctypes.MetadataHasLeader)
	}
	return ctx, cancel
}
func invoke[T any](r *recorder, label string, require bool, d time.Duration, f func(context.Context) (T, error)) (T, error) {
	ctx, cancel := ctxFor(d, require)
	defer cancel()
	start := time.Now()
	resp, err := f(ctx)
	r.log(map[string]any{"event": "unary", "label": label, "require_leader": require, "duration_ms": time.Since(start).Milliseconds(), "deadline_ms": d.Milliseconds(), "code": status.Code(err).String(), "error": fmt.Sprint(err), "response": fmt.Sprint(resp)})
	return resp, err
}
func check(err error, message string) error {
	if err != nil {
		return fmt.Errorf("%s: %w", message, err)
	}
	return nil
}
func wantCode(err error, code codes.Code, message string) error {
	if status.Code(err) != code {
		return fmt.Errorf("%s: wanted %s, got %v", message, code, err)
	}
	return nil
}
func wait(d time.Duration, f func() bool) bool {
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		if f() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

type watch struct {
	mu             sync.Mutex
	created, ended bool
	err            error
	keys           map[string]bool
	cancel         context.CancelFunc
}

func startWatch(r *recorder, cc *grpc.ClientConn, name string, require bool) (*watch, error) {
	ctx, cancel := ctxFor(45*time.Second, require)
	w := &watch{cancel: cancel, keys: map[string]bool{}}
	stream, err := pb.NewWatchClient(cc).Watch(ctx)
	if err != nil {
		cancel()
		return nil, err
	}
	if err := stream.Send(&pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{CreateRequest: &pb.WatchCreateRequest{Key: []byte("/leader/"), RangeEnd: []byte("/leader0")}}}); err != nil {
		cancel()
		return nil, err
	}
	r.log(map[string]any{"event": "watch_start", "name": name, "require_leader": require})
	go func() {
		defer cancel()
		for {
			resp, err := stream.Recv()
			w.mu.Lock()
			if err != nil {
				w.ended = true
				w.err = err
			} else {
				if resp.Created {
					w.created = true
				}
				for _, e := range resp.Events {
					w.keys[string(e.Kv.Key)] = true
				}
			}
			w.mu.Unlock()
			r.log(map[string]any{"event": "watch_recv", "name": name, "require_leader": require, "code": status.Code(err).String(), "error": fmt.Sprint(err), "response": fmt.Sprint(resp)})
			if err != nil {
				return
			}
		}
	}()
	return w, nil
}
func (w *watch) state() (bool, bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.created, w.ended, w.err
}
func (w *watch) saw(key string) bool { w.mu.Lock(); defer w.mu.Unlock(); return w.keys[key] }
func (w *watch) expectCreated() error {
	if !wait(2*time.Second, func() bool { c, e, _ := w.state(); return c || e }) {
		return errors.New("watch did not answer create")
	}
	c, e, err := w.state()
	if !c || e {
		return fmt.Errorf("watch create failed: created=%v ended=%v err=%v", c, e, err)
	}
	return nil
}
func (w *watch) expectNoLeader() error {
	if !wait(7*time.Second, func() bool { _, e, _ := w.state(); return e }) {
		return errors.New("require-leader watch remained open")
	}
	_, _, err := w.state()
	return wantCode(err, codes.Unavailable, "require-leader watch termination")
}

type cluster struct {
	commands []*exec.Cmd
	logs     []*os.File
	conns    []*grpc.ClientConn
	rec      *recorder
	stopped  []int
}

func (c *cluster) close() {
	for _, i := range c.stopped {
		_ = c.commands[i].Process.Signal(syscall.SIGCONT)
	}
	for _, cc := range c.conns {
		_ = cc.Close()
	}
	for _, cmd := range c.commands {
		_ = cmd.Process.Kill()
	}
	for _, cmd := range c.commands {
		_ = cmd.Wait()
	}
	for _, f := range c.logs {
		_ = f.Close()
	}
}
func (c *cluster) resume() error {
	for _, i := range c.stopped {
		if err := c.commands[i].Process.Signal(syscall.SIGCONT); err != nil {
			return err
		}
	}
	c.rec.log(map[string]any{"event": "fault_heal", "resumed_nodes": c.stopped})
	c.stopped = nil
	return nil
}
func begin(binary, cert, out, store string) (*cluster, error) {
	c := &cluster{}
	if err := os.MkdirAll(out, 0755); err != nil {
		return nil, err
	}
	f, err := os.Create(filepath.Join(out, "history.jsonl"))
	if err != nil {
		return nil, err
	}
	c.logs = append(c.logs, f)
	c.rec = &recorder{enc: json.NewEncoder(f), start: time.Now()}
	initial := "n1=https://127.0.0.1:12380,n2=https://127.0.0.1:12382,n3=https://127.0.0.1:12384"
	for i := 0; i < 3; i++ {
		client := fmt.Sprintf("http://127.0.0.1:%d", 12379+2*i)
		args := []string{"--name", fmt.Sprintf("n%d", i+1), "--data-dir", filepath.Join(out, fmt.Sprintf("data%d", i+1)), "--initial-cluster", initial, "--initial-cluster-token", "leader-probe", "--listen-client-urls", client, "--advertise-client-urls", client, "--listen-peer-urls", fmt.Sprintf("https://127.0.0.1:%d", 12380+2*i), "--peer-cert-file", filepath.Join(cert, "peer.crt"), "--peer-key-file", filepath.Join(cert, "peer.key"), "--peer-trusted-ca-file", filepath.Join(cert, "ca.crt")}
		if store == "etcd" {
			args = append(args, "--initial-advertise-peer-urls", fmt.Sprintf("https://127.0.0.1:%d", 12380+2*i), "--peer-client-cert-auth", "--log-level", "warn")
		}
		logf, err := os.Create(filepath.Join(out, fmt.Sprintf("node%d.log", i+1)))
		if err != nil {
			c.close()
			return nil, err
		}
		c.logs = append(c.logs, logf)
		cmd := exec.Command(binary, args...)
		cmd.Stdout = logf
		cmd.Stderr = logf
		if err := cmd.Start(); err != nil {
			c.close()
			return nil, err
		}
		c.commands = append(c.commands, cmd)
		c.rec.log(map[string]any{"event": "node_start", "node": i, "pid": cmd.Process.Pid, "args": args})
		cc, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", 12379+2*i), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry())
		if err != nil {
			c.close()
			return nil, err
		}
		c.conns = append(c.conns, cc)
	}
	for i, cc := range c.conns {
		ready := wait(30*time.Second, func() bool {
			ctx, cancel := ctxFor(500*time.Millisecond, false)
			defer cancel()
			_, err := pb.NewKVClient(cc).Range(ctx, &pb.RangeRequest{Key: []byte("/leader/seed")})
			return err == nil
		})
		if !ready {
			c.close()
			return nil, fmt.Errorf("node %d failed readiness", i)
		}
	}
	return c, nil
}
func scenario(binary, cert, out, store, role string) (err error) {
	if !time.Now().Before(deadline) {
		return errors.New("deadline reached")
	}
	c, err := begin(binary, cert, out, store)
	if err != nil {
		return err
	}
	defer c.close()
	r := c.rec
	defer func() { r.log(map[string]any{"event": "verdict", "passed": err == nil, "error": fmt.Sprint(err)}) }()
	// Pick the live endpoint by its observed role, not by a fixed node number.
	ctx, cancel := ctxFor(time.Second, false)
	st, err := pb.NewMaintenanceClient(c.conns[0]).Status(ctx, &pb.StatusRequest{})
	cancel()
	if err != nil {
		return err
	}
	survivor := -1
	for i, cc := range c.conns {
		s, e := invoke(r, "healthy_status", false, time.Second, func(ctx context.Context) (*pb.StatusResponse, error) {
			return pb.NewMaintenanceClient(cc).Status(ctx, &pb.StatusRequest{})
		})
		if e != nil {
			return e
		}
		if (role == "leader" && s.Header.MemberId == st.Leader) || (role == "follower" && s.Header.MemberId != st.Leader && survivor < 0) {
			survivor = i
		}
	}
	if survivor < 0 {
		return errors.New("could not pick survivor")
	}
	cc := c.conns[survivor]
	kv := pb.NewKVClient(cc)
	maint := pb.NewMaintenanceClient(cc)
	_, err = invoke(r, "seed_put", false, time.Second, func(ctx context.Context) (*pb.PutResponse, error) {
		return kv.Put(ctx, &pb.PutRequest{Key: []byte("/leader/seed"), Value: []byte("acked-before-loss")})
	})
	if err != nil {
		return err
	}
	ordinary, err := startWatch(r, cc, "existing_plain", false)
	if err != nil {
		return err
	}
	defer ordinary.cancel()
	required, err := startWatch(r, cc, "existing_required", true)
	if err != nil {
		return err
	}
	defer required.cancel()
	if err = ordinary.expectCreated(); err != nil {
		return err
	}
	if err = required.expectCreated(); err != nil {
		return err
	}
	for i, cmd := range c.commands {
		if i == survivor {
			continue
		}
		if err := cmd.Process.Signal(syscall.SIGSTOP); err != nil {
			return err
		}
		c.stopped = append(c.stopped, i)
		// Linux process state must show T before the fault counts.
		stopped := wait(time.Second, func() bool {
			b, e := os.ReadFile(fmt.Sprintf("/proc/%d/status", cmd.Process.Pid))
			if e != nil {
				return false
			}
			for j := 0; j+8 < len(b); j++ {
				if string(b[j:j+8]) == "State:\tT" {
					return true
				}
			}
			return false
		})
		if !stopped {
			return errors.New("SIGSTOP was not observed")
		}
	}
	r.log(map[string]any{"event": "fault_installed", "survivor": survivor, "survivor_role": role, "stopped_nodes": c.stopped, "proc_state": "T"})
	if !wait(10*time.Second, func() bool {
		s, e := invoke(r, "loss_status", false, time.Second, func(ctx context.Context) (*pb.StatusResponse, error) { return maint.Status(ctx, &pb.StatusRequest{}) })
		return e == nil && s.Leader == 0
	}) {
		return errors.New("survivor never became leaderless")
	}
	r.log(map[string]any{"event": "leaderless_confirmed"})
	freshPlain, err := startWatch(r, cc, "leaderless_plain", false)
	if err != nil {
		return err
	}
	defer freshPlain.cancel()
	freshRequired, err := startWatch(r, cc, "leaderless_required", true)
	if err != nil {
		return err
	}
	defer freshRequired.cancel()
	if err = freshPlain.expectCreated(); err != nil {
		return err
	}
	if err = freshRequired.expectNoLeader(); err != nil {
		return err
	}
	for _, require := range []bool{false, true} {
		serial, e := invoke(r, "leaderless_serializable_range", require, time.Second, func(ctx context.Context) (*pb.RangeResponse, error) {
			return kv.Range(ctx, &pb.RangeRequest{Key: []byte("/leader/seed"), Serializable: true})
		})
		if require {
			if err = wantCode(e, codes.Unavailable, "required serializable range"); err != nil {
				return err
			}
		} else if e != nil || len(serial.Kvs) != 1 || string(serial.Kvs[0].Value) != "acked-before-loss" {
			return fmt.Errorf("local seed range lost acked value: %v %v", serial, e)
		}
		_, e = invoke(r, "leaderless_linearizable_range", require, time.Second, func(ctx context.Context) (*pb.RangeResponse, error) {
			return kv.Range(ctx, &pb.RangeRequest{Key: []byte("/leader/seed")})
		})
		if e == nil {
			return errors.New("linearizable range succeeded with no quorum")
		}
		if require {
			if err = wantCode(e, codes.Unavailable, "required linearizable range"); err != nil {
				return err
			}
		}
		key := fmt.Sprintf("/leader/put-%v", require)
		_, e = invoke(r, "leaderless_put", require, time.Second, func(ctx context.Context) (*pb.PutResponse, error) {
			return kv.Put(ctx, &pb.PutRequest{Key: []byte(key), Value: []byte("candidate")})
		})
		if e == nil {
			return errors.New("put succeeded without quorum")
		}
		if require {
			if err = wantCode(e, codes.Unavailable, "required put"); err != nil {
				return err
			}
		}
		key = fmt.Sprintf("/leader/txn-%v", require)
		_, e = invoke(r, "leaderless_txn", require, time.Second, func(ctx context.Context) (*pb.TxnResponse, error) {
			return kv.Txn(ctx, &pb.TxnRequest{Compare: []*pb.Compare{{Key: []byte(key), Target: pb.Compare_VERSION, Result: pb.Compare_EQUAL, TargetUnion: &pb.Compare_Version{Version: 0}}}, Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte(key), Value: []byte("candidate")}}}}})
		})
		if e == nil {
			return errors.New("txn succeeded without quorum")
		}
		if require {
			if err = wantCode(e, codes.Unavailable, "required txn"); err != nil {
				return err
			}
		}
		_, e = invoke(r, "leaderless_memberlist", require, time.Second, func(ctx context.Context) (*pb.MemberListResponse, error) {
			return pb.NewClusterClient(cc).MemberList(ctx, &pb.MemberListRequest{})
		})
		if require {
			if err = wantCode(e, codes.Unavailable, "required memberlist"); err != nil {
				return err
			}
		} else if e != nil {
			return e
		}
		_, e = invoke(r, "leaderless_status", require, time.Second, func(ctx context.Context) (*pb.StatusResponse, error) { return maint.Status(ctx, &pb.StatusRequest{}) })
		if require {
			if err = wantCode(e, codes.Unavailable, "required status"); err != nil {
				return err
			}
		} else if e != nil {
			return e
		}
	}
	if err = required.expectNoLeader(); err != nil {
		return err
	}
	for _, w := range []*watch{ordinary, freshPlain} {
		if _, ended, e := w.state(); ended {
			return fmt.Errorf("plain watch ended during loss: %v", e)
		}
	}
	// Submit while leaderless, then heal before a 5 s client deadline. Immediate rejection is legal.
	finished := make(chan error, 1)
	go func() {
		_, e := invoke(r, "put_across_heal", false, 5*time.Second, func(ctx context.Context) (*pb.PutResponse, error) {
			return kv.Put(ctx, &pb.PutRequest{Key: []byte("/leader/across-heal"), Value: []byte("candidate")})
		})
		finished <- e
	}()
	time.Sleep(300 * time.Millisecond)
	if err = c.resume(); err != nil {
		return err
	}
	if !wait(15*time.Second, func() bool {
		_, e := invoke(r, "heal_linearizable_range", false, time.Second, func(ctx context.Context) (*pb.RangeResponse, error) {
			return kv.Range(ctx, &pb.RangeRequest{Key: []byte("/leader/seed")})
		})
		return e == nil
	}) {
		return errors.New("cluster failed to regain quorum")
	}
	acrossErr := <-finished
	// A received success must be durable and visible. Any error remains an unknown outcome.
	if acrossErr == nil {
		v, e := invoke(r, "acked_across_heal_read", false, time.Second, func(ctx context.Context) (*pb.RangeResponse, error) {
			return kv.Range(ctx, &pb.RangeRequest{Key: []byte("/leader/across-heal")})
		})
		if e != nil || len(v.Kvs) != 1 || string(v.Kvs[0].Value) != "candidate" {
			return fmt.Errorf("acked across-heal write missing: %v %v", v, e)
		}
	}
	recovered, err := invoke(r, "healed_contents", false, time.Second, func(ctx context.Context) (*pb.RangeResponse, error) {
		return kv.Range(ctx, &pb.RangeRequest{Key: []byte("/leader/"), RangeEnd: []byte("/leader0")})
	})
	if err != nil {
		return err
	}
	seedOK := false
	for _, v := range recovered.Kvs {
		if string(v.Key) == "/leader/seed" && string(v.Value) == "acked-before-loss" {
			seedOK = true
		}
		if string(v.Key) == "/leader/put-true" || string(v.Key) == "/leader/txn-true" {
			return errors.New("require-leader-rejected write applied")
		}
	}
	if !seedOK {
		return errors.New("acked seed missing after heal")
	}
	_, err = invoke(r, "sentinel_put", false, time.Second, func(ctx context.Context) (*pb.PutResponse, error) {
		return kv.Put(ctx, &pb.PutRequest{Key: []byte("/leader/sentinel"), Value: []byte("after-heal")})
	})
	if err != nil {
		return err
	}
	for _, w := range []*watch{ordinary, freshPlain} {
		if !wait(3*time.Second, func() bool { return w.saw("/leader/sentinel") }) {
			return errors.New("plain watch missed post-heal sentinel")
		}
	}
	afterRequired, err := startWatch(r, cc, "healed_required", true)
	if err != nil {
		return err
	}
	defer afterRequired.cancel()
	if err = afterRequired.expectCreated(); err != nil {
		return err
	}
	return nil
}
func main() {
	binary := flag.String("bin", "", "server executable")
	cert := flag.String("cert", "", "peer cert directory")
	out := flag.String("out", "", "evidence directory")
	store := flag.String("store", "", "store name")
	flag.Parse()
	if os.Getenv("GITHUB_ACTIONS") != "true" {
		fmt.Fprintln(os.Stderr, "Actions only")
		os.Exit(2)
	}
	failed := false
	for _, role := range []string{"leader", "follower"} {
		if err := scenario(*binary, *cert, filepath.Join(*out, role), *store, role); err != nil {
			fmt.Fprintln(os.Stderr, *store, role, err)
			failed = true
		}
	}
	if failed {
		os.Exit(1)
	}
}
