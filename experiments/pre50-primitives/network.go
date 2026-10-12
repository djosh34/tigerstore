package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"
)

func writeFull(w io.Writer, p []byte) error {
	n, err := w.Write(p)
	if err != nil {
		return err
	}
	if n != len(p) {
		return io.ErrShortWrite
	}
	return nil
}
func receiveFrame(r io.Reader) ([]byte, error) {
	var h [4]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(h[:])
	if n > 2*1024*1024 {
		return nil, errors.New("oversize frame")
	}
	b := make([]byte, n)
	_, err := io.ReadFull(r, b)
	return b, err
}
func sendFrame(w io.Writer, b []byte) error {
	var h [4]byte
	binary.BigEndian.PutUint32(h[:], uint32(len(b)))
	if err := writeFull(w, h[:]); err != nil {
		return err
	}
	return writeFull(w, b)
}

type logStore struct {
	file   *os.File
	memory []byte
	offset int
}

func newLog(dir, name, mode string) (*logStore, error) {
	if mode == "nondurable" {
		return &logStore{memory: make([]byte, 16*1024*1024)}, nil
	}
	if mode != "durable" {
		return nil, errors.New("invalid mode")
	}
	f, err := os.Create(filepath.Join(dir, name+".wal"))
	if err != nil {
		return nil, err
	}
	if err = f.Sync(); err != nil {
		return nil, err
	}
	d, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	if err = d.Sync(); err != nil {
		return nil, err
	}
	if err = d.Close(); err != nil {
		return nil, err
	}
	return &logStore{file: f}, nil
}
func (l *logStore) append(b []byte) error {
	if l.file != nil {
		if err := sendFrame(l.file, b); err != nil {
			return err
		}
		return l.file.Sync()
	}
	if l.offset+len(b)+4 > len(l.memory) {
		l.offset = 0
	}
	binary.BigEndian.PutUint32(l.memory[l.offset:], uint32(len(b)))
	l.offset += 4
	copy(l.memory[l.offset:], b)
	l.offset += len(b)
	return nil
}
func (l *logStore) close() error {
	if l.file != nil {
		return l.file.Close()
	}
	return nil
}
func follower(args []string) error {
	if len(args) != 6 {
		return errors.New("follower certdir datadir address mode rep name")
	}
	certdir, dir, address, mode, rep, name := args[0], args[1], args[2], args[3], args[4], args[5]
	_, sc, err := tlsConfigs(certdir)
	if err != nil {
		return err
	}
	store, err := newLog(dir, name, mode)
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(dir, name+".ready"), []byte(ln.Addr().String()), 0600); err != nil {
		return err
	}
	raw, err := ln.Accept()
	if err != nil {
		return err
	}
	c := tls.Server(raw, sc)
	if err = c.Handshake(); err != nil {
		return err
	}
	if err = emit(map[string]any{"kind": "TLS-network", "role": name, "version": c.ConnectionState().Version, "cipher": tls.CipherSuiteName(c.ConnectionState().CipherSuite)}); err != nil {
		return err
	}
	// Warmup uses the same append semantics, but is outside CPU accounting.
	for i := 0; i < 20; i++ {
		b, e := receiveFrame(c)
		if e != nil {
			return e
		}
		if e = store.append(b); e != nil {
			return e
		}
		if e = writeFull(c, []byte{1}); e != nil {
			return e
		}
	}
	var count [4]byte
	if _, err = io.ReadFull(c, count[:]); err != nil {
		return err
	}
	n := int(binary.BigEndian.Uint32(count[:]))
	if err = writeFull(c, []byte{1}); err != nil {
		return err
	}
	start := time.Now()
	cpu := cpuNS()
	for i := 0; i < n; i++ {
		b, e := receiveFrame(c)
		if e != nil {
			return e
		}
		var entry raftpb.Entry
		if e = proto.Unmarshal(b, &entry); e != nil {
			return e
		}
		if len(entry.Data) != 1024 || entry.GetIndex() != uint64(i+21) {
			return errors.New("bad raft entry")
		}
		if e = store.append(b); e != nil {
			return e
		}
		if e = writeFull(c, []byte{1}); e != nil {
			return e
		}
	}
	used := cpuNS() - cpu
	wall := time.Since(start).Nanoseconds()
	a, err := affinity()
	if err != nil {
		return err
	}
	result := map[string]any{"kind": "quorum-process", "role": name, "mode": mode, "rep": rep, "n": n, "pid": os.Getpid(), "cpu_ns": used, "wall_ns": wall, "cpu_ns_per_op": float64(used) / float64(n), "affinity": a}
	if err = emit(result); err != nil {
		return err
	}
	b, err := json.Marshal(result)
	if err != nil {
		return err
	}
	if err = sendFrame(c, b); err != nil {
		return err
	}
	if err = c.Close(); err != nil {
		return err
	}
	if err = ln.Close(); err != nil {
		return err
	}
	return store.close()
}
func quorum(args []string) error {
	if len(args) != 6 {
		return errors.New("quorum certdir datadir address1 address2 mode rep")
	}
	certdir, dir, mode, rep := args[0], args[1], args[4], args[5]
	cc, _, err := tlsConfigs(certdir)
	if err != nil {
		return err
	}
	store, err := newLog(dir, "leader", mode)
	if err != nil {
		return err
	}
	conns := make([]*tls.Conn, 2)
	for i := range conns {
		c, e := tls.Dial("tcp", args[2+i], cc)
		if e != nil {
			return e
		}
		conns[i] = c
		if e = emit(map[string]any{"kind": "TLS-network", "role": "leader", "peer": i, "version": c.ConnectionState().Version, "cipher": tls.CipherSuiteName(c.ConnectionState().CipherSuite)}); e != nil {
			return e
		}
	}
	entry := &raftpb.Entry{Term: proto.Uint64(1), Index: proto.Uint64(1), Type: raftpb.EntryNormal.Enum(), Data: bytes.Repeat([]byte{0x61}, 1024)}
	n := 10000
	if mode == "durable" {
		n = 3000
	}
	for i := 0; i < 20; i++ {
		*entry.Index = uint64(i + 1)
		b, e := proto.Marshal(entry)
		if e != nil {
			return e
		}
		for _, c := range conns {
			if e = sendFrame(c, b); e != nil {
				return e
			}
			var ack [1]byte
			if _, e = io.ReadFull(c, ack[:]); e != nil {
				return e
			}
		}
		if e = store.append(b); e != nil {
			return e
		}
	}
	for _, c := range conns {
		var count [4]byte
		binary.BigEndian.PutUint32(count[:], uint32(n))
		if err = writeFull(c, count[:]); err != nil {
			return err
		}
		var ack [1]byte
		if _, err = io.ReadFull(c, ack[:]); err != nil {
			return err
		}
	}
	// Persistent sender goroutines, one per follower. The leader persists locally
	// while both sends and follower appends are in flight.
	type outcome struct{ err error }
	jobs := []chan []byte{make(chan []byte), make(chan []byte)}
	done := make(chan outcome, 2)
	var wg sync.WaitGroup
	for j, c := range conns {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for b := range jobs[j] {
				e := sendFrame(c, b)
				if e == nil {
					var ack [1]byte
					_, e = io.ReadFull(c, ack[:])
					if e == nil && ack[0] != 1 {
						e = errors.New("bad ack")
					}
				}
				done <- outcome{e}
			}
		}()
	}
	samples := make([]int64, 0, n)
	allSamples := make([]int64, 0, n)
	start := time.Now()
	cpu := cpuNS()
	for i := 0; i < n; i++ {
		t := time.Now()
		*entry.Index = uint64(i + 21)
		b, e := proto.Marshal(entry)
		if e != nil {
			return e
		}
		jobs[0] <- b
		jobs[1] <- b
		if e = store.append(b); e != nil {
			return e
		}
		first := <-done
		if first.err != nil {
			return first.err
		}
		// A quorum means local completion and either follower completion. Still
		// drain the second follower before proposing the next entry.
		samples = append(samples, time.Since(t).Nanoseconds())
		second := <-done
		if second.err != nil {
			return second.err
		}
		allSamples = append(allSamples, time.Since(t).Nanoseconds())
	}
	used := cpuNS() - cpu
	wall := time.Since(start).Nanoseconds()
	for _, j := range jobs {
		close(j)
	}
	wg.Wait()
	if err = emit(map[string]any{"kind": "quorum-process", "role": "leader", "mode": mode, "rep": rep, "n": n, "pid": os.Getpid(), "cpu_ns": used, "wall_ns": wall, "cpu_ns_per_op": float64(used) / float64(n), "elapsed_ns_per_op": float64(wall) / float64(n), "quorum_quantiles": stats(samples), "all_followers_quantiles": stats(allSamples), "ops_per_second": float64(n) * 1e9 / float64(wall)}); err != nil {
		return err
	}
	for i, c := range conns {
		b, e := receiveFrame(c)
		if e != nil {
			return e
		}
		var result map[string]any
		if e = json.Unmarshal(b, &result); e != nil {
			return e
		}
		if e = emit(map[string]any{"kind": "follower-result-received", "peer": i, "result": result}); e != nil {
			return e
		}
		if e = c.Close(); e != nil {
			return e
		}
	}
	return store.close()
}

type rpcService struct {
	pb.UnimplementedKVServer
	count       atomic.Int64
	startCPU    int64
	start       time.Time
	resultPath  string
	rep         string
	concurrency string
}

func (s *rpcService) Put(ctx context.Context, r *pb.PutRequest) (*pb.PutResponse, error) {
	switch string(r.Key) {
	case "@start":
		s.count.Store(0)
		s.start = time.Now()
		s.startCPU = cpuNS()
	case "@stop":
		cpu := cpuNS() - s.startCPU
		wall := time.Since(s.start).Nanoseconds()
		n := s.count.Load()
		a, e := affinity()
		if e != nil {
			return nil, e
		}
		v := map[string]any{"kind": "rpc-process", "role": "server", "rep": s.rep, "concurrency": s.concurrency, "n": n, "cpu_ns": cpu, "wall_ns": wall, "cpu_ns_per_op": float64(cpu) / float64(n), "affinity": a, "pid": os.Getpid()}
		b, e := json.Marshal(v)
		if e != nil {
			return nil, e
		}
		if e = os.WriteFile(s.resultPath, b, 0600); e != nil {
			return nil, e
		}
		if e = emit(v); e != nil {
			return nil, e
		}
	default:
		if len(r.Value) != 1024 {
			return nil, errors.New("bad value size")
		}
		s.count.Add(1)
	}
	return &pb.PutResponse{Header: &pb.ResponseHeader{ClusterId: 1, MemberId: 2, Revision: 12345, RaftTerm: 3}}, nil
}
func rpcServer(args []string) error {
	if len(args) != 6 {
		return errors.New("rpc-server certdir datadir address rep concurrency name")
	}
	_, sc, err := tlsConfigs(args[0])
	if err != nil {
		return err
	}
	sc.NextProtos = []string{"h2"}
	ln, err := net.Listen("tcp", args[2])
	if err != nil {
		return err
	}
	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(sc)))
	s := &rpcService{resultPath: filepath.Join(args[1], args[5]+".result.json"), rep: args[3], concurrency: args[4]}
	pb.RegisterKVServer(srv, s)
	if err = os.WriteFile(filepath.Join(args[1], args[5]+".ready"), []byte(ln.Addr().String()), 0600); err != nil {
		return err
	}
	return srv.Serve(ln)
}
func rpcClient(args []string) error {
	if len(args) != 4 {
		return errors.New("rpc-client certdir address rep concurrency")
	}
	cc, _, err := tlsConfigs(args[0])
	if err != nil {
		return err
	}
	cc.NextProtos = []string{"h2"}
	conn, err := grpc.NewClient(args[1], grpc.WithTransportCredentials(credentials.NewTLS(cc)))
	if err != nil {
		return err
	}
	client := pb.NewKVClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	request := &pb.PutRequest{Key: []byte("/registry/pods/ns/pre50-00000001"), Value: bytes.Repeat([]byte{0x61}, 1024)}
	for i := 0; i < 500; i++ {
		if _, err = client.Put(ctx, request); err != nil {
			return err
		}
	}
	if _, err = client.Put(ctx, &pb.PutRequest{Key: []byte("@start")}); err != nil {
		return err
	}
	concurrency, err := strconv.Atoi(args[3])
	if err != nil {
		return err
	}
	var n atomic.Int64
	var firstErr error
	var mu sync.Mutex
	var wg sync.WaitGroup
	// Latency is closed-loop, one outstanding RPC per worker, one connection.
	samples := make([][]int64, concurrency)
	cpu := cpuNS()
	start := time.Now()
	until := start.Add(2 * time.Second)
	for j := 0; j < concurrency; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(until) {
				t := time.Now()
				r, e := client.Put(ctx, request)
				if e != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = e
					}
					mu.Unlock()
					return
				}
				if r.Header == nil || r.Header.Revision != 12345 {
					mu.Lock()
					firstErr = errors.New("bad reply")
					mu.Unlock()
					return
				}
				samples[j] = append(samples[j], time.Since(t).Nanoseconds())
				n.Add(1)
			}
		}()
	}
	wg.Wait()
	used := cpuNS() - cpu
	wall := time.Since(start).Nanoseconds()
	if firstErr != nil {
		return firstErr
	}
	if _, err = client.Put(ctx, &pb.PutRequest{Key: []byte("@stop")}); err != nil {
		return err
	}
	all := []int64{}
	for _, s := range samples {
		all = append(all, s...)
	}
	if err = emit(map[string]any{"kind": "rpc-process", "role": "client", "rep": args[2], "concurrency": args[3], "n": n.Load(), "cpu_ns": used, "wall_ns": wall, "cpu_ns_per_op": float64(used) / float64(n.Load()), "ops_per_second": float64(n.Load()) * 1e9 / float64(wall), "latency": stats(all)}); err != nil {
		return err
	}
	return conn.Close()
}
