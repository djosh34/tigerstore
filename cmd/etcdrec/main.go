// Command etcdrec records the etcd calls of a real kube-apiserver. It
// stands between the apiserver and a three-node store, one listener for
// each node, and passes every gRPC message through unchanged. It logs a
// copy of each message, decoded, with keys, revisions, lease IDs and
// sizes, and no value bytes (internal/k8strace.Record), as gzipped JSON
// lines.
//
//	etcdrec -listen <host:port>,... -backend <host:port>,... -cacert <file>
//	        -cert <file> -key <file> -client-cert <file> -client-key <file>
//	        -out <file.jsonl.gz>
//
// The i-th listener forwards to the i-th backend. Clients must present a
// certificate from -cacert, as they must to the store. etcdrec ends on
// SIGINT or SIGTERM and closes the log.
package main

import (
	"compress/gzip"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"

	"github.com/djosh34/tigerstore/internal/k8strace"
)

// rawCodec passes messages as their bytes, so that the proxy sends what it
// received.
type rawCodec struct{}

// errNotBytes is a message that is not the *[]byte rawCodec passes.
var errNotBytes = errors.New("rawCodec needs *[]byte")

func (rawCodec) Marshal(v any) ([]byte, error) {
	b, ok := v.(*[]byte)
	if !ok {
		return nil, errNotBytes
	}
	return *b, nil
}

func (rawCodec) Unmarshal(data []byte, v any) error {
	b, ok := v.(*[]byte)
	if !ok {
		return errNotBytes
	}
	*b = append((*b)[:0], data...)
	return nil
}

func (rawCodec) Name() string { return "proto" }

// maxMessage is the largest message passed, as large as the store takes.
const maxMessage = 2 << 30

func main() {
	listen := flag.String("listen", "", "comma-separated host:port to listen on, one per node")
	backend := flag.String("backend", "", "comma-separated host:port of the nodes, in the order of -listen")
	cacert := flag.String("cacert", "", "CA of the clients and the nodes")
	cert := flag.String("cert", "", "server certificate shown to clients")
	key := flag.String("key", "", "key of -cert")
	clientCert := flag.String("client-cert", "", "client certificate shown to the nodes")
	clientKey := flag.String("client-key", "", "key of -client-cert")
	out := flag.String("out", "", "the log, gzipped JSON lines")
	flag.Parse()
	if err := run(strings.Split(*listen, ","), strings.Split(*backend, ","), *cacert, *cert, *key, *clientCert, *clientKey, *out); err != nil {
		fmt.Fprintln(os.Stderr, "etcdrec:", err)
		os.Exit(1)
	}
}

func run(listen, backend []string, cacert, cert, key, clientCert, clientKey, out string) error {
	if len(listen) != len(backend) || out == "" {
		return errors.New("need as many -listen as -backend addresses, and -out")
	}
	pem, err := os.ReadFile(cacert)
	if err != nil {
		return err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return fmt.Errorf("%s: no certificates", cacert)
	}
	serverKP, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		return err
	}
	clientKP, err := tls.LoadX509KeyPair(clientCert, clientKey)
	if err != nil {
		return err
	}
	f, err := os.Create(out)
	if err != nil {
		return err
	}
	l := newJournal(f)
	serverTLS := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{serverKP}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert}
	clientTLS := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool, Certificates: []tls.Certificate{clientKP}}
	var servers []*grpc.Server
	served := make(chan error, len(listen))
	for i := range listen {
		cc, err := grpc.NewClient(backend[i], grpc.WithTransportCredentials(credentials.NewTLS(clientTLS)),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMessage), grpc.MaxCallSendMsgSize(maxMessage)))
		if err != nil {
			return err
		}
		defer cc.Close()
		ln, err := net.Listen("tcp", listen[i])
		if err != nil {
			return err
		}
		p := &proxy{log: l, node: i + 1, backend: cc}
		s := grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTLS)), grpc.ForceServerCodec(rawCodec{}),
			grpc.UnknownServiceHandler(p.call), grpc.StatsHandler(p),
			grpc.MaxRecvMsgSize(maxMessage), grpc.MaxSendMsgSize(maxMessage))
		servers = append(servers, s)
		go func() { served <- s.Serve(ln) }()
		fmt.Printf("etcdrec: node %d: %s -> %s\n", i+1, listen[i], backend[i])
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	var serveErr error
	select {
	case <-signals:
	case serveErr = <-served:
	}
	for _, s := range servers {
		s.Stop()
	}
	return errors.Join(serveErr, l.close())
}

// journal writes the records, one at a time, in the order of their times.
type journal struct {
	mu   sync.Mutex
	f    *os.File
	gz   *gzip.Writer
	enc  *json.Encoder
	err  error
	call atomic.Int64
	conn atomic.Int64
}

func newJournal(f *os.File) *journal {
	gz, _ := gzip.NewWriterLevel(f, gzip.BestSpeed) // a valid level never fails
	return &journal{f: f, gz: gz, enc: json.NewEncoder(gz)}
}

// write stamps r with the time and logs it. The first error stops the log
// and is returned by close; the calls go on.
func (l *journal) write(r k8strace.Record) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err != nil {
		return
	}
	r.T = time.Now().UnixNano()
	l.err = l.enc.Encode(&r)
}

func (l *journal) close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return errors.Join(l.err, l.gz.Close(), l.f.Close())
}

// proxy forwards the calls of one listener to its node.
type proxy struct {
	log     *journal
	node    int
	backend *grpc.ClientConn
}

type connKey struct{}

// TagConn numbers each client connection.
func (p *proxy) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return context.WithValue(ctx, connKey{}, p.log.conn.Add(1))
}

func (p *proxy) HandleConn(context.Context, stats.ConnStats)                     {}
func (p *proxy) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context { return ctx }
func (p *proxy) HandleRPC(context.Context, stats.RPCStats)                       {}

// call forwards one gRPC call, unary or streaming, both ways at once, and
// logs each message.
func (p *proxy) call(_ any, ss grpc.ServerStream) (callErr error) {
	method, _ := grpc.MethodFromServerStream(ss)
	id := p.log.call.Add(1)
	conn, _ := ss.Context().Value(connKey{}).(int64)
	p.log.write(k8strace.Record{Event: k8strace.EventOpen, Call: id, Conn: conn, Node: p.node, Method: method})
	defer func() {
		r := k8strace.Record{Event: k8strace.EventEnd, Call: id}
		if callErr != nil {
			r.Err = callErr.Error()
		}
		p.log.write(r)
	}()
	ctx, cancel := context.WithCancel(ss.Context())
	defer cancel()
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		ctx = metadata.NewOutgoingContext(ctx, md.Copy())
	}
	cs, err := p.backend.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true, ClientStreams: true}, method, grpc.ForceCodec(rawCodec{}))
	if err != nil {
		return err
	}
	sent := make(chan error, 1)
	go func() {
		for {
			var b []byte
			err := ss.RecvMsg(&b)
			if errors.Is(err, io.EOF) {
				sent <- cs.CloseSend()
				return
			}
			if err != nil {
				cancel()
				sent <- err
				return
			}
			r := k8strace.Record{Event: k8strace.EventReq, Call: id, Bytes: len(b)}
			r.Req, err = decodeRequest(method, b)
			if err != nil {
				r.Err = err.Error()
			}
			p.log.write(r)
			if err := cs.SendMsg(&b); err != nil {
				sent <- err
				return
			}
		}
	}()
	header, err := cs.Header()
	if err != nil {
		return err
	}
	if err := ss.SendHeader(header); err != nil {
		return err
	}
	for {
		var b []byte
		err := cs.RecvMsg(&b)
		if errors.Is(err, io.EOF) {
			ss.SetTrailer(cs.Trailer())
			return nil
		}
		if err != nil {
			ss.SetTrailer(cs.Trailer())
			return err
		}
		r := k8strace.Record{Event: k8strace.EventResp, Call: id, Bytes: len(b)}
		r.Resp, err = decodeResponse(method, b)
		if err != nil {
			r.Err = err.Error()
		}
		p.log.write(r)
		if err := ss.SendMsg(&b); err != nil {
			return err
		}
	}
}
