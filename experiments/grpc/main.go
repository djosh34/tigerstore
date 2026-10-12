package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"syscall"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"golang.org/x/net/http2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

var generator = flag.String("generator", "grpc", "grpc or lean load generator")
var mode = flag.String("mode", "server", "cert, server, load or smoke")
var kind = flag.String("kind", "grpc-default", "grpc-default, grpc-shared, grpc-workers, grpc-tuned, handler or raw")
var addr = flag.String("addr", "127.0.0.1:22379", "TLS listen address")
var control = flag.String("control", "127.0.0.1:22380", "local benchmark control")
var certDir = flag.String("cert-dir", "certs", "certificate directory")
var workers = flag.Uint("workers", 64, "NumStreamWorkers")
var profile = flag.String("profile", "", "server CPU profile")
var connections = flag.Int("connections", 2, "load connections")
var streams = flag.Int("streams", 128, "outstanding calls per connection")
var duration = flag.Duration("duration", 5*time.Second, "load measurement duration")
var warmup = flag.Duration("warmup", time.Second, "warmup")

func main() {
	flag.Parse()
	var e error
	switch *mode {
	case "cert":
		e = makeCert()
	case "server":
		e = server()
	case "load":
		e = load()
	case "smoke":
		e = smoke()
	default:
		e = errors.New("unknown mode")
	}
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func makeCert() error {
	if e := os.MkdirAll(*certDir, 0700); e != nil {
		return e
	}
	key, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if e != nil {
		return e
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "throwaway localhost"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(48 * time.Hour), DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	der, e := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if e != nil {
		return e
	}
	kb, e := x509.MarshalPKCS8PrivateKey(key)
	if e != nil {
		return e
	}
	if e := os.WriteFile(filepath.Join(*certDir, "cert.pem"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); e != nil {
		return e
	}
	return os.WriteFile(filepath.Join(*certDir, "key.pem"), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: kb}), 0600)
}
func tlsConfig(server bool) (*tls.Config, error) {
	cert, e := tls.LoadX509KeyPair(filepath.Join(*certDir, "cert.pem"), filepath.Join(*certDir, "key.pem"))
	if e != nil {
		return nil, e
	}
	ca, e := os.ReadFile(filepath.Join(*certDir, "cert.pem"))
	if e != nil {
		return nil, e
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, errors.New("invalid cert")
	}
	c := &tls.Config{MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}, RootCAs: pool, ClientCAs: pool, ServerName: "localhost", NextProtos: []string{"h2"}}
	if server {
		c.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return c, nil
}
func cpuSeconds() float64 {
	var r syscall.Rusage
	if e := syscall.Getrusage(syscall.RUSAGE_SELF, &r); e != nil {
		return -1
	}
	return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
}
func server() error {
	cfg, e := tlsConfig(true)
	if e != nil {
		return e
	}
	if *profile != "" {
		f, e := os.Create(*profile)
		if e != nil {
			return e
		}
		defer f.Close()
		if e := pprof.StartCPUProfile(f); e != nil {
			return e
		}
		defer pprof.StopCPUProfile()
	}
	b := newBackend()
	mux := http.NewServeMux()
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		if e := json.NewEncoder(w).Encode(map[string]any{"cpu_seconds": cpuSeconds(), "gomaxprocs": runtime.GOMAXPROCS(0), "pid": os.Getpid()}); e != nil {
			fmt.Fprintln(os.Stderr, e)
		}
	})
	mux.HandleFunc("/stop", func(w http.ResponseWriter, r *http.Request) {
		go func() {
			time.Sleep(100 * time.Millisecond)
			if *profile != "" {
				pprof.StopCPUProfile()
			}
			os.Exit(0)
		}()
		w.WriteHeader(200)
	})
	go func() {
		if e := http.ListenAndServe(*control, mux); e != nil {
			fmt.Fprintln(os.Stderr, e)
		}
	}()
	l, e := net.Listen("tcp", *addr)
	if e != nil {
		return e
	}
	defer l.Close()
	if *kind == "grpc-default" || *kind == "grpc-unshared" || *kind == "grpc-shared" || *kind == "grpc-workers" || *kind == "grpc-tuned" {
		opts := []grpc.ServerOption{grpc.Creds(credentials.NewTLS(cfg))}
		if *kind == "grpc-unshared" {
			opts = append(opts, grpc.SharedWriteBuffer(false))
		}
		if *kind == "grpc-shared" || *kind == "grpc-tuned" {
			opts = append(opts, grpc.SharedWriteBuffer(true))
		}
		if *kind == "grpc-workers" || *kind == "grpc-tuned" {
			opts = append(opts, grpc.NumStreamWorkers(uint32(*workers)))
		}
		if *kind == "grpc-tuned" {
			opts = append(opts, grpc.MaxConcurrentStreams(1024), grpc.InitialWindowSize(1<<20), grpc.InitialConnWindowSize(16<<20), grpc.WriteBufferSize(64<<10), grpc.ReadBufferSize(64<<10))
		}
		s := grpc.NewServer(opts...)
		pb.RegisterKVServer(s, b)
		pb.RegisterWatchServer(s, b)
		return s.Serve(l)
	}
	tl := tls.NewListener(l, cfg)
	for {
		conn, e := tl.Accept()
		if e != nil {
			return e
		}
		go func() {
			tc := conn.(*tls.Conn)
			if e := tc.Handshake(); e != nil {
				fmt.Fprintln(os.Stderr, e)
				conn.Close()
				return
			}
			switch *kind {
			case "raw":
				c := &rawConn{Conn: conn, b: b}
				_ = c.serve()
			case "handler":
				s := &http2.Server{MaxConcurrentStreams: 1024, MaxUploadBufferPerConnection: 16 << 20, MaxUploadBufferPerStream: 1 << 20}
				s.ServeConn(conn, &http2.ServeConnOpts{Handler: http.HandlerFunc(b.httpHandler)})
			default:
				fmt.Fprintln(os.Stderr, "unknown server kind")
				conn.Close()
			}
		}()
	}
}
func readMessage(r io.Reader) ([]byte, error) {
	h := make([]byte, 5)
	if _, e := io.ReadFull(r, h); e != nil {
		return nil, e
	}
	if h[0] != 0 {
		return nil, errors.New("compression unsupported")
	}
	n := binary.BigEndian.Uint32(h[1:])
	if n > maxMessage {
		return nil, errors.New("message too large")
	}
	p := make([]byte, n)
	_, e := io.ReadFull(r, p)
	return p, e
}
func writeMessage(w io.Writer, p []byte) error {
	h := make([]byte, 5)
	binary.BigEndian.PutUint32(h[1:], uint32(len(p)))
	if _, e := w.Write(h); e != nil {
		return e
	}
	_, e := w.Write(p)
	return e
}
func (b *backend) httpHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/grpc")
	w.Header().Set("Trailer", "Grpc-Status")
	w.WriteHeader(200)
	if r.URL.Path == "/etcdserverpb.Watch/Watch" {
		p, e := readMessage(r.Body)
		if e != nil {
			w.Header().Set("Grpc-Status", "3")
			return
		}
		req := new(pb.WatchRequest)
		if e := proto.Unmarshal(p, req); e != nil || req.GetCreateRequest() == nil {
			w.Header().Set("Grpc-Status", "3")
			return
		}
		ch := make(chan *pb.WatchResponse, 256)
		s := b.subscribe(req.GetCreateRequest(), 1, func(v *pb.WatchResponse) {
			select {
			case ch <- v:
			case <-r.Context().Done():
			}
		})
		defer b.unsubscribe(s)
		for {
			select {
			case v := <-ch:
				p, e := proto.Marshal(v)
				if e != nil {
					return
				}
				if e := writeMessage(w, p); e != nil {
					return
				}
				if e := http.NewResponseController(w).Flush(); e != nil {
					return
				}
			case <-r.Context().Done():
				return
			}
		}
	}
	p, e := readMessage(r.Body)
	if e != nil {
		w.Header().Set("Grpc-Status", "3")
		return
	}
	p, e = b.unary(r.URL.Path, p)
	if e != nil {
		w.Header().Set("Grpc-Status", strconv.Itoa(int(status.Code(e))))
		return
	}
	if e := writeMessage(w, p); e != nil {
		return
	}
	w.Header().Set("Grpc-Status", "0")
}
func dial() (*grpc.ClientConn, error) {
	c, e := tlsConfig(false)
	if e != nil {
		return nil, e
	}
	return grpc.NewClient(*addr, grpc.WithTransportCredentials(credentials.NewTLS(c)))
}
