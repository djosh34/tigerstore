// Research-only transparent gRPC byte proxy. Only decoded shapes, never values,
// are written to disk. Wire payloads are forwarded without reserialization.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

type rawCodec struct{}

func (rawCodec) Name() string                  { return "proto" }
func (rawCodec) Marshal(v any) ([]byte, error) { return *v.(*[]byte), nil }
func (rawCodec) Unmarshal(b []byte, v any) error {
	*v.(*[]byte) = append((*v.(*[]byte))[:0], b...)
	return nil
}

type connKey struct{}
type recorder struct {
	mu         sync.Mutex
	enc        *json.Encoder
	conn, call atomic.Int64
}

func (r *recorder) emit(v map[string]any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	v["t"] = float64(time.Now().UnixNano()) / 1e9
	if err := r.enc.Encode(v); err != nil {
		log.Fatal(err)
	}
}
func (r *recorder) TagConn(ctx context.Context, i *stats.ConnTagInfo) context.Context {
	id := r.conn.Add(1)
	r.emit(map[string]any{"phase": "conn_open", "conn": id, "remote": i.RemoteAddr.String()})
	return context.WithValue(ctx, connKey{}, id)
}
func (r *recorder) HandleConn(ctx context.Context, s stats.ConnStats) {
	if _, ok := s.(*stats.ConnEnd); ok {
		r.emit(map[string]any{"phase": "conn_close", "conn": ctx.Value(connKey{})})
	}
}
func (r *recorder) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context { return ctx }
func (*recorder) HandleRPC(context.Context, stats.RPCStats)                         {}

// Every bytes field except keys/range ends becomes a byte length. Message and
// oneof field names are retained; enums are names and scalar defaults included.
func shape(m protoreflect.Message) map[string]any {
	out := map[string]any{"_bytes": proto.Size(m.Interface())}
	fs := m.Descriptor().Fields()
	for i := 0; i < fs.Len(); i++ {
		f := fs.Get(i)
		if f.ContainingOneof() != nil && !m.Has(f) {
			continue
		}
		if f.Kind() == protoreflect.MessageKind && !f.IsList() && !m.Has(f) {
			continue
		}
		name := string(f.Name())
		val := m.Get(f)
		convert := func(v protoreflect.Value) any {
			switch f.Kind() {
			case protoreflect.MessageKind:
				return shape(v.Message())
			case protoreflect.BytesKind:
				if name == "key" || name == "range_end" {
					return string(v.Bytes())
				}
				return len(v.Bytes())
			case protoreflect.EnumKind:
				e := f.Enum().Values().ByNumber(v.Enum())
				if e == nil {
					return int32(v.Enum())
				}
				return string(e.Name())
			default:
				return v.Interface()
			}
		}
		if f.Kind() == protoreflect.BytesKind && name != "key" && name != "range_end" {
			name += "_bytes"
		}
		if f.IsList() {
			a := []any{}
			l := val.List()
			for j := 0; j < l.Len(); j++ {
				a = append(a, convert(l.Get(j)))
			}
			out[name] = a
		} else {
			out[name] = convert(val)
		}
	}
	return out
}
func decode(method string, b []byte, response bool) map[string]any {
	parts := strings.Split(strings.TrimPrefix(method, "/"), "/")
	if len(parts) != 2 {
		return map[string]any{"unknown": true}
	}
	d, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(parts[0]))
	if err != nil {
		return map[string]any{"decode_error": err.Error()}
	}
	md := d.(protoreflect.ServiceDescriptor).Methods().ByName(protoreflect.Name(parts[1]))
	if md == nil {
		return map[string]any{"unknown": true}
	}
	desc := md.Input()
	if response {
		desc = md.Output()
	}
	m := dynamicpb.NewMessage(desc)
	if err = proto.Unmarshal(b, m); err != nil {
		return map[string]any{"decode_error": err.Error()}
	}
	// A Range response needs counts/size, not every key. Avoid huge logs on LISTs.
	if response && parts[1] == "Range" {
		fs := desc.Fields()
		return map[string]any{"count": m.Get(fs.ByName("count")).Int(), "kvs_count": m.Get(fs.ByName("kvs")).List().Len(), "more": m.Get(fs.ByName("more")).Bool(), "header": shape(m.Get(fs.ByName("header")).Message())}
	}
	return shape(m.ProtoReflect())
}
func (r *recorder) proxy(cc *grpc.ClientConn) grpc.StreamHandler {
	return func(_ any, ss grpc.ServerStream) (ret error) {
		method, _ := grpc.MethodFromServerStream(ss)
		id := r.call.Add(1)
		conn := ss.Context().Value(connKey{})
		emit := func(phase string, extra map[string]any) {
			v := map[string]any{"phase": phase, "method": method, "call": id, "conn": conn}
			for k, x := range extra {
				v[k] = x
			}
			r.emit(v)
		}
		emit("start", nil)
		defer func() {
			e := ""
			if ret != nil {
				e = ret.Error()
			}
			emit("end", map[string]any{"error": e})
		}()
		ctx, cancel := context.WithCancel(ss.Context())
		defer cancel()
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			ctx = metadata.NewOutgoingContext(ctx, md.Copy())
		}
		cs, err := cc.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true, ClientStreams: true}, method, grpc.ForceCodec(rawCodec{}))
		if err != nil {
			return err
		}
		sendErr := make(chan error, 1)
		go func() {
			for {
				var b []byte
				err := ss.RecvMsg(&b)
				if err == io.EOF {
					sendErr <- cs.CloseSend()
					return
				}
				if err != nil {
					sendErr <- err
					cancel()
					return
				}
				emit("request", map[string]any{"bytes": len(b), "data": decode(method, b, false)})
				if err = cs.SendMsg(&b); err != nil {
					sendErr <- err
					return
				}
			}
		}()
		header, err := cs.Header()
		if err != nil {
			return err
		}
		if err = ss.SendHeader(header); err != nil {
			return err
		}
		for {
			var b []byte
			err = cs.RecvMsg(&b)
			if err != nil {
				ss.SetTrailer(cs.Trailer())
				if err == io.EOF {
					select {
					case e := <-sendErr:
						if e != nil && e != io.EOF {
							return e
						}
					default:
					}
					return nil
				}
				return err
			}
			emit("response", map[string]any{"bytes": len(b), "data": decode(method, b, true)})
			if err = ss.SendMsg(&b); err != nil {
				return err
			}
		}
	}
}
func main() {
	listen := flag.String("listen", "172.30.0.1:20010", "")
	backend := flag.String("backend", "127.0.0.1:2379", "")
	certDir := flag.String("cert-dir", "", "")
	output := flag.String("out", "record.jsonl", "")
	flag.Parse()
	cert, err := tls.LoadX509KeyPair(*certDir+"/node1.pem", *certDir+"/node1-key.pem")
	if err != nil {
		log.Fatal(err)
	}
	pem, err := os.ReadFile(*certDir + "/ca.pem")
	if err != nil {
		log.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		log.Fatal("CA")
	}
	clientCert, err := tls.LoadX509KeyPair(*certDir+"/client.pem", *certDir+"/client-key.pem")
	if err != nil {
		log.Fatal(err)
	}
	cc, err := grpc.NewClient(*backend, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool, Certificates: []tls.Certificate{clientCert}})), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(128<<20), grpc.MaxCallSendMsgSize(128<<20)))
	if err != nil {
		log.Fatal(err)
	}
	defer cc.Close()
	f, err := os.Create(*output)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	r := &recorder{enc: json.NewEncoder(f)}
	s := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert})), grpc.ForceServerCodec(rawCodec{}), grpc.UnknownServiceHandler(r.proxy(cc)), grpc.StatsHandler(r), grpc.MaxRecvMsgSize(128<<20), grpc.MaxSendMsgSize(128<<20))
	l, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("recording", *listen, "->", *backend)
	log.Fatal(s.Serve(l))
}
