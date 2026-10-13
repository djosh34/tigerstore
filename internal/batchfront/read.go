package batchfront

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/batchcmd"
	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/node"
)

const maxMessage = core.MaxRequestBytes + (512 << 10)

type budget struct {
	input                  int
	admitted, ready, reset bool
	http, httpDone         bool
	timer                  *time.Timer
}
type conn struct {
	net.Conn
	s                       *Server
	ctx                     context.Context
	cancel                  context.CancelFunc
	once                    sync.Once
	out                     chan []encoded
	control                 chan control
	mu                      sync.Mutex
	budgets                 map[uint32]*budget
	inputBytes, outputBytes int
	writeTimeout            time.Duration // zero uses the experiment's 5 s default
}

func (c *conn) close() {
	c.once.Do(func() {
		c.cancel()
		// The owner can abort a slow client here. tls.Conn.Close may write a
		// close_notify and wait five seconds for a peer that stopped reading.
		// Abort the raw socket instead; no protocol write belongs on the owner.
		raw := c.Conn
		if tc, ok := raw.(*tls.Conn); ok {
			raw = tc.NetConn()
		}
		_ = raw.Close()
	})
}

func (c *conn) sendControl(o control) {
	select {
	case c.control <- o:
		c.s.stats.controlSends.Add(1)
	default:
		c.close()
	}
}

func (c *conn) start(id uint32) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.budgets) >= MaxStreams {
		return false
	}
	c.budgets[id] = &budget{}
	return true
}

func (c *conn) receive(id uint32, size int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	b := c.budgets[id]
	if b == nil || b.input+size > maxMessage+5 || c.inputBytes+size > MaxInputBytes {
		return false
	}
	b.input += size
	c.inputBytes += size
	return true
}

func (c *conn) admit(id uint32) {
	c.mu.Lock()
	if b := c.budgets[id]; b != nil {
		b.admitted = true
	}
	c.mu.Unlock()
}

func (c *conn) reset(id uint32) {
	c.mu.Lock()
	if b := c.budgets[id]; b != nil {
		b.reset = true
		if b.timer != nil {
			b.timer.Stop()
		}
		// An admitted payload still belongs to the core until completion.
		if !b.admitted || b.ready {
			c.inputBytes -= b.input
			delete(c.budgets, id)
		}
	}
	c.mu.Unlock()
	c.sendControl(control{id: id, reset: true})
}

func (c *conn) live(id uint32) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	b := c.budgets[id]
	return b != nil && !b.reset
}

func (c *conn) finish(id uint32) {
	c.mu.Lock()
	if b := c.budgets[id]; b != nil {
		c.inputBytes -= b.input
		if b.timer != nil {
			b.timer.Stop()
		}
		delete(c.budgets, id)
	}
	c.mu.Unlock()
}

type incoming struct {
	method   string
	path     string
	body     []byte
	received uint32
}
type admission struct {
	commands   []batchcmd.Command
	ops        []proto.Message
	tags       []uint32
	legacyTags []uint32
	bytes      int
}

func (c *conn) flush(a *admission) error {
	if len(a.tags) > 0 {
		c.s.stats.sends.Add(1)
		if err := c.s.n.SubmitBatch(c.ctx, a.commands, a.tags, c); err != nil {
			return err
		}
	}
	if len(a.legacyTags) > 0 {
		c.s.stats.sends.Add(1)
		if err := c.s.n.SubmitLegacyBatch(c.ctx, a.ops, a.legacyTags, c); err != nil {
			return err
		}
	}
	*a = admission{}
	return nil
}

func legacy(path string, body []byte) (proto.Message, error) {
	var op proto.Message
	switch path {
	case "/etcdserverpb.KV/Put":
		op = &pb.PutRequest{}
	case "/etcdserverpb.KV/Txn":
		op = &pb.TxnRequest{}
	case "/etcdserverpb.KV/Range":
		op = &pb.RangeRequest{}
	case "/etcdserverpb.KV/DeleteRange":
		op = &pb.DeleteRangeRequest{}
	case "/etcdserverpb.KV/Compact":
		op = &pb.CompactionRequest{}
	default:
		return nil, status.Error(codes.Unimplemented, "wholebatch: method not supported")
	}
	if err := proto.Unmarshal(body, op); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return op, nil
}

func (c *conn) dispatch(id uint32, s *incoming, a *admission) error {
	if s.method == "GET" {
		return c.dispatchHTTP(id, s, a)
	}
	if len(s.body) < 5 || s.body[0] != 0 || int(binary.BigEndian.Uint32(s.body[1:5])) != len(s.body)-5 {
		code := codes.InvalidArgument
		if len(s.body) > 0 && s.body[0] != 0 {
			code = codes.Unimplemented
		}
		c.Complete([]node.Completion{{Tag: id, Err: status.Error(code, "wholebatch: invalid unary envelope or unsupported compression")}})
		return nil
	}
	body := s.body[5:]
	if s.path == "/etcdserverpb.Cluster/MemberList" {
		if err := proto.Unmarshal(body, &pb.MemberListRequest{}); err != nil {
			c.Complete([]node.Completion{{Tag: id, Err: status.Error(codes.InvalidArgument, err.Error())}})
			return nil
		}
		r := &pb.MemberListResponse{Header: &pb.ResponseHeader{ClusterId: c.s.cfg.ClusterID, MemberId: c.s.n.ID()}}
		for _, m := range c.s.cfg.Members {
			r.Members = append(r.Members, &pb.Member{ID: m.ID, Name: m.Name, PeerURLs: m.PeerURLs, ClientURLs: m.ClientURLs})
		}
		c.Complete([]node.Completion{{Tag: id, Resp: r}})
		return nil
	}
	if s.path == "/etcdserverpb.Maintenance/Status" {
		if err := c.flush(a); err != nil {
			return err
		}
		if err := proto.Unmarshal(body, &pb.StatusRequest{}); err != nil {
			c.Complete([]node.Completion{{Tag: id, Err: status.Error(codes.InvalidArgument, err.Error())}})
			return nil
		}
		// Rare harness setup call. No per-RPC goroutine in the batch modes.
		r, err := c.s.n.Status(c.ctx)
		c.Complete([]node.Completion{{Tag: id, Resp: r, Err: err}})
		return nil
	}
	var op proto.Message
	var cmd batchcmd.Command
	var err error
	compact := c.s.mode == "full" && (s.path == "/etcdserverpb.KV/Put" || s.path == "/etcdserverpb.KV/Txn")
	if compact {
		cmd, err = batchcmd.Decode(s.path, body)
	} else {
		op, err = legacy(s.path, body)
	}
	if err != nil {
		c.Complete([]node.Completion{{Tag: id, Err: err}})
		return nil //nolint:nilerr // The error is a stream status, not a fatal connection error.
	}
	c.admit(id)
	c.s.stats.commands.Add(1)
	if c.s.mode == "rpc" {
		c.s.stats.sends.Add(1)
		// Deliberate legacy ablation: multiplex the reader rather than waiting
		// synchronously on Node.Do, which would serialize streams.
		go func() { r, err := c.s.n.Do(c.ctx, op); c.Complete([]node.Completion{{Tag: id, Resp: r, Err: err}}) }()
		return nil
	}
	if compact {
		a.commands = append(a.commands, cmd)
		a.tags = append(a.tags, id)
	} else {
		a.ops = append(a.ops, op)
		a.legacyTags = append(a.legacyTags, id)
	}
	a.bytes += len(body)
	if len(a.tags)+len(a.legacyTags) >= maxBatchCommands || a.bytes >= maxBatchBytes {
		return c.flush(a)
	}
	return nil
}

// readyFrame tests only already buffered bytes. A partial frame never holds
// an unsubmitted batch while ReadFrame waits for the rest of that frame.
func readyFrame(r *bufio.Reader) bool {
	if r.Buffered() < 9 {
		return false
	}
	b, err := r.Peek(9)
	if err != nil {
		return false
	}
	n := int(b[0])<<16 | int(b[1])<<8 | int(b[2])
	// MetaHeaders may read CONTINUATION frames internally unless END_HEADERS is set.
	return (b[3] != byte(http2.FrameHeaders) || b[4]&byte(http2.FlagHeadersEndHeaders) != 0) && r.Buffered() >= 9+n
}

func (c *conn) readLoop() error {
	r := bufio.NewReaderSize(c.Conn, 64<<10)
	pre := make([]byte, len(http2.ClientPreface))
	if _, err := io.ReadFull(r, pre); err != nil {
		return err
	}
	if string(pre) != http2.ClientPreface {
		return errors.New("bad HTTP/2 preface")
	}
	f := http2.NewFramer(io.Discard, r)
	f.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
	f.MaxHeaderListSize = 16 << 10
	f.SetMaxReadFrameSize(16 << 10)
	streams := make(map[uint32]*incoming)
	var last, received uint32
	var a admission
	for {
		if !readyFrame(r) {
			if err := c.flush(&a); err != nil {
				return err
			}
		}
		frame, err := f.ReadFrame()
		if err != nil {
			return err
		}
		switch v := frame.(type) {
		case *http2.SettingsFrame:
			if v.IsAck() {
				continue
			}
			o := control{ackSettings: true}
			if err := v.ForeachSetting(func(s http2.Setting) error {
				if err := s.Valid(); err != nil {
					return err
				}
				switch s.ID {
				case http2.SettingHeaderTableSize, http2.SettingEnablePush, http2.SettingMaxConcurrentStreams, http2.SettingMaxHeaderListSize, http2.SettingEnableConnectProtocol, http2.SettingNoRFC7540Priorities:
					// Stateless response HPACK needs no dynamic-table negotiation.
				case http2.SettingInitialWindowSize:
					o.initial = int64(s.Val)
					o.setInitial = true
				case http2.SettingMaxFrameSize:
					o.maxFrame = s.Val
				}
				return nil
			}); err != nil {
				return err
			}
			c.sendControl(o)
		case *http2.MetaHeadersFrame:
			if v.Truncated {
				return errors.New("headers too large")
			}
			s := streams[v.StreamID]
			if s == nil {
				if v.StreamID <= last || v.StreamID%2 == 0 || !c.start(v.StreamID) {
					return errors.New("stream admission limit or invalid stream ID")
				}
				last = v.StreamID
				s = &incoming{}
				for _, h := range v.Fields {
					switch h.Name {
					case ":path":
						s.path = h.Value
					case ":method":
						s.method = h.Value
					}
				}
				streams[v.StreamID] = s
				c.sendControl(control{id: v.StreamID, open: true})
			}
			if v.StreamEnded() {
				if err := c.dispatch(v.StreamID, s, &a); err != nil {
					return err
				}
				delete(streams, v.StreamID)
			}
		case *http2.DataFrame:
			s := streams[v.StreamID]
			received += v.Header().Length
			if received >= 32<<10 {
				c.sendControl(control{receive: received})
				received = 0
			}
			if s == nil {
				continue
			}
			data := v.Data()
			needed := len(s.body) + len(data)
			if needed > maxMessage+5 {
				return errors.New("message limit")
			}
			if needed > cap(s.body) {
				capacity := min(maxMessage+5, max(needed, max(5, cap(s.body)*2)))
				if len(s.body) == 0 && len(data) >= 5 {
					declared := uint64(binary.BigEndian.Uint32(data[1:5])) + 5
					if declared > maxMessage+5 {
						return errors.New("message limit")
					}
					capacity = max(needed, int(declared))
				}
				if !c.receive(v.StreamID, capacity-cap(s.body)) {
					return errors.New("input byte limit")
				}
				owned := make([]byte, len(s.body), capacity)
				copy(owned, s.body)
				s.body = owned
			}
			// Framer.Data is reused by ReadFrame. Copy into this stream's owned
			// reassembly buffer; never change that buffer after dispatch. Capacity,
			// not just body length, counts against connection input admission.
			s.body = append(s.body, data...)
			s.received += v.Header().Length
			if s.received >= 32<<10 {
				c.sendControl(control{id: v.StreamID, receive: s.received})
				s.received = 0
			}
			if v.StreamEnded() {
				if err := c.dispatch(v.StreamID, s, &a); err != nil {
					return err
				}
				delete(streams, v.StreamID)
			}
		case *http2.RSTStreamFrame:
			delete(streams, v.StreamID)
			c.reset(v.StreamID)
		case *http2.WindowUpdateFrame:
			c.sendControl(control{id: v.StreamID, delta: int64(v.Increment)})
		case *http2.PingFrame:
			if !v.IsAck() {
				c.sendControl(control{ping: true, pingData: v.Data})
			}
		case *http2.GoAwayFrame:
			return nil
		}
	}
}
