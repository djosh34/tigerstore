package network

import (
	"errors"
	"net/netip"
	"sync"
	"syscall"
	"time"

	"github.com/jellevandenhooff/gosim/internal/simulation/syscallabi"
)

type PacketKind byte

const (
	PacketKindOpenStream   PacketKind = iota // uses InBuffer, OutBuffer
	PacketKindOpenedStream                   // uses nothing
	PacketKindStreamClose                    // uses nothing
	PacketKindStreamData                     // uses Offset, Count
	PacketKindStreamAck                      // uses Offset
	PacketKindStreamWindow                   // uses Count
	PacketKindStreamFin                      // uses Offset; the sender closed the stream after sending Offset bytes
)

// XXX: for connId, add a secret extra field so we can gracefully handle delayed
// packets when machines restart (replay chaos). otherwise, implement sequence
// numbers, tcp wait, ...

type Packet struct {
	ConnId              ConnId
	Kind                PacketKind
	InBuffer, OutBuffer *circularBuffer
	Offset, Count       int
	ArrivalTime         timeIndex // used by delayQueue
}

var packetPool = &sync.Pool{
	New: func() any {
		return &Packet{}
	},
}

func allocPacket(kind PacketKind, id ConnId) *Packet {
	p := packetPool.Get().(*Packet)
	p.Kind = kind
	p.ConnId = id
	return p
}

func freePacket(p *Packet) {
	// XXX: poison values so we catch reuse?
	p.ConnId = ConnId{}
	p.Kind = 255
	p.InBuffer = nil
	p.OutBuffer = nil
	p.Offset = -1
	p.Count = -1
	packetPool.Put(p)
}

type Stack struct {
	mu sync.Mutex

	// XXX: check shutdown everywhere?
	shutdown bool

	streams   map[ConnId]*Stream
	listeners map[uint16]*Listener

	endpoint *Endpoint

	nextPort uint16
}

func (s *Stack) Shutdown(graceful bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.shutdown = true

	for _, stream := range s.streams {
		s.endpoint.Send(allocPacket(PacketKindStreamClose, stream.id))

		// clean up timers
		if stream.ackFailedTimer != nil {
			stream.ackFailedTimer.Stop()
			stream.ackFailedTimer = nil
		}
		if stream.openFailedTimer != nil {
			stream.openFailedTimer.Stop()
			stream.openFailedTimer = nil
		}
		if stream.keepaliveTimer != nil {
			stream.keepaliveTimer.Stop()
			stream.keepaliveTimer = nil
		}

		// nil out buffers to catch lingering code
		stream.inBuffer = nil
		stream.outBuffer = nil
	}

	// nil out maps to catch lingering code
	s.streams = nil
	s.listeners = nil
}

func (b *Stack) Endpoint() *Endpoint {
	return b.endpoint
}

func (b *Stack) setEndpoint(ep *Endpoint) {
	b.endpoint = ep
}

func NewStack() *Stack {
	b := &Stack{
		streams:   make(map[ConnId]*Stream),
		listeners: make(map[uint16]*Listener),

		nextPort: 10000,
	}
	return b
}

// ErrNoFreePort means every port has a listener.
var ErrNoFreePort = errors.New("no free port")

// allocatePort returns a port no listener uses.
func (n *Stack) allocatePort() (uint16, error) {
	for range 1 << 16 {
		port := n.nextPort
		n.nextPort++
		if _, ok := n.listeners[port]; !ok {
			return port, nil
		}
	}
	return 0, ErrNoFreePort
}

func (b *Stack) processIncomingPacket(packet *Packet) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.shutdown {
		return
	}

	switch packet.Kind {
	case PacketKindOpenStream:
		// XXX: pass to listener?
		id := packet.ConnId.Flip()

		listener, ok := b.listeners[packet.ConnId.Ports.DestPort]
		if !ok {
			// reject, not listening
			b.endpoint.Send(allocPacket(PacketKindStreamClose, id))
			break
		}

		if listener.incomingCount == len(listener.incoming) {
			// reject, too many pending
			b.endpoint.Send(allocPacket(PacketKindStreamClose, id))
			break
		}

		s := NewStream(id, packet.InBuffer, packet.OutBuffer)
		s.opened = true
		b.resetKeepaliveTimerLocked(s)
		// XXX: check if id already exists?
		b.streams[id] = s

		b.endpoint.Send(allocPacket(PacketKindOpenedStream, id))

		pos := listener.incomingPos + listener.incomingCount
		if pos >= len(listener.incoming) {
			pos -= len(listener.incoming)
		}
		listener.incoming[pos] = s
		listener.incomingCount++
		b.updateListenerPollers(listener)

	case PacketKindOpenedStream:
		id := packet.ConnId.Flip()

		stream, ok := b.streams[id]
		if !ok {
			break // maybe send go away instead?
		}

		stream.processIncomingPacket(b, packet)

	case PacketKindStreamAck:
		id := packet.ConnId.Flip()

		stream, ok := b.streams[id]
		if !ok {
			break // maybe send go away instead?
		}

		stream.processIncomingPacket(b, packet)

	case PacketKindStreamData:
		id := packet.ConnId.Flip()

		stream, ok := b.streams[id]
		if !ok {
			break // maybe send go away instead?
		}

		stream.processIncomingPacket(b, packet)

	case PacketKindStreamWindow:
		id := packet.ConnId.Flip()

		stream, ok := b.streams[id]
		if !ok {
			break // maybe send go away instead?
		}

		stream.processIncomingPacket(b, packet)

	case PacketKindStreamClose, PacketKindStreamFin:
		id := packet.ConnId.Flip()

		stream, ok := b.streams[id]
		if !ok {
			break // maybe send go away instead?
		}

		// After a close by the peer, reads end with EOF instead of an
		// error, if all the data the peer sent arrived.
		stream.finished = packet.Kind == PacketKindStreamFin && packet.Offset == stream.recvPos

		if err := b.closeStream(stream, false); err != nil {
			// XXX
			panic(err)
		}

	default:
		panic(packet)
	}
}

// listeners, streams

type Listener struct {
	incoming                   []*Stream
	incomingPos, incomingCount int

	closed bool

	port uint16

	pollers syscallabi.Pollers
}

var ErrPortAlreadyInUse = errors.New("port already in use")

// OpenListener listens on port, or on a new port when port is 0.
func (n *Stack) OpenListener(port uint16) (*Listener, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if port == 0 {
		var err error
		if port, err = n.allocatePort(); err != nil {
			return nil, err
		}
	}
	if _, ok := n.listeners[port]; ok {
		return nil, ErrPortAlreadyInUse
	}

	l := &Listener{
		incoming: make([]*Stream, 5),

		closed: false,

		port: port,
	}
	n.listeners[port] = l

	return l, nil
}

// Port returns the port the listener listens on.
func (l *Listener) Port() uint16 {
	return l.port
}

var ErrListenerClosed = errors.New("listener closed")

func (n *Stack) Accept(listener *Listener) (*Stream, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	l := listener

	if l.closed {
		return nil, ErrListenerClosed
	}

	if l.incomingCount == 0 {
		return nil, syscall.EWOULDBLOCK
	}

	stream := l.incoming[l.incomingPos]
	l.incoming[l.incomingPos] = nil
	l.incomingPos++
	l.incomingCount--
	if l.incomingPos == len(l.incoming) {
		l.incomingPos = 0
	}
	n.updateListenerPollers(l)
	return stream, nil
}

func (n *Stack) updateListenerPollers(listener *Listener) {
	listener.pollers.NotifyCanRead(listener.closed || listener.incomingCount > 0)
}

func (n *Stack) CloseListener(listener *Listener) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	l := listener

	if l.closed {
		return ErrListenerClosed
	}

	l.closed = true

	for l.incomingCount > 0 {
		// A pending stream the peer already closed needs no close.
		if s := l.incoming[l.incomingPos]; !s.closed {
			n.closeStream(s, true)
		}
		l.incoming[l.incomingPos] = nil
		l.incomingPos++
		if l.incomingPos == len(l.incoming) {
			l.incomingPos = 0
		}
		l.incomingCount--
	}

	delete(n.listeners, l.port)

	n.updateListenerPollers(l)

	return nil
}

func (n *Stack) closeStream(stream *Stream, sendClose bool) error {
	if stream.closed {
		panic(stream)
	}

	// fmt.Fprintf(gosimruntime.LogOut, "close stream\n")

	if sendClose {
		n.endpoint.Send(allocPacket(PacketKindStreamClose, stream.id))
	}

	delete(n.streams, stream.id)
	stream.closed = true
	n.updateStreamPollers(stream)

	if stream.openFailedTimer != nil {
		stream.openFailedTimer.Stop()
		stream.openFailedTimer = nil
	}
	if stream.ackFailedTimer != nil {
		stream.ackFailedTimer.Stop()
		stream.ackFailedTimer = nil
	}
	if stream.keepaliveTimer != nil {
		stream.keepaliveTimer.Stop()
		stream.keepaliveTimer = nil
	}

	return nil
}

// A circularBuffer is a buffer for data in a TCP stream to minimize allocations
// and/or copies. The stream implementation passes buffer pointers on stream
// open, and afterwards all "data" packets are merely notifications that data
// can be read from the appropriate buffer. This way TCP reads and writes copy
// the data only twice (once from userspace to the circular buffer, and then
// from the circular buffer back to userspace), and with zero allocations.
type circularBuffer struct {
	data        []byte
	read, write int
	used        int
}

func newCircularBuffer(n int) *circularBuffer {
	return &circularBuffer{
		data: make([]byte, n),
	}
}

func (c *circularBuffer) free() int {
	return len(c.data) - c.used
}

func (c *circularBuffer) Write(data syscallabi.ByteSliceView) int {
	if free := c.free(); free < data.Len() {
		data = data.Slice(0, free)
	}
	n := data.Read(c.data[c.write:])
	c.write += n
	c.used += n
	if c.write == len(c.data) {
		c.write = 0
		m := data.SliceFrom(n).Read(c.data[c.write:])
		c.write += m
		c.used += m
		n += m
	}
	return n
}

func (c *circularBuffer) Read(data syscallabi.ByteSliceView) int {
	if c.used < data.Len() {
		data = data.Slice(0, c.used)
	}
	n := data.Write(c.data[c.read:])
	c.read += n
	c.used -= n
	if c.read == len(c.data) {
		c.read = 0
		m := data.SliceFrom(n).Write(c.data[c.read:])
		c.read += m
		c.used -= m
		n += m
	}
	return n
}

type Stream struct {
	id ConnId

	opened bool
	closed bool
	// finished is set when the peer closed the stream.
	finished bool

	openFailedTimer *time.Timer
	ackFailedTimer  *time.Timer
	keepaliveTimer  *time.Timer

	sendPos int
	ackPos  int
	recvPos int

	// in-memory shared between both ends to skip allocating
	// todo: stick these in a pool somehow
	inBuffer, outBuffer *circularBuffer

	incomingAvailable int
	outgoingWindow    int

	pollers syscallabi.Pollers
}

func (s *Stream) ID() ConnId {
	return s.id
}

// reset every time we send something that might trigger an ack
func (n *Stack) resetKeepaliveTimerLocked(s *Stream) {
	if s.keepaliveTimer == nil {
		s.keepaliveTimer = time.AfterFunc(30*time.Second, func() {
			n.mu.Lock()
			defer n.mu.Unlock()
			if n.shutdown || s.closed {
				return
			}

			// only send keepalive if we have not already send any data
			if s.ackPos != s.sendPos {
				return
			}

			packet := allocPacket(PacketKindStreamData, s.id)
			packet.Offset = s.sendPos
			packet.Count = 0
			n.endpoint.Send(packet)

			n.resetKeepaliveTimerLocked(s)
			n.resetAckFailedTimerLocked(s)
		})
	} else {
		s.keepaliveTimer.Reset(15 * time.Second)
	}
}

func (n *Stack) resetAckFailedTimerLocked(s *Stream) {
	if s.ackFailedTimer == nil {
		// TODO: this time is way too low, it breaks when latency goes to 2.5s?
		s.ackFailedTimer = time.AfterFunc(5*time.Second, func() {
			n.mu.Lock()
			defer n.mu.Unlock()
			if n.shutdown || s.closed {
				return
			}

			// fmt.Fprintf(gosimruntime.LogOut, "ack failed\n")

			// fail no matter the ack pos; for keep alives we have ackpos ==
			// sendpos. when we do get the data, this timer is stopped, so this
			// if should only help in case of a race

			// if s.ackPos != s.sendPos {
			// stream.failureReason?
			// stream.openFailed?
			n.closeStream(s, false)
			// }
		})
	} else {
		s.ackFailedTimer.Reset(5 * time.Second)
	}
}

func NewStream(id ConnId, inBuffer, outBuffer *circularBuffer) *Stream {
	return &Stream{
		id: id,

		inBuffer:       inBuffer,
		outBuffer:      outBuffer,
		outgoingWindow: len(outBuffer.data),
	}
}

func (n *Stack) OpenStream(addr netip.AddrPort) (*Stream, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// A loopback address is the machine itself.
	if addr.Addr().IsLoopback() || addr.Addr().IsUnspecified() {
		addr = netip.AddrPortFrom(n.endpoint.Addr(), addr.Port())
	}

	localPort, err := n.allocatePort()
	if err != nil {
		return nil, err
	}
	id := ConnId{
		Hosts: HostPair{
			SourceHost: n.endpoint.Addr(),
			DestHost:   addr.Addr(),
		},
		Ports: PortPair{
			SourcePort: localPort,
			DestPort:   addr.Port(),
		},
	}

	inBuffer := newCircularBuffer(1024)
	outBuffer := newCircularBuffer(1024)
	stream := NewStream(id, inBuffer, outBuffer)

	n.streams[stream.id] = stream
	// XXX: check stream id doesn't already exist

	packet := allocPacket(PacketKindOpenStream, id)
	packet.InBuffer = outBuffer
	packet.OutBuffer = inBuffer
	n.endpoint.Send(packet)

	stream.openFailedTimer = time.AfterFunc(time.Second, func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		if n.shutdown {
			return
		}
		if stream.opened || stream.closed {
			return
		}
		// stream.failureReason?
		// stream.openFailed?
		n.closeStream(stream, false)
	})

	return stream, nil
}

var ErrStreamClosed = errors.New("stream closed")

func (n *Stack) StreamClose(stream *Stream) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if stream.closed {
		return ErrStreamClosed
	}

	fin := allocPacket(PacketKindStreamFin, stream.id)
	fin.Offset = stream.sendPos
	n.endpoint.Send(fin)
	if err := n.closeStream(stream, false); err != nil {
		return err
	}

	return nil
}

func (s *Stream) processIncomingPacket(b *Stack, packet *Packet) {
	switch packet.Kind {
	case PacketKindOpenedStream:
		s.opened = true
		s.openFailedTimer.Stop()
		s.openFailedTimer = nil
		b.resetKeepaliveTimerLocked(s)
		b.updateStreamPollers(s)

	case PacketKindStreamData:
		if packet.Offset != s.recvPos {
			// XXX reject
			return
		}

		s.recvPos += packet.Count

		s.incomingAvailable += packet.Count

		response := allocPacket(PacketKindStreamAck, s.id)
		response.Offset = packet.Offset + packet.Count
		b.endpoint.Send(response)
		b.updateStreamPollers(s)

	case PacketKindStreamAck:
		if packet.Offset < s.ackPos {
			// XXX: reject
			return
		}
		s.ackPos = packet.Offset
		if s.ackPos == s.sendPos {
			s.ackFailedTimer.Stop()
		}

	case PacketKindStreamWindow:
		s.outgoingWindow += packet.Count
		b.updateStreamPollers(s)
	}
}

func (n *Stack) updateStreamPollers(stream *Stream) {
	stream.pollers.NotifyCanWrite(stream.closed || stream.outgoingWindow > 0)
	stream.pollers.NotifyCanRead(stream.closed || stream.incomingAvailable > 0)
}

func (n *Stack) StreamSend(stream *Stream, data syscallabi.ByteSliceView) (int, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	s := stream
	if s.closed {
		return 0, ErrStreamClosed
	}

	if !s.opened {
		return 0, syscall.EWOULDBLOCK
	}

	if s.outgoingWindow == 0 {
		return 0, syscall.EWOULDBLOCK
	}

	m := min(s.outgoingWindow, data.Len())
	written := stream.outBuffer.Write(data.Slice(0, m))
	if written != m {
		panic("bad")
	}

	offset := s.sendPos
	s.sendPos += written

	packet := allocPacket(PacketKindStreamData, s.id)
	packet.Offset = offset
	packet.Count = written
	n.endpoint.Send(packet)

	s.outgoingWindow -= written

	n.resetAckFailedTimerLocked(s)
	n.resetKeepaliveTimerLocked(s)

	n.updateStreamPollers(s)

	return written, nil
}

func (n *Stack) StreamRecv(s *Stream, data syscallabi.ByteSliceView) (int, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if s.incomingAvailable == 0 {
		if s.finished {
			return 0, nil
		}
		if s.closed {
			return 0, ErrStreamClosed
		}
		return 0, syscall.EWOULDBLOCK
	}

	m := min(s.incomingAvailable, data.Len())
	read := s.inBuffer.Read(data.Slice(0, m))
	if read != m {
		panic("bad")
	}
	s.incomingAvailable -= m

	packet := allocPacket(PacketKindStreamWindow, s.id)
	packet.Count = m
	n.endpoint.Send(packet)

	n.updateStreamPollers(s)

	return m, nil
}

type StreamStatus struct {
	Open   bool
	Closed bool
}

func (n *Stack) StreamStatus(stream *Stream) StreamStatus {
	n.mu.Lock()
	defer n.mu.Unlock()

	return StreamStatus{
		Open:   stream.opened,
		Closed: stream.closed,
	}
}

func (n *Stack) RegisterStreamPoller(poller *syscallabi.PollDesc, stream *Stream) {
	n.mu.Lock()
	defer n.mu.Unlock()

	stream.pollers.Add(poller)
}

func (n *Stack) DeregisterStreamPoller(poller *syscallabi.PollDesc, stream *Stream) {
	n.mu.Lock()
	defer n.mu.Unlock()

	stream.pollers.Remove(poller)
}

func (n *Stack) RegisterListenerPoller(poller *syscallabi.PollDesc, listener *Listener) {
	n.mu.Lock()
	defer n.mu.Unlock()

	listener.pollers.Add(poller)
}

func (n *Stack) DeregisterListenerPoller(poller *syscallabi.PollDesc, listener *Listener) {
	n.mu.Lock()
	defer n.mu.Unlock()

	listener.pollers.Remove(poller)
}
