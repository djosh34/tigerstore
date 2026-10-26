package network

import (
	"net/netip"
	"sync"
	"time"
)

type HostPair struct {
	SourceHost, DestHost netip.Addr
}

func (id HostPair) Flip() HostPair {
	return HostPair{SourceHost: id.DestHost, DestHost: id.SourceHost}
}

type PortPair struct {
	SourcePort uint16
	DestPort   uint16
}

func (id PortPair) Flip() PortPair {
	return PortPair{
		SourcePort: id.DestPort,
		DestPort:   id.SourcePort,
	}
}

type ConnId struct {
	Hosts HostPair
	Ports PortPair
}

func (id ConnId) Source() netip.AddrPort {
	return netip.AddrPortFrom(id.Hosts.SourceHost, id.Ports.SourcePort)
}

func (id ConnId) Dest() netip.AddrPort {
	return netip.AddrPortFrom(id.Hosts.DestHost, id.Ports.DestPort)
}

func (id ConnId) Flip() ConnId {
	return ConnId{
		Hosts: id.Hosts.Flip(),
		Ports: id.Ports.Flip(),
	}
}

type Endpoint struct {
	net   *Network
	addr  netip.Addr
	stack *Stack

	// XXX: make this reorder prevention happen at the conn ID level?
	// XXX: make the stack take care of reordering?
	last map[netip.Addr]timeIndex

	closed bool
}

func (e *Endpoint) Addr() netip.Addr {
	return e.addr
}

func (e *Endpoint) Send(packet *Packet) {
	if packet.ConnId.Hosts.SourceHost != e.addr {
		panic("bad packet")
	}

	e.net.mu.Lock()
	defer e.net.mu.Unlock()

	if e.closed {
		return
		// log.Printf("closed")
		// return errors.New("closed")
	}

	_, ok := e.net.endpoints[packet.ConnId.Hosts.DestHost]
	if !ok {
		return
		// log.Printf("no such dest")
		// return errors.New("no such dest")
	}

	if e.net.disconnected[packet.ConnId.Hosts] {
		return
		// XXX: silently fail -- but maybe fail loudly optionally? ENOPATH or something?
		// return nil
		// return errors.New("disconnected")
	}

	arrival := timeIndex{
		time:  time.Now().Add(e.net.delay[packet.ConnId.Hosts]).UnixNano(),
		index: 0,
	}
	if last := e.last[packet.ConnId.Hosts.DestHost]; last.time >= arrival.time {
		arrival.time = last.time
		arrival.index = last.index + 1
	}
	e.last[packet.ConnId.Hosts.DestHost] = arrival

	packet.ArrivalTime = arrival
	e.net.queue.enqueue(packet)
}

type Network struct {
	mu        sync.Mutex
	endpoints map[netip.Addr]*Endpoint
	nextIp    netip.Addr

	disconnected map[HostPair]bool
	delay        map[HostPair]time.Duration

	queue *delayQueue
}

func NewNetwork() *Network {
	return &Network{
		endpoints: make(map[netip.Addr]*Endpoint),
		nextIp:    netip.AddrFrom4([4]byte{11, 0, 0, 1}), // XXX: randomize?

		disconnected: make(map[HostPair]bool),
		delay:        make(map[HostPair]time.Duration),

		queue: newDelayQueue(),
	}
}

func (n *Network) SetConnected(p HostPair, connected bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if !connected {
		n.disconnected[p] = true
		n.disconnected[p.Flip()] = true
	} else {
		delete(n.disconnected, p)
		delete(n.disconnected, p.Flip())
	}
}

func (n *Network) SetDelay(p HostPair, delay time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if delay != 0 {
		n.delay[p] = delay
		n.delay[p.Flip()] = delay
	} else {
		delete(n.delay, p)
		delete(n.delay, p.Flip())
	}
}

func (n *Network) NextIP() netip.Addr {
	n.mu.Lock()
	defer n.mu.Unlock()

	addr := n.nextIp
	n.nextIp = n.nextIp.Next()

	return addr
}

func (n *Network) AttachStack(addr netip.Addr, stack *Stack) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if _, ok := n.endpoints[addr]; ok {
		panic(addr)
	}

	ep := &Endpoint{
		net:   n,
		addr:  addr,
		stack: stack,

		last: make(map[netip.Addr]timeIndex),
	}

	n.endpoints[addr] = ep
	stack.setEndpoint(ep)
}

func (n *Network) DetachStack(stack *Stack) {
	n.mu.Lock()
	defer n.mu.Unlock()

	endpoint, ok := n.endpoints[stack.endpoint.addr]
	if !ok {
		panic(stack.endpoint.addr)
	}

	endpoint.closed = true
	delete(n.endpoints, stack.endpoint.addr)
	stack.setEndpoint(nil) // XXX: good idea?
}

func (n *Network) getStack(packet *Packet) *Stack {
	n.mu.Lock()
	defer n.mu.Unlock()

	endpoint, ok := n.endpoints[packet.ConnId.Hosts.DestHost]
	if !ok {
		// XXX: dropped, log?
		return nil
	}

	if endpoint.stack == nil {
		// XXX: dropped, log
		return nil
	}

	return endpoint.stack
}

func (n *Network) Run() {
	for {
		packet := n.queue.dequeue()
		if stack := n.getStack(packet); stack != nil {
			// call stack without holding n.mu so stack can send packets which
			// grabs n.mu
			stack.processIncomingPacket(packet)
		}
		freePacket(packet)
	}
}
