package server

import (
	"time"

	"golang.org/x/net/http2"
)

// gRPC keepalive works as grpc-go's server does with etcd's settings:
// --grpc-keepalive-min-time is the enforcement policy's MinTime, without
// pings while no call is open, and --grpc-keepalive-interval and
// --grpc-keepalive-timeout are the server's pings. etcd hands grpc-go
// each setting only when it is above 0, so grpc-go's defaults take the
// others' place.
//
// etcd serves gRPC with grpc-go's own server only where it serves gRPC
// alone: on a client URL without TLS, and on one with TLS when
// --listen-client-http-urls takes the HTTP traffic. A TLS client URL that
// serves both runs gRPC inside net/http's HTTP/2 server, which answers
// every ping and sends none; so does tigerstore on such a connection.

const (
	// maxPingStrikes and idlePingTime are grpc-go's: the third ping too
	// soon closes the connection, and with no call open, any ping within
	// two hours of the last one is too soon.
	maxPingStrikes = 2
	idlePingTime   = 2 * time.Hour
)

// goAwayError ends a connection with a GOAWAY of the code and debug data.
type goAwayError struct {
	code  http2.ErrCode
	debug string
}

func (e goAwayError) Error() string { return "server: GOAWAY " + e.code.String() + " " + e.debug }

// errTooManyPings is grpc-go's answer to a client that pings too often.
var errTooManyPings = goAwayError{code: http2.ErrCodeEnhanceYourCalm, debug: "too_many_pings"}

// checkPing counts a client's ping against it when it comes sooner than
// the minimum time after the last one, or, with no call open, sooner than
// two hours after it; a HEADERS or DATA frame sent since forgives every
// count. Only a connection that serves gRPC alone counts. Reader only.
func (c *conn) checkPing() error {
	if c.traffic != TrafficGRPC {
		return nil
	}
	now := time.Now()
	defer func() { c.lastPing = now }()
	if c.wrote.CompareAndSwap(true, false) {
		c.pingStrikes = 0
		return nil
	}
	c.mu.Lock()
	open := len(c.streams)
	c.mu.Unlock()
	least := c.s.keepaliveMin
	if open == 0 {
		least = idlePingTime
	}
	if c.lastPing.Add(least).After(now) {
		c.pingStrikes++
	}
	if c.pingStrikes > maxPingStrikes {
		return errTooManyPings
	}
	return nil
}

// keepalive pings a client that sent nothing for the keepalive time and
// closes the connection when nothing arrives for the timeout after that,
// as grpc-go's server does.
func (c *conn) keepalive() {
	every, timeout := c.s.keepaliveTime, c.s.keepaliveTimeout
	prev := c.lastRead.Load()
	outstanding := false
	var left time.Duration
	t := time.NewTimer(every)
	defer t.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-t.C:
		}
		if last := c.lastRead.Load(); last > prev {
			outstanding = false
			t.Reset(time.Duration(last-time.Now().UnixNano()) + every)
			prev = last
			continue
		}
		if outstanding && left <= 0 {
			c.close()
			return
		}
		if !outstanding {
			c.queueCtrl(ctrlFrame{kind: ctrlPing})
			left, outstanding = timeout, true
		}
		sleep := min(every, left)
		left -= sleep
		t.Reset(sleep)
	}
}
