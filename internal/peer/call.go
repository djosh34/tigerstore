package peer

import (
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// callBit marks the dialer's ID on a connection that carries a Call.
const callBit = 1 << 62

// maxCall bounds a request or an answer of a Call.
const maxCall = 4 << 20

// Call sends peer id a request on a connection of its own and returns
// the answer, or the error the peer answered with.
func (t *TCP) Call(ctx context.Context, id uint64, req []byte) ([]byte, error) {
	s, ok := t.peer(id)
	if !ok {
		return nil, fmt.Errorf("peer: unknown peer %d", id)
	}
	return t.call(ctx, s, t.cfg.ID, req)
}

// Ask sends the node at addr a request as Call does, from a node that
// has no Raft ID and no transport yet: one that joins a running cluster
// asks for its members. The other end answers it as from Raft ID 0. cfg
// gives the certificates and the allowed names.
func Ask(ctx context.Context, cfg Config, addr string, req []byte) ([]byte, error) {
	if cfg.minVersion == 0 {
		cfg.minVersion, cfg.maxVersion = minVersion, maxVersion
	}
	verify, err := verifyPeer(cfg.AllowedCNs, cfg.AllowedHostnames)
	if err != nil {
		return nil, err
	}
	_, clientCert := cfg.certificates()
	t := &TCP{cfg: cfg, clientCert: clientCert, verify: verify, ctx: ctx, conns: map[net.Conn]struct{}{}, inbound: map[net.Conn]inbound{}, logged: map[string]time.Time{}}
	t.removed.Store(&map[uint64]bool{})
	return t.call(ctx, &sender{t: t, addr: addr}, 0, req)
}

func (t *TCP) call(ctx context.Context, s *sender, id uint64, req []byte) ([]byte, error) {
	c, _, err := s.dial(ctx, id|callBit)
	if err != nil {
		return nil, err
	}
	defer t.untrack(c)
	deadline := time.Now().Add(cmp.Or(t.cfg.WriteTimeout, defaultTimeout))
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err := c.SetDeadline(deadline); err != nil {
		return nil, err
	}
	if err := writeChunk(c, req); err != nil {
		return nil, err
	}
	b, err := readChunk(c)
	if err != nil {
		return nil, err
	}
	switch {
	case len(b) >= 1 && b[0] == 0:
		return b[1:], nil
	case len(b) >= 5 && b[0] == 1:
		return nil, status.Error(codes.Code(binary.BigEndian.Uint32(b[1:])), string(b[5:]))
	}
	return nil, fmt.Errorf("peer %d sent a malformed answer", id)
}

// answer answers one Call, then closes the connection.
func (t *TCP) answer(c net.Conn, r io.Reader, from uint64) error {
	req, err := readChunk(r)
	if err != nil {
		return err
	}
	var out []byte
	if t.cfg.Answer == nil {
		err = errors.New("this node answers no calls")
	} else {
		out, err = t.cfg.Answer(from, req)
	}
	if err != nil {
		st := status.Convert(err)
		out = binary.BigEndian.AppendUint32([]byte{1}, uint32(st.Code()))
		out = append(out, st.Message()...)
	} else {
		out = append([]byte{0}, out...)
	}
	if err := c.SetWriteDeadline(t.writeDeadline()); err != nil {
		return err
	}
	return writeChunk(c, out)
}

func writeChunk(w io.Writer, b []byte) error {
	if len(b) > maxCall {
		return fmt.Errorf("peer: a call of %d bytes is over the limit", len(b))
	}
	_, err := w.Write(binary.BigEndian.AppendUint32(make([]byte, 0, 4+len(b)), uint32(len(b))))
	if err == nil {
		_, err = w.Write(b)
	}
	return err
}

func readChunk(r io.Reader) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n > maxCall {
		return nil, fmt.Errorf("peer: a call of %d bytes is over the limit", n)
	}
	b := make([]byte, n)
	_, err := io.ReadFull(r, b)
	return b, err
}
