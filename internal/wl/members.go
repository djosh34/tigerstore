package wl

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/url"
	"slices"
	"time"

	"github.com/djosh34/tigerstore/internal/planted"
	pb "go.etcd.io/raft/v3/raftpb"
)

// Membership. The members are replicated through Raft, as etcd does: a
// ConfChange entry adds, removes, updates or promotes one, with the
// member in its context, and the apply changes the list every node
// keeps; each node then publishes its name and client URLs with an op of
// its own in a batch. Checkpoints hold the list. A node dials the members
// the list holds, and takes connections only from them.

// member is one member of the cluster, as the apply keeps it.
type member struct {
	id         uint64
	name       string
	peerURLs   []string
	clientURLs []string
	learner    bool
	// The version gate's (format.go): the newest format the member
	// announced it supports, in which run, and the epoch it confirmed.
	format    uint8
	run       uint64
	confirmed uint64
}

// peerAddr is where the node dials m: the host and port of its first
// peer URL.
func (m *member) peerAddr() string {
	if len(m.peerURLs) == 0 {
		return ""
	}
	u, err := url.Parse(m.peerURLs[0])
	if err != nil {
		return ""
	}
	return u.Host
}

// appendMember appends m as etcd's Member message: ID, name, peer URLs,
// client URLs and whether it is a learner.
func appendMember(b []byte, m *member) []byte {
	b = appendVarintField(b, 1, m.id)
	if m.name != "" {
		b = appendBytesField(b, 2, []byte(m.name))
	}
	for _, u := range m.peerURLs {
		b = appendBytesField(b, 3, []byte(u))
	}
	for _, u := range m.clientURLs {
		b = appendBytesField(b, 4, []byte(u))
	}
	if m.learner {
		b = appendVarintField(b, 5, 1)
	}
	return b
}

// A member's state for the version gate follows etcd's Member message
// in the node's own records, never in what a client gets: in an
// announcement, its format, run and the epoch it confirms; in a
// checkpoint, its format, run and the epoch it confirmed.
const (
	fieldFormat    = 50
	fieldRun       = 51
	fieldConfirmed = 52
	fieldEpoch     = 53
)

// appendMemberState appends m as a checkpoint keeps it.
func appendMemberState(b []byte, m *member) []byte {
	b = appendMember(b, m)
	b = appendVarintField(b, fieldFormat, uint64(m.format))
	b = appendVarintField(b, fieldRun, m.run)
	return appendVarintField(b, fieldConfirmed, m.confirmed)
}

// appendAnnouncement appends m's announcement of its name, client URLs,
// format and run, confirming epoch.
func appendAnnouncement(b []byte, m *member, epoch uint64) []byte {
	b = appendMember(b, m)
	b = appendVarintField(b, fieldFormat, uint64(m.format))
	b = appendVarintField(b, fieldRun, m.run)
	return appendVarintField(b, fieldEpoch, epoch)
}

// parseMember reads etcd's Member message, with the gate's state.
func parseMember(b []byte) (member, error) {
	m, _, err := parseAnnouncement(b)
	return m, err
}

// parseAnnouncement reads an announcement: the member and the epoch it
// confirms.
func parseAnnouncement(b []byte) (member, uint64, error) {
	var m member
	var epoch uint64
	for len(b) > 0 {
		num, _, v, p, rest, err := field(b)
		if err != nil {
			return m, 0, err
		}
		switch num {
		case 1:
			m.id = v
		case 2:
			m.name = string(p)
		case 3:
			m.peerURLs = append(m.peerURLs, string(p))
		case 4:
			m.clientURLs = append(m.clientURLs, string(p))
		case 5:
			m.learner = v != 0
		case fieldFormat:
			m.format = uint8(min(v, 255))
		case fieldRun:
			m.run = v
		case fieldConfirmed:
			m.confirmed = v
		case fieldEpoch:
			epoch = v
		}
		b = rest
	}
	return m, epoch, nil
}

// A ConfChange's context: the node that proposed it and the request's
// sequence there, so that node answers its client, then the member.
const ccHdr = 16

func ccContext(origin, seq uint64, m *member) []byte {
	b := binary.LittleEndian.AppendUint64(nil, origin)
	b = binary.LittleEndian.AppendUint64(b, seq)
	return appendMember(b, m)
}

func parseCCContext(b []byte) (origin, seq uint64, m member, err error) {
	if len(b) < ccHdr {
		return 0, 0, m, fmt.Errorf("a ConfChange context of %d bytes", len(b))
	}
	m, err = parseMember(b[ccHdr:])
	return binary.LittleEndian.Uint64(b), binary.LittleEndian.Uint64(b[8:]), m, err
}

// maxLearners is etcd's --max-learners default.
const maxLearners = 1

// The errors of a membership change that the apply rejects, as etcd
// names them.
var (
	errMemberExist   = requestErr(codeFailedPrecondition, "etcdserver: member ID already exist")
	errPeerURLExist  = requestErr(codeFailedPrecondition, "etcdserver: Peer URLs already exists")
	errMemberMissing = requestErr(codeNotFound, "etcdserver: member not found")
	errNotLearner    = requestErr(codeFailedPrecondition, "etcdserver: can only promote a learner member")
	errTooManyLearn  = requestErr(codeFailedPrecondition, "etcdserver: too many learner members in cluster")
)

// ErrRemoved stops a node the cluster removed: wl.Run returns it.
var ErrRemoved = errors.New("the member was removed from the cluster")

// removedNotice is what a member says on a connection from a member the
// cluster removed, before it closes it.
const removedNotice = 0x6465766f6d6572 // "removed"

func (n *node) memberAt(id uint64) int {
	return slices.IndexFunc(n.members, func(m member) bool { return m.id == id })
}

// checkConfChange is the check every node makes of a membership change
// as it applies it, on the list as it is then, so all reject it alike.
func (n *node) checkConfChange(cc *pb.ConfChange, m *member) error {
	i := n.memberAt(cc.GetNodeId())
	switch cc.GetType() {
	case pb.ConfChangeAddNode, pb.ConfChangeAddLearnerNode:
		if i >= 0 {
			if cc.GetType() == pb.ConfChangeAddNode && n.members[i].learner {
				return nil // a promotion
			}
			if cc.GetType() == pb.ConfChangeAddNode {
				return errNotLearner
			}
			return errMemberExist
		}
		if n.removed[cc.GetNodeId()] {
			return errMemberExist
		}
		if err := n.peerURLsFree(cc.GetNodeId(), m.peerURLs); err != nil {
			return err
		}
		if cc.GetType() == pb.ConfChangeAddLearnerNode && n.learners() >= maxLearners {
			return errTooManyLearn
		}
	case pb.ConfChangeRemoveNode:
		if i < 0 {
			return errMemberMissing
		}
	case pb.ConfChangeUpdateNode:
		if i < 0 {
			return errMemberMissing
		}
		return n.peerURLsFree(cc.GetNodeId(), m.peerURLs)
	}
	return nil
}

func (n *node) peerURLsFree(id uint64, urls []string) error {
	for _, x := range n.members {
		if x.id == id {
			continue
		}
		for _, u := range urls {
			if slices.Contains(x.peerURLs, u) {
				return errPeerURLExist
			}
		}
	}
	return nil
}

func (n *node) learners() int {
	k := 0
	for _, m := range n.members {
		if m.learner {
			k++
		}
	}
	return k
}

// applyConfChange applies the membership change at index: the member list changes
// unless the check rejects it, Raft's configuration with it, and the node
// that proposed it answers its client.
func (n *node) applyConfChange(cc *pb.ConfChange, index uint64) error {
	origin, seq, m, err := parseCCContext(cc.GetContext())
	if err != nil {
		return fmt.Errorf("apply a membership change: %w", err)
	}
	rerr := n.checkConfChange(cc, &m)
	if rerr != nil {
		// Raft leaves a change of node None out.
		cc.NodeId = new(uint64(0))
	} else {
		added := false
		switch i := n.memberAt(cc.GetNodeId()); cc.GetType() {
		case pb.ConfChangeAddNode, pb.ConfChangeAddLearnerNode:
			if i >= 0 {
				n.members[i].learner = false
			} else {
				added = true
				m.id, m.learner = cc.GetNodeId(), cc.GetType() == pb.ConfChangeAddLearnerNode
				n.members = append(n.members, m)
				slices.SortFunc(n.members, func(a, b member) int { return compareIDs(a.id, b.id) })
			}
		case pb.ConfChangeRemoveNode:
			n.members = slices.Delete(n.members, i, i+1)
			if n.removed == nil {
				n.removed = map[uint64]bool{}
			}
			n.removed[cc.GetNodeId()] = true
		case pb.ConfChangeUpdateNode:
			if !planted.On("B28") {
				// Planted (B28): the change is answered, and the peer URLs
				// stay as they were.
				n.members[i].peerURLs = m.peerURLs
			}
		}
		// Every member confirms the new membership's support again.
		n.formatEpoch = index
		if added && n.budgetKeeps(len(n.members)) != nil {
			// The leader checked the change as it proposed it, on its
			// own budget (budgetKeeps): this member's is smaller, or a
			// leader before it took the change. It applies the change as
			// every member does, and says so.
			n.logf("a memory budget of %d bytes does not keep the cluster's %d members, which need at least %d bytes: every member should run with the same budget (#760)",
				n.st.budget, len(n.members), leastBudget(len(n.members)-1))
		}
	}
	n.confState = n.rn.ApplyConfChange(cc)
	n.storage.confIndex = index
	if origin == n.id {
		n.answerMemberRequest(seq, rerr)
	}
	if rerr == nil && cc.GetType() == pb.ConfChangeRemoveNode && cc.GetNodeId() == n.id {
		return ErrRemoved
	}
	if n.shell != nil {
		n.shell.membersChanged()
	}
	return nil
}

func compareIDs(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// applyMemberAttrs applies a member's announcement, committed at index.
func (n *node) applyMemberAttrs(req []byte, index uint64) error {
	m, epoch, err := parseAnnouncement(req)
	if err != nil {
		return badRequest(err)
	}
	return n.applyAnnouncement(&m, epoch, index)
}

// publishEvery is how often a node proposes its announcement again while
// the member list does not have it.
const publishEvery = time.Second

// publish proposes the node's announcement: its name, client URLs, newest
// format and run, and the epoch it confirms, while it has one to make.
func (n *node) publish() {
	i := n.memberAt(n.id)
	if i < 0 || n.now.Before(n.publishAt) || !n.announces(&n.members[i]) {
		return
	}
	n.publishAt = n.now.Add(publishEvery)
	self := member{id: n.id, name: n.self.name, clientURLs: n.self.clientURLs, format: maxFormat, run: n.run}
	n.addOp(noHandle, kMemberAttrs, appendAnnouncement(nil, &self, n.formatEpoch))
}

// encodeMembers appends the member list: a count, then each member's
// length and Member message; then the removed IDs, a count and each.
func (n *node) encodeMembers(b []byte) []byte {
	b = binary.AppendUvarint(b, uint64(len(n.members)))
	for i := range n.members {
		m := appendMemberState(nil, &n.members[i])
		b = binary.AppendUvarint(b, uint64(len(m)))
		b = append(b, m...)
	}
	ids := make([]uint64, 0, len(n.removed))
	for id := range n.removed {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	b = binary.AppendUvarint(b, uint64(len(ids)))
	for _, id := range ids {
		b = binary.AppendUvarint(b, id)
	}
	b = binary.AppendUvarint(b, uint64(len(n.alarms)))
	for _, a := range n.alarms {
		b = binary.AppendUvarint(b, a.member)
		b = binary.AppendUvarint(b, a.typ)
	}
	b = binary.AppendUvarint(b, uint64(n.downgrade))
	b = binary.AppendUvarint(b, uint64(n.format))
	b = binary.AppendUvarint(b, n.formatEpoch)
	auth := n.auth
	if auth == nil {
		auth = newAuthStore()
	}
	return auth.encode(b)
}

// ckptMembers is the member list a checkpoint holds, with the raised
// alarms and the version gate's state.
type ckptMembers struct {
	members []member
	removed map[uint64]bool
	alarms  []alarm
	// downgrade is the minor version a downgrade is on to, 0 for none.
	downgrade   int64
	format      uint8
	formatEpoch uint64
	auth        *authStore
}

// takeMembers takes the member list a checkpoint holds.
func (n *node) takeMembers(cm *ckptMembers) error {
	n.members, n.removed = cm.members, cm.removed
	n.alarms = cm.alarms
	n.downgrade = cm.downgrade
	n.format, n.formatEpoch = cm.format, cm.formatEpoch
	n.auth = cm.auth
	clear(n.tokens)
	n.alarmsChanged()
	if n.format > maxFormat {
		return tooOld(n.format)
	}
	return nil
}

// readMembers reads what encodeMembers wrote.
func (cr *ckptReader) readMembers() (ckptMembers, error) {
	var cm ckptMembers
	k, err := cr.uvarint()
	if err != nil {
		return cm, err
	}
	for range k {
		l, err := cr.uvarint()
		if err != nil {
			return cm, err
		}
		b, err := cr.bytes(l)
		if err != nil {
			return cm, err
		}
		m, err := parseMember(b)
		if err != nil {
			return cm, err
		}
		cm.members = append(cm.members, m)
	}
	if k, err = cr.uvarint(); err != nil {
		return cm, err
	}
	cm.removed = map[uint64]bool{}
	for range k {
		id, err := cr.uvarint()
		if err != nil {
			return cm, err
		}
		cm.removed[id] = true
	}
	if k, err = cr.uvarint(); err != nil {
		return cm, err
	}
	for range k {
		var a alarm
		if a.member, err = cr.uvarint(); err != nil {
			return cm, err
		}
		if a.typ, err = cr.uvarint(); err != nil {
			return cm, err
		}
		cm.alarms = append(cm.alarms, a)
	}
	d, err := cr.uvarint()
	if err != nil {
		return cm, err
	}
	cm.downgrade = int64(d)
	f, err := cr.uvarint()
	if err != nil {
		return cm, err
	}
	cm.format = uint8(min(f, 255))
	if cm.formatEpoch, err = cr.uvarint(); err != nil {
		return cm, err
	}
	cm.auth, err = cr.readAuth()
	return cm, err
}

// A node that joins a running cluster asks a member for the list first:
// its hello is membersAsk, of no cluster, and the member answers with its
// cluster's ID and the list, as a checkpoint holds it.
const membersAsk = ^uint64(0)

// answerMembers answers a node that asks for the member list, in the
// accept goroutine, with the list the loop has.
func (l *loop) answerMembers(s *sock) error {
	got := make(chan []byte, 1)
	l.post(func() error {
		got <- l.n.membersAnswer()
		return nil
	})
	select {
	case b := <-got:
		putFrame(s, b)
		return flushAll(s)
	case <-time.After(snapTimeout):
		return errors.New("the loop did not answer")
	case <-l.ctx.Done():
		return l.ctx.Err()
	}
}

// membersAnswer is the answer to a node that asks for the member list:
// the cluster's ID and the list, with an empty auth store. Any holder of
// a peer certificate may ask, before its cluster is known, and a joining
// node reads the members alone; the auth store's users, password hashes
// and roles went with it (#689's area-2 review).
func (n *node) membersAnswer() []byte {
	auth := n.auth
	n.auth = nil
	b := n.encodeMembers(binary.LittleEndian.AppendUint64(nil, n.clusterID))
	n.auth = auth
	return b
}

// RemoteMembers asks the members at addrs, one after another, for their
// cluster's ID and member list, through the peer TLS cfg: a node that
// joins a running cluster learns its ID there.
// sys is the simulator's, nil in the real binary (Config.Sys).
func RemoteMembers(ctx context.Context, sys Sys, cfg *tls.Config, addrs []string) ([]Member, uint64, error) {
	env, err := newSysEnv(&Config{Sys: sys})
	if err != nil {
		return nil, 0, err
	}
	var errs []error
	for _, addr := range addrs {
		ms, cluster, err := askMembers(ctx, env, cfg, addr)
		if err == nil {
			return ms, cluster, nil
		}
		errs = append(errs, fmt.Errorf("%s: %w", addr, err))
	}
	return nil, 0, fmt.Errorf("no member answered with the member list: %w", errors.Join(errs...))
}

func askMembers(ctx context.Context, sys sysEnv, cfg *tls.Config, addr string) ([]Member, uint64, error) {
	nc, err := sys.dial(ctx, addr, 0)
	if err != nil {
		return nil, 0, err
	}
	c := cfg.Clone()
	c.ServerName, _, _ = net.SplitHostPort(addr)
	h, err := handshake(sys, nc, c, kindPeerOut)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = sysClose(h.fd) }()
	if err := sys.blocking(h.fd, snapTimeout); err != nil {
		return nil, 0, err
	}
	s := newSock(h, &counters{})
	s.put(peerHelloBytes(membersAsk, 0, ""))
	if err := flushAll(s); err != nil {
		return nil, 0, err
	}
	b, err := readFrame(s)
	if err != nil {
		return nil, 0, err
	}
	if len(b) < 8 {
		return nil, 0, errors.New("a short answer")
	}
	cr := &ckptReader{r: bufio.NewReader(bytes.NewReader(b[8:]))}
	cm, err := cr.readMembers()
	if err != nil {
		return nil, 0, err
	}
	out := make([]Member, 0, len(cm.members))
	for _, m := range cm.members {
		out = append(out, Member{ID: m.id, Name: m.name, PeerURLs: m.peerURLs, ClientURLs: m.clientURLs})
	}
	return out, binary.LittleEndian.Uint64(b), nil
}
