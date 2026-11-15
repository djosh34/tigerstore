package wl

import (
	"bytes"
	"crypto/sha1" //nolint:gosec // etcd's member IDs are a SHA-1 of the peer URLs, not a secret
	"encoding/binary"
	"errors"
	"net"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"
)

// The Cluster service's changes: each is a ConfChange the node proposes,
// and the node answers its client once it applies it, or once it times
// out as a write does.

// memberReq is a membership change a client waits on.
type memberReq struct {
	h    handle
	kind uint8
	id   uint64 // the member it changes
	at   time.Time
}

var (
	errBadURLs          = requestErr(codeInvalidArgument, "etcdserver: given member URLs are invalid")
	errNotEnoughStarted = requestErr(codeFailedPrecondition, "etcdserver: re-configuration failed due to not enough started members")
	errLearnerNotReady  = requestErr(codeFailedPrecondition, "etcdserver: can only promote a learner member which is in sync with leader")
	errNotLeader        = requestErr(codeFailedPrecondition, "etcdserver: not leader")
	errLearner          = requestErr(codeFailedPrecondition, "etcdserver: rpc not supported for learner")
)

// learnerReady is how far a learner's log must have come, of the
// leader's, before it is promoted: etcd's 90%.
const learnerReady = 0.9

// memberRequest takes a MemberAdd, MemberRemove, MemberUpdate or
// MemberPromote.
func (n *node) memberRequest(c *client, s *stream, msg []byte) error {
	var id uint64
	var urls []string
	learner := false
	for m := msg; len(m) > 0; {
		num, _, v, p, rest, err := field(m)
		if err != nil {
			return err
		}
		switch {
		case s.kind == kMemberAdd && num == 1, s.kind == kMemberUpdate && num == 2:
			urls = append(urls, string(p))
		case s.kind == kMemberAdd && num == 2:
			learner = v != 0
		case s.kind != kMemberAdd && num == 1:
			id = v
		}
		m = rest
	}
	cc := pb.ConfChange{}
	mem := member{id: id, peerURLs: urls}
	var err error
	switch s.kind {
	case kMemberAdd:
		if err = checkURLs(urls); err != nil {
			break
		}
		mem.id = n.newMemberID(urls)
		if learner {
			cc.Type = pb.ConfChangeAddLearnerNode.Enum()
		} else {
			cc.Type = pb.ConfChangeAddNode.Enum()
			err = n.readyForVoters(1)
		}
	case kMemberUpdate:
		cc.Type = pb.ConfChangeUpdateNode.Enum()
		if err = checkURLs(urls); err == nil && n.memberAt(id) < 0 {
			err = errMemberMissing
		}
	case kMemberRemove:
		// As etcd's, a member the list does not hold is refused before
		// anything is proposed: Raft would drop the change while another
		// is pending, and the client would wait for nothing.
		cc.Type = pb.ConfChangeRemoveNode.Enum()
		switch i := n.memberAt(id); {
		case i < 0:
			err = errMemberMissing
		case !n.members[i].learner:
			err = n.readyToRemove(id)
		}
	case kMemberPromote:
		cc.Type = pb.ConfChangeAddNode.Enum()
		err = n.readyToPromote(id)
		if errors.Is(err, errNotLeader) {
			return n.forwardPromote(c, s, msg)
		}
		if re, ok := errors.AsType[*requestError](err); ok && n.prom != nil {
			n.prom.LearnerPromoteFailures.WithLabelValues(re.msg).Inc()
		}
	}
	if err != nil {
		n.reject(c, s, err)
		return nil
	}
	n.memberSeq++
	cc.NodeId = new(mem.id)
	cc.Context = ccContext(n.id, n.memberSeq, &mem)
	if err := n.rn.ProposeConfChange(&cc); err != nil {
		// Raft takes no change without a leader: the client gets what a
		// write that timed out gets, and may try again.
		n.logf("a membership change not proposed: %v", err)
		n.replyError(c, s.id, codeUnavailable, msgTimeout)
		return nil
	}
	if n.memberReqs == nil {
		n.memberReqs = map[uint64]memberReq{}
	}
	n.memberReqs[n.memberSeq] = memberReq{h: mkHandle(c, s.id), kind: s.kind, id: mem.id, at: n.now}
	return nil
}

// checkURLs is etcd's check of a member's peer URLs: each an http or
// https URL of a host and port and nothing else.
func checkURLs(urls []string) error {
	if len(urls) == 0 {
		return errBadURLs
	}
	for _, raw := range urls {
		u, err := url.Parse(raw)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Path != "" && u.Path != "/" || u.RawQuery != "" || u.Fragment != "" {
			return errBadURLs
		}
		if _, _, err := net.SplitHostPort(u.Host); err != nil {
			return errBadURLs
		}
	}
	return nil
}

// newMemberID is etcd's member ID of a new member, a SHA-1 of its peer
// URLs, the cluster and the time, kept to 32 bits: a batch names the
// node that proposed it in 32. It is never 0 nor an ID the cluster had.
func (n *node) newMemberID(urls []string) uint64 {
	sorted := slices.Sorted(slices.Values(urls))
	b := []byte(strings.Join(sorted, ""))
	b = strconv.AppendUint(b, n.clusterID, 10)
	b = strconv.AppendInt(b, n.now.Unix(), 10)
	sum := sha1.Sum(b) //nolint:gosec // see the import
	id := uint64(binary.BigEndian.Uint32(sum[:4]))
	for id == 0 || n.memberAt(id) >= 0 || n.removed[id] {
		id = uint64(uint32(id + 1))
	}
	return id
}

// started counts the voters that published their name and that the node
// reaches: itself, or one it has a connection to.
func (n *node) started(skip uint64) int {
	k := 0
	for _, m := range n.members {
		if m.learner || m.id == skip || m.name == "" {
			continue
		}
		if p := n.peersOut[m.id]; m.id == n.id || p != nil && !p.dead {
			k++
		}
	}
	return k
}

func (n *node) voters() int {
	return len(n.members) - n.learners()
}

// readyForVoters is etcd's strict check before more voters: the voters
// started must make a quorum of the voters there would be.
func (n *node) readyForVoters(more int) error {
	if n.started(0) < (n.voters()+more)/2+1 {
		return errNotEnoughStarted
	}
	return nil
}

// moveReq is a MoveLeader that waits for the leader to move.
type moveReq struct {
	h      handle
	target uint64
	at     time.Time
}

var (
	errBadTransferee   = requestErr(codeFailedPrecondition, "etcdserver: bad leader transferee")
	errTransferTimeout = requestErr(codeUnavailable, "etcdserver: request timed out, leader transfer took too long")
)

// moveTimeout bounds how long a MoveLeader waits.
const moveTimeout = 5 * time.Second

// moveLeader is etcd's MoveLeader: only the leader moves, only to a
// voter, and it answers once the leader is the target, with an empty
// response.
func (n *node) moveLeader(c *client, s *stream, msg []byte) error {
	target := uint64(0)
	for m := msg; len(m) > 0; {
		num, _, v, _, rest, err := field(m)
		if err != nil {
			return err
		}
		if num == 1 {
			target = v
		}
		m = rest
	}
	if !n.leading() {
		n.reject(c, s, errNotLeader)
		return nil
	}
	if i := n.memberAt(target); i < 0 || n.members[i].learner {
		n.reject(c, s, errBadTransferee)
		return nil
	}
	if target == n.id {
		n.replyEmpty(c, s)
		return nil
	}
	n.rn.TransferLeader(target)
	n.moves = append(n.moves, moveReq{h: mkHandle(c, s.id), target: target, at: n.now})
	return nil
}

// checkMoves answers the MoveLeaders whose target leads now, or that
// waited too long.
func (n *node) checkMoves() {
	k := 0
	for _, mv := range n.moves {
		switch {
		case n.lead == mv.target:
			if c, s := n.clientOf(mv.h); c != nil {
				n.replyEmpty(c, s)
			}
		case n.now.Sub(mv.at) > moveTimeout:
			if c, s := n.clientOf(mv.h); c != nil {
				n.reject(c, s, errTransferTimeout)
			}
		default:
			n.moves[k] = mv
			k++
		}
	}
	clear(n.moves[k:])
	n.moves = n.moves[:k]
}

// forwardPromote has the leader promote a learner, as etcd's follower
// forwards a promotion: only the leader knows how far the learner is. It
// calls MemberPromote on the leader's client URLs in a side call, and
// answers with the leader's answer.
func (n *node) forwardPromote(c *client, s *stream, msg []byte) error {
	i := n.memberAt(n.lead)
	if i < 0 || len(n.members[i].clientURLs) == 0 || n.shell == nil {
		n.reject(c, s, errNotLeader)
		return nil
	}
	h := mkHandle(c, s.id)
	n.sideCalls[h] = n.shell.forwardPromote(h, slices.Clone(n.members[i].clientURLs), bytes.Clone(msg))
	return nil
}

// readyToRemove is etcd's strict check before a voter is removed: the
// other voters started must make a quorum of those left.
func (n *node) readyToRemove(id uint64) error {
	if n.started(id) < (n.voters()-1)/2+1 {
		return errNotEnoughStarted
	}
	return nil
}

// readyToPromote checks, on the leader, that learner id's log is close
// to the leader's and that one more voter keeps a quorum started. Only
// the leader knows how far the learner is.
func (n *node) readyToPromote(id uint64) error {
	i := n.memberAt(id)
	if i < 0 {
		return errMemberMissing
	}
	if !n.members[i].learner {
		return errNotLearner
	}
	if !n.leading() {
		return errNotLeader
	}
	st := n.rn.Status()
	if float64(st.Progress[id].Match) < learnerReady*float64(st.Progress[n.id].Match) {
		return errLearnerNotReady
	}
	return n.readyForVoters(1)
}

// answerMemberRequest answers the client of request seq, applied with
// the error err or changed.
func (n *node) answerMemberRequest(seq uint64, err error) {
	r, ok := n.memberReqs[seq]
	if !ok {
		return
	}
	delete(n.memberReqs, seq)
	c, s := n.clientOf(r.h)
	if c == nil {
		return
	}
	if err != nil {
		n.reject(c, s, err)
		return
	}
	if r.kind == kMemberPromote && n.prom != nil {
		n.prom.LearnerPromotes.Inc()
	}
	var body []byte
	list := 2
	if r.kind == kMemberAdd {
		if i := n.memberAt(r.id); i >= 0 {
			body = appendBytesField(body, 2, appendMember(nil, &n.members[i]))
		}
		list = 3
	}
	for i := range n.members {
		body = appendBytesField(body, list, appendMember(nil, &n.members[i]))
	}
	var w msgW
	// etcd's cluster calls name no revision in their header.
	n.beginReply(c, s, &w, sizeBytesField(1, n.hdrSizeAt(0))+len(body))
	n.writeHdrAt(&w, 0)
	w.write(body)
	n.endReply(c, s, &w)
}

// expireMemberRequests answers the membership changes not applied in
// requestTimeout as a write that timed out: Raft may drop a change while
// another is pending.
func (n *node) expireMemberRequests() {
	for seq, r := range n.memberReqs {
		if n.now.Sub(r.at) >= requestTimeout {
			delete(n.memberReqs, seq)
			n.replyErrorH(r.h, codeUnavailable, msgTimeout)
		}
	}
}

// learnerServes reports whether a learner serves a request of kind:
// etcd's learners serve only Status, MemberList and serializable ranges,
// which the range checks itself.
func learnerServes(kind uint8) bool {
	switch kind {
	case kStatus, kMemberList, kRange, kRangeStream:
		return true
	}
	return false
}

// isLearner reports whether the node is a learner in the member list.
func (n *node) isLearner() bool {
	i := n.memberAt(n.id)
	return i >= 0 && n.members[i].learner
}
