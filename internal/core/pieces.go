package core

import (
	"github.com/djosh34/tigerstore/internal/mvcc"
)

// A large answer is built a piece at a time (#505). Its first reply has
// Rest set and holds the first piece, with the count and the encoded size
// of the whole answer. The receiver asks for each next piece with Next
// once it took the one before, so a slow client holds back the work, and
// an answer holds at most one piece in memory. An answer that fits in one
// piece is a reply as any other.

// PieceBytes is about the most one piece of an answer holds: its keys and
// values as the message encodes them.
const PieceBytes = 1 << 20

// pieced is the rest of an answer that goes out in pieces: the cursors of
// its ranges, in the order its message holds them.
type pieced struct {
	cursors []*mvcc.Cursor
}

// Next builds the next piece of the answer to request id, for the next
// Effects: a Reply with the piece's KVs in Range.KVs, and Rest set while
// more follow. Its values wait for a read from the log as a reply's do.
// A compaction past the answer's revision ends it with ErrCompacted. An
// answer that ended, or that the node dropped, gets nothing.
func (n *Node) Next(id uint64) {
	p, ok := n.pieced[id]
	if !ok || n.err != nil {
		return
	}
	kvs, done, err := p.cursors[0].Next(n.cfg.PieceBytes)
	if err != nil && !isAnswer(err) {
		// A value that cannot be read never reaches a client.
		n.fail(err)
		return
	}
	if done {
		p.cursors = p.cursors[1:]
	}
	rep := Reply{ID: id, Err: err, Range: mvcc.RangeResult{KVs: kvs}, Rest: err == nil && len(p.cursors) > 0}
	if !rep.Rest {
		delete(n.pieced, id)
	}
	n.sendPiece(rep, n.kv.TakeFills())
}

// sendPiece sends a reply that holds a piece, at once or once a helper
// read the values in fills.
func (n *Node) sendPiece(r Reply, fills []mvcc.Fill) {
	if len(fills) == 0 {
		n.out.Replies = append(n.out.Replies, r)
		return
	}
	v := n.readValues(fills)
	v.replies = append(v.replies, r)
}

// startPieces keeps the cursors of an answer whose first reply r goes out
// with Rest set.
func (n *Node) startPieces(r *Reply, cursors []*mvcc.Cursor) {
	if len(cursors) == 0 {
		return
	}
	r.Rest = true
	n.pieced[r.ID] = &pieced{cursors: cursors}
}
