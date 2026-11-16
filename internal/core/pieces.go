package core

// A large answer is built a piece at a time (#505). Its first reply has
// MorePieces set and holds the first piece, with the count and the
// encoded size of the whole answer. The receiver asks for each next
// piece with Next once it took the one before, so a slow client holds back
// the work, and an answer holds at most one piece in memory. An answer that fits in one
// piece is a reply as any other.

// PieceBytes is about the most one piece of an answer holds: its keys and
// values as the message encodes them.
const PieceBytes = 1 << 20

// pieced is the rest of an answer that goes out in pieces: the cursors of
// its ranges, in the order its message holds them.
type pieced struct{}

// startPieces keeps the cursors of r, a reply that goes out, and sets
// MorePieces if it has any: those of a Range, or of the Rests of a Txn
// or a DeleteRange.
func (n *Node) startPieces(r *Reply) {
	cursors := r.cursors
	for _, rest := range r.Rests {
		cursors = append(cursors, rest.Cursor)
	}
	r.cursors = nil
	if len(cursors) == 0 {
		return
	}

	n.pieced[r.ID] = &pieced{}
}
