package core

import (
	"slices"

	"github.com/djosh34/tigerstore/internal/mvcc"
)

// keptHashes is how many compaction hashes a node keeps, as etcd's
// hashStorageMaxSize.
const keptHashes = 10

// keepHash keeps the hash a compaction took, for HashKV and the
// compaction hash check.
func (n *Node) keepHash(h mvcc.KVHash) {
	n.hashes = append(n.hashes, h)
	if len(n.hashes) > keptHashes {
		n.hashes = n.hashes[1:]
	}
}

// hashKV answers HashKV as etcd's hash storage does: with the hash a
// compaction took at that revision, if one did, or one taken now.
func (n *Node) hashKV(rev int64) (mvcc.KVHash, error) {
	for _, h := range n.hashes {
		if rev == h.Revision {
			return h, nil
		}
	}
	return n.kv.HashKV(rev)
}

// CompactHashes are the hashes the last compactions took, oldest first,
// for the compaction hash check.
func (n *Node) CompactHashes() []mvcc.KVHash { return slices.Clone(n.hashes) }
