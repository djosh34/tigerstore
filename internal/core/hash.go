package core

import (
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
