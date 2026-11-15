# Raft v3.7.0, forked for tigerstore

This is `go.etcd.io/raft/v3` at v3.7.0, Apache-2.0 (see `LICENSE`). The root `go.mod` uses it through a `replace` directive. Upstream's TLA+ specs and scripts are left out.

## The one change

`raft.appendEntry` no longer clones the entries it appends with `proto.Clone`.
- **Before:** on crd-heavy, the clone and its copy of every entry's data cost the leader about 0.2 µs per op ([#602](https://github.com/djosh34/tigerstore/issues/602)).
- **Now:** each entry gets a new `Entry` with its own type, term and index, all in one allocation per call. The new entry shares the proposed data. A nil type stays nil, as the clone kept it, so the entries encode as before.

## What the caller owes

Whoever proposes hands the data over and never changes it after. In tigerstore:
- **Local proposals:**
  - the slow path's batches are encoded fresh (`core.encodeBatch`);
  - the fast path's open batch is dropped once it is sealed (`core.sealOpen`);
  - configuration changes are marshalled fresh.
- **Peer messages:** `core.Node.Step` gives every message memory of its own (`core.Own`) before Raft sees it. That includes a proposal a follower forwarded, whose bytes the peer transport lends from its receive ring.

## Tests

Upstream's tests run as they are: `cd third_party/raft && go test ./...`.
