# Leaderless source comparison

Inspected main at `b5f2e28ef2edd94393d62952b1568763ebc12fe2`, etcd v3.7.2 at `68c065e562994b89e333e77b039ad066f933c586`, and the shared raft v3.7.0 dependency at `b867cf13f6bc0dae21204302df97bc2355c3af55`. No product changes.

## Mutation admission explains a difference

Etcd uses raft's asynchronous `Node.Propose`. Its [run loop](https://github.com/etcd-io/raft/blob/b867cf13f6bc0dae21204302df97bc2355c3af55/node.go#L343-L380) disables the proposal channel when it has no leader. [`Propose` and `stepWithWaitOption`](https://github.com/etcd-io/raft/blob/b867cf13f6bc0dae21204302df97bc2355c3af55/node.go#L471-L549) therefore wait for a leader or context cancellation before the proposal can enter raft. Etcd [bounds this call with its request timeout](https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/server/etcdserver/v3_server.go#L1108-L1133). A request may succeed if quorum returns before its deadline.

Tigerstore calls [`RawNode.Propose`](https://github.com/etcd-io/raft/blob/b867cf13f6bc0dae21204302df97bc2355c3af55/rawnode.go#L89-L96) directly from [its batch proposal function](https://github.com/djosh34/tigerstore/blob/b5f2e28ef2edd94393d62952b1568763ebc12fe2/internal/core/node.go#L584-L611). RawNode immediately steps `MsgProp`. A [candidate](https://github.com/etcd-io/raft/blob/b867cf13f6bc0dae21204302df97bc2355c3af55/raft.go#L1683-L1686) or [leaderless follower](https://github.com/etcd-io/raft/blob/b867cf13f6bc0dae21204302df97bc2355c3af55/raft.go#L1718-L1730) returns `ErrProposalDropped`. Tigerstore translates that failure to `Unavailable`, `etcdserver: no leader`, and removes the waiting requests. This particular rejected batch cannot later apply. It never entered raft.

These source paths predict an error-timing and admission difference, not a linearizability violation. Rejecting a mutation without changing the state is legal during quorum loss. The existing Jepsen report preserves the conservative upstream classification of `Unavailable` mutations as unknown. The source finding does not justify rewriting those histories. A timed-out mutation accepted before leader loss may still apply, and a gRPC error alone does not identify the server path.

## Require-leader metadata agrees in the normal case

[`WithRequireLeader`](https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/client/v3/ctx.go#L26-L37) sends `hasleader=true`. It does not mean that the endpoint must itself be the leader. Both servers check whether the endpoint knows a leader.

- Etcd's [unary interceptor](https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/server/etcdserver/api/v3rpc/interceptor.go#L47-L78) rejects every unary call with this metadata when `Leader()==0`, including an otherwise local serializable read, MemberList, and Status.
- Tigerstore's [general unary path](https://github.com/djosh34/tigerstore/blob/b5f2e28ef2edd94393d62952b1568763ebc12fe2/internal/server/stream.go#L326-L344) and [batched unary path](https://github.com/djosh34/tigerstore/blob/b5f2e28ef2edd94393d62952b1568763ebc12fe2/internal/server/batch.go#L47-L60) perform the same check. Its [HTTP/2 decoder](https://github.com/djosh34/tigerstore/blob/b5f2e28ef2edd94393d62952b1568763ebc12fe2/internal/server/h2.go#L501-L502) recognizes `hasleader=true`.
- Etcd's [stream interceptor](https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/server/etcdserver/api/v3rpc/interceptor.go#L215-L267) rejects new require-leader streams while leaderless. Its [monitor](https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/server/etcdserver/api/v3rpc/interceptor.go#L315-L355) cancels existing required streams after three consecutive no-leader checks, one election timeout apart.
- Tigerstore's [stream start and monitor](https://github.com/djosh34/tigerstore/blob/b5f2e28ef2edd94393d62952b1568763ebc12fe2/internal/server/stream.go#L346-L382) follow the same policy. Its one-second checks match the fixed one-second election timeout used by its executable and the default etcd configuration.

Plain watches have no require-leader guard. They may remain open on an isolated member and resume delivery after recovery. A plain watch remaining open without fresh events is not evidence of a consistency violation. The probe requires a post-heal sentinel on both the existing plain watch and a plain watch created while leaderless.

## Probe boundaries

The throwaway probe uses stock protobuf gRPC clients, one fixed endpoint, `grpc.WithDisableRetry`, and no etcd client reconnection code. Each runner first runs etcd, then tigerstore. Each store gets fresh three-node storage for a leader-survivor case and a follower-survivor case. The probe stops two processes, requires `/proc` state `T`, and observes `Status.Leader==0` before sending its leaderless requests. Both stores use peer mTLS and client HTTP with default Raft timing.

The checks cover direct Put and CAS Txn, linearizable and serializable Range, MemberList, Status, new and existing watches with and without require-leader metadata, quorum recovery, and an acknowledged seed. A plain Put submitted while leaderless gets a five-second deadline; the probe resumes the stopped processes after 300 ms. A success must read back. An error remains unknown. Three independent runner repetitions repeat each role case twice beyond the initial observation.

This is a targeted reliability check, not Jepsen certification. It does not decide any saved history whose checker ran out of memory.
