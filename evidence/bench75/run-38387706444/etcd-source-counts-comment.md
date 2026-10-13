### Stock etcd source counts

[Etcd release audit](@BLOB@/etcd-source-audit/audit.md) and [verified sites](@BLOB@/etcd-source-audit/verified-sites.md) bind the exact release and its stock dependency buildinfo. The fixed core/request/reply count is **3 sends, 3 receives, 2 closes and 2 fresh reply channels per Put or Txn**, plus batched/conditional work. The Raft proposal acceptance result is separate from the applied response.

| Fixed handoff | Send | Receive | Close and creation |
|---|---|---|---|
| Proposal | raft v3.7.0 `node.go:531` | raft `node.go:386` | none |
| Proposal acceptance | raft `node.go:391` | raft `node.go:541` | close `node.go:392`; create `node.go:528` |
| Applied response | etcd `pkg/wait/wait.go:83` | `server/etcdserver/v3_server.go:1123` | close `pkg/wait/wait.go:84`; create `pkg/wait/wait.go:65` |

For N requests and R Ready cohorts, the enumerated stable-stream sends are `3N+4R+Ws+O+2I` and receives `3N+4R+Wr+O+2I`. Ws/Wr count coalesced scheduler wakes, O outbound and I inbound peer messages. Divide by N, but do not assume R or a batch size. Every additional counted handoff is:

| Batched/conditional handoff | Send | Receive |
|---|---|---|
| Ready | raft `node.go:435` | `server/etcdserver/raft.go:185` |
| Apply cohort | `server/etcdserver/raft.go:232` | `server/etcdserver/server.go:843` |
| WAL completion | leader `server/etcdserver/raft.go:327`, follower alternative `server/etcdserver/raft.go:302` | `server/etcdserver/server.go:983` |
| Advance | raft `node.go:557` | raft `node.go:443` |
| Scheduler wake | `pkg/schedule/schedule.go:116`, conditional | `pkg/schedule/schedule.go:174` |
| Outbound peer stream | `server/etcdserver/api/rafthttp/peer.go:247` | `server/etcdserver/api/rafthttp/stream.go:207` |
| Inbound peer stream | `server/etcdserver/api/rafthttp/stream.go:530` | `server/etcdserver/api/rafthttp/peer.go:177` |
| Peer processor to Raft | raft `node.go:517` | raft `node.go:395` |

Background ticks remain separate, ticker receive at `server/etcdserver/raft.go:183`, successful tick send at raft `node.go:460`, and tick receive at raft `node.go:433`. A tick send can fail. No-snapshot normal Ready work does not use the raftAdvanced channel. Stock gRPC also closes one deadline synchronization channel per timed request at grpc v1.83.2 `internal/transport/http2_server.go:615`; that library close is outside the fixed core table. Its timeout callback receive does not fire here. Other context/gRPC lifecycle channels remain unenumerated, not zero.

Etcd has **8 known immediate leader new-value stages per Put**, excluding peers, optional codec coalescing and deferred backend pages. Every counted stage is:

1. gRPC's owning bytes decode, grpc v1.83.2 `encoding/proto/proto.go:96` and protobuf v1.36.11 `internal/impl/codec_gen.go:5497`.
2. InternalRaftRequest encoding at `server/etcdserver/v3_server.go:1093`.
3. Retained Raft clone at raft v3.7.0 `raft.go:817`, using protobuf `internal/impl/merge.go:188`.
4. WAL Entry encoding at `server/storage/wal/wal.go:976`.
5. WAL Record encoding at `server/storage/wal/encoder.go:84` or `server/storage/wal/encoder.go:89`.
6. WAL PageWriter staging at `pkg/ioutil/pagewriter.go:59` and `pkg/ioutil/pagewriter.go:72` as applicable. These short records cannot take the direct-page bypass. Prefix/tail chunks total one value equivalent.
7. Owning apply decode at `server/etcdserver/apply/apply.go:29`, using the same protobuf bytes decoder.
8. Backend KeyValue encoding at `server/storage/mvcc/kvstore_txn.go:250`.

The protobuf bytes encoding instruction is `encoding/protowire/wire.go:455`. The call and its implementing instruction count once. Backend buffer and bbolt Put retain value slices. Deferred bbolt serialization adds one stage per actual KV occurrence in a leaf page at bbolt v1.5.0 `internal/common/inode.go:101`, reached through `node.go:199` and `node.go:334`. Older residents can be serialized again. Remapping can copy resident values at `node.go:479`. These populations are not a fixed request-time count.

A successful etcd CAS has **8 new-input stages plus 1 stored-value stage**. Even a MOD compare executes a backend Range at `server/etcdserver/txn/txn.go:274` and owns the old KeyValue.Value at `server/storage/mvcc/kvstore_txn.go:142`. A failed compare has **7 new-input stages plus 3 stored-value stages**. It omits the new backend KV encoding, repeats the stored-value decode for its else-Range, then encodes that old value in the reply through grpc `server.go:1189` and `encoding/proto/proto.go:67` or `encoding/proto/proto.go:75`. Measured failures were zero. The summed stages cover distinct payload identities, not nine copies of the proposed value.

Optional grpc codec coalescing adds a new-input copy when multiple buffers need one destination at grpc `mem/buffer_slice.go:115`, implemented at `mem/buffer_slice.go:82`. A single buffer aliases instead. Each transmitted peer occurrence adds an encoding, MsgAppV2 alternatives at `server/etcdserver/api/rafthttp/msgappv2_codec.go:114`, `server/etcdserver/api/rafthttp/msgappv2_codec.go:122` or `server/etcdserver/api/rafthttp/msgappv2_codec.go:143`. Each received occurrence adds a decode at `server/etcdserver/api/rafthttp/msgappv2_codec.go:227` or `server/etcdserver/api/rafthttp/msgappv2_codec.go:243`. General-stream alternatives are `server/etcdserver/api/rafthttp/msg_codec.go:38` and `server/etcdserver/api/rafthttp/msg_codec.go:69`. A newly persisted/applied follower adds its WAL, apply and backend stages. The audit separates these conditional populations. Different frontend boundaries and excluded transport work mean **tigerstore 4 versus etcd 8 is not an end-to-end physical-copy ratio**.
