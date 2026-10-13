# Dependency proof — exact final measurement targets

Dependency sources are not repository-commit files. The module/version + module-relative path is the citation identity. Each final commit uses the same listed version and matching go.sum hash. `dependency-source-manifest.json` records the original cache path and SHA256 of each copied source. `verify-dependencies.py` independently recomputes the Go module ZIP h1 (sorted per-file SHA256/name lines), checks each target go.sum and cache .ziphash, and verifies evidence files equal both ZIP and cache bytes. No Go command, build or download was used. `dependency-integrity.json` preserves the results.

| Target | Commit |
| --- | --- |
| main | `e1fd889c345bc3c06d8100eb6d635c88ad6967cd` |
| batch | `29c3f8f95e3c9bcecf5bb39aa1f3bca49fe99c1f` |
| wire | `f2edb7b2b1e81f65d7ca7592638654f09217b933` |

| Module | Version | Computed ZIP h1 |
| --- | --- | --- |
| go.etcd.io/etcd/api/v3 | v3.7.2 | `h1:xgt/6el1LsPWWYNLkhMAK4tZm6dF+1sCqDecpE5gdbk=` |
| go.etcd.io/raft/v3 | v3.7.0 | `h1:BGzlwx07bLv8PW6OU5HObuz1y4hlPZUXA07pM1mPUh4=` |
| golang.org/x/net | v0.58.0 | `h1:ynWG7rqYi4ccpTEuPZ2QGWHktVEM9DMCj9yzDE0Q7To=` |
| google.golang.org/grpc | v1.84.0 | `h1:soMyaPJ8pAak5PIQ0DGBUir0XRo2fRoMqhNWMLlLxO0=` |
| google.golang.org/protobuf | v1.36.11 | `h1:fV6ZwhNocDyBLK0dj+fg8ektcVegBBuEolpbTQyBNVE=` |

The project's go.mod has no replacement for these modules; main differs only in x/net being indirect rather than direct. go.sum is identical across the three final targets. Copied dependency go.mod files also show the relevant cross-pins (Raft/API/grpc protobuf v1.36.11; no higher cross-version for these cited dependencies). These source conclusions concern the archived module pins, not a disassembly or assertion about compiler/runtime physical copy expansion.

## Codec/ownership chain

| Proof | Pinned file:line (same dependency in M/B/W) | Effect / applicable path |
| --- | --- | --- |
| PutRequest.Value descriptor | `dependency-sources/etcd-api-v3.7.2/etcdserverpb/rpc.pb.go:760` | proto3 nonempty bytes |
| Main generated decoder call | `dependency-sources/etcd-api-v3.7.2/etcdserverpb/rpc_grpc.pb.go:248` | allocates PutRequest then dec(in), only main frontend uses this adapter |
| grpc protobuf codec | `dependency-sources/grpc-v1.84.0/encoding/proto/proto.go:96` | main proto.Unmarshal of request bytes |
| Proto3 bytes codec selection | `dependency-sources/protobuf-v1.36.11/internal/impl/codec_tables.go:278` | no-presence []byte uses coderBytesNoZero |
| Proto3 Put.Value owning decode | `dependency-sources/protobuf-v1.36.11/internal/impl/codec_gen.go:5497` | append(nil, v...) full copy: M/B frontend + M/B apply; wire Put bypasses |
| Proto3 bytes encoder boundary | `dependency-sources/protobuf-v1.36.11/internal/impl/codec_gen.go:5483` | AppendBytes used for entry encoding M/B; no duplicate stage |
| Actual bytes encoder | `dependency-sources/protobuf-v1.36.11/encoding/protowire/wire.go:455` | append length-prefixed v to output: M/B entry + all peer Entry.Data encode |
| RawNode proposal | `dependency-sources/raft-v3.7.0/rawnode.go:94` | Data initially aliases caller data; not the end of leader path |
| Leader proposal dispatch | `dependency-sources/raft-v3.7.0/raft.go:1349` | calls appendEntry |
| Leader clone call | `dependency-sources/raft-v3.7.0/raft.go:817` | proto.Clone Entry once, all targets |
| Entry protocol syntax | `dependency-sources/raft-v3.7.0/raftpb/raft.proto:1` | Entry is proto2 |
| Entry.Data descriptor | `dependency-sources/raft-v3.7.0/raftpb/raft.pb.go:348` | bytes with presence, NOT proto3 Value field |
| Proto2 bytes coder selection | `dependency-sources/protobuf-v1.36.11/internal/impl/codec_tables.go:428` | presence []byte uses coderBytes |
| Clone implementation | `dependency-sources/protobuf-v1.36.11/proto/merge.go:58` | new destination then mergeMessage |
| Actual proto2 clone copy | `dependency-sources/protobuf-v1.36.11/internal/impl/merge.go:188` | mergeBytes append(emptyBuf[:], *src.Bytes()...): full Entry.Data copy, all leaders |
| Actual proto2 owning decode | `dependency-sources/protobuf-v1.36.11/internal/impl/codec_gen.go:5418` | consumeBytes append(emptyBuf[:], v...): full Entry.Data copy, all peer receivers |
| Wire parse consume | `dependency-sources/protobuf-v1.36.11/encoding/protowire/wire.go:468` | subslice only; no copy on wire frontend/apply |
| MemoryStorage append alternative 1 | `dependency-sources/raft-v3.7.0/storage.go:318` | entry pointers only; zero Data copy |
| MemoryStorage append alternative 2 | `dependency-sources/raft-v3.7.0/storage.go:320` | entry pointers only; zero Data copy |
| Outbound append construction | `dependency-sources/raft-v3.7.0/raft.go:656` | entry pointers aliased, before peer codec materialization |
| Follower append path | `dependency-sources/raft-v3.7.0/raft.go:1800` | maybeAppend, not leader appendEntry clone |
| Follower log append | `dependency-sources/raft-v3.7.0/log.go:127` | pointer entries passed to append |
| Unstable log append | `dependency-sources/raft-v3.7.0/log_unstable.go:205` | []*Entry append, not Data bytes |
| Async append message | `dependency-sources/raft-v3.7.0/rawnode.go:228` | rd.Entries retained as pointers |
| Async apply message | `dependency-sources/raft-v3.7.0/rawnode.go:393` | ents retained as pointers |
| Conditional grpc materialization | `dependency-sources/grpc-v1.84.0/mem/buffer_slice.go:115` | multi-buffer only; excluded fixed subtotal |
| Actual grpc CopyTo instruction | `dependency-sources/grpc-v1.84.0/mem/buffer_slice.go:82` | chunks copied into one destination, one full-request equivalent |

**Critical distinction:** `protobuf-v1.36.11/internal/impl/merge.go:188` is the Entry.Data proto2 deep-clone byte copy. `internal/impl/codec_gen.go:5497` is Put.Value proto3 owning decode, not that clone. Entry.Data peer receive uses its proto2 decoder at `codec_gen.go:5418`. `AppendBytes` at `encoding/protowire/wire.go:455` copies during encoding; `ConsumeBytes` at `:468` aliases during wire parsing. None of these lines can be substituted for another stage merely because all operate on byte slices.

The default generated message fast-path is established by the byte-field descriptors and codec selection. Clone's documented deep-copy semantics and merge fallback also require independent byte ownership; the identified fast-path instruction is not a speculative optimization. The source proof does not count extra growth relocations or physical transport/runtime copies.

