# Dependency proof — immutable source binding

**FINAL measurement binding — requester-confirmed; branch heads are NOT substituted.**

| Label | Role | Exact audited commit | Source directory |
|---|---|---|---|
| M | main | `8efd83a173394dd94a3981c72b4c51a1f3d061f1` | `sources/main/` |
| W | wire | `936bc5b69ae8a0c59585521734b41222006a0a2c` | `sources/wire/` |
| H | hpack | `129e49a842b4802e9a36b74c830b3538525f846f` | `sources/hpack/` |
| T | txn | `6179f12c2bbaaffc9960782533ad83c292605962` | `sources/txn/` |

Requester-confirmed resolver binding: **2026-10-13T11:04:41Z**, [Actions run 38387706444](https://github.com/djosh34/tigerstore/actions/runs/38387706444). Only these four exact commits are audited in the final report. PR77 uses retained `refs/pull/77/head` after its branch deletion; main includes merged PR75, PR76 and PR77. Main and PR77 have byte-identical production sources (indeed identical complete Git tree IDs, independently checked in `tree-control.json`): they are a **same-code control**, not a claimed source optimization comparison. Later branch heads are not substituted.

Abandoned run **38387063293** used an empty Put CANDIDATE after PR77 branch deletion and is **not** measurement binding, including any CAS result from it. Older pre-merge preliminary files retained under `earlier-preliminary/` are provenance only, explicitly excluded from final counts/sites.


All target go.mod files directly pin the versions below; no target replace/exclude/toolchain override is present. The relevant copied dependency go.mod cross-pins do not select a higher version of these cited dependencies. This verifies source pins, not the measured binary: build/module identities belong in runtime evidence. Module ZIP h1 is recomputed without Go as SHA256 of sorted SHA256/name lines (Go x/mod dirhash.Hash1). Each copied evidence file equals its module ZIP member and cache source; every h1 matches every target go.sum and cache .ziphash. See dependency-integrity.json and dependency-source-manifest.json.

| Module | Recomputed ZIP h1 |
|---|---|
| `go.etcd.io/etcd/api/v3@v3.7.2` | `h1:xgt/6el1LsPWWYNLkhMAK4tZm6dF+1sCqDecpE5gdbk=` |
| `go.etcd.io/raft/v3@v3.7.0` | `h1:BGzlwx07bLv8PW6OU5HObuz1y4hlPZUXA07pM1mPUh4=` |
| `golang.org/x/net@v0.58.0` | `h1:ynWG7rqYi4ccpTEuPZ2QGWHktVEM9DMCj9yzDE0Q7To=` |
| `google.golang.org/grpc@v1.84.0` | `h1:soMyaPJ8pAak5PIQ0DGBUir0XRo2fRoMqhNWMLlLxO0=` |
| `google.golang.org/protobuf@v1.36.11` | `h1:fV6ZwhNocDyBLK0dj+fg8ektcVegBBuEolpbTQyBNVE=` |

## Exact owning encode/decode, Raft clone and alias reflection sites
| Site | Module-bound file:line | Exact text |
|---|---|---|
| lib-put-value-field | `dependency-sources/etcd-api-v3.7.2/etcdserverpb/rpc.pb.go:760` | <code>Value []byte `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`</code> |
| lib-returned-value-field | `dependency-sources/etcd-api-v3.7.2/mvccpb/kv.pb.go:84` | <code>Value []byte `protobuf:"bytes,5,opt,name=value,proto3" json:"value,omitempty"`</code> |
| lib-proto3-codec-selection | `dependency-sources/protobuf-v1.36.11/internal/impl/codec_tables.go:278` | <code>return nil, coderBytesNoZero</code> |
| lib-proto3-decode-copy | `dependency-sources/protobuf-v1.36.11/internal/impl/codec_gen.go:5497` | <code>*p.Bytes() = append(([]byte)(nil), v...)</code> |
| proto3-bytes-encoder | `dependency-sources/protobuf-v1.36.11/internal/impl/codec_gen.go:5483` | <code>b = protowire.AppendBytes(b, v)</code> |
| lib-bytes-encode-copy | `dependency-sources/protobuf-v1.36.11/encoding/protowire/wire.go:455` | <code>return append(AppendVarint(b, uint64(len(v))), v...)</code> |
| lib-bytes-consume-alias | `dependency-sources/protobuf-v1.36.11/encoding/protowire/wire.go:468` | <code>return b[n:][:m], n + int(m)</code> |
| lib-raft-propose-alias | `dependency-sources/raft-v3.7.0/rawnode.go:94` | <code>Entries: []*pb.Entry{{Data: data}},</code> |
| lib-raft-propose-clone-call | `dependency-sources/raft-v3.7.0/raft.go:1349` | <code>if !r.appendEntry(m.GetEntries()...) {</code> |
| lib-raft-clone | `dependency-sources/raft-v3.7.0/raft.go:817` | <code>cloned[i] = proto.Clone(es[i]).(*pb.Entry)</code> |
| lib-entry-proto2 | `dependency-sources/raft-v3.7.0/raftpb/raft.proto:1` | <code>syntax = "proto2";</code> |
| lib-entry-data-field | `dependency-sources/raft-v3.7.0/raftpb/raft.pb.go:348` | <code>Data          []byte                 `protobuf:"bytes,4,opt,name=Data" json:"Data,omitempty"`</code> |
| lib-proto2-codec-selection | `dependency-sources/protobuf-v1.36.11/internal/impl/codec_tables.go:428` | <code>return nil, coderBytes</code> |
| lib-proto-clone-merge | `dependency-sources/protobuf-v1.36.11/proto/merge.go:58` | <code>mergeOptions{}.mergeMessage(dst, src)</code> |
| lib-proto2-clone-copy | `dependency-sources/protobuf-v1.36.11/internal/impl/merge.go:188` | <code>*dst.Bytes() = append(emptyBuf[:], *src.Bytes()...)</code> |
| lib-proto2-decode-copy | `dependency-sources/protobuf-v1.36.11/internal/impl/codec_gen.go:5418` | <code>*p.Bytes() = append(emptyBuf[:], v...)</code> |
| lib-storage-pointer-append1 | `dependency-sources/raft-v3.7.0/storage.go:318` | <code>ms.ents = append(ms.ents[:offset:offset], entries...)</code> |
| lib-storage-pointer-append2 | `dependency-sources/raft-v3.7.0/storage.go:320` | <code>ms.ents = append(ms.ents, entries...)</code> |
| lib-peer-construction-alias | `dependency-sources/raft-v3.7.0/raft.go:656` | <code>Entries: ents,</code> |
| follower-maybeappend | `dependency-sources/raft-v3.7.0/raft.go:1800` | <code>if mlastIndex, ok := r.raftLog.maybeAppend(a, m.GetCommit()); ok {</code> |
| follower-logappend | `dependency-sources/raft-v3.7.0/log.go:127` | <code>l.append(a.entries[ci-offset:]...)</code> |
| unstable-append | `dependency-sources/raft-v3.7.0/log_unstable.go:205` | <code>u.entries = append(u.entries, ents...)</code> |
| async-append-alias | `dependency-sources/raft-v3.7.0/rawnode.go:228` | <code>Entries: rd.Entries,</code> |
| async-apply-alias | `dependency-sources/raft-v3.7.0/rawnode.go:393` | <code>Entries: ents,</code> |
| reflect-set | `dependency-sources/protobuf-v1.36.11/internal/impl/message_reflect_gen.go:97` | <code>fi.set(m.pointer(), v)</code> |
| reflect-scalar-set | `dependency-sources/protobuf-v1.36.11/internal/impl/message_reflect_field.go:317` | <code>rv.Set(conv.GoValueOf(v))</code> |
| reflect-bytes-convert | `dependency-sources/protobuf-v1.36.11/internal/impl/convert.go:358` | <code>return reflect.ValueOf(v.Bytes()).Convert(c.goType)</code> |

**Critical distinction:** proto2 Entry.Data deep clone uses mergeBytes append(emptyBuf[:], src...). Proto3 Put.Value owning decode uses append(nil, v...). Peer Entry.Data owning decode uses proto2 consumeBytes. AppendBytes copies to a destination; ConsumeBytes returns a subslice. A call plus its dependency instruction is one materialization. None can be substituted for another just because they operate on []byte.

**PR80 reflection proof:** repository scalar produces ValueOfBytes(slices.Clip(x)), Set dispatches fi.set, the scalar setter uses rv.Set(conv.GoValueOf(v)), and bytesConverter returns reflect.ValueOf(v.Bytes()).Convert(c.goType). Put.Value is native []byte, so conversion/set preserves its backing store. Mutable and AppendMutable still allocate/build submessage objects; zero value copies is not zero struct construction. The caller-owned input must not change. String/unknown-field materializations are conditional and outside canonical bytes-value scope.

**Follower ownership:** MsgApp uses maybeAppend and append of []*Entry references rather than leader proposal appendEntry clone. Peer receiver owns its proto2-decoded Entry.Data. The WAL stages it separately; storage/index alias that receiver-owned entry (for wire/alias apply). Leader successful wire/alias apply aliases its Raft-deep-cloned entry, not frontend or WAL staging bytes.

