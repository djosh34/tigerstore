## Pre-work: what protocol semantics and test faults should #47 use?

[Throwaway branch](https://github.com/djosh34/tigerstore/tree/exp/pre-47-semantics/experiments/pre-47-semantics). Nothing is for merging. It contains the probe, raw TLS responses, request traces, a 75-file source/hash manifest and the proposed mutant inventory.

I read #46, #1, #47, #33 and their comments, the named compatibility decisions #3, #7 and #18, and `docs/design.md` on main at `91c4f974a6661ac02c2e7747781ea215353c643b`. The following pins match the tags chosen in #33, not every patch release in each minor:

| Source | Commit | Actual etcd client dependency |
|---|---|---|
| etcd v3.7.2, server, API and client | `68c065e562994b89e333e77b039ad066f933c586` | v3.7.2 for the reference probe |
| Kubernetes v1.35.0 | `66452049f3d692768c39c797b21b793dce80314e` | v3.6.5, [go.mod lines 59-61][deps35] |
| Kubernetes v1.36.0 | `ecf6decece6a6de25a57aad9ba90b6ce580f6f78` | v3.6.8, [go.mod lines 56-58][deps36] |
| Kubernetes v1.37.1 | `f78e722310e50bcaca9276be22276d9e91d91308` | v3.7.0, [go.mod lines 56-58][deps37] |

The three vendored `client/v3/kubernetes/client.go` files are byte-identical. So are the three lease managers and compactors. The manifest records their hashes. This matters because the Kubernetes client is not necessarily the reference server's client version.

### 1. RPCs and options actually used

This inventory covers the production etcd3 storage, its feature checker and the storagebackend client factory. It does not claim that every Kubernetes component sharing an etcd endpoint uses this subset.

The common helper implementation is at [1.35 lines 47-126][helper35], [1.36 lines 47-126][helper36], [1.37 lines 47-126][helper37], and [etcd 3.7.2 lines 47-126][helper372]. The protobuf encoder is [etcd client `op.go` lines 177-231][opencode].

| RPC or selected Txn operation | Request fields |
|---|---|
| `KV.Range`, ordinary object Get and current-revision reads | Exact `key`, empty `range_end`, `limit=1`, `revision=0`. A nonrecursive list can pass a requested revision instead. |
| `KV.Range`, recursive List | `key=prefix` or the continuation key, `range_end=GetPrefixRangeEnd(originalPrefix)`, requested `limit`, requested `revision`. |
| `KV.Range`, initial watch snapshot | Exact key with default limit 0, or prefix range with initial page limit 10,000. Subsequent pages start at `lastKey + "\x00"` and pin `revision` to the first response header. |
| `KV.Range`, Count | Prefix range, `count_only=true`, limit/revision 0. |
| `KV.Range`, GetListKeys | Prefix range, `keys_only=true`, limit/revision 0. |
| `KV.Range`, health probe | Exact `path.Join("/", prefix, "health")`, with default options. An absent key is fine. This is not HTTP `/health`. |
| `KV.Range`, compaction coordination | Exact `compact_rev_key`, default options, both standalone and in the failed Txn branch. |
| `KV.Txn`, create | One exact-key compare, `target=MOD`, `result=EQUAL`, `mod_revision=0`; success contains one Put. Failure is empty. |
| `KV.Txn`, update | One exact-key MOD/EQUAL compare against the object's observed mod revision; success contains one Put; failure contains one exact-key Range when `GetOnFailure=true`. |
| `KV.Txn`, delete | Same MOD/EQUAL compare; success contains one exact-key DeleteRange, not a prefix delete; failure contains one exact-key Range when `GetOnFailure=true`. |
| Put inside the object Txn | `key`, opaque `value`, `lease=0` or a granted ID. `prev_kv=false`, `ignore_value=false`, `ignore_lease=false`. |
| DeleteRange inside the object Txn | Exact `key`, empty `range_end`, `prev_kv=false`. |
| `KV.Txn`, compactor | One exact-key `VERSION/EQUAL` compare on `compact_rev_key`; success Put stores the decimal prior sampled revision, without a lease; failure Range reads that key. |
| `Lease.LeaseGrant` | Requested padded TTL, `ID=0`, so the server chooses the ID. |
| `KV.Compact` | Prior sampled revision, `physical=false`. Kubernetes does not use `WithCompactPhysical`. |
| `Maintenance.Status` | Empty `StatusRequest`, sent to a specified endpoint. Uses are below. |
| `KV.RangeStream`, 1.37 only | Uses a `RangeRequest`, not a separate continuation protocol. Recursive lists send prefix range, optional revision and limit, and optional continuation start key. Initial recursive watch snapshots send the prefix range with default revision/limit. Return gRPC `Unimplemented` to trigger the supported fallback. |

All those Range requests leave `serializable=false`, sort order `NONE` and sort target `KEY`, and all min/max create/mod revision filters at zero. There is no requested custom sort. Default range iteration is bytewise key order. The client represents a prefix's upper bound by incrementing the last non-0xff byte, or `"\x00"` for an unbounded upper range. See [option construction lines 398-442][prefix]. `Count` describes the matching range, not just the limited page. Return it on ordinary List too, even though `count_only=false`: 1.35 and 1.36 calculate whether more results exist from `len(Kvs) < Count`. Initial watch snapshot pagination uses the Range `More` flag. The schema spells out Range count and limit in [rpc.proto lines 434-517][range-schema].

Call sites and pagination are in [1.35 store lines 658-927][store35], [1.36 store lines 658-933][store36], and [1.37 store lines 667-711 and 748-1100][store37]. Initial snapshot pagination is in [1.35 watcher lines 267-325][sync35], [1.36 watcher lines 268-326][sync36], and [1.37 watcher lines 295-415][sync37].

`RangeStream` is absent from these 1.35 and 1.36 paths. In 1.37, `EtcdRangeStream` is beta and default true, [feature definition lines 396-398][streamgate]. Lists fall back on `Unimplemented`, not an arbitrary error, [store lines 814-822 and 934-979][streamfallback]. Watch initialization does the same, [watcher lines 299-312][sync37]. An unsupported stream stays disabled for 10 minutes before the feature checker permits another attempt, [lines 45-47 and 99-105][stream-recheck]. The client may report a server streaming error in the first received channel item rather than directly from `GetStream`, [etcd 3.7.2 kv.go lines 169-191][getstream]. A server must return the real gRPC status, not an empty successful stream.

#### Watch requests

Object watches use:

- `start_revision=initialRev+1`, inclusive on the etcd side.
- Exact key or prefix range when recursive.
- `prev_kv=true`.
- `progress_notify=true` only when the storage watch asks for progress.
- No event filters, no custom watch ID, no `WithFragment`. Therefore `fragment=false`.
- A require-leader context. It sends metadata `hasleader: true`.

Those option lists are explicit in [1.35 watcher lines 380-388][watch35], [1.36 lines 381-389][watch36], and [1.37 lines 470-477][watch37]. The context comes from [1.35 store lines 974-983][leader35], [1.36 lines 980-989][leader36], and [1.37 lines 1156-1165][leader37]. The exact metadata strings and context copying are in [etcd 3.7.2 `md.go` lines 16-22][md] and [`ctx.go` lines 26-51][ctx]. The retry interceptors also attach `client-api-version: 3.6` for the 3.6 clients and `3.7` for the 3.7 clients, [unary lines 40-45 and stream lines 111-116][retry-version]. Tracing can propagate context headers too; those are not storage request flags.

`RequestProgress` is a message on the bidirectional `Watch/Watch` RPC, with an empty `WatchProgressRequest`. It is not a unary RPC. Store and watch-channel request paths are [store lines 102-105][progress-store] and [watcher lines 291-293][progress-watch]. The encoder is [etcd watch.go lines 1064-1085][watch-encode]. It requests progress for watches on the context/metadata-selected stream. Normal requested progress uses `watch_id=-1` and the client distributes it to the watch channels. Periodic progress is per watch ID. I observed both. Created responses, explicit WatchCancelRequest messages and empty progress responses are distinct from events.

The compactor has a second watch shape. It first Gets `compact_rev_key`, then watches that exact key from the Get header's **current revision**, not revision+1. It does not request previous values, progress, fragmentation or require-leader. See [compact.go lines 257-290][compact-watch]. The source is identical in all three tags.

`fragment=false` does not mean the server may split a revision anyway. In the probe, a 24-key update returned one complete 24-event response with fragmentation off. With the flag on, the same revision returned 19 events with `fragment=true`, then 5 with `fragment=false`. The server path is [watch.go lines 479-491 and 581-636][server-fragment]. The public client rejoins fragments before emitting a response, [client watch.go lines 588-636][client-fragment]. The tiny probe requests fragmentation to test the reference protocol; kube-apiserver's inspected watch calls do not request it.

The event parser requires PrevKV only on non-create events, [1.35/1.36 lines 58-63][prev35], [1.37 lines 64-69][prev37]. Creates correctly have no previous KV. Deletes carry a tombstone KV with key and mod revision; the old value and metadata are in PrevKV.

Require-leader failure is gRPC `Unavailable`, `etcdserver: no leader`. The unary interceptor checks it at request admission. Streams also have leader-loss monitoring, with a threshold of three election timeouts for existing streams. See [server interceptor lines 62-74, 232-243 and 315-351][server-leader]. Fresh Range and Watch calls against the surviving minority returned that exact error in the probe.

#### Status, membership and client setup

`Status.Version` is a real dependency. The feature checker sends Status to each configured endpoint and parses the semantic version. Progress is unsupported below 3.4.31, or in the 3.5 line below 3.5.13. A bad version string also disables it. See [1.35 lines 157-172][feature35], [1.36 lines 157-172][feature36], [1.37 lines 193-208][feature37]. The prober's monitor picks one configured endpoint and reads `DbSize`, [factory lines 269-284][monitor]. 1.35 and 1.36 also have the deprecated per-endpoint DB-size monitor, [1.35 lines 485-518][db35], [1.36 lines 488-521][db36]; 1.37 removes it. Its configured default interval is 30 seconds, with 0.5 jitter; interval 0 disables it.

I found **no unconditional MemberList call** in these production apiserver paths. The factory does not set `AutoSyncInterval`, `RejectOldCluster`, username or password. AutoSync defaults to zero, so it does not Sync. Explicit client Sync does MemberList and updates endpoints from named non-learner members, [etcd client lines 186-224][autosync]. MemberList defaults to `linearizable=true`, [cluster.go lines 125-130][memberlist]. `etcdctl endpoint status --cluster` does use MemberList to discover endpoints, [ep_command.go lines 246-299][ctl-cluster]. Thus MemberList belongs in #47's operator compatibility even though it is not an ordinary apiserver object-storage call. Do not infer Auth.Authenticate use merely because the client constructs an Auth object; it skips authentication without credentials, [client.go lines 285-304][authskip].

All three factories configure a 20-second dial timeout and HTTP/2 keepalive at 30 seconds with a 10-second timeout. They configure CA trust and a client certificate/key, or nil TLS config when none are supplied. They add metrics interceptors, tracing and an optional egress dialer. See [1.35 factory lines 61-69 and 286-364][factory35], [1.36 lines 61-69 and 289-367][factory36], [1.37 lines 61-69 and 286-362][factory37]. The 1.35 and 1.36 factories use `grpc.WithBlock`. The 1.37 factory explicitly waits for the gRPC connection to become Ready, [lines 364-388][ready37]. These HTTP/2 pings are **not lease keepalives**. The unoverridden etcd client call defaults include WaitForReady, a 2 MiB send limit and MaxInt32 receive limit, [client options.go lines 24-65][callopts].

Standalone Put and DeleteRange are needed for the agreed etcdctl debugging commands. Ordinary object writes in the inspected apiserver paths use the Txn operations listed above. Nothing here establishes a requirement for Auth, Defragment, Snapshot, HashKV, Alarm, Lock, Election, nested transactions or custom compare-on-value.

### 2. Lease semantics in etcd 3.7.2

- Grant with ID 0 chooses a positive nonzero int64 ID, then goes through Raft, [v3_server.go lines 434-455][grant-server]. An existing explicit ID returns `FailedPrecondition`, `etcdserver: lease already exists`. A nonexistent attached lease or revoke returns `NotFound`, `etcdserver: requested lease not found`. The error definitions are [rpctypes/error.go lines 32-38][error-codes].
- The minimum TTL is configuration-dependent, not a universal one-second minimum. The server computes `ceil(duration((3*ElectionTicks)/2) * heartbeat / second)`, [server.go lines 340-348][min-ttl]. Integer division happens before duration conversion. Default 10 election ticks and 100 ms heartbeat give **2 seconds**. Lessor clamps smaller values, including TTL 0 and negative TTL, [lessor.go lines 281-322][grant-lessor]. The probe sent -1, 0, 1 and 2; each returned TTL 2. TTL over **9,000,000,000 seconds** returns `OutOfRange`, `etcdserver: too large lease TTL`, [lessor.go lines 39-40 and 281-287][ttl-max].
- Grant persists the lease. Only the primary lessor arms expiry; other lessors keep expiry at forever. Promotion refreshes from the checkpointed remaining TTL when set, otherwise the full TTL, and can extend expiries under a large expiration backlog. Demotion resets local expiries. See [lessor.go lines 301-320, 480-544][lease-promotion] and [lease.go lines 75-96][lease-refresh]. Checkpoints persist in clusters at least v3.6, [lessor.go lines 367-391][lease-checkpoint]. Leader changes can extend life; do not promise expiry at an exact client wall-clock instant.
- The primary examines expiry every 500 ms, rate-limits revokes and submits LeaseRevoke through Raft after checking leadership. See [lessor.go lines 620-663][expiry-loop], [server.go lines 858-901][expiry-revoke], and [v3_server.go lines 470-485][revoke-server]. Expiry is not a follower independently deleting its keys. It can be delayed by scheduling, load or lack of quorum.
- Revoke deletes **all currently attached keys** in one sorted MVCC write transaction, and deletes the lease in that same backend transaction, [lessor.go lines 326-361][lease-revoke]. Two attached keys expired or revoked at one shared revision in the probe, with two delete events. Grant, renewal and revoking an empty lease do not manufacture KV revisions.
- Each key has one lease association. A Put with a different lease detaches the old association and attaches the new one. Ordinary Put with lease 0 detaches it. Delete detaches it. See [kvstore_txn.go lines 223-287 and 308-345][lease-attach]. Revoking the old lease must not delete a reattached key, a lease-free overwritten key, or an unrelated recreated key. The first two cases passed the probe.
- Keepalive renews the granted TTL on the leader. It normally does not run through Raft; follower requests forward to the leader. A checkpoint reset can require a Raft entry. An already-expired lease waits for revoke or demotion rather than being resurrected by Renew. See [v3_server.go lines 487-552][keepalive-server] and [lessor.go lines 394-455][renew-lessor]. The raw LeaseKeepAlive stream answers a nonexistent/revoked ID with TTL 0 rather than a NotFound stream error, [lease RPC lines 127-148][missing-keepalive]. Raw LeaseTimeToLive answers TTL -1 when the ID is missing, [lines 62-75][missing-ttl]. The probe observed both. A keepalive at about 1.5 seconds returned TTL 2; the key still existed about 2.6 seconds after grant, beyond its original expiry.

Kubernetes uses **Grant only** in these storage paths. It does not KeepAlive, Revoke, TimeToLive or LeaseLeases. Its manager reuses one cached lease, not one lease per object. It adds `min(floor(0.05*ttl), 60)` seconds, defaults to at most 1,000 attached-object attempts, counts failed attempts too, and reuses only when the cached expiry is strictly between now+ttl and now+ttl+reuse. It caches now plus its requested padded TTL, not the server's returned TTL. See the byte-identical [1.35][lease35], [1.36][lease36] and [1.37 lease_manager.go lines 29-131][lease37]. TTL 0 object writes skip Grant and use lease ID 0, [create lines 311-327 and update lines 595-608][leased-put]. Expiry bursts therefore need multi-key, one-revision deletes, not independent per-key revisions.

### 3. Compaction boundaries and the apiserver loop

`KV.Compact` sends `CompactionRequest{revision:R, physical:false}` in the apiserver. Logical compaction goes through Raft. With physical=true, the reference waits for physical deletion and forces the completed compaction to commit to the backend, [v3_server.go lines 382-431][compact-server]. It does not increment the KV revision just for compaction.

Let C be the compacted revision and H the current revision:

| Operation | Exact behavior |
|---|---|
| Range at positive R < C | gRPC `OutOfRange`, `etcdserver: mvcc: required revision has been compacted`. |
| Range at R = C | Allowed. Compaction does not make this snapshot unreadable. |
| Range at R > H | gRPC `OutOfRange`, `etcdserver: mvcc: required revision is a future revision`. |
| Range revision <= 0 | Current revision, not a request for historical revision 0 or -1. |
| Watch from positive R < C | Created response, then normal WatchResponse with `canceled=true`, `compact_revision=C`, **empty `cancel_reason`**. It is not a gRPC OutOfRange failure of the shared stream. The observed cancellation header had revision 0. |
| Watch from R = C | Allowed, inclusive. The delete event at C remains deliverable. |
| Watch from future R > H | Created and waits. It does not get the Range future-revision error and must not deliver earlier events. |
| Compact R <= existing C | `OutOfRange` with the compacted message. |
| Compact R > H | `OutOfRange` with the future-revision message. |

Sources are [MVCC range lines 73-85][range-boundary], [watcher_group.go lines 241-268][watch-boundary], [watch RPC lines 459-466][watch-cancel], [compaction frontier lines 197-221][compact-boundary], and [error definitions lines 32-38][error-codes]. Negative watch start is a separate special case: it immediately returns created+canceled, watch ID -1, and a **nonempty** compacted cancel reason, without a compact_revision, [watch.go lines 268-284][negative-watch]. Kubernetes's inspected object watches do not send that shape.

The public Go client converts a compacted WatchResponse into `rpctypes.ErrCompacted`, [watch.go lines 123-135][watch-err]. It also maps known Range errors to `rpctypes.EtcdError`. Its `Code()` is OutOfRange, but **`grpc/status.Code` on that mapped client error is Unknown**, because it does not implement GRPCStatus. The new probe explicitly verified both Range and Watch. The wrapper is [rpctypes/error.go lines 239-272][client-error-wrapper]. Assert the raw server status or the mapped etcd error correctly; otherwise the differential test itself is wrong.

Measured with C=26: Compact 25 succeeded, Compact 26 succeeded, repeating 26 was compacted, and Compact/Range 27 was future while H was still 26. Watch 25 canceled at compact_revision 26 with empty reason and header revision 0. Watch 26 returned its delete. A future watch from 28 ignored a matching Put at 27 and delivered the update at 28.

There is a compatibility trap at equality. After physical compaction at C, stock etcd delivered the C delete **without PrevKV**, because its predecessor at C-1 was already compacted. The server tries Range at event.mod_revision-1 and silently leaves PrevKV absent if it cannot fetch it, [watch.go lines 446-455][prev-server]. That is legal stock behavior but the Kubernetes parser rejects a non-create event without PrevKV. Keep the exact stock boundary fixture separate from strict Kubernetes watch fixtures whose predecessor remains available. I have not proved a blanket rule that such a watch must instead be canceled, and would not invent one. Active watch retention, replay at the frontier and compatibility with that parser need an explicit implementation decision.

The apiserver compactor defaults to five minutes, [storagebackend config.go lines 36-39 and 115-120][compact-default]. Each tick performs the VERSION compare Txn on the unprefixed `compact_rev_key`. Successful Put increments its per-key Version and samples the Txn's current header revision. The first successful round stores "0" and does not Compact. A later successful round compacts the revision sampled in the previous round, so it retains roughly the last interval of history. A compare failure reads the remote key's Version/value, adopts the current Txn header revision and does not call Compact on that iteration. See [compact.go lines 193-253][compact-loop]. Different apiservers coordinate using that key; this is not an etcd TTL lease. The optional default-enabled ListFromCacheSnapshot watch tracks the key, [compact.go lines 93-105 and 257-290][compact-watch]. The factory shares a compactor per transport, replaces it if a smaller interval is requested, reference-counts users, and disables compaction for interval 0, [factory lines 402-453][compact-factory].

### 4. HTTP on the client port and kubeadm 1.37

These are actual response bodies from the downloaded etcd **3.7.2**, over client-certificate TLS on the same port as gRPC:

```text
GET /health, healthy
status 200
Content-Type: text/plain; charset=utf-8
body, 29 bytes, no trailing newline:
{"health":"true","reason":""}

GET /version, initialized 3.7 cluster
status 200
Content-Type: application/json
body, 62 bytes, no trailing newline:
{"etcdserver":"3.7.2","etcdcluster":"3.7.0","storage":"3.7.0"}

GET /health, initialized member with no leader
status 503
Content-Type: text/plain; charset=utf-8
X-Content-Type-Options: nosniff
body, 45 bytes, including a trailing newline:
{"health":"false","reason":"RAFT NO LEADER"}\n
```

`health` is a JSON **string**, not a boolean. Healthy output still contains empty `reason`. `/health` first checks nonexcluded alarms, then leader presence, then an internal v3 Range with `keys_only=true`, `limit=1` and the requested serializable flag. It does not perform a Put. Query `serializable=true` skips the leader requirement and uses a local read; it returned 200 in the minority probe. Query `exclude=NOSPACE` or `exclude=CORRUPT` excludes that alarm. Source-only other error reasons are `ALARM NOSPACE`, `ALARM CORRUPT`, `ALARM UNKNOWN`, or `RANGE ERROR:<error>`. All unhealthy outputs use http.Error and have the newline. See [health.go lines 60-107, 152-155 and 183-231][http-health]. I did not inject alarms for this experiment. Curl negotiated HTTP/2. The recorded responses also have Content-Length 29, 62 or 45 and a generated Date header. With the default CORS config they include `Access-Control-Allow-Origin: *`, `Access-Control-Allow-Methods: POST, GET, OPTIONS, PUT, DELETE`, and `Access-Control-Allow-Headers: accept, content-type, authorization`, [serve.go lines 486-498][http-cors]. All raw headers are on the branch.

`/version` does not prove quorum health and remained 200 in the minority. It returns the binary server version, cluster version and backend storage version. Nil versions use `not_decided` for cluster and `unknown` for storage, [version.go lines 34-65][http-version]. These fallback strings are source facts, not responses I measured from an unready fresh cluster. Fresh minority-only startup waits for readiness before serving the client handlers, [embed/serve.go lines 131-140][http-ready], which is why the failure probe first bootstraps three nodes and then stops two.

Kubeadm v1.37.1's external-etcd preflight Gets `/version` for **each configured HTTP endpoint**, using the configured CA and optional client certificate/key. HTTP endpoints default to the gRPC endpoints, [v1beta4/defaults.go lines 138-145][kubeadm-default]. It parses only `etcdserver` for the version decision and requires **>= 3.5.24-0**. It does not validate `etcdcluster`, `storage`, `/health`, the gRPC API or Raft durability in this check. See [preflight lines 724-792][kubeadm-check] and [constants lines 326-327][kubeadm-min]. It uses a 10-second HTTP timeout, up to four attempts, with five-second delays between retries. Transport errors and 5xx retry. Other HTTP status codes proceed to JSON decoding rather than an explicit generic 2xx check, [preflight lines 60-62 and 831-873][kubeadm-http]. The preflight list also checks configured external certificate files exist, [lines 1080-1093][kubeadm-files]. I audited this source; I did not run kubeadm or a Kubernetes cluster.

### 5. What etcdctl 3.7.2 endpoint status prints

Default simple and table output use the same 16-column construction, [printer.go lines 224-251][status-table]:

1. Endpoint address, supplied by the command rather than Status.
2. ID from `Header.MemberId`, hexadecimal.
3. `Version`.
4. `StorageVersion`.
5. `DbSize`, humanized.
6. `DbSizeInUse`, humanized.
7. Percentage not in use, computed as `100 - DbSizeInUse*100/DbSize` with integer arithmetic.
8. `DbSizeQuota`, humanized.
9. Is leader, computed as `Leader == Header.MemberId`.
10. `IsLearner`.
11. `RaftTerm`.
12. `RaftIndex`.
13. `RaftAppliedIndex`.
14. `Errors`, joined with comma+space.
15. `DowngradeInfo.TargetVersion`.
16. `DowngradeInfo.Enabled`.

`--write-out=fields` additionally prints Header.ClusterId, Header.MemberId, positive Header.Revision and **Header.RaftTerm**, then Version, StorageVersion, sizes, quota, numeric Leader, IsLearner, indexes, Errors, endpoint and downgrade fields. Its RaftTerm comes from the header, unlike the table's response field. See [printer_fields.go lines 44-59 and 201-219][status-fields]. JSON prints the endpoint plus the full protobuf Status response, including its header, [printer_json.go line 77][status-json].

The server fills those fields in [maintenance.go lines 261-296][status-server]. Its RaftIndex is the committed index, not the last locally appended index. No leader and active alarms populate Errors. Default quota is 2 GiB. A zero DbSize is not an innocent dummy value: this etcdctl table implementation divides by it and can panic. Populate a meaningful positive size and consistent IDs/terms/indexes. The reference smoke printed all formats and `--cluster`. The final host sample reported DbSize 3,276,800 bytes, in-use 1,789,952, quota 2,147,483,648, storage version 3.7.0, leader true, term 2, committed/applied index 53 and no errors. These are a tiny probe's storage numbers, not a capacity benchmark.

### 6. Concrete proposed planted-bug inventory

The count comes from the [14 Opus classes in #33][bugs-opus], plus its [four explicitly accepted additions][bugs-additions]: cancellation treated as abort, premature segment deletion, a global wait across stalled fsync, and a watcher leak. This makes **18 named classes**, not a claim that exactly 18 switches already exist. To retain the Astra alternatives too, four classes below have multiple separately enabled variants, making 24 proposed mutant switches. [The merged comment][bugs-merged] leaves budgets unspecified.

The complete scenario/assertion inventory is in [`planted-bugs.json`](https://github.com/djosh34/tigerstore/blob/exp/pre-47-semantics/experiments/pre-47-semantics/planted-bugs.json). Below are the proposed first-detection seed caps. They are **unmeasured proposals**, not kills observed against tigerstore. Each mutant starts with a forced schedule as seed 0, then seeded timing/cut-point variants. Each named variant in a row must independently fail its expected assertion within that row's cap. Killing one sibling does not cover the others. Preserve the forced schedule independently of PRNG consumption. Count a kill only when the expected assertion fails; a panic, outer checker timeout, Unknown or missing fault is not a successful detection. Baseline code must pass the same case.

| ID | Injected fault | Assertion that must detect it | Max seeds |
|---|---|---|---:|
| B01 | Two variants: skip ReadIndex; reply before local apply reaches its returned index | After a healthy-pair Put completes, a read cannot return the preceding value. Isolate the former leader for the first variant; deliver ReadIndex but hold apply for the second. Independent Porcupine rejects the bad history. | 64 each |
| B02 | Leader replies before local fsync | With only one durable follower and the third blocked, erase that follower and power-crash the leader. The receipt-ledger sentinel must recover; success also requires its durability dependency. | 128 |
| B03 | Follower acknowledges append before fsync | Block the third node, acknowledge on leader plus volatile follower, erase the leader and power-crash the follower. The acknowledged sentinel must recover. | 128 |
| B04 | Two variants: accept a torn record; tail repair truncates preceding synced records | Tear length/checksum/payload/footer after a synced sentinel. No incomplete/invented state and no loss of the independently known durable prefix. Complete corruption fences the node. | 256 each |
| B05 | Snapshot becomes authority before file/directory sync | Every authoritative root resolves to complete verified stable bytes. Crash at the install boundary and cold-recover one consistent index/term/membership/revision/state. | 128 |
| B06 | Four variants: drop event; duplicate event; omit retained PrevKV; progress overtakes an event | The expected identities are complete and unique through an independent target. Update/delete PrevKV matches its available predecessor; create may omit it. Progress R cannot precede expected events through R. Test requested and periodic progress. | 32 each |
| B07 | Resume from an older revision | Reconnect after a completely received revision. No duplicate revision/key/type identity and no gap are allowed in the logical watch. | 64 |
| B08 | Compaction drops the delete event at C | Put, delete at C, physical Compact C, Watch C. The independently expected delete must arrive. Do not falsely require its compacted predecessor in this stock boundary fixture. | 32 |
| B09 | Two variants: revision per changed key; reuse old key version/create revision on recreation | Multi-key Txn/revoke has one increment and one shared revision; read-only/empty delete has none. Delete/recreate resets version to 1 and create revision to the new revision; old MOD CAS fails. | 16 each |
| B10 | A follower expires leases by local wall clock | Jump only that isolated follower's wall time. No deletion without applied replicated expiry; equal applied prefixes agree; no premature expiry under the declared clock envelope. | 64 |
| B11 | Retry fsync EIO instead of fencing | On the first EIO, the node stops serving, voting and sending leadership traffic, even if the next sync would succeed. The healthy pair recovers liveness. | 64 |
| B12 | ENOSPC partial append is accepted on recovery | Short-write with ENOSPC, crash and restart twice. No success or replay depends on the partial record, and prior acknowledged data still recovers. | 256 |
| B13 | Slow watcher blocks apply | Fill a tiny queue with a nondraining watcher. Bound/cancel it; an independent healthy client read and Put finish within the declared 10-election-timeout bound. | 64 |
| B14 | Peer writer lacks a progress/write deadline | Stall one peer writer. The configured deadline drops it, releases bounded bytes and reports unreachable; the healthy quorum still commits within its bound. | 64 |
| B15 | Treat canceled/timed-out admitted mutation as definitely aborted | A known legal history with a late committed mutation and a confirming read must pass the checker. Keep the ambiguous operation; observed data survives cold restart. Discarding it fails the checker's legal-history self-test. | 32 |
| B16 | Unlink old segment before replacement bytes/manifest sync | Pin live and historical values, clean and crash at early unlink. Every recoverable root and pinned read resolves to verified bytes; cold recovery and historical Range match the model. | 128 |
| B17 | Hold the core or a global wait across stalled fsync | A stalled leader must still process its watchdog and fence itself. The two healthy nodes elect, read and write within the declared bound. | 64 |
| B18 | Leak one watcher per reconnect | After 32 completed create/cancel/reconnect cycles and a healthy drain, live watcher, queue-byte and handle counts return to baseline. Do not rely only on noisy RSS. | 16 |

The 18 base caps sum to 1,600 seeds; testing all 24 named variants separately sums to **2,032**. Neither is a runtime estimate. Measure the real harness on the free ARM runner before fixing its wall-time/memory budget. The 256 MiB-per-worker target in #33 remains a target, not a result of this protocol probe.

Also feed legal histories directly to the checker: late-committing timeouts, overwritten/deleted/expired acknowledged values, historical Range with a newer header, read-only Txn and a mutating failed branch. The last three passed the reference probe. A blanket "failed compare never increments revision" or "read value must match the header revision" would convict legal etcd.

### Reproduction, evidence and limits

The tested throwaway Go source is `274a12f8ca8aa534c952fdc279d1ad98d9c26a61`. The experiment uses Go **1.27.2** and the downloaded official **etcd 3.7.2 linux/arm64** binary, which reports build Go **1.26.8** and Git SHA `68c065e`. The binary archive SHA-256 is committed with the results. No installed host etcd was used.

```sh
git clone --branch exp/pre-47-semantics https://github.com/djosh34/tigerstore.git
cd tigerstore/experiments/pre-47-semantics
WORK="$PWD/disposable" bash run.sh
# Remove only this experiment's read-only module cache and build/download output.
chmod -R u+w disposable/mod
rm -rf disposable
```

`run.sh` keeps its caches, generated private keys, data and binaries under WORK. It generates a one-day RSA certificate with localhost/127.0.0.1 SAN and server/client EKUs, enables client-certificate authentication, and puts gRPC and HTTP on `https://127.0.0.1:32379`. Peer port is 32380. Healthy protocol tests use one node, normal fsync, default 100 ms heartbeat/1 s election timeout, default 2 GiB quota, `--watch-progress-notify-interval=100ms`, and `--max-request-bytes=2097152`. The client uses CA+certificate+key, 30 s/10 s HTTP/2 keepalive and a short probe dial timeout. This is not a throughput test. Raw request logs preserve small values and replace large base64 value fields with their lengths.

The short leaderless check then boots three fresh nodes on client ports 32379/32381/32382 and peer ports 32380/32391/32392, stops two and tests the initialized survivor. Its peer links are plaintext loopback, so this is **not peer-TLS acceptance evidence**. No host network or system settings changed. All experiment processes stopped. I deleted the owned build caches, downloaded binaries, generated TLS keys and disposable datasets after capturing the results.

Passed cheap checks include TLS etcdctl get/put/del/watch/endpoint status in all output formats and with `--cluster`; raw and mapped compaction errors; equality/future watch boundaries; create/update/delete previous values; requested and periodic progress; require-leader failures; TTL clamping, max TTL, duplicate IDs, missing leases, detach/reattach, atomic revoke/expiry; keepalive; fragmentation on/off; and historical/read-only/failed-branch revision examples. `go vet ./...` and `go test ./...` compile the prototype; the executable supplies the assertions, so the Go test command alone is not the evidence.

The host is the shared 8-vCPU ARM64 VM, Linux `5.14.0-611.26.1.el9_7`, without CPU pinning. The client-version probes at source `4a8fa1c` also passed with clients **3.6.5, 3.6.8 and 3.7.0** against the same downloaded server. Their module locks and traces are in `results/client-*`. They observed two-key expiry at 2,367 / 2,129 / 2,493 ms and the expected client-api-version metadata. These are helper/client protocol probes, **not full Kubernetes binaries or the Kubernetes storage suite**.

The final on-push probe on free `ubuntu-24.04-arm` is [run 38317956272](https://github.com/djosh34/tigerstore/actions/runs/38317956272). It passed in 50 seconds total, 37 seconds for download/build/probe. Two-key expiry took 2,122 ms from Grant invocation. It delivered the two fragments as 19+5 events at one revision, all 24 events together without fragmentation, and the expected Range/Watch/mapped-error/future-watch results. The final host run measured expiry at 2,446 ms. These are single-case elapsed times, not latency percentiles or TTL upper-bound guarantees.

Pitfalls I hit and fixed:

- My first fragmentation fixture used 24 x 1 KiB values and did not fragment. The server threshold is max-request-bytes **plus 512 KiB overhead**, [config.go lines 37-39 and 358-360][fragment-overhead]. I changed it to 24 x 64 KiB values with PrevKV and a 2 MiB request cap. The selected Txn fits; its watch response exceeds the 2.5 MiB threshold.
- An etcdctl watch from revision 1 after Compact 26 correctly canceled. The smoke now starts at the first ctl-key write's revision and asserts all three create/update/delete events.
- A fresh cluster with only one of three initial members never became ready to serve the client handlers. The minority check now bootstraps all three first. I added a connection timeout to readiness polling.
- Curl negotiated HTTP/2, so an assertion looking for the HTTP/1 reason phrase failed despite the correct 503. The assertion now checks the status number.
- The first Actions run, [38316827794](https://github.com/djosh34/tigerstore/actions/runs/38316827794), passed the probe and failed during cleanup because Go module directories are read-only. The fix adds `chmod -R u+w` before removing that owned cache. The next run, [38317101564](https://github.com/djosh34/tigerstore/actions/runs/38317101564), passed in 47 seconds total, 36 seconds for download/build/probe; it measured two-key expiry at 2,262 ms. The red cleanup run is not relabeled green.
- TTL lower-bound timing must start before Grant invocation, not after receipt of its response. A delayed response consumes lease lifetime. The final probe uses invocation time.

I verified the cited request shapes and the listed reference-server cases under this configuration. I audited source, but did not run kubeadm's complete preflight path, alarm cases, nil version fallbacks or Kubernetes's RangeStream fallback. The probe did not run tigerstore, the Kubernetes storage suite or durability chaos. It did not implement mutants or measure their seed cost, all request-size boundaries or production performance. The later worker still needs to implement and measure the proposed faults and budgets. This pre-work does not close any #47 acceptance gate.

[deps35]: https://github.com/kubernetes/kubernetes/blob/v1.35.0/go.mod#L59-L61
[deps36]: https://github.com/kubernetes/kubernetes/blob/v1.36.0/go.mod#L56-L58
[deps37]: https://github.com/kubernetes/kubernetes/blob/v1.37.1/go.mod#L56-L58
[helper35]: https://github.com/kubernetes/kubernetes/blob/v1.35.0/vendor/go.etcd.io/etcd/client/v3/kubernetes/client.go#L47-L126
[helper36]: https://github.com/kubernetes/kubernetes/blob/v1.36.0/vendor/go.etcd.io/etcd/client/v3/kubernetes/client.go#L47-L126
[helper37]: https://github.com/kubernetes/kubernetes/blob/v1.37.1/vendor/go.etcd.io/etcd/client/v3/kubernetes/client.go#L47-L126
[helper372]: https://github.com/etcd-io/etcd/blob/v3.7.2/client/v3/kubernetes/client.go#L47-L126
[opencode]: https://github.com/etcd-io/etcd/blob/v3.7.2/client/v3/op.go#L177-L231
[prefix]: https://github.com/etcd-io/etcd/blob/v3.7.2/client/v3/op.go#L398-L442
[range-schema]: https://github.com/etcd-io/etcd/blob/v3.7.2/api/etcdserverpb/rpc.proto#L434-L517
[store35]: https://github.com/kubernetes/kubernetes/blob/v1.35.0/staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go#L658-L927
[store36]: https://github.com/kubernetes/kubernetes/blob/v1.36.0/staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go#L658-L933
[store37]: https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go#L667-L1100
[sync35]: https://github.com/kubernetes/kubernetes/blob/v1.35.0/staging/src/k8s.io/apiserver/pkg/storage/etcd3/watcher.go#L267-L325
[sync36]: https://github.com/kubernetes/kubernetes/blob/v1.36.0/staging/src/k8s.io/apiserver/pkg/storage/etcd3/watcher.go#L268-L326
[sync37]: https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/storage/etcd3/watcher.go#L295-L415
[streamgate]: https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/features/kube_features.go#L396-L398
[streamfallback]: https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go#L814-L979
[stream-recheck]: https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/storage/feature/feature_support_checker.go#L45-L105
[getstream]: https://github.com/etcd-io/etcd/blob/v3.7.2/client/v3/kv.go#L169-L191
[watch35]: https://github.com/kubernetes/kubernetes/blob/v1.35.0/staging/src/k8s.io/apiserver/pkg/storage/etcd3/watcher.go#L380-L388
[watch36]: https://github.com/kubernetes/kubernetes/blob/v1.36.0/staging/src/k8s.io/apiserver/pkg/storage/etcd3/watcher.go#L381-L389
[watch37]: https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/storage/etcd3/watcher.go#L470-L477
[leader35]: https://github.com/kubernetes/kubernetes/blob/v1.35.0/staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go#L974-L983
[leader36]: https://github.com/kubernetes/kubernetes/blob/v1.36.0/staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go#L980-L989
[leader37]: https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go#L1156-L1165
[md]: https://github.com/etcd-io/etcd/blob/v3.7.2/api/v3rpc/rpctypes/md.go#L16-L22
[ctx]: https://github.com/etcd-io/etcd/blob/v3.7.2/client/v3/ctx.go#L26-L51
[retry-version]: https://github.com/etcd-io/etcd/blob/v3.7.2/client/v3/retry_interceptor.go#L40-L116
[progress-store]: https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go#L102-L105
[progress-watch]: https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/storage/etcd3/watcher.go#L291-L293
[watch-encode]: https://github.com/etcd-io/etcd/blob/v3.7.2/client/v3/watch.go#L1064-L1085
[compact-watch]: https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/storage/etcd3/compact.go#L93-L290
[server-fragment]: https://github.com/etcd-io/etcd/blob/v3.7.2/server/etcdserver/api/v3rpc/watch.go#L479-L636
[client-fragment]: https://github.com/etcd-io/etcd/blob/v3.7.2/client/v3/watch.go#L588-L636
[prev35]: https://github.com/kubernetes/kubernetes/blob/v1.35.0/staging/src/k8s.io/apiserver/pkg/storage/etcd3/event.go#L58-L63
[prev37]: https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/storage/etcd3/event.go#L64-L69
[server-leader]: https://github.com/etcd-io/etcd/blob/v3.7.2/server/etcdserver/api/v3rpc/interceptor.go#L62-L351
[feature35]: https://github.com/kubernetes/kubernetes/blob/v1.35.0/staging/src/k8s.io/apiserver/pkg/storage/feature/feature_support_checker.go#L157-L172
[feature36]: https://github.com/kubernetes/kubernetes/blob/v1.36.0/staging/src/k8s.io/apiserver/pkg/storage/feature/feature_support_checker.go#L157-L172
[feature37]: https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/storage/feature/feature_support_checker.go#L193-L208
[monitor]: https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/storage/storagebackend/factory/etcd3.go#L269-L284
[db35]: https://github.com/kubernetes/kubernetes/blob/v1.35.0/staging/src/k8s.io/apiserver/pkg/storage/storagebackend/factory/etcd3.go#L485-L518
[db36]: https://github.com/kubernetes/kubernetes/blob/v1.36.0/staging/src/k8s.io/apiserver/pkg/storage/storagebackend/factory/etcd3.go#L488-L521
[autosync]: https://github.com/etcd-io/etcd/blob/v3.7.2/client/v3/client.go#L186-L224
[memberlist]: https://github.com/etcd-io/etcd/blob/v3.7.2/client/v3/cluster.go#L125-L130
[ctl-cluster]: https://github.com/etcd-io/etcd/blob/v3.7.2/etcdctl/ctlv3/command/ep_command.go#L246-L299
[authskip]: https://github.com/etcd-io/etcd/blob/v3.7.2/client/v3/client.go#L285-L304
[factory35]: https://github.com/kubernetes/kubernetes/blob/v1.35.0/staging/src/k8s.io/apiserver/pkg/storage/storagebackend/factory/etcd3.go#L286-L364
[factory36]: https://github.com/kubernetes/kubernetes/blob/v1.36.0/staging/src/k8s.io/apiserver/pkg/storage/storagebackend/factory/etcd3.go#L289-L367
[factory37]: https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/storage/storagebackend/factory/etcd3.go#L286-L362
[ready37]: https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/storage/storagebackend/factory/etcd3.go#L364-L388
[callopts]: https://github.com/etcd-io/etcd/blob/v3.7.2/client/v3/options.go#L24-L65
[grant-server]: https://github.com/etcd-io/etcd/blob/v3.7.2/server/etcdserver/v3_server.go#L434-L455
[error-codes]: https://github.com/etcd-io/etcd/blob/v3.7.2/api/v3rpc/rpctypes/error.go#L32-L38
[min-ttl]: https://github.com/etcd-io/etcd/blob/v3.7.2/server/etcdserver/server.go#L340-L348
[grant-lessor]: https://github.com/etcd-io/etcd/blob/v3.7.2/server/lease/lessor.go#L281-L322
[ttl-max]: https://github.com/etcd-io/etcd/blob/v3.7.2/server/lease/lessor.go#L39-L40
[lease-promotion]: https://github.com/etcd-io/etcd/blob/v3.7.2/server/lease/lessor.go#L480-L544
[lease-refresh]: https://github.com/etcd-io/etcd/blob/v3.7.2/server/lease/lease.go#L75-L96
[lease-checkpoint]: https://github.com/etcd-io/etcd/blob/v3.7.2/server/lease/lessor.go#L367-L391
[expiry-loop]: https://github.com/etcd-io/etcd/blob/v3.7.2/server/lease/lessor.go#L620-L663
[expiry-revoke]: https://github.com/etcd-io/etcd/blob/v3.7.2/server/etcdserver/server.go#L858-L901
[revoke-server]: https://github.com/etcd-io/etcd/blob/v3.7.2/server/etcdserver/v3_server.go#L470-L485
[lease-revoke]: https://github.com/etcd-io/etcd/blob/v3.7.2/server/lease/lessor.go#L326-L361
[lease-attach]: https://github.com/etcd-io/etcd/blob/v3.7.2/server/storage/mvcc/kvstore_txn.go#L223-L345
[keepalive-server]: https://github.com/etcd-io/etcd/blob/v3.7.2/server/etcdserver/v3_server.go#L487-L552
[renew-lessor]: https://github.com/etcd-io/etcd/blob/v3.7.2/server/lease/lessor.go#L394-L455
[missing-keepalive]: https://github.com/etcd-io/etcd/blob/v3.7.2/server/etcdserver/api/v3rpc/lease.go#L127-L148
[missing-ttl]: https://github.com/etcd-io/etcd/blob/v3.7.2/server/etcdserver/api/v3rpc/lease.go#L62-L75
[lease35]: https://github.com/kubernetes/kubernetes/blob/v1.35.0/staging/src/k8s.io/apiserver/pkg/storage/etcd3/lease_manager.go#L29-L131
[lease36]: https://github.com/kubernetes/kubernetes/blob/v1.36.0/staging/src/k8s.io/apiserver/pkg/storage/etcd3/lease_manager.go#L29-L131
[lease37]: https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/storage/etcd3/lease_manager.go#L29-L131
[leased-put]: https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go#L311-L608
[compact-server]: https://github.com/etcd-io/etcd/blob/v3.7.2/server/etcdserver/v3_server.go#L382-L431
[range-boundary]: https://github.com/etcd-io/etcd/blob/v3.7.2/server/storage/mvcc/kvstore_txn.go#L73-L85
[watch-boundary]: https://github.com/etcd-io/etcd/blob/v3.7.2/server/storage/mvcc/watcher_group.go#L241-L268
[watch-cancel]: https://github.com/etcd-io/etcd/blob/v3.7.2/server/etcdserver/api/v3rpc/watch.go#L459-L466
[compact-boundary]: https://github.com/etcd-io/etcd/blob/v3.7.2/server/storage/mvcc/kvstore.go#L197-L221
[negative-watch]: https://github.com/etcd-io/etcd/blob/v3.7.2/server/etcdserver/api/v3rpc/watch.go#L268-L284
[watch-err]: https://github.com/etcd-io/etcd/blob/v3.7.2/client/v3/watch.go#L123-L135
[client-error-wrapper]: https://github.com/etcd-io/etcd/blob/v3.7.2/api/v3rpc/rpctypes/error.go#L239-L272
[prev-server]: https://github.com/etcd-io/etcd/blob/v3.7.2/server/etcdserver/api/v3rpc/watch.go#L446-L455
[compact-default]: https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/storage/storagebackend/config.go#L36-L120
[compact-loop]: https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/storage/etcd3/compact.go#L193-L253
[compact-factory]: https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/storage/storagebackend/factory/etcd3.go#L402-L453
[http-health]: https://github.com/etcd-io/etcd/blob/v3.7.2/server/etcdserver/api/etcdhttp/health.go#L60-L231
[http-version]: https://github.com/etcd-io/etcd/blob/v3.7.2/server/etcdserver/api/etcdhttp/version.go#L34-L65
[http-cors]: https://github.com/etcd-io/etcd/blob/v3.7.2/server/embed/serve.go#L486-L498
[http-ready]: https://github.com/etcd-io/etcd/blob/v3.7.2/server/embed/serve.go#L131-L140
[kubeadm-default]: https://github.com/kubernetes/kubernetes/blob/v1.37.1/cmd/kubeadm/app/apis/kubeadm/v1beta4/defaults.go#L138-L145
[kubeadm-check]: https://github.com/kubernetes/kubernetes/blob/v1.37.1/cmd/kubeadm/app/preflight/checks.go#L724-L792
[kubeadm-min]: https://github.com/kubernetes/kubernetes/blob/v1.37.1/cmd/kubeadm/app/constants/constants.go#L326-L327
[kubeadm-http]: https://github.com/kubernetes/kubernetes/blob/v1.37.1/cmd/kubeadm/app/preflight/checks.go#L831-L873
[kubeadm-files]: https://github.com/kubernetes/kubernetes/blob/v1.37.1/cmd/kubeadm/app/preflight/checks.go#L1080-L1093
[status-table]: https://github.com/etcd-io/etcd/blob/v3.7.2/etcdctl/ctlv3/command/printer.go#L224-L251
[status-fields]: https://github.com/etcd-io/etcd/blob/v3.7.2/etcdctl/ctlv3/command/printer_fields.go#L44-L219
[status-json]: https://github.com/etcd-io/etcd/blob/v3.7.2/etcdctl/ctlv3/command/printer_json.go#L77
[status-server]: https://github.com/etcd-io/etcd/blob/v3.7.2/server/etcdserver/api/v3rpc/maintenance.go#L261-L296
[bugs-opus]: https://github.com/djosh34/tigerstore/issues/33#issuecomment-6132294098
[bugs-additions]: https://github.com/djosh34/tigerstore/issues/33#issuecomment-6132326276
[bugs-merged]: https://github.com/djosh34/tigerstore/issues/33#issuecomment-6132354149
[fragment-overhead]: https://github.com/etcd-io/etcd/blob/v3.7.2/server/config/config.go#L358-L360
