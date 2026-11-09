# Ticket 579: real Kubernetes 1.37 → stock etcd recording

## Evidence / configuration

All headline measurements below are successful **ubuntu-24.04-arm** Actions runs,
using single-member **stock etcd 3.7.2**, kind 0.33.0 and the repository's pinned
Kubernetes 1.37.0 image. No tigerstore backend is involved. Measurement code:
`210677f3b444cda1b64b99ea2b96e1f320189f3e`; summary parser: `5bf540ce`.
Branch: `research/579-recording`. No PR or issue comment was created.

- [100 nodes: idle + complete CL2 lifecycle](https://github.com/djosh34/tigerstore/actions/runs/42704905657)
  — [raw recording / original artifacts](https://github.com/djosh34/tigerstore/actions/runs/42704905657/artifacts/13715461734).
- [500 nodes: idle](https://github.com/djosh34/tigerstore/actions/runs/42705301411)
  — [raw recording / original artifacts](https://github.com/djosh34/tigerstore/actions/runs/42705301411/artifacts/13715558431).
- [Corrected summaries](https://github.com/djosh34/tigerstore/actions/runs/42712632184/artifacts/13717210346)
  — regenerated on Actions from the immutable recordings, splitting MOD=0
  creates from MOD>0 updates and explicitly accounting for finite RangeStream.
  JSON has **all** prefixes, call kinds, read/write sizes, key counts, complete
  active-watch configurations, compaction timestamps and errors.

KWOK v0.8.0, release stages except `pod-complete` disabled so pause Jobs remain
Running as CL2 expects. CL2 pinned to perf-tests
`01a4367bce12691ea1e910394290b72a90b4fa65`, upstream `testing/load/config.yaml`:
100 fake nodes, nominal 30 pods/node, create/delete throughput 10 pods/s, default
API QPS 10. Create / scale-and-update / delete modules and object templates are
unchanged. PVs, global real-network/Prometheus/SLO measurements, DNS checks, and
separate scheduler/startup microbenchmarks are omitted. CL2 exited **0** and
logged `Test Finished`; this is **not** a production or SLO-capacity test.

One real worker plus control plane; fake nodes are 32 CPU / 128Gi / 110 pods,
with distinct bridge InternalIPs. Expanded pod subnet `10.240.0.0/12` accommodates
500 node CIDRs. Kind's infrastructure DaemonSets select real nodes. All system
pods were Running with zero restarts at settling and after the final runs.
Three minutes settling precede each idle window.

No `--watch-cache` or `--feature-gates` override. Metrics prove
`ListFromCacheSnapshot=1`, `EtcdRangeStream=1`, `WatchList=1`,
`WatchListClient=1`, `WatchListCompression=1`,
`ConsistentListFromCacheSkipTimeoutFallback=0`. No separate
`ConsistentListFromCache` metric was exported; its behavior was not overridden.
The complete explicit apiserver arguments are below and in each `apiserver.yaml`.

## Call mix

A “message” is a client request, including messages on persistent Watch streams;
RPC starts are reported separately. Shares use request messages, not watch
responses or events. All sizes are uncompressed protobuf bytes. Values are never
logged, only their lengths. Instrumentation overhead is unquantified.

| Window | Seconds | Messages | Messages/s | New RPCs/s |
|---|---:|---:|---:|---:|
| Idle 100 | 600.02 | 9,626 | 16.043 | 14.938 |
| Idle 500 | 600.02 | 33,575 | 55.956 | 54.845 |
| Load 100 | 817.75 | 95,152 | 116.358 | 115.065 |

Each cell is **count (share %)**. `put+lease` means an etcd lease is attached;
Kubernetes node Lease objects normally use the ordinary non-attached put shape.

| Request shape | Idle 100 | Idle 500 | Load 100 |
|---|---:|---:|---:|
| Range single key, limit 0 | 766 (7.958) | 756 (2.252) | 1,051 (1.105) |
| Range single key, limit 1 | 916 (9.516) | 922 (2.746) | 32,010 (33.641) |
| Range prefix, limit 1 / >1 / unlimited | 0 | 0 | 0 |
| RangeStream prefix, limit 10,000 | 135 (1.402) | 146 (0.435) | 160 (0.168) |
| RangeStream prefix, unlimited | 60 (0.623) | 60 (0.179) | 88 (0.092) |
| Range count_only | 12 (0.125) | 13 (0.039) | 18 (0.019) |
| Txn MOD>0 EQUAL → put; failure range | 6,785 (70.486) | 30,735 (91.541) | 32,733 (34.401) |
| Txn MOD>0 EQUAL → put+lease; failure range | 120 (1.247) | 120 (0.357) | 601 (0.632) |
| Txn MOD=0 EQUAL → put; no failure ops | 0 | 0 | 5,430 (5.707) |
| Txn MOD=0 EQUAL → put+lease; no failure ops | 0 | 0 | 6,271 (6.591) |
| Txn MOD>0 EQUAL → delete; failure range | 0 | 0 | 15,552 (16.344) |
| Txn VERSION=0 EQUAL → put; failure range | 1 (0.010) | 1 (0.003) | 0 |
| Txn VERSION>0 EQUAL → put; failure range | 1 (0.010) | 1 (0.003) | 3 (0.003) |
| LeaseGrant | 120 (1.247) | 120 (0.357) | 173 (0.182) |
| WatchProgressRequest | 663 (6.888) | 667 (1.987) | 1,057 (1.111) |
| WatchCreateRequest | 46 (0.478) | 33 (0.098) | 2 (0.002) |
| Compact | 1 (0.010) | 1 (0.003) | 3 (0.003) |
| All other shapes | 0 | 0 | 0 |

In particular: zero plain Put/DeleteRange, LeaseKeepAlive messages, LeaseRevoke,
LeaseTimeToLive, Status, MemberList, WatchCancel, CREATE-revision compares, or
keys-only ranges in these windows. Idle grants all TTL=15; load grants were
164×TTL 15 and 9×TTL 3660.

### LIST-shaped reads versus revision probes

**The anticipated “prefix Range limit 1” shape did not occur.** Measured resource
root reads instead had `limit=1`, **empty range_end**, and bare keys such as
`/registry/pods` or `/registry/kwok.x-k8s.io/clusterattaches`. There were
**670 / 676 / 1,174** such reads (idle100 / idle500 / load100), matched against
observed watch resource roots. Other single-key limit-1 reads addressed actual
objects. Every single-key limit-0 read targeted **`/registry/health`**.
[Kubernetes GetCurrentResourceVersion](https://github.com/kubernetes/kubernetes/blob/v1.37.0/staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go#L728-L745)
uses a nonrecursive get of the resource prefix. A revision probe is not evidence
of an individual HTTP LIST; wire-only caller attribution is unproven.

Non-count prefix reads were **195 / 206 / 248**, all **KV/RangeStream**:

- Limit 10,000: 135 / 146 / 160 reads, over 73 / 73 / 76 storage keys/prefixes.
  The shape and revisions match the built-in cache consistency checker, whose
  [source](https://github.com/kubernetes/kubernetes/blob/v1.37.0/staging/src/k8s.io/apiserver/pkg/storage/cacher/consistency/checker.go)
  specifies 5-minute checks (jittered) and 10,000-object pages. This caller
  attribution is inferred from source/pattern, not an RPC caller tag.
- Unlimited: 60 / 60 / 82 reads of `/registry/masterleases/`. The load also had
  six namespace-scoped event reads: twice each for `test-e2bzsv-1`,
  `test-e2bzsv-2`, and `cluster-loader`.
- Count-only: 12 / 13 / 18 reads, all `/registry/events/`.

Independent `apiserver_request_total{verb="LIST"}` metric deltas were just
**3 / 3 / 154 HTTP LISTs**. The idle LISTs were all Services. These cannot be
paired one-to-one with etcd reads: internal scans are not HTTP LISTs, and cached
HTTP LISTs do not produce a data Range. Start/end metrics are preserved in the
raw artifacts; no exact HTTP-to-storage attribution percentage is claimed.

## Resources

Calls: T=Txn, R=Range, S=RangeStream, W=Watch create request. Write rates count
successful put/delete operations, not failed CAS attempts. Put size is **mean /
p50 / p99 bytes**, with no logical-object decompression. Read mean includes
whole Range replies, completed RangeStream chunks summed per RPC, and nested
Txn failure-range replies. Resource groups in storage paths are grouped under
their first registry component; `/registry/services/` includes legacy Endpoints.
Only selected prefixes are printed here; the JSON artifacts include every prefix.

### Idle, 100 nodes

| Prefix after /registry/ | Calls T/R/S/W | Writes/s | Put B mean/p50/p99 | Mean read B | Watch events/s |
|---|---|---:|---:|---:|---:|
| leases | 6675/9/2/0 | 11.125 | 524.6/532/532 | 11,293.1 | 11.125 |
| minions | 110/8/2/0 | 0.183 | 2841.8/2776/4276 | 58,119.3 | 0.183 |
| pods | 0/11/2/1 | 0 | — | 9,712.2 | 0 |
| events | 0/12/0/0 | 0 | — | 31.0 | 0 |
| masterleases | 60/60/60/0 | 0.100 | 63/63/63 | 151.5 | 0 |
| peerserverleases | 60/60/0/0 | 0.100 | 139/139/139 | 258.0 | 0 |

### Idle, 500 nodes

| Prefix after /registry/ | Calls T/R/S/W | Writes/s | Put B mean/p50/p99 | Mean read B | Watch events/s |
|---|---|---:|---:|---:|---:|
| leases | 30223/9/2/0 | 50.370 | 530.2/532/532 | 54,276.0 | 50.370 |
| minions | 512/10/2/0 | 0.853 | 2792.4/2780/2780 | 236,604.1 | 0.853 |
| pods | 0/9/2/1 | 0 | — | 11,473.9 | 0 |
| events | 0/13/0/0 | 0 | — | 32.0 | 0 |
| masterleases | 60/60/60/0 | 0.100 | 63/63/63 | 153.5 | 0 |
| peerserverleases | 60/60/0/0 | 0.100 | 139/139/139 | 260.0 | 0 |

### Load, 100 nodes

| Prefix after /registry/ | Calls T/R/S/W | Puts/deletes | Writes/s | Put B mean/p50/p99 | Mean read B | Watch events/s |
|---|---|---:|---:|---:|---:|---:|
| leases | 9056/20/2/0 | 9056/0 | 11.074 | 524.6/532/532 | 5,675.7 | 11.074 |
| minions | 106/18/2/0 | 106/0 | 0.130 | 2857.5/2776/4276 | 29,079.8 | 0.130 |
| pods | 24766/13956/2/0 | 17438/3460 | 25.555 | 2783.3/3125/3223 | 3,139.9 | 25.555 |
| events | 12979/12997/12/0 | 6708/6271 | 15.872 | 663.1/667/679 | 1,170.6 | 0 |
| endpointslices | 3691/434/2/0 | 3525/166 | 4.514 | 2768.2/1450/13677 | 1,776.0 | 4.514 |
| services | 3235/621/4/0 | 2907/328 | 3.956 | 4030.5/1045/30965 | 1,173.1 | 3.956 |
| deployments | 2643/1273/3/0 | 2315/324 | 3.227 | 2829.3/3018/3067 | 3,781.5 | 3.227 |
| replicasets | 2230/667/3/0 | 1906/324 | 2.727 | 2768.4/2824/2837 | 4,147.3 | 2.727 |
| configmaps | 652/343/2/0 | 326/326 | 0.797 | 320.3/310/310 | 1,277.9 | 0.797 |
| secrets | 646/341/2/0 | 323/323 | 0.790 | 275.8/278/278 | 1,005.1 | 0.790 |

Events were written heavily but had **no etcd watch**. Watches on other cached
resources generally carried one event per successful write/delete. With prev_kv,
watch event bytes can exceed put-value bytes: load Pod events alone totaled
**100,081,376 serialized event bytes**; EndpointSlices **20,075,753**; Services
**23,894,754**; Leases **10,591,227**. Per-prefix bytes/counts are in the JSON.

## Popularity, concurrency, watches, compaction

| Measure | Idle 100 | Idle 500 | Load 100 |
|---|---:|---:|---:|
| Distinct keys successfully put | 210 | 1,010 | 11,911 |
| Successful puts | 6,907 | 30,857 | 45,017 |
| Top 1% keys' share of puts | 9.527% | 3.669% | 24.680% |
| Top 10% keys' share of puts | 24.917% | 20.877% | 46.365% |
| Repeat-put interval mean / p50 / p99, s | 9.593 / 10.178 / 10.401 | 10.067 / 10.201 / 10.400 | 49.533 / 0.0993 / 553.359 |
| Unique apiserver connections seen | 128 | 115 | 84 |
| Max simultaneously open connections | 84 | 86 | 83 |
| In-flight all RPCs max / p99 | 78 / 75 | 79 / 75 | 99 / 76 |
| In-flight nonpersistent RPCs max / p99 | 4 / 1 | 5 / 1 | 25 / 2 |
| Watch streams active at window start | 74 | 74 | 74 |
| New Watch streams / creates | 46 | 33 | 2 |
| WatchProgressRequest messages | 663 | 667 | 1,057 |
| Progress response messages | 677 | 698 | 1,132 |
| Txn failed / all Txn responses | 0/6,907 | 0/30,857 | 3,872/60,590 |
| Txn CAS failure share | 0% | 0% | 6.390% |

Percentiles for in-flight requests are **time-weighted**, not per-call samples.
Nonpersistent includes finite RangeStream and excludes persistent Watch and
LeaseKeepAlive. Connection counts describe the apiserver side; the proxy itself
uses one backend connection. Key popularity is puts only; top sets use ceil(1%
or 10% of keys). Intervals are consecutive puts within each window, censored at
its boundaries. Load includes full namespace/object cleanup.

Each resource watch normally occupied its own stream. At window start all three
had 73 resource-prefix watches with `prev_kv=true`, `progress_notify=true`, no
filters, plus a `compact_rev_key` watch with both false. Full individual prefix
configurations are in `active_watch_keys_at_start` / `active_watch_configs_at_start`.
There was one each for Pods, Leases, Minions, EndpointSlices, etc.; none for
Events, masterleases or peerserverleases; two under Services (specs/endpoints)
and 12 KWOK CRD resource prefixes. Stream renewals logged context cancellations;
two idle-100 streams ended `Unavailable: transport is closing` and were renewed.
There were no non-Watch RPC errors in the windows.

The controller-manager and scheduler leader-election keys each got about one put
every **2.005s**. Node / apiserver identity leases generally renewed around
**10.2s**. The hottest load key was
`/registry/services/endpoints/test-e2bzsv-1/big-service-0`: **501 puts**, median
repeat interval **0.0485s**, compared with 408 puts each to the scheduler and
controller-manager lease keys. Exact top-20 keys/intervals are in the JSON.

Compaction requests: idle100 **one at revision 4,495**; idle500 **one at 17,774**.
Load **three at 7,846 → 14,301 → 37,093**, separated by **300.0036s / 300.0048s**,
all `physical=false`. The preceding idle100 compaction was 300.0038s before the
first load compaction. These are requested cutoff revisions, not a measured
count of MVCC versions physically reclaimed. `compact_rev_key` coordination
accounts for the small VERSION-comparison transaction rows.

## Failed attempts / limitations

The first load attempt was stopped after a real incompatibility appeared:
[run 42696671160](https://github.com/djosh34/tigerstore/actions/runs/42696671160),
`clusterloader2.log:26`:

> got context deadline exceeded while waiting for 30 pods to be running …
> 90 created, 30 desired, 0 running … 90 inactive

KWOK's generic fast Job-completion stage conflicts with CL2's indefinitely
Running pause Jobs; removing that stage fixed the lifecycle. Earlier fake nodes
also used a shared KWOK InternalIP / insufficient default node-CIDR space; an
[earlier 500-node run](https://github.com/djosh34/tigerstore/actions/runs/42697150297)
showed a kindnet pod in Error. Those runs are excluded from headline results.
The final unique bridge addresses / expanded pod subnet runs have no such churn.
An initial workflow YAML error was fixed before experiments ran.

No watch-cache-off run was attempted. Fake KWOK heartbeat/stage timing and
simple pause-object sizes are not production kubelet/app distributions. This
proves the wire mix of these exact scenarios, not all Kubernetes installations,
real networking throughput, multi-member raft traffic, or an uninstrumented
capacity/latency result. Source-pattern caller attribution is explicitly labeled
above; no other unmeasured workload shares are asserted.

## Complete explicit kube-apiserver flags

100-node address is `172.30.0.3`; 500-node address is `172.30.0.2`.
Some explicit flags equal upstream defaults; this is the complete manifest list,
a superset of every non-default flag. No other apiserver flags were injected.

```text
--advertise-address=172.30.0.3                 # .2 in the 500-node run
--allow-privileged=true
--authorization-mode=Node,RBAC
--client-ca-file=/etc/kubernetes/pki/ca.crt
--enable-admission-plugins=NodeRestriction
--enable-bootstrap-token-auth=true
--etcd-cafile=/etc/kubernetes/pki/external/ca.pem
--etcd-certfile=/etc/kubernetes/pki/external/client.pem
--etcd-keyfile=/etc/kubernetes/pki/external/client-key.pem
--etcd-servers=https://172.30.0.1:20010
--kubelet-client-certificate=/etc/kubernetes/pki/apiserver-kubelet-client.crt
--kubelet-client-key=/etc/kubernetes/pki/apiserver-kubelet-client.key
--kubelet-preferred-address-types=InternalIP,ExternalIP,Hostname
--proxy-client-cert-file=/etc/kubernetes/pki/front-proxy-client.crt
--proxy-client-key-file=/etc/kubernetes/pki/front-proxy-client.key
--requestheader-allowed-names=front-proxy-client
--requestheader-client-ca-file=/etc/kubernetes/pki/front-proxy-ca.crt
--requestheader-extra-headers-prefix=X-Remote-Extra-
--requestheader-group-headers=X-Remote-Group
--requestheader-username-headers=X-Remote-User
--secure-port=6443
--service-account-issuer=https://kubernetes.default.svc.cluster.local
--service-account-key-file=/etc/kubernetes/pki/sa.pub
--service-account-signing-key-file=/etc/kubernetes/pki/sa.key
--service-cluster-ip-range=10.96.0.0/16
--tls-cert-file=/etc/kubernetes/pki/apiserver.crt
--tls-private-key-file=/etc/kubernetes/pki/apiserver.key
--runtime-config=
```
