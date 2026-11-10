# Recordings of real Kubernetes control planes

The bench's `k8s` workload replays these traces (`cmd/loadgen`, workload `k8s`). Each trace is ten minutes of the etcd calls that a real Kubernetes 1.37 control plane sent to a three-node tigerstore while a cluster of one shape was under load.

## How a recording is made

`.github/workflows/k8s-record.yml` makes one recording per shape, each on its own `ubuntu-24.04-arm` runner:

1. `k8stest/boot.sh`, with `RECORD` set, builds tigerstore and `cmd/etcdrec` from the commit and starts three tigerstore nodes. It starts kind v0.33.0 with Kubernetes v1.37.0: one control plane (kube-apiserver with its watch cache, controller-manager, scheduler) and two workers with real kubelets. kube-apiserver's three etcd endpoints are `cmd/etcdrec`, which passes every gRPC message to the nodes unchanged. It logs each message with its keys, revisions, lease IDs and sizes, and no value bytes.
2. `k8stest/record.sh` installs kwok v0.8.0 and adds the shape's fake nodes. It taints the real workers so that the shape's pods land on kwok nodes, then runs `k8stest/load.py`.
3. `load.py setup` creates the shape of `k8stest/shapes.json` through kubectl and waits until every deployment is available.
4. After a minute of quiet, `load.py churn` runs the shape's number of workers for ten minutes. Each worker does one kubectl action after another: rollout restarts, scaling, updates of config maps, secrets and custom objects, custom object status updates, jobs, deleted pods, and namespaces made and removed. The controllers, scheduler and kwok turn these into the calls a real cluster makes. These ten minutes are the recording's window.
5. `cmd/k8strace convert` turns the log into the trace (below). The job's summary and its `trace-summary.md` show the trace's numbers.

To make a recording again, after this is on `main`:

```sh
gh workflow run k8s-record.yml --repo djosh34/tigerstore -f shapes="<shape>" -f seconds=600
```

Then download the job's artifact `record-<shape>` and convert its log with the window and description the job used (`window-start`, `window-end`, and line 3 of `trace-summary.md`):

```sh
go run ./cmd/k8strace convert -shape <shape> -about "<line 3 of trace-summary.md>" \
  -start "$(cat window-start)" -end "$(cat window-end)" -out bench/recordings/<shape>.trace.gz record.jsonl.gz
go run ./cmd/k8strace show bench/recordings/*.trace.gz
```

`k8strace show` prints what each trace holds, and how it was made.

## The recordings

All five come from run [42716639345](https://github.com/djosh34/tigerstore/actions/runs/42716639345), on commit 6eb2be95. The converter of the commit that adds them built the traces from that run's logs. That run keeps its artifacts, the logs among them, for 30 days, until 2026-12-09; the workflow now keeps them 90. After that, a change to the converter needs new recordings, which are new workloads.

| Shape | Kwok nodes | Namespaces | Deployments x replicas each | Config maps, secrets, services each | CRDs x objects each | Calls in 10 min | Calls/s | Write calls | Calls that wait on a reply | Longest chain | Keys at the start |
|---|---|---|---|---|---|---|---|---|---|---|---|
| small | 10 | 5 | 2 x 3 | 5, 3, 1 | none | 253,625 | 423 | 102,526 | 143,014 | 756 | 722 |
| medium-crds | 50 | 20 | 4 x 5 | 10, 5, 2 | 3 x 20 | 421,587 | 702 | 197,450 | 275,766 | 295 | 3,991 |
| large | 200 | 40 | 5 x 12 | 8, 4, 3 | none | 348,951 | 581 | 166,916 | 217,010 | 295 | 9,377 |
| crd-heavy | 20 | 10 | 2 x 2 | 5, 2, 1 | 15 x 40 | 722,470 | 1,190 | 354,956 | 514,584 | 390 | 6,939 |
| many-namespaces | 30 | 250 | 1 x 1 | 3, 2, 1 | 1 x 3 | 289,690 | 483 | 123,885 | 164,095 | 297 | 5,922 |

In every shape, 40 to 47% of the calls are single-key GETs, which kube-apiserver's watch cache does not serve. Most of the rest are CAS Txns: updates, creates and deletes, each comparing a mod revision. 3 to 15% are progress requests on watch streams, which back consistent reads from the cache. LISTs are about 1%. kube-apiserver watches 74 to 89 resource prefixes, one stream each, and cancels and reopens each every few minutes from its revision; a trace keeps each watch as one stream.

## What a trace holds

A trace (`internal/k8strace`) holds:

- the keys, value sizes and leases alive when the window opened;
- the watches kube-apiserver had open;
- every call sent in the window, in the order it was sent.

Each call lists the earlier replies it waited on:

- the reply its apiserver learned a revision or lease from: a GET's answer, a Txn's answer, or the write behind a watch event;
- every earlier call on one of its keys that had answered before it was sent, if either writes the key, so conflicts on a key keep their order; calls that overlapped stay free to race;
- the write that made that revision.

Revisions and leases are references to those replies, never the recorded numbers.

A trace file is gzipped varints, about 9 bytes a call. `internal/k8strace/codec.go` describes the format.

## How the bench replays a trace

`cmd/loadgen -workload k8s`:

- **Seeds.** A seed draws one of the traces, and for each of 64 copies of the cluster a call in the first quarter of the trace to start at. The copy starts from the keys and leases the trace has at that call (`Trace.From`). Every store runs the same seeds, so a seed is one shape and one workload.
- **Copies.** The copies make one cluster 64 times bigger. Each copy's keys get a tag right after their watched resource prefix (`/registry/pods/c007.ns/name`). The recorded watches stay one set and see every copy's objects, so the store has the watchers of one real cluster. Keys under no watched prefix get a prefix per copy. A whole-resource read by the watch cache is sent by copy 0 only, since a bigger cluster still has one watch cache per resource. Single-key reads, such as the revision a consistent LIST waits on, every copy sends.
- **Waits.** A call goes out once the replies it waited on have come. It names the revisions and leases those replies gave in this replay. So each copy runs as fast as the store lets it, and conflicts on a key happen where they happened when it was recorded. A compare against a revision the window never saw gets 1, which fails as it did.
- **Leases.** The preload grants its leases at least 600 s, longer than a run, so short ones such as kube-apiserver's 15 s master leases do not end while 64 copies preload.
- **End of trace.** A run fails if a copy reaches the end of its trace. Then the recordings need longer windows.
