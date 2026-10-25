# tigerstore

A drop-in etcd replacement for Kubernetes: the etcd v3 API that kube-apiserver uses, consensus with [go.etcd.io/raft](https://github.com/etcd-io/raft), and its own storage. The Raft log is the value store. [docs/design.md](docs/design.md) describes what is built and what comes next.

It serves every gRPC method of etcd v3.7.2's API (KV, Watch, Lease, Cluster, Maintenance and Auth), the Lock and Election services and the gRPC health service, with gzip compression, and etcd's grpc-gateway under `/v3/`. The client port also answers `/health`, `/readyz`, `/livez`, `/version`, `/metrics` with etcd's metric names, `/debug/vars` and, with `--enable-pprof`, `/debug/pprof/`. It takes etcd's flags, `ETCD_*` variables and `--config-file`, saves snapshots for `etcdctl snapshot save` and restores them with `tigerstore snapshot restore`, and runs etcd's gRPC proxy and TCP gateway as `tigerstore grpc-proxy start` and `tigerstore gateway start`. [Differences from etcd v3.7.2](#differences-from-etcd-v372) lists what is not the same.

## Install

Each [release](https://github.com/djosh34/tigerstore/releases) has a binary for linux amd64 and arm64, their checksums in `SHA256SUMS`, and the image `ghcr.io/djosh34/tigerstore:<version>` for both platforms:

```sh
v=v0.1.0 arch=amd64               # or arm64
curl -fLO https://github.com/djosh34/tigerstore/releases/download/$v/tigerstore-$v-linux-$arch
curl -fLO https://github.com/djosh34/tigerstore/releases/download/$v/SHA256SUMS
sha256sum -c --ignore-missing SHA256SUMS
install -m 755 tigerstore-$v-linux-$arch tigerstore
./tigerstore --version
docker run --rm ghcr.io/djosh34/tigerstore:$v --version
```

Or build it from this repository with Go 1.27: `go build -o tigerstore ./cmd/tigerstore`. `--version` prints tigerstore's version and etcd's. `/version` on the client port answers with etcd v3.7.2's, which kubeadm reads.

## Run a 3-node cluster on one machine

You need this repository's scripts (`git clone --branch v0.1.0 https://github.com/djosh34/tigerstore`), the `tigerstore` binary in its root, openssl and, to talk to it, etcdctl.

```sh
scripts/dev-certs.sh certs        # throwaway CA and certificates
scripts/local-cluster.sh          # nodes on client ports 2379, 22379, 12379
```

Each node is one process. `scripts/local-cluster.sh` runs, for node 1:

```sh
./tigerstore --name node1 --data-dir data/node1 \
  --initial-cluster node1=https://127.0.0.1:2380,node2=https://127.0.0.1:22380,node3=https://127.0.0.1:12380 \
  --listen-peer-urls https://127.0.0.1:2380 --initial-advertise-peer-urls https://127.0.0.1:2380 \
  --listen-client-urls https://127.0.0.1:2379 --advertise-client-urls https://127.0.0.1:2379 \
  --cert-file certs/node1.pem --key-file certs/node1-key.pem \
  --trusted-ca-file certs/ca.pem --client-cert-auth \
  --peer-cert-file certs/node1.pem --peer-key-file certs/node1-key.pem \
  --peer-trusted-ca-file certs/ca.pem
```

The flags, `ETCD_*` environment variables and `--config-file` are etcd v3.7.2's, checked as etcd checks them. Every node gets the same `--initial-cluster`; a node's Raft ID is the position of its name in that list. Peer URLs must be https: traffic between nodes is always TLS 1.3 with certificates checked on both ends.

Talk to it with etcdctl:

```sh
etcdctl --endpoints=https://127.0.0.1:2379,https://127.0.0.1:22379,https://127.0.0.1:12379 \
  --cacert certs/ca.pem --cert certs/client.pem --key certs/client-key.pem \
  put foo bar
```

Stop the cluster with `kill $(cat data/*.pid)`. Starting it again recovers each node from its data directory.

The same cluster from the image, on Linux: each node in a container on the host's network, run as your user, with 1 GiB of memory, which tigerstore takes as its memory budget.

```sh
scripts/dev-certs.sh certs
mkdir -p data
cluster=node1=https://127.0.0.1:2380,node2=https://127.0.0.1:22380,node3=https://127.0.0.1:12380
client=(2379 22379 12379) peer=(2380 22380 12380)
for i in 1 2 3; do
  c=https://127.0.0.1:${client[i-1]} p=https://127.0.0.1:${peer[i-1]}
  docker run -d --name tigerstore-node$i --network host --user "$(id -u):$(id -g)" --memory 1g \
    -v "$PWD:/work" -w /work ghcr.io/djosh34/tigerstore:v0.1.0 \
    --name node$i --data-dir data/node$i --initial-cluster "$cluster" \
    --listen-peer-urls $p --initial-advertise-peer-urls $p \
    --listen-client-urls $c --advertise-client-urls $c \
    --cert-file certs/node$i.pem --key-file certs/node$i-key.pem \
    --trusted-ca-file certs/ca.pem --client-cert-auth \
    --peer-cert-file certs/node$i.pem --peer-key-file certs/node$i-key.pem \
    --peer-trusted-ca-file certs/ca.pem
done
```

`docker stop tigerstore-node1 tigerstore-node2 tigerstore-node3` stops it, and `docker start` with the same names starts it again on its data.

## Use it as external etcd for Kubernetes

kube-apiserver talks to tigerstore as it talks to an external etcd: over TLS, with a client certificate. CI's Kubernetes jobs boot this setup with `k8stest/boot.sh`.

### kind

kind's nodes are containers, so they reach the store at the host's address on kind's Docker network, and the certificates must name that address. Create the network with a known gateway (`docker network rm kind` first if an unused one exists), make certificates for it, and start the three nodes with their client ports on it:

```sh
docker network create kind --subnet 172.30.0.0/16 --gateway 172.30.0.1
CERT_IPS=172.30.0.1 scripts/dev-certs.sh certs
CLIENT_HOST=172.30.0.1 scripts/local-cluster.sh
mkdir -p kind-certs && cp certs/ca.pem certs/client.pem certs/client-key.pem kind-certs/
cat > kind.yaml <<EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    extraMounts:
      - hostPath: $PWD/kind-certs
        containerPath: /etc/kubernetes/pki/external
        readOnly: true
  - role: worker
kubeadmConfigPatches:
  - |
    apiVersion: kubeadm.k8s.io/v1beta4
    kind: ClusterConfiguration
    etcd:
      external:
        endpoints:
          - https://172.30.0.1:2379
          - https://172.30.0.1:22379
          - https://172.30.0.1:12379
        caFile: /etc/kubernetes/pki/external/ca.pem
        certFile: /etc/kubernetes/pki/external/client.pem
        keyFile: /etc/kubernetes/pki/external/client-key.pem
EOF
kind create cluster --name tigerstore --config kind.yaml
kubectl get nodes
etcdctl --endpoints=https://172.30.0.1:2379 --cacert certs/ca.pem --cert certs/client.pem --key certs/client-key.pem \
  get /registry/namespaces --prefix --keys-only
```

The peers stay on 127.0.0.1. A host firewall must let kind's network reach ports 2379, 22379 and 12379 on the host.

### kubeadm

Run one tigerstore node on each of three hosts, here 10.0.0.11, 10.0.0.12 and 10.0.0.13, with certificates that name all three:

```sh
CERT_IPS="10.0.0.11 10.0.0.12 10.0.0.13" scripts/dev-certs.sh certs   # once, then copy certs/ to each host
# on host i (1, 2 or 3), whose address is in ip:
./tigerstore --name node$i --data-dir /var/lib/tigerstore \
  --initial-cluster node1=https://10.0.0.11:2380,node2=https://10.0.0.12:2380,node3=https://10.0.0.13:2380 \
  --listen-peer-urls https://$ip:2380 --initial-advertise-peer-urls https://$ip:2380 \
  --listen-client-urls https://$ip:2379 --advertise-client-urls https://$ip:2379 \
  --cert-file certs/node$i.pem --key-file certs/node$i-key.pem \
  --trusted-ca-file certs/ca.pem --client-cert-auth \
  --peer-cert-file certs/node$i.pem --peer-key-file certs/node$i-key.pem \
  --peer-trusted-ca-file certs/ca.pem
```

On the control-plane host, put `ca.pem`, `client.pem` and `client-key.pem` in `/etc/kubernetes/pki/external/`, and start Kubernetes with `sudo kubeadm init --config kubeadm.yaml`:

```yaml
# kubeadm.yaml
apiVersion: kubeadm.k8s.io/v1beta4
kind: ClusterConfiguration
etcd:
  external:
    endpoints:
      - https://10.0.0.11:2379
      - https://10.0.0.12:2379
      - https://10.0.0.13:2379
    caFile: /etc/kubernetes/pki/external/ca.pem
    certFile: /etc/kubernetes/pki/external/client.pem
    keyFile: /etc/kubernetes/pki/external/client-key.pem
```

kubeadm then runs no etcd of its own. `kubectl get --raw /readyz/etcd` asks the API server whether it reaches the store. The certificates of `scripts/dev-certs.sh` expire after 30 days; a cluster you keep needs your own CA.

## Replace a member whose disk was lost or whose log is refused

A node that finds a damaged record in its log stops, and a restart refuses that log. A node whose disk was erased must not start again under its old name with empty storage: it would vote and acknowledge as if it still held what it had promised, and an acknowledged write could be lost. Bring either back as a new member, as with etcd, while the other two are up:

```sh
ctl="etcdctl --endpoints=https://127.0.0.1:2379,https://127.0.0.1:22379 \
  --cacert certs/ca.pem --cert certs/client.pem --key certs/client-key.pem"
$ctl member list                             # the ID of the broken member, here node3
$ctl member remove <ID>
$ctl member add node3 --peer-urls=https://127.0.0.1:12380
rm -rf data/node3                            # start on empty storage
./tigerstore --name node3 --data-dir data/node3 --initial-cluster-state existing \
  --initial-cluster node1=https://127.0.0.1:2380,node2=https://127.0.0.1:22380,node3=https://127.0.0.1:12380 \
  ...                                        # the node's other flags, as before
```

The new member gets a new ID and catches up from the leader, with a snapshot once the leader no longer has the log from its start. With `--strict-reconfig-check` (the default), a removal or an add waits until the node that takes it has been connected to its peers for 5 s; until then it answers that the cluster is unhealthy, and the call can be repeated. The `replace-member` step of the nightly chaos run (`cmd/chaos -env`) does this after an erased disk and after a refused log, under every checker (#161).

## Save and restore a snapshot

`etcdctl snapshot save` saves a snapshot of a running cluster in tigerstore's own format. Each node of the new cluster restores the same file into an empty data directory, with its own `--name` and the new cluster's `--initial-cluster`, and then starts on it as a new cluster:

```sh
etcdctl --endpoints=https://127.0.0.1:2379 --cacert certs/ca.pem --cert certs/client.pem --key certs/client-key.pem \
  snapshot save snap.db
./tigerstore snapshot restore snap.db --name node1 --data-dir data/node1 \
  --initial-cluster node1=https://127.0.0.1:2380,node2=https://127.0.0.1:22380,node3=https://127.0.0.1:12380
```

The restored cluster has the keys, revisions, leases, alarms, users and compaction of the snapshot, and the members of `--initial-cluster`.

## Differences from etcd v3.7.2

tigerstore answers as etcd v3.7.2 does except for these points, from [Research: what tigerstore lacks against etcd v3.7.2](https://github.com/djosh34/tigerstore/issues/399):

- Snapshots are in tigerstore's own format: `etcdutl snapshot status` and `etcdutl snapshot restore` cannot read them.
- tigerstore cannot take over an etcd cluster's data: it does not start on an etcd data directory or restore an etcd snapshot. `etcdctl make-mirror` copies keys and values only, without revisions or leases.
- etcd cannot take over tigerstore's data either: tigerstore has no export to etcd's format.
- `tigerstore snapshot restore` has no `--bump-revision` or `--mark-compacted`.
- There is no `etcdutl defrag` or `etcdutl migrate`; `Defragment` runs the log's cleaner online.
- A downgrade moves the version tigerstore reports, but its storage never goes down, a downgrade ends only by CANCEL, and `--downgrade-check-time` is ignored.
- Peers speak tigerstore's own protocol, not etcd's Raft over HTTP, so etcd and tigerstore nodes cannot be in one cluster.
- Peer URLs must be https, with TLS 1.3 and certificates checked on both ends; `--tls-max-version TLS1.2` is refused.
- A member with several peer URLs is dialed at the first.
- A client address serves http or https, not both.
- The feature gate `SetMemberLocalAddr` is accepted but does not bind the address of outgoing peer connections.
- With DNS SRV discovery over https and no peer CA file, a peer's certificate is checked for the host of its URL, not for the discovery domain.
- `--snapshot-count`, `--snapshot-catchup-entries`, `--max-snapshots`, `--max-wals`, `--backend-*`, `--compaction-batch-limit` and `--compaction-sleep-interval` are accepted and ignored: tigerstore has no bbolt and cuts checkpoints by bytes.
- The feature gates `FastLeaseKeepAlive`, `TxnModeWriteWithSharedBuffer` and `LeaseCheckpointPersist` are accepted and do nothing; lease checkpoints always persist.
- `Hash` and `HashKV` count the versions etcd counts, but their numbers are not etcd's for the same data; the members of one cluster still compare with each other.
- A lease keepalive is a Raft write: it needs a quorum and takes a round trip more than etcd's, which renews on the leader alone.
- `LeaseLeases` and `LeaseTimeToLive` are linearizable reads, and on a follower `LeaseTimeToLive` reports the granted TTL, not the time left.
- A watch stream closes with `Unavailable` when its client falls 4,096 responses or 64 MiB behind, when the node is short of memory, or when it installs a snapshot; etcd keeps slow watchers waiting. clientv3 and kube-apiserver resume the watch.
- A watch with `prev_kv` from the compaction revision is canceled with that revision; etcd sends the events at it without their previous values, which kube-apiserver rejects.
- A Range whose answer exceeds the memory budget is refused with `ResourceExhausted`; a smaller limit pages it.
- The request size limit counts a 10-byte request ID, so a write a few bytes below `--max-request-bytes` can be refused where etcd takes it.
- `--max-request-bytes` above 67,106,816 bytes (64 MiB less 2 KiB) is refused at startup: a write travels in one peer frame and one log record of at most 64 MiB.
- `Status.dbSize`, `dbSizeInUse`, `etcd_mvcc_db_total_size_in_bytes` and `etcd_mvcc_db_total_size_in_use_in_bytes` are the size the quota compares, an estimate of etcd's database for the versions kept, so they are equal and `etcdctl endpoint status` shows no fragmentation. The log's own size is `tigerstore_disk_bytes`, with its budget in `tigerstore_disk_budget_bytes`. A compaction frees quota without a defragmentation.
- `etcd_disk_backend_commit_duration_seconds` times a sync of the log, tigerstore's nearest to etcd's backend commit.
- `ETCD_VERIFY` is not used; tigerstore has its own corruption checks.
- `--version` prints a `tigerstore Version:` line before etcd's lines.

## Tests

`go test ./...` runs the unit tests, a 3-node cluster over TLS in one process, and the simulator on 16 seeds. `scripts/smoke.sh` runs etcdctl against a local cluster. `scripts/difftest.sh etcd` and `scripts/difftest.sh tigerstore` run the differential tests against etcd v3.7.2 and against a local cluster (see `difftest/doc.go`). CI runs the simulator on 400 seeds per pull request and 1,600 on each push to `main` and nightly; see `.github/workflows/ci.yml`.
