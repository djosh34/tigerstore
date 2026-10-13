# tigerstore

A drop-in etcd replacement for Kubernetes: the etcd v3 API that kube-apiserver uses, consensus with [go.etcd.io/raft](https://github.com/etcd-io/raft), and its own storage. The Raft log is the value store. [docs/design.md](docs/design.md) describes what is built and what comes next.

Work in progress. Today it serves Range, Put, DeleteRange, Txn, Watch and MemberList over TLS. Leases, compaction, health and version are not built yet.

## Run a 3-node cluster on one machine

You need Go 1.27, openssl and, to talk to it, etcdctl.

```sh
go build -o tigerstore ./cmd/tigerstore
scripts/dev-certs.sh certs        # throwaway CA and certificates
scripts/local-cluster.sh          # nodes on client ports 2379, 22379, 32379
```

Each node is one process. `scripts/local-cluster.sh` runs, for node 1:

```sh
./tigerstore --name node1 --data-dir data/node1 \
  --initial-cluster node1=https://127.0.0.1:2380,node2=https://127.0.0.1:22380,node3=https://127.0.0.1:32380 \
  --listen-client-urls https://127.0.0.1:2379 \
  --cert-file certs/node1.pem --key-file certs/node1-key.pem \
  --trusted-ca-file certs/ca.pem --client-cert-auth \
  --peer-cert-file certs/node1.pem --peer-key-file certs/node1-key.pem \
  --peer-trusted-ca-file certs/ca.pem
```

Every node gets the same `--initial-cluster`; a node's Raft ID is its position in that list. Peer URLs must be https: traffic between nodes is always TLS 1.3 with certificates checked on both ends.

Talk to it with etcdctl:

```sh
etcdctl --endpoints=https://127.0.0.1:2379,https://127.0.0.1:22379,https://127.0.0.1:32379 \
  --cacert certs/ca.pem --cert certs/client.pem --key certs/client-key.pem \
  put foo bar
```

Stop the cluster with `kill $(cat data/*.pid)`. Starting it again recovers each node from its data directory.

## Tests

`go test ./...` runs the unit tests, a 3-node cluster over TLS in one process, and the deterministic simulator on 16 seeds. `scripts/smoke.sh` runs etcdctl against a local cluster. `scripts/difftest.sh etcd` and `scripts/difftest.sh tigerstore` run the differential tests against etcd v3.7.2 and against a local cluster (see `difftest/doc.go`). CI runs the simulator on 1,600 seeds per push; see `.github/workflows/ci.yml`.
