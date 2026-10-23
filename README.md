# tigerstore

A drop-in etcd replacement for Kubernetes: the etcd v3 API that kube-apiserver uses, consensus with [go.etcd.io/raft](https://github.com/etcd-io/raft), and its own storage. The Raft log is the value store. [docs/design.md](docs/design.md) describes what is built and what comes next.

Work in progress. It serves Range, Put, DeleteRange, Txn, Compact, Watch, leases, MemberList, Maintenance.Status, Maintenance.Alarm, and the Lock and Election services over TLS, the gRPC health service, and `/health`, `/readyz`, `/livez` and `/version` on the client port.

## Run a 3-node cluster on one machine

You need Go 1.27, openssl and, to talk to it, etcdctl.

```sh
go build -o tigerstore ./cmd/tigerstore
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

## Tests

`go test ./...` runs the unit tests, a 3-node cluster over TLS in one process, and the simulator on 16 seeds. `scripts/smoke.sh` runs etcdctl against a local cluster. `scripts/difftest.sh etcd` and `scripts/difftest.sh tigerstore` run the differential tests against etcd v3.7.2 and against a local cluster (see `difftest/doc.go`). CI runs the simulator on 400 seeds per pull request and 1,600 on each push to `main` and nightly; see `.github/workflows/ci.yml`.
