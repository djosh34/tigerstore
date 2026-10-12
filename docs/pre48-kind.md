# Throwaway kind and mass-delete experiment

Question: does kind v0.33.0 with Kubernetes v1.37.0 boot against three external stock etcd v3.7.2 members with client and peer TLS, and how long does namespace deletion of 50,000 ConfigMaps take on free GitHub runners?

This branch is pre-work for issue #48. It is not product code and must not merge.

## Run

On a disposable Linux Docker host, from the repository root:

```sh
bash experiments/pre48/boot.sh
python3 experiments/pre48/massdelete.py
```

The scripts use `.pre48/` for downloads, data, certificates, temporary files and results. They require Docker, curl, OpenSSL and Python 3. No Python packages or Go build are needed. The workflow `.github/workflows/pre48-kind.yml` runs both commands on `ubuntu-24.04-arm` and `ubuntu-24.04`, with a 15-minute job limit. It triggers only on the experiment branch.

The exact certificate commands, etcd flags and generated kind configuration are in `experiments/pre48/boot.sh`. It downloads the native stock etcd release archive, verifies the release SHA256SUMS and records binary hashes. The node image is `kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5`.

## Where the servers run

The three etcd processes run in three sibling Docker containers on the user-defined `kind` network, outside Kubernetes. Their DNS names are `external-etcd-1`, `external-etcd-2` and `external-etcd-3`. Each listens on container port 2379 for clients and 2380 for peers. No host port publishing is required. The downloaded stock executable is mounted into `alpine:3.22`; no custom etcd build is involved.

The kind control-plane container and two worker containers join the same Docker network. The API server is a host-network static pod inside the control-plane container. It can resolve and reach those external container names through Docker DNS. Runner host `127.0.0.1:2379` would not work: inside a kind node it refers to that node's network namespace, not the runner host.

Every server certificate has its `external-etcd-N` DNS SAN and both serverAuth and clientAuth EKUs, since the same ephemeral node certificate is used for both client serving and peer TLS. All client and peer listeners require certificates. A separate clientAuth certificate is given to kube-apiserver. Only the CA public certificate and that client key/certificate are mounted into the kind control plane. This is a two-day throwaway CA, not a production certificate policy. Private keys are excluded from evidence and artifacts.

The kind config patches `kubeadm.k8s.io/v1beta4` ClusterConfiguration:

```yaml
etcd:
  external:
    endpoints:
    - https://external-etcd-1:2379
    - https://external-etcd-2:2379
    - https://external-etcd-3:2379
    caFile: /etc/kubernetes/pki/external/ca.crt
    certFile: /etc/kubernetes/pki/external/client.crt
    keyFile: /etc/kubernetes/pki/external/client.key
```

Kind's `extraMounts` mounts the three client credential files into `/etc/kubernetes/pki/external`. Kubeadm's normal `/etc/kubernetes/pki` static-pod mount already includes them. No API server extraVolume is needed for that path. The exact generated kind YAML, stored kubeadm configuration and API server manifest are in the evidence directory.

Kind deliberately skips kubeadm preflight. A successful kind boot alone does not prove the ordinary kubeadm external-etcd `/version` check. The script separately GETs `/version` over mTLS from inside the kind control plane to all three members. Each returned `{"etcdserver":"3.7.2","etcdcluster":"3.7.0","storage":"3.7.0"}`.

## Measured result

[Run 38316307378](https://github.com/djosh34/tigerstore/actions/runs/38316307378), commit `617e1063ce8c65e889377f5facbc887f2e9f1411`, passed on both runners. Small raw artifacts are also committed under `evidence/pre48/38316307378/` so the evidence does not depend on artifact retention.

| Measurement | ubuntu-24.04-arm | ubuntu-24.04 |
| --- | ---: | ---: |
| Download and external-etcd setup, seconds | 6.175 | 11.839 |
| kind create to all nodes and CoreDNS ready, seconds | 45.819 | 48.943 |
| Setup plus boot, seconds | 51.994 | 60.781 |
| Create 50,000 ConfigMaps, seconds | 35.515 | 27.478 |
| Initial namespace DELETE response, seconds | 0.0052 | 0.0047 |
| Namespace DELETE to namespace 404, seconds | 137.419 | 76.908 |
| Whole Actions job, seconds | 233 | 175 |

One observation per architecture, not a performance comparison or a stable runtime promise. ARM had four Neoverse-N2 CPUs and 16,722,046,976 bytes of RAM. AMD64 had four AMD EPYC 9V45 vCPUs and 16,766,414,848 bytes of RAM. There was no CPU pinning or injected network delay. Both etcd and all Kubernetes nodes shared one runner VM. Etcd quota was 8 GiB per member.

All three Kubernetes nodes reached Ready; CoreDNS rolled out; API `/readyz` passed. No local etcd static-pod manifest existed. A ConfigMap created through the Kubernetes API produced the expected key in external etcd.

The mass-delete client used 64 Python threads with persistent HTTPS connections, 256-byte ConfigMap values, no finalizers and no owner references. It checked exactly 50,000 labelled ConfigMaps through paginated API reads before deletion. Each etcd member returned a prefix count of 50,001, including Kubernetes's automatic `kube-root-ca.crt` ConfigMap. Timing includes namespace-controller scheduling, retries and finalization, with a 0.5-second namespace poll interval. After namespace 404, the API returned an empty ConfigMap list and linearizable prefix counts from all three individual etcd endpoints were zero.

Default `--delete-collection-workers=1` remained unchanged. Both runs crossed the API server's 60-second DeleteCollection request timeout. Logs show one timed-out collection request on AMD64 and two on ARM. The namespace controller retried and cleanup completed. This was an eventual-cleanup pass, not an error-free deletion trace. The initial namespace DELETE response alone would have been a false measure of completion.

## Failed first attempt

[Run 38315952861](https://github.com/djosh34/tigerstore/actions/runs/38315952861), commit `a28344d6b6e9b4f1c0b9408844d994341cd8adb6`, booted both clusters, then failed the proof command before creating 50k objects. `etcdctl get --count-only` requires `--write-out=fields`. The fix added that output format and parsed the `"Count"` field. No Kubernetes or etcd behavior changed.

## Sources

- [kind v0.33.0 release and node image digest](https://github.com/kubernetes-sigs/kind/releases/tag/v0.33.0)
- [kind skips kubeadm preflight](https://github.com/kubernetes-sigs/kind/blob/v0.33.0/pkg/cluster/internal/kubeadm/config.go#L170-L174)
- [Kubeadm external-etcd certificate mounts](https://github.com/kubernetes/kubernetes/blob/v1.37.0/cmd/kubeadm/app/phases/controlplane/volumes.go)
- [Kubeadm external-etcd version check](https://github.com/kubernetes/kubernetes/blob/v1.37.0/cmd/kubeadm/app/preflight/checks.go#L741-L791)
- [Etcd v3.7.2 release](https://github.com/etcd-io/etcd/releases/tag/v3.7.2)
- [Earlier namespace-deletion request analysis, issue #27](https://github.com/djosh34/tigerstore/issues/27)

## Cleanup

On a disposable host after inspecting results:

```sh
.pre48/bin/kind delete cluster --name pre48
docker rm -f external-etcd-1 external-etcd-2 external-etcd-3
sudo rm -rf .pre48/data .pre48/bin .pre48/certs .pre48/kube-certs .pre48/api-*.pem .pre48/tmp .pre48/etcd.tgz
```

The workflow already does this. Do not delete another task's network or containers on a shared machine.
