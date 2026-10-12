# Kubernetes value compression experiment

Throwaway pre-work for #50 and #52, design experiment 4. Nothing here is intended to merge.

Run on a disposable free `ubuntu-24.04-arm` GitHub runner:

```sh
bash experiments/compress50/run.sh
```

The workflow has a 15-minute job limit and a 13-minute script limit. It refuses to start at or after 2026-10-13T22:00:00Z.

## Corpus

The script creates kind v0.33.0 with the release-pinned Kubernetes v1.37.0 node image and its stock etcd. It records the actual server and etcd versions. The workload creates 120 applications in 12 namespaces. Each application has one Deployment, three unequal ConfigMaps, one fake Secret, a Service, eight synthetic Events, two Leases and two externally managed EndpointSlices. Kubernetes creates the Pods, ReplicaSets, service EndpointSlices and its own Events. Application containers use pause rather than a business application. Configuration text includes application-specific routes and hashes. Secret payloads are seeded random bytes, not working external credentials.

Four apply generations change configuration, Deployment templates, Events and Leases. Between generations 1 and 2, the workload deletes and recreates one complete application per namespace. It uses server-side apply, so there is no giant client-side last-applied annotation. It waits for every Deployment to become available and records object counts and Pod phases. The dump is a single `etcdctl get '' --prefix --write-out=json`, including all current keys, not historical MVCC versions. No JSON re-encoding replaces the stored value bytes. Compression starts after kind deletes the cluster.

This is a controlled synthetic application fleet stored by a real kube-apiserver. It is not a production trace. Shared templates and resource proportions affect the ratio. The snapshot does not reproduce the order or frequency of raft writes during churn.

## Data handling

The raw dump stays in a private temporary directory on the disposable runner. Stock Kubernetes can add bootstrap tokens or other credentials even though application Secrets are fake. The script deletes the dump on exit. It never uploads the dump or dictionary. Only aggregate CSV, JSON, versions and hashes go into the artifact and experiment branch. Do not add raw corpus or dictionaries to Git.

The codec module documents its timing, dictionary split and framing. Its results exclude etcd keys, commands, raft metadata, checksums, network, fsync and integration work. They measure compression of values, not an end-to-end tigerstore speedup.
