#!/usr/bin/env bash
# Research runner: adapted from k8stest/boot.sh. Single-member stock etcd,
# proxy is the ONLY apiserver endpoint. No tigerstore code is run.
set -euo pipefail
root=${REC_ROOT:?}
mkdir -p "$root"/{bin,certs,client-certs,data,results}
case $(uname -m) in aarch64) arch=arm64;; x86_64) arch=amd64;; esac
curl -fsSL --retry 3 "https://github.com/kubernetes-sigs/kind/releases/download/v0.33.0/kind-linux-$arch" -o "$root/bin/kind"
curl -fsSL --retry 3 "https://github.com/kubernetes-sigs/kind/releases/download/v0.33.0/kind-linux-$arch.sha256sum" -o "$root/kind.sha256"
echo "$(awk '{print $1}' "$root/kind.sha256")  $root/bin/kind" | sha256sum -c
curl -fsSL --retry 3 "https://dl.k8s.io/release/v1.37.0/bin/linux/$arch/kubectl" -o "$root/bin/kubectl"
curl -fsSL --retry 3 "https://dl.k8s.io/release/v1.37.0/bin/linux/$arch/kubectl.sha256" -o "$root/kubectl.sha256"
echo "$(<"$root/kubectl.sha256")  $root/bin/kubectl" | sha256sum -c
curl -fsSL --retry 3 "https://github.com/etcd-io/etcd/releases/download/v3.7.2/etcd-v3.7.2-linux-$arch.tar.gz" -o "$root/etcd.tgz"
curl -fsSL --retry 3 https://github.com/etcd-io/etcd/releases/download/v3.7.2/SHA256SUMS -o "$root/etcd.sha256"
expected=$(awk -v file="etcd-v3.7.2-linux-$arch.tar.gz" '$2==file {print $1}' "$root/etcd.sha256")
test -n "$expected"; echo "$expected  $root/etcd.tgz" | sha256sum -c
tar xzf "$root/etcd.tgz" -C "$root/bin" --strip-components=1 "etcd-v3.7.2-linux-$arch/etcd" "etcd-v3.7.2-linux-$arch/etcdctl"
chmod +x "$root/bin/"*
export PATH="$root/bin:$PATH"
{ git rev-parse HEAD; uname -a; lscpu; free -b; df -B1 "$root"; docker version; kind version; kubectl version --client; etcdctl version; sha256sum "$root/bin/"*; } > "$root/results/environment.txt"
docker network create kind --subnet 172.30.0.0/16 --gateway 172.30.0.1
CERT_IPS=172.30.0.1 bash scripts/dev-certs.sh "$root/certs"
cp "$root/certs/"{ca.pem,client.pem,client-key.pem} "$root/client-certs/"
etcd --name n1 --data-dir "$root/data/n1" --listen-client-urls https://127.0.0.1:2379 --advertise-client-urls https://127.0.0.1:2379 \
 --listen-peer-urls https://127.0.0.1:2380 --initial-advertise-peer-urls https://127.0.0.1:2380 --initial-cluster n1=https://127.0.0.1:2380 \
 --cert-file "$root/certs/node1.pem" --key-file "$root/certs/node1-key.pem" --trusted-ca-file "$root/certs/ca.pem" --client-cert-auth \
 --peer-cert-file "$root/certs/node1.pem" --peer-key-file "$root/certs/node1-key.pem" --peer-trusted-ca-file "$root/certs/ca.pem" --peer-client-cert-auth \
 --quota-backend-bytes 8589934592 > "$root/results/etcd.log" 2>&1 &
echo $! > "$root/etcd.pid"
etcdrec --cert-dir "$root/certs" --out "$root/results/record.jsonl" > "$root/results/proxy.log" 2>&1 &
echo $! > "$root/proxy.pid"
# Health check bypasses the recorder, so it cannot contaminate the recording.
export ETCDCTL_ENDPOINTS=https://127.0.0.1:2379 ETCDCTL_CACERT="$root/certs/ca.pem" ETCDCTL_CERT="$root/certs/client.pem" ETCDCTL_KEY="$root/certs/client-key.pem"
for i in $(seq 30); do if etcdctl endpoint health; then break;fi;sleep 1;done
etcdctl endpoint health
# Keep k8stest's kubeadm defaults and mount, reduce to one real worker, and use
# one endpoint (the recorder) rather than the normal three etcd members.
python3 - "$root" <<'PY'
import sys
from pathlib import Path
r=Path(sys.argv[1]);s=Path('k8stest/kind.yaml').read_text().replace('CERTS',str(r/'client-certs'))
s=s.replace('  - role: worker\n  - role: worker\n','  - role: worker\n')
# /16 has only 256 default /24 node CIDRs; 500 synthetic nodes need more.
s=s.replace('apiVersion: kind.x-k8s.io/v1alpha4\n', 'apiVersion: kind.x-k8s.io/v1alpha4\nnetworking:\n  podSubnet: "10.240.0.0/12"\n')
s=s.replace('          - https://172.30.0.1:20020\n','').replace('          - https://172.30.0.1:20030\n','')
(r/'results/kind.yaml').write_text(s)
PY
kind create cluster --name recording --image kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5 --config "$root/results/kind.yaml" --wait 5m
kubectl wait --for=condition=Ready nodes --all --timeout=180s
kubectl -n kube-system rollout status deployment/coredns --timeout=180s
docker exec recording-control-plane cat /etc/kubernetes/manifests/kube-apiserver.yaml > "$root/results/apiserver.yaml"
docker exec recording-control-plane test ! -e /etc/kubernetes/manifests/etcd.yaml
kubectl get --raw '/metrics' > "$root/results/apiserver-metrics-before.txt"
# Pin and save both the release manifests and resolved image digest.
curl -fsSL --retry 3 https://github.com/kubernetes-sigs/kwok/releases/download/v0.8.0/kwok.yaml -o "$root/results/kwok.yaml"
curl -fsSL --retry 3 https://github.com/kubernetes-sigs/kwok/releases/download/v0.8.0/stage-fast.yaml -o "$root/results/stage-fast.yaml"
kubectl apply -f "$root/results/kwok.yaml"
kubectl apply -f "$root/results/stage-fast.yaml"
# Upstream load Jobs run pause forever; CL2 waits for parallelism Running pods,
# not completed Jobs. KWOK's generic fast stage would instantly complete them.
kubectl delete stage pod-complete
kubectl get stages -o yaml > "$root/results/kwok-stages-effective.yaml"
kubectl -n kube-system rollout status deployment/kwok-controller --timeout=240s
kubectl -n kube-system get pod -l app=kwok-controller -o json > "$root/results/kwok-pods.json"
# Fake nodes have no taint so the unmodified CL2 load templates can schedule.
# Real worker is left available for infrastructure only via its NoSchedule taint.
kubectl taint nodes recording-worker research=infra:NoSchedule
kubectl label nodes --all research-real=true
# Fake nodes should not grow fake kube-proxy/kindnet infrastructure pods.
for ds in kube-proxy kindnet; do
 kubectl -n kube-system patch ds "$ds" --type merge -p '{"spec":{"template":{"spec":{"nodeSelector":{"research-real":"true"}}}}}'
done
python3 - "${NODES:-100}" <<'PY' | kubectl create -f -
import json,sys
nodes=[]
for i in range(int(sys.argv[1])):
 nodes.append({'apiVersion':'v1','kind':'Node','metadata':{'name':f'kwok-{i:04d}','annotations':{'kwok.x-k8s.io/node':'fake'},'labels':{'kubernetes.io/hostname':f'kwok-{i:04d}','kubernetes.io/os':'linux','kubernetes.io/arch':'arm64','node-role.kubernetes.io/agent':'','research-579':'fake'}},'spec':{},'status':{'addresses':[{'type':'InternalIP','address':f'172.30.{128+i//254}.{1+i%254}'},{'type':'Hostname','address':f'kwok-{i:04d}'}],'capacity':{'cpu':'32','memory':'128Gi','pods':'110'},'allocatable':{'cpu':'32','memory':'128Gi','pods':'110'}}})
print(json.dumps({'apiVersion':'v1','kind':'List','items':nodes}))
PY
kubectl wait --for=condition=Ready node -l research-579=fake --timeout=240s
kubectl get nodes -o wide > "$root/results/nodes-before.txt"
# 3 minutes lets node creation, scheduler caches and controller work settle.
sleep 180
kubectl get --raw '/readyz?verbose' > "$root/results/readyz.txt"
kubectl -n kube-system get pods -o wide > "$root/results/pods-settled.txt"
# Refuse a supposed idle baseline with infrastructure CrashLoop churn.
kubectl -n kube-system get pods -o json | python3 -c 'import json,sys; p=json.load(sys.stdin)["items"]; bad=[x["metadata"]["name"] for x in p for c in x.get("status",{}).get("containerStatuses",[]) if c.get("state",{}).get("waiting",{}).get("reason")=="CrashLoopBackOff"]; assert not bad, bad'
