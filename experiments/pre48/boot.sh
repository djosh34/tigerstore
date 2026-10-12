#!/usr/bin/env bash
# Question: can kind 0.33.0 / Kubernetes 1.37.0 boot against three stock etcd 3.7.2 members with client and peer TLS?
set -euo pipefail
root=$(pwd)/.pre48
mkdir -p "$root"/{bin,certs,data,results,tmp}
export TMPDIR="$root/tmp"
arch=$(uname -m); case "$arch" in aarch64) arch=arm64;; x86_64) arch=amd64;; esac
start=$(date +%s.%N)
curl -fsSL --retry 3 "https://github.com/kubernetes-sigs/kind/releases/download/v0.33.0/kind-linux-$arch" -o "$root/bin/kind"
curl -fsSL --retry 3 "https://dl.k8s.io/release/v1.37.0/bin/linux/$arch/kubectl" -o "$root/bin/kubectl"
curl -fsSL --retry 3 "https://github.com/etcd-io/etcd/releases/download/v3.7.2/etcd-v3.7.2-linux-$arch.tar.gz" -o "$root/etcd.tgz"
tar -xzf "$root/etcd.tgz" -C "$root/bin" --strip-components=1 "etcd-v3.7.2-linux-$arch/etcd" "etcd-v3.7.2-linux-$arch/etcdctl"
chmod +x "$root/bin/"*
export PATH="$root/bin:$PATH"
{ uname -a; lscpu; free -b; df -B1 .; docker version; kind version; kubectl version --client; etcd --version; sha256sum "$root/bin/"*; } > "$root/results/environment.txt"
cd "$root/certs"
openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.crt -days 2 -subj /CN=pre48-ca -addext basicConstraints=critical,CA:TRUE -addext keyUsage=critical,keyCertSign,cRLSign
for n in 1 2 3; do
  openssl req -newkey rsa:2048 -nodes -keyout "etcd$n.key" -out "etcd$n.csr" -subj "/CN=external-etcd-$n"
  printf 'subjectAltName=DNS:external-etcd-%s,DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth,clientAuth\nkeyUsage=digitalSignature,keyEncipherment\n' "$n" > "etcd$n.ext"
  openssl x509 -req -in "etcd$n.csr" -CA ca.crt -CAkey ca.key -CAcreateserial -days 2 -out "etcd$n.crt" -extfile "etcd$n.ext"
done
openssl req -newkey rsa:2048 -nodes -keyout client.key -out client.csr -subj /CN=kube-apiserver-etcd-client
printf 'extendedKeyUsage=clientAuth\nkeyUsage=digitalSignature,keyEncipherment\n' > client.ext
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 2 -out client.crt -extfile client.ext
# Only public CA and client credentials go into kind nodes. Never publish private keys in artifacts.
mkdir -p "$root/kube-certs"
cp ca.crt client.crt client.key "$root/kube-certs/"
cd - >/dev/null
# User-defined bridge DNS makes external-etcd-N reachable from kind node containers.
docker network create kind
cluster='external-etcd-1=https://external-etcd-1:2380,external-etcd-2=https://external-etcd-2:2380,external-etcd-3=https://external-etcd-3:2380'
for n in 1 2 3; do
  mkdir -p "$root/data/$n"
  docker run -d --name "external-etcd-$n" --network kind \
    -v "$root/bin:/bin-etcd:ro" -v "$root/certs:/certs:ro" -v "$root/data/$n:/data" \
    --entrypoint /bin-etcd/etcd alpine:3.22 \
    --name "external-etcd-$n" --data-dir /data \
    --listen-client-urls https://0.0.0.0:2379 --advertise-client-urls "https://external-etcd-$n:2379" \
    --listen-peer-urls https://0.0.0.0:2380 --initial-advertise-peer-urls "https://external-etcd-$n:2380" \
    --initial-cluster "$cluster" --initial-cluster-token pre48 --initial-cluster-state new \
    --cert-file "/certs/etcd$n.crt" --key-file "/certs/etcd$n.key" --trusted-ca-file /certs/ca.crt --client-cert-auth \
    --peer-cert-file "/certs/etcd$n.crt" --peer-key-file "/certs/etcd$n.key" --peer-trusted-ca-file /certs/ca.crt --peer-client-cert-auth \
    --quota-backend-bytes 8589934592 --logger zap --log-level warn
done
endpoints='https://external-etcd-1:2379,https://external-etcd-2:2379,https://external-etcd-3:2379'
for attempt in $(seq 1 30); do
  if docker exec external-etcd-1 /bin-etcd/etcdctl --endpoints "$endpoints" --cacert /certs/ca.crt --cert /certs/client.crt --key /certs/client.key endpoint health; then break; fi
  sleep 1
done
docker exec external-etcd-1 /bin-etcd/etcdctl --endpoints "$endpoints" --cacert /certs/ca.crt --cert /certs/client.crt --key /certs/client.key endpoint status -w json > "$root/results/etcd-status-before.json"
python3 - "$root" <<'PY'
import sys
from pathlib import Path
root = Path(sys.argv[1])
config = '''kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
  extraMounts:
  - hostPath: CERT_DIR
    containerPath: /etc/kubernetes/pki/external
    readOnly: true
- role: worker
- role: worker
kubeadmConfigPatches:
- |
  apiVersion: kubeadm.k8s.io/v1beta4
  kind: ClusterConfiguration
  etcd:
    external:
      endpoints:
      - https://external-etcd-1:2379
      - https://external-etcd-2:2379
      - https://external-etcd-3:2379
      caFile: /etc/kubernetes/pki/external/ca.crt
      certFile: /etc/kubernetes/pki/external/client.crt
      keyFile: /etc/kubernetes/pki/external/client.key
'''
(root / 'results/kind.yaml').write_text(config.replace('CERT_DIR', str(root / 'kube-certs')))
PY
boot_start=$(date +%s.%N)
kind create cluster --name pre48 --image kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5 --config "$root/results/kind.yaml" --wait 180s
kubectl wait --for=condition=Ready nodes --all --timeout=180s
kubectl -n kube-system rollout status deployment/coredns --timeout=180s
boot_end=$(date +%s.%N)
kubectl get nodes -o wide > "$root/results/nodes.txt"
kubectl get pods -A -o wide > "$root/results/pods.txt"
kubectl get --raw '/readyz?verbose' > "$root/results/readyz.txt"
kubectl -n kube-system get cm kubeadm-config -o yaml > "$root/results/kubeadm-config.yaml"
docker exec pre48-control-plane cat /etc/kubernetes/manifests/kube-apiserver.yaml > "$root/results/apiserver.yaml"
docker exec pre48-control-plane test ! -e /etc/kubernetes/manifests/etcd.yaml
# Exercise the external store through Kubernetes and inspect the raw key.
kubectl create configmap pre48-proof --from-literal=backend=external-etcd-3.7.2
docker exec external-etcd-1 /bin-etcd/etcdctl --endpoints "$endpoints" --cacert /certs/ca.crt --cert /certs/client.crt --key /certs/client.key get /registry/configmaps/default/pre48-proof --count-only > "$root/results/proof-key-count.txt"
test "$(tr -d '\r\n' < "$root/results/proof-key-count.txt")" = 1
kubectl delete configmap pre48-proof
python3 - "$start" "$boot_start" "$boot_end" > "$root/results/boot-timing.json" <<'PY'
import sys,json
start,boot,end=map(float,sys.argv[1:])
print(json.dumps({'downloads_etcd_setup_seconds':boot-start,'kind_boot_ready_seconds':end-boot,'total_boot_seconds':end-start}))
PY
