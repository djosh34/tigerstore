#!/usr/bin/env bash
# A three-member TLS store on the Docker bridge, one kind control plane
# and two workers. Run only on a disposable Actions runner.
set -euo pipefail
root=${K8S_ROOT:?}
store=${STORE:-tigerstore}
mkdir -p "$root"/{bin,certs,client-certs,data,results}
case $(uname -m) in
  aarch64) arch=arm64 ;;
  x86_64) arch=amd64 ;;
  *) echo 'unsupported architecture' >&2; exit 1 ;;
esac
curl -fsSL --retry 3 "https://github.com/kubernetes-sigs/kind/releases/download/v0.33.0/kind-linux-$arch" -o "$root/bin/kind"
curl -fsSL --retry 3 "https://github.com/kubernetes-sigs/kind/releases/download/v0.33.0/kind-linux-$arch.sha256sum" -o "$root/kind.sha256"
echo "$(awk '{print $1}' "$root/kind.sha256")  $root/bin/kind" | sha256sum -c
curl -fsSL --retry 3 "https://dl.k8s.io/release/v1.37.0/bin/linux/$arch/kubectl" -o "$root/bin/kubectl"
curl -fsSL --retry 3 "https://dl.k8s.io/release/v1.37.0/bin/linux/$arch/kubectl.sha256" -o "$root/kubectl.sha256"
echo "$(<"$root/kubectl.sha256")  $root/bin/kubectl" | sha256sum -c
curl -fsSL --retry 3 "https://github.com/etcd-io/etcd/releases/download/v3.7.2/etcd-v3.7.2-linux-$arch.tar.gz" -o "$root/etcd.tgz"
curl -fsSL --retry 3 https://github.com/etcd-io/etcd/releases/download/v3.7.2/SHA256SUMS -o "$root/etcd.sha256"
expected=$(awk -v file="etcd-v3.7.2-linux-$arch.tar.gz" '$2==file {print $1}' "$root/etcd.sha256")
test -n "$expected"
echo "$expected  $root/etcd.tgz" | sha256sum -c
tar -xzf "$root/etcd.tgz" -C "$root/bin" --strip-components=1 "etcd-v3.7.2-linux-$arch/etcd" "etcd-v3.7.2-linux-$arch/etcdctl"
chmod +x "$root/bin/kind" "$root/bin/kubectl"
export PATH="$root/bin:$PATH"
case "$store" in
  tigerstore) CGO_ENABLED=0 go build -trimpath -o "$root/bin/tigerstore" ./cmd/tigerstore ;;
  etcd) ;;
  *) echo "unknown store: $store" >&2; exit 1 ;;
esac
{
  git rev-parse HEAD
  uname -a
  lscpu
  free -b
  df -B1 "$root"
  docker version
  kind version
  kubectl version --client
  etcdctl version
  sha256sum "$root/bin/"*
  if [ "$store" = tigerstore ]; then go version -m "$root/bin/tigerstore"; fi
} > "$root/results/environment.txt"
gateway=172.30.0.1
docker network create kind --subnet 172.30.0.0/16 --gateway "$gateway"
CERT_IPS=$gateway bash scripts/dev-certs.sh "$root/certs"
# Do not mount the CA key or peer keys in Kubernetes nodes.
cp "$root/certs/ca.pem" "$root/certs/client.pem" "$root/certs/client-key.pem" "$root/client-certs/"
initial="n1=https://$gateway:20011,n2=https://$gateway:20021,n3=https://$gateway:20031"
for i in 1 2 3; do
  client="https://$gateway:$((20000 + 10 * i))"
  peer="https://$gateway:$((20001 + 10 * i))"
  extra=()
  if [ "$store" = tigerstore ]; then extra+=(--memory-budget-bytes 1073741824); fi
  "$root/bin/$store" --name "n$i" --data-dir "$root/data/n$i" \
    --listen-client-urls "$client" --advertise-client-urls "$client" \
    --listen-peer-urls "$peer" --initial-advertise-peer-urls "$peer" \
    --initial-cluster "$initial" --initial-cluster-token k8stest \
    --cert-file "$root/certs/node$i.pem" --key-file "$root/certs/node$i-key.pem" \
    --trusted-ca-file "$root/certs/ca.pem" --client-cert-auth \
    --peer-cert-file "$root/certs/node$i.pem" --peer-key-file "$root/certs/node$i-key.pem" \
    --peer-trusted-ca-file "$root/certs/ca.pem" --peer-client-cert-auth \
    --quota-backend-bytes 8589934592 "${extra[@]}" >"$root/results/n$i.log" 2>&1 &
  echo "$!" >> "$root/pids"
done
export ETCDCTL_ENDPOINTS="https://$gateway:20010,https://$gateway:20020,https://$gateway:20030"
export ETCDCTL_CACERT="$root/certs/ca.pem" ETCDCTL_CERT="$root/certs/client.pem" ETCDCTL_KEY="$root/certs/client-key.pem"
export ETCDCTL_COMMAND_TIMEOUT=5s
for attempt in $(seq 30); do
  if etcdctl endpoint health; then break; fi
  sleep 1
done
etcdctl endpoint health
etcdctl endpoint status -w json > "$root/results/status-before.json"
sed "s|CERTS|$root/client-certs|g" k8stest/kind.yaml > "$root/results/kind.yaml"
kind create cluster --name tigerstore --image kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5 \
  --config "$root/results/kind.yaml" --wait 5m
kubectl wait --for=condition=Ready nodes --all --timeout=180s
kubectl -n kube-system rollout status deployment/coredns --timeout=180s
kubectl get nodes -o wide > "$root/results/nodes.txt"
kubectl get --raw '/readyz?verbose' > "$root/results/readyz.txt"
docker exec tigerstore-control-plane cat /etc/kubernetes/manifests/kube-apiserver.yaml > "$root/results/apiserver.yaml"
docker exec tigerstore-control-plane test ! -e /etc/kubernetes/manifests/etcd.yaml
kubectl create configmap external-store-proof --from-literal=store="$store"
for endpoint in ${ETCDCTL_ENDPOINTS//,/ }; do
  etcdctl --endpoints "$endpoint" get /registry/configmaps/default/external-store-proof --count-only -w json | jq -e '.count == 1'
done
kubectl delete configmap external-store-proof
{
  echo "ETCDCTL_ENDPOINTS=$ETCDCTL_ENDPOINTS"
  echo "ETCDCTL_CACERT=$ETCDCTL_CACERT"
  echo "ETCDCTL_CERT=$ETCDCTL_CERT"
  echo "ETCDCTL_KEY=$ETCDCTL_KEY"
  echo "ETCDCTL_COMMAND_TIMEOUT=30s"
} >> "$GITHUB_ENV"
