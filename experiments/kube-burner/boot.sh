#!/usr/bin/env bash
# Adapted from pre48 boot.sh at 617e1063ce8c65e889377f5facbc887f2e9f1411.
set -euo pipefail
[[ $(date -u +%s) -lt $(date -u -d '2026-10-13T22:00:00Z' +%s) ]]
: "${PRIVATE:?}" "${OUT:?}" "${BIN:?}" "${STORE:?}"
mkdir -p "$PRIVATE"/{certs,data,kube-certs}
cd "$PRIVATE/certs"
openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.crt -days 2 -subj /CN=kube-burner-ca -addext basicConstraints=critical,CA:TRUE -addext keyUsage=critical,keyCertSign,cRLSign
for n in 1 2 3; do
  openssl req -newkey rsa:2048 -nodes -keyout "etcd$n.key" -out "etcd$n.csr" -subj "/CN=external-etcd-$n"
  printf 'subjectAltName=DNS:external-etcd-%s,DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth,clientAuth\nkeyUsage=digitalSignature,keyEncipherment\n' "$n" > "etcd$n.ext"
  openssl x509 -req -in "etcd$n.csr" -CA ca.crt -CAkey ca.key -CAcreateserial -days 2 -out "etcd$n.crt" -extfile "etcd$n.ext"
done
openssl req -newkey rsa:2048 -nodes -keyout client.key -out client.csr -subj /CN=kube-apiserver-etcd-client
printf 'extendedKeyUsage=clientAuth\nkeyUsage=digitalSignature,keyEncipherment\n' > client.ext
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 2 -out client.crt -extfile client.ext
cp ca.crt client.crt client.key "$PRIVATE/kube-certs/"
cd - >/dev/null
docker network create kind
cluster='external-etcd-1=https://external-etcd-1:2380,external-etcd-2=https://external-etcd-2:2380,external-etcd-3=https://external-etcd-3:2380'
binary=$STORE; [[ "$STORE" != etcd ]] || binary=etcd
for n in 1 2 3; do
  mkdir -p "$PRIVATE/data/$n"
  extra=()
  if [[ "$STORE" == etcd ]]; then
    extra=(--initial-advertise-peer-urls "https://external-etcd-$n:2380" --initial-cluster-state new --peer-client-cert-auth --quota-backend-bytes 8589934592 --logger zap --log-level warn)
  fi
  # Tigerstore always requires peer certificates. Its flag set has no peer-client-cert-auth switch.
  docker run -d --name "external-etcd-$n" --network kind \
    -v "$BIN:/bin-etcd:ro" -v "$PRIVATE/certs:/certs:ro" -v "$PRIVATE/data/$n:/data" \
    --entrypoint "/bin-etcd/$binary" alpine:3.22 \
    --name "external-etcd-$n" --data-dir /data \
    --listen-client-urls https://0.0.0.0:2379 --advertise-client-urls "https://external-etcd-$n:2379" \
    --listen-peer-urls https://0.0.0.0:2380 --initial-cluster "$cluster" --initial-cluster-token kube-burner \
    --cert-file "/certs/etcd$n.crt" --key-file "/certs/etcd$n.key" --trusted-ca-file /certs/ca.crt --client-cert-auth \
    --peer-cert-file "/certs/etcd$n.crt" --peer-key-file "/certs/etcd$n.key" --peer-trusted-ca-file /certs/ca.crt "${extra[@]}"
done
endpoints='https://external-etcd-1:2379,https://external-etcd-2:2379,https://external-etcd-3:2379'
ctl=(docker exec external-etcd-1 /bin-etcd/etcdctl --endpoints "$endpoints" --cacert /certs/ca.crt --cert /certs/client.crt --key /certs/client.key --command-timeout=10s)
# Keep endpoint-health compatibility failures visible, but readiness for both
# stores uses linearizable reads. Tigerstore main lacks Maintenance.Alarm.
set +e
"${ctl[@]}" endpoint health > "$OUT/endpoint-health.stdout" 2> "$OUT/endpoint-health.stderr"
health_rc=$?
set -e
printf '%s\n' "$health_rc" > "$OUT/endpoint-health.exit"
for n in 1 2 3; do
  ready=0
  for _ in $(seq 1 30); do
    if docker exec external-etcd-1 /bin-etcd/etcdctl --endpoints "https://external-etcd-$n:2379" --cacert /certs/ca.crt --cert /certs/client.crt --key /certs/client.key --command-timeout=5s get burner-ready; then ready=1; break; fi
    sleep 1
  done
  test "$ready" = 1
done
"${ctl[@]}" endpoint status -w json > "$OUT/etcd-status-boot.json"
python3 - "$PRIVATE" "$OUT" <<'PY'
import sys
from pathlib import Path
private, out = map(Path, sys.argv[1:])
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
(out / 'kind.yaml').write_text(config.replace('CERT_DIR', str(private / 'kube-certs')))
PY
kind create cluster --name burner --retain --image kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5 --config "$OUT/kind.yaml" --wait 180s
kubectl wait --for=condition=Ready nodes --all --timeout=180s
kubectl -n kube-system rollout status deployment/coredns --timeout=180s
kind load docker-image --name burner registry.k8s.io/pause:3.10
kubectl version -o json > "$OUT/kubernetes-version.json"
kubectl get nodes -o wide > "$OUT/nodes.txt"
kubectl get --raw '/readyz?verbose' > "$OUT/readyz.txt"
for n in 1 2 3; do
  docker exec burner-control-plane curl -fsS --cacert /etc/kubernetes/pki/external/ca.crt --cert /etc/kubernetes/pki/external/client.crt --key /etc/kubernetes/pki/external/client.key "https://external-etcd-$n:2379/version" > "$OUT/etcd-$n-version.json"
done
docker exec burner-control-plane test ! -e /etc/kubernetes/manifests/etcd.yaml
kubectl create configmap burner-proof --from-literal=backend=external
"${ctl[@]}" get /registry/configmaps/default/burner-proof --count-only -w fields > "$OUT/proof-key-count.txt"
grep -qx '"Count" : 1' "$OUT/proof-key-count.txt"
kubectl delete configmap burner-proof
