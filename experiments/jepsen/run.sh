#!/usr/bin/env bash
# Runner only. Never run Docker, builds, or faults on the agent host.
set -euo pipefail
[[ ${GITHUB_ACTIONS:-} == true ]] || { echo 'GitHub runner required' >&2; exit 2; }
STORE=${1:?store}; WORKLOAD=${2:?workload}; NEMESIS=${3:?nemesis}; DURATION=${4:-60}
export STORE
root=$PWD
out=$root/experiments/jepsen/results
mkdir -p "$out" "$root/.jepsen/assets"
assets=$root/.jepsen/assets
exec > >(tee "$out/runner.log") 2>&1
printf 'start=%s store=%s workload=%s nemesis=%s duration=%s\n' "$(date -u +%FT%TZ)" "$STORE" "$WORKLOAD" "$NEMESIS" "$DURATION"
git rev-parse HEAD
uname -a
docker version
git clone https://github.com/jepsen-io/etcd.git .jepsen/etcd
git -C .jepsen/etcd checkout b47cc401f27bfb7c28afd9ac088b5f0dca4501b8
git clone https://github.com/nurturenature/jepsen-docker.git .jepsen/docker
git -C .jepsen/docker checkout 4b5707813a0a24966082ae4fd2dd223da447e499
python3 experiments/jepsen/patch-db.py .jepsen/etcd
git -C .jepsen/etcd diff > "$out/upstream.patch"
if [[ $STORE == etcd ]]; then
  curl -fsSL --retry 3 https://github.com/etcd-io/etcd/releases/download/v3.7.2/etcd-v3.7.2-linux-amd64.tar.gz -o .jepsen/etcd.tar.gz
  tar -xzf .jepsen/etcd.tar.gz -C .jepsen
  cp .jepsen/etcd-v3.7.2-linux-amd64/etcd "$assets/server"
  "$assets/server" --version
else
  git worktree add --detach .jepsen/product 90870876b6459334c7eddd67c0f9d184267f2eba
  (cd .jepsen/product; GOFLAGS=-p=2 GOMEMLIMIT=2GiB CGO_ENABLED=0 go build -o "$assets/server" ./cmd/tigerstore)
fi
sha256sum "$assets/server" | tee "$out/server.sha256"
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$assets/ca.key" -out "$assets/ca.crt" -days 2 -subj /CN=jepsen-test-ca
openssl req -newkey rsa:2048 -nodes -keyout "$assets/peer.key" -out "$assets/peer.csr" -subj /CN=jepsen-test-peer
printf 'subjectAltName=DNS:n1,DNS:n2,DNS:n3\nextendedKeyUsage=serverAuth,clientAuth\n' > "$assets/extensions"
openssl x509 -req -in "$assets/peer.csr" -CA "$assets/ca.crt" -CAkey "$assets/ca.key" -CAcreateserial -out "$assets/peer.crt" -days 2 -extfile "$assets/extensions"
# Three DB containers, otherwise the upstream compose file.
python3 - <<'PY'
from pathlib import Path
p = Path('.jepsen/docker/jepsen-compose.yaml')
s = p.read_text()
s = s.replace('      n4:\n        condition: service_healthy\n      n5:\n        condition: service_healthy\n', '')
s = s[:s.index('  n4:\n')]
p.write_text(s)
PY
export JEPSEN_REGISTRY=ghcr.io/nurturenature/jepsen-docker/
compose=(docker compose -f .jepsen/docker/jepsen-compose.yaml)
cleanup() {
  rc=$?
  trap - EXIT
  set +e
  docker cp jepsen-control:/jepsen/etcd/store "$out/store"
  "${compose[@]}" logs --no-color > "$out/containers.log" 2>&1
  docker inspect jepsen-control jepsen-n1 jepsen-n2 jepsen-n3 > "$out/containers.json"
  "${compose[@]}" down -v --timeout 10
  printf 'end=%s rc=%s\n' "$(date -u +%FT%TZ)" "$rc"
  exit "$rc"
}
trap cleanup EXIT
"${compose[@]}" pull
for image in jepsen-control jepsen-node jepsen-setup; do
  docker image inspect "$JEPSEN_REGISTRY$image" --format '{{json .RepoDigests}}'
done
"${compose[@]}" up -d --wait --wait-timeout 120
sans=DNS:n1,DNS:n2,DNS:n3
for node in n1 n2 n3; do
  ip=$(docker inspect "jepsen-$node" --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}')
  sans="$sans,IP:$ip"
done
printf 'subjectAltName=%s\nextendedKeyUsage=serverAuth,clientAuth\n' "$sans" > "$assets/extensions"
openssl x509 -req -in "$assets/peer.csr" -CA "$assets/ca.crt" -CAkey "$assets/ca.key" -CAcreateserial -out "$assets/peer.crt" -days 2 -extfile "$assets/extensions"
for node in n1 n2 n3; do
  docker exec "jepsen-$node" mkdir -p /opt/test-assets
  docker cp "$assets/server" "jepsen-$node:/opt/test-assets/server"
  for file in ca.crt peer.crt peer.key; do docker cp "$assets/$file" "jepsen-$node:/opt/test-assets/$file"; done
done
docker cp .jepsen/etcd jepsen-control:/jepsen/etcd
# Record clocks' namespace identities. Docker shares CLOCK_REALTIME with host.
for node in control n1 n2 n3; do
  docker exec "jepsen-$node" sh -c 'hostname; readlink /proc/self/ns/time; date -u +%s.%N'
done
docker exec jepsen-control java -version
docker exec -w /jepsen/etcd jepsen-control lein version
timeout --signal=TERM --kill-after=15s 360s docker exec -e LEIN_ROOT=true -w /jepsen/etcd jepsen-control lein deps
printf '%s\n' "lein run test --nodes n1,n2,n3 --username root --ssh-private-key /root/.ssh/id_rsa --strict-host-key-checking false --version 3.7.2 --workload $WORKLOAD --nemesis $NEMESIS --concurrency 2n --rate 30 --time-limit $DURATION --nemesis-interval 5" > "$out/command.txt"
timeout --signal=TERM --kill-after=20s 480s docker exec -e LEIN_ROOT=true -e STORE="$STORE" -w /jepsen/etcd jepsen-control \
  lein run test --nodes n1,n2,n3 --username root --ssh-private-key /root/.ssh/id_rsa \
  --strict-host-key-checking false --version 3.7.2 --workload "$WORKLOAD" --nemesis "$NEMESIS" \
  --concurrency 2n --rate 30 --time-limit "$DURATION" --nemesis-interval 5
