#!/usr/bin/env bash
# Runner only. Never run Docker, builds, or faults on the agent host.
set -euo pipefail
[[ ${GITHUB_ACTIONS:-} == true ]] || { echo 'GitHub runner required' >&2; exit 2; }
STORE=${1:?store}; WORKLOAD=${2:?workload}; NEMESIS=${3:?nemesis}; DURATION=${4:-60}; OPS_PER_KEY=${5:-200}
export STORE
# Only the four faults of #148. Upstream's `all` includes its clock nemesis,
# and Docker nodes share the runner's clock (#114).
case $NEMESIS in
  kill|pause|partition|lazyfs) ;;
  *) echo "Unsupported nemesis $NEMESIS: use kill, pause, partition or lazyfs" >&2; exit 2 ;;
esac
root=$PWD
out=$root/results/jepsen
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
python3 scripts/jepsen/patch-db.py .jepsen/etcd
mkdir -p .jepsen/etcd/test/jepsen/etcd
cp scripts/jepsen/pause_test.clj scripts/jepsen/errors_test.clj .jepsen/etcd/test/jepsen/etcd/
git -C .jepsen/etcd diff > "$out/upstream.patch"
if [[ $STORE == etcd ]]; then
  curl -fsSL --retry 3 https://github.com/etcd-io/etcd/releases/download/v3.7.2/etcd-v3.7.2-linux-amd64.tar.gz -o .jepsen/etcd.tar.gz
  tar -xzf .jepsen/etcd.tar.gz -C .jepsen
  cp .jepsen/etcd-v3.7.2-linux-amd64/etcd "$assets/server"
  "$assets/server" --version
else
  git worktree add --detach .jepsen/product "${PRODUCT_SHA:?pin the main commit to test}"
  git -C .jepsen/product rev-parse HEAD | tee "$out/product-commit.txt"
  bash scripts/diag300/apply.sh .jepsen/product
  git -C .jepsen/product diff > "$out/diag300-product.patch"
  (cd .jepsen/product; GOFLAGS=-p=2 GOMEMLIMIT=2GiB CGO_ENABLED=0 go build -o "$assets/server" ./cmd/tigerstore)
fi
sha256sum "$assets/server" | tee "$out/server.sha256"
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$assets/ca.key" -out "$assets/ca.crt" -days 2 -subj /CN=jepsen-test-ca
openssl req -newkey rsa:2048 -nodes -keyout "$assets/peer.key" -out "$assets/peer.csr" -subj /CN=jepsen-test-peer
# Three DB containers, otherwise the upstream compose file.
python3 - <<'PY'
from pathlib import Path
p = Path('.jepsen/docker/jepsen-compose.yaml')
s = p.read_text()
s = s.replace('      n4:\n        condition: service_healthy\n      n5:\n        condition: service_healthy\n', '')
s = s[:s.index('  n4:\n')]
for image, digest in {
    'jepsen-control': 'b0d8e672156022baa30818bd38bf84ab8e10f5045c62d5fe81e7b4a7205c9145',
    'jepsen-node': '44dd2a6ea919af66a32d47fcc9f8d8f08cd0c3147d759687419567b6b6cf1698',
    'jepsen-setup': 'f7f21fc1a07458a7f7011fec0370f675e5a746a827e0a529843a2078eca98c82',
}.items():
    s = s.replace('${JEPSEN_REGISTRY:-}' + image, 'ghcr.io/nurturenature/jepsen-docker/' + image + '@sha256:' + digest)
p.write_text(s)
PY
export JEPSEN_REGISTRY=ghcr.io/nurturenature/jepsen-docker/
compose=(docker compose -f .jepsen/docker/jepsen-compose.yaml)
cleanup() {
  rc=$?
  trap - EXIT
  if [[ -n ${observer:-} ]]; then
    kill "$observer" || rc=1
    wait "$observer" || echo "Observer stopped"
  fi
  if (( rc != 0 )); then
    for node in n1 n2 n3; do
      docker exec "jepsen-$node" ps auxf > "$out/processes-$node.txt" 2>&1 || rc=1
      if docker exec "jepsen-$node" test -d /opt/etcd; then
        docker cp "jepsen-$node:/opt/etcd" "$out/unfinished-$node" || rc=1
      fi
    done
  fi
  for node in n1 n2 n3; do
    docker exec "jepsen-$node" sh -c 'ls -laR /opt/etcd' > "$out/diag300-ls-$node.txt" 2>&1 || true
    docker cp "jepsen-$node:/opt/etcd/$node.etcd.lazyfs" "$out/diag300-lazyfs-$node" || true
  done
  docker cp jepsen-control:/jepsen/etcd/store "$out/store" || rc=1
  "${compose[@]}" logs --no-color > "$out/containers.log" 2>&1 || rc=1
  docker inspect jepsen-control jepsen-n1 jepsen-n2 jepsen-n3 > "$out/containers.json" || rc=1
  "${compose[@]}" down -v --timeout 10 || rc=1
  printf 'end=%s rc=%s\n' "$(date -u +%FT%TZ)" "$rc"
  exit "$rc"
}
trap cleanup EXIT
"${compose[@]}" pull
docker image ls --digests --no-trunc
"${compose[@]}" up -d --wait --wait-timeout 120
# The peer certificate names the nodes and their addresses, known only now.
sans=DNS:n1,DNS:n2,DNS:n3
for node in n1 n2 n3; do
  ip=$(docker inspect "jepsen-$node" --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}')
  sans="$sans,IP:$ip"
done
printf 'subjectAltName=%s\nextendedKeyUsage=serverAuth,clientAuth\n' "$sans" > "$assets/extensions"
openssl x509 -req -in "$assets/peer.csr" -CA "$assets/ca.crt" -CAkey "$assets/ca.key" -CAcreateserial -out "$assets/peer.crt" -days 2 -extfile "$assets/extensions"
builds=()
for node in n1 n2 n3; do
  docker exec "jepsen-$node" mkdir -p /opt/test-assets
  docker cp "$assets/server" "jepsen-$node:/opt/test-assets/server"
  for file in ca.crt peer.crt peer.key; do docker cp "$assets/$file" "jepsen-$node:/opt/test-assets/$file"; done
  if [[ $NEMESIS == lazyfs ]]; then
    # The image's LazyFS 0.3.0 has the rename race of #267. Replace it with
    # the chaos jobs' pin and patch, where Jepsen mounts it from.
    for file in lazyfs.sh lazyfs-namespace.patch; do docker cp "scripts/$file" "jepsen-$node:/opt/test-assets/$file"; done
    # From /: the image's working directory is the one the build replaces.
    docker exec -w / "jepsen-$node" bash -euc '
      bin=$(bash /opt/test-assets/lazyfs.sh /opt/jepsen/lazyfs)
      test "$bin" = /opt/jepsen/lazyfs/lazyfs/build/lazyfs
      git -C /opt/jepsen/lazyfs rev-parse HEAD
      sha256sum /opt/test-assets/lazyfs-namespace.patch "$bin"
      test -c /dev/fuse || mknod /dev/fuse c 10 229
      chmod a+rw /dev/fuse
      sed -i "/user_allow_other/s/^#//g" /etc/fuse.conf
    ' > "$out/lazyfs-binary-$node.txt" 2>&1 &
    builds+=($!)
  fi
done
if [[ $NEMESIS == lazyfs ]]; then
  for pid in "${builds[@]}"; do wait "$pid"; done
  cat "$out"/lazyfs-binary-n?.txt
fi
docker cp .jepsen/etcd jepsen-control:/jepsen/etcd
# Record clocks' namespace identities. Docker shares CLOCK_REALTIME with host.
for node in control n1 n2 n3; do
  docker exec "jepsen-$node" sh -c 'hostname; readlink /proc/self/ns/time; date -u +%s.%N'
done
# Independent observations; the upstream fault generators and implementations stay unchanged.
(
  while true; do
    for node in n1 n2 n3; do
      docker exec "jepsen-$node" sh -c 'date -u +%FT%T.%N; hostname; ps -C etcd -o pid=,stat=; iptables-save -c' >> "$out/fault-observer-$node.log" 2>&1 || echo "Observation failed for $node" >> "$out/observer-errors.log"
    done
    sleep 1
  done
) &
observer=$!
docker exec jepsen-control java -version
docker exec -w /jepsen/etcd jepsen-control lein version
timeout --signal=TERM --kill-after=15s 360s docker exec -e LEIN_ROOT=true -w /jepsen/etcd jepsen-control lein deps
timeout --signal=TERM --kill-after=15s 120s docker exec -e LEIN_ROOT=true -w /jepsen/etcd jepsen-control lein test jepsen.etcd.pause-test jepsen.etcd.errors-test
extra=()
if [[ $NEMESIS == lazyfs ]]; then
  NEMESIS=kill
  extra=(--lazyfs)
fi
printf '%s\n' "lein run test ${extra[*]} --nodes n1,n2,n3 --username root --ssh-private-key /root/.ssh/id_rsa --version 3.7.2 --workload $WORKLOAD --nemesis $NEMESIS --concurrency 2n --rate 30 --time-limit $DURATION --nemesis-interval 5 --ops-per-key $OPS_PER_KEY" > "$out/command.txt"
timeout --signal=TERM --kill-after=20s 720s docker exec -e LEIN_ROOT=true -e STORE="$STORE" -w /jepsen/etcd jepsen-control \
  lein run test --nodes n1,n2,n3 --username root --ssh-private-key /root/.ssh/id_rsa \
  --version 3.7.2 --workload "$WORKLOAD" --nemesis "$NEMESIS" "${extra[@]}" \
  --concurrency 2n --rate 30 --time-limit "$DURATION" --nemesis-interval 5 --ops-per-key "$OPS_PER_KEY"
