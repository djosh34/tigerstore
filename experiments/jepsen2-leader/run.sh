#!/usr/bin/env bash
set -euo pipefail
[[ ${GITHUB_ACTIONS:-} == true ]] || { echo 'Actions only'; exit 1; }
deadline() { test "$(date -u +%s)" -lt "$(date -u -d 2026-10-13T22:00:00Z +%s)"; }
deadline
root=$(pwd)
exp="$root/experiments/jepsen2-leader"
out="$exp/evidence"
mkdir -p "$out" "$RUNNER_TEMP/leader-build"
exec > >(tee "$out/run.log") 2>&1
printf 'start=%s\nworkflow_sha=%s\ntiger_sha=%s\netcd_sha=%s\nrepetition=%s\n' "$(date -u +%FT%TZ)" "$GITHUB_SHA" b5f2e28ef2edd94393d62952b1568763ebc12fe2 68c065e562994b89e333e77b039ad066f933c586 "$REPETITION" | tee "$out/versions.txt"
uname -a >> "$out/versions.txt"
lscpu >> "$out/versions.txt"
go version >> "$out/versions.txt"
git worktree add --detach "$RUNNER_TEMP/leader-build/tiger" b5f2e28ef2edd94393d62952b1568763ebc12fe2
deadline
(cd "$RUNNER_TEMP/leader-build/tiger" && CGO_ENABLED=0 go build -o "$RUNNER_TEMP/leader-build/tigerstore" ./cmd/tigerstore)
deadline
CGO_ENABLED=0 go build -o "$RUNNER_TEMP/leader-build/probe" ./experiments/jepsen2-leader/probe
curl -fsSL -o "$RUNNER_TEMP/leader-build/etcd.tgz" https://github.com/etcd-io/etcd/releases/download/v3.7.2/etcd-v3.7.2-linux-amd64.tar.gz
echo "3a3679bc51a4ee9d30bccea1da7cd4fe62c6fc1d2ca1255068d2c53bf3026135  $RUNNER_TEMP/leader-build/etcd.tgz" | sha256sum -c
tar xzf "$RUNNER_TEMP/leader-build/etcd.tgz" -C "$RUNNER_TEMP/leader-build"
"$RUNNER_TEMP/leader-build/etcd-v3.7.2-linux-amd64/etcd" --version >> "$out/versions.txt"
sha256sum "$RUNNER_TEMP/leader-build/tigerstore" "$RUNNER_TEMP/leader-build/probe" "$RUNNER_TEMP/leader-build/etcd-v3.7.2-linux-amd64/etcd" >> "$out/versions.txt"
cert="$RUNNER_TEMP/leader-build/cert"
mkdir -p "$cert"
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$cert/ca.key" -out "$cert/ca.crt" -days 1 -subj /CN=leader-probe-ca
openssl req -newkey rsa:2048 -nodes -keyout "$cert/peer.key" -out "$cert/peer.csr" -subj /CN=localhost
printf 'subjectAltName=IP:127.0.0.1,DNS:localhost\nextendedKeyUsage=serverAuth,clientAuth\n' > "$cert/ext"
openssl x509 -req -in "$cert/peer.csr" -CA "$cert/ca.crt" -CAkey "$cert/ca.key" -CAcreateserial -out "$cert/peer.crt" -days 1 -extfile "$cert/ext"
# Fresh storage and identical flags, peer mTLS, client HTTP, default 100 ms / 1 s Raft timing.
failed=0
for store in etcd tigerstore; do
  deadline
  binary="$RUNNER_TEMP/leader-build/tigerstore"
  if [[ $store == etcd ]]; then binary="$RUNNER_TEMP/leader-build/etcd-v3.7.2-linux-amd64/etcd"; fi
  "$RUNNER_TEMP/leader-build/probe" -store "$store" -bin "$binary" -cert "$cert" -out "$out/$store" || failed=1
done
printf 'end=%s\n' "$(date -u +%FT%TZ)" >> "$out/versions.txt"
exit "$failed"
