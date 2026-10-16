#!/usr/bin/env bash
set -euo pipefail
[[ ${GITHUB_ACTIONS:-} == true ]] || { echo 'GitHub runner required' >&2; exit 2; }
store=${1:?store}
export TRAFFIC=${2:?traffic}
root=$PWD
export RESULTS=$root/results/robustness
mkdir -p "$RESULTS" .robustness
exec > >(tee "$RESULTS/runner.log") 2>&1
git rev-parse HEAD
printf 'product=%s traffic=%s store=%s\n' "$PRODUCT_SHA" "$TRAFFIC" "$store"
git clone https://github.com/etcd-io/etcd.git .robustness/etcd
git -C .robustness/etcd checkout 68c065e562994b89e333e77b039ad066f933c586
mkdir -p .robustness/etcd/tests/tigerstore_external
cp scripts/jepsen/robustness_test.go.txt .robustness/etcd/tests/tigerstore_external/external_test.go
gofmt -w .robustness/etcd/tests/tigerstore_external/external_test.go
cp .robustness/etcd/tests/tigerstore_external/external_test.go "$RESULTS/"
(cd .robustness/etcd/tests; go test -c -o "$root/.robustness/traffic.test" ./tigerstore_external)
if [[ $store == tigerstore ]]; then
  git worktree add --detach .robustness/product "${PRODUCT_SHA:?}"
  (cd .robustness/product; CGO_ENABLED=0 go build -o "$root/.robustness/server" ./cmd/tigerstore)
else
  curl -fsSL --retry 3 https://github.com/etcd-io/etcd/releases/download/v3.7.2/etcd-v3.7.2-linux-amd64.tar.gz -o .robustness/etcd.tar.gz
  tar -xzf .robustness/etcd.tar.gz -C .robustness
  cp .robustness/etcd-v3.7.2-linux-amd64/etcd .robustness/server
fi
sha256sum .robustness/server > "$RESULTS/server.sha256"
bash scripts/dev-certs.sh "$root/.robustness/certs"
certs=$root/.robustness/certs
cluster='n1=https://127.0.0.1:2380,n2=https://127.0.0.1:2480,n3=https://127.0.0.1:2580'
export ENDPOINTS='http://127.0.0.1:2379,http://127.0.0.1:2479,http://127.0.0.1:2579'
pids=()
start_node() {
  local n=$1
  local client=$((2279 + n * 100)) peer=$((2280 + n * 100))
  "$root/.robustness/server" --name "n$n" --data-dir "$RESULTS/n$n.data" \
    --listen-client-urls "http://127.0.0.1:$client" --advertise-client-urls "http://127.0.0.1:$client" \
    --listen-peer-urls "https://127.0.0.1:$peer" --initial-advertise-peer-urls "https://127.0.0.1:$peer" \
    --initial-cluster "$cluster" --initial-cluster-token robustness148 \
    --peer-cert-file "$certs/node$n.pem" --peer-key-file "$certs/node$n-key.pem" \
    --peer-trusted-ca-file "$certs/ca.pem" --peer-client-cert-auth \
    >> "$RESULTS/n$n.log" 2>&1 &
  pids[$n]=$!
  printf '%s start n%s pid=%s\n' "$(date -u +%FT%T.%N)" "$n" "${pids[$n]}" >> "$RESULTS/faults.log"
}
cleanup() {
  rc=$?
  trap - EXIT
  for pid in "${pids[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill -CONT "$pid" || rc=1
      kill -TERM "$pid" || rc=1
      wait "$pid" || echo "Server $pid exited nonzero during cleanup"
    fi
  done
  exit "$rc"
}
trap cleanup EXIT
for n in 1 2 3; do start_node "$n"; done
"$root/.robustness/traffic.test" -test.v -test.run '^TestExternalTraffic$' -test.timeout 4m > "$RESULTS/test.log" 2>&1 &
traffic_pid=$!
for attempt in $(seq 1 90); do
  [[ -f $RESULTS/traffic-ready ]] && break
  kill -0 "$traffic_pid" 2>/dev/null || { wait "$traffic_pid"; exit 1; }
  sleep 1
done
[[ -f $RESULTS/traffic-ready ]]
sleep 5
kill -STOP "${pids[1]}"
printf '%s pause n1 pid=%s\n' "$(date -u +%FT%T.%N)" "${pids[1]}" >> "$RESULTS/faults.log"
ps -p "${pids[1]}" -o pid=,stat= | tee -a "$RESULTS/faults.log" | grep -E '[[:space:]]T'
sleep 3
kill -CONT "${pids[1]}"
printf '%s resume n1\n' "$(date -u +%FT%T.%N)" >> "$RESULTS/faults.log"
sleep 5
kill -KILL "${pids[2]}"
status=0
wait "${pids[2]}" || status=$?
printf '%s kill n2 pid=%s wait=%s\n' "$(date -u +%FT%T.%N)" "${pids[2]}" "$status" >> "$RESULTS/faults.log"
[[ $status == 137 ]]
sleep 3
start_node 2
wait "$traffic_pid"
for n in 1 2 3; do
  kill -0 "${pids[$n]}"
done
