#!/usr/bin/env bash
set -euo pipefail
[[ ${GITHUB_ACTIONS:-} == true ]] || { echo 'GitHub runner required' >&2; exit 2; }
store=${1:?store}
export TRAFFIC=${2:?traffic}
root=$PWD
. scripts/jepsen/stopped.sh
export RESULTS=$root/results/robustness
mkdir -p "$RESULTS" .robustness
exec > >(tee "$RESULTS/runner.log") 2>&1
git rev-parse HEAD
printf 'product=%s traffic=%s store=%s\n' "$PRODUCT_SHA" "$TRAFFIC" "$store"
git clone --depth 1 --branch v3.7.2 https://github.com/etcd-io/etcd.git .robustness/etcd
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
# start_node starts node $1 with its data and log in directory $2.
start_node() {
  local n=$1 dir=$2
  local client=$((2279 + n * 100)) peer=$((2280 + n * 100))
  "$root/.robustness/server" --name "n$n" --data-dir "$dir/n$n.data" \
    --listen-client-urls "http://127.0.0.1:$client" --advertise-client-urls "http://127.0.0.1:$client" \
    --listen-peer-urls "https://127.0.0.1:$peer" --initial-advertise-peer-urls "https://127.0.0.1:$peer" \
    --initial-cluster "$cluster" --initial-cluster-token robustness148 \
    --peer-cert-file "$certs/node$n.pem" --peer-key-file "$certs/node$n-key.pem" \
    --peer-trusted-ca-file "$certs/ca.pem" --peer-client-cert-auth \
    >> "$dir/n$n.log" 2>&1 &
  pids[$n]=$!
  printf '%s start n%s pid=%s\n' "$(date -u +%FT%T.%N)" "$n" "${pids[$n]}" >> "$dir/faults.log"
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
# Like etcd's own suite, each history has one fault, on a fresh cluster, with
# traffic for 1 s before the fault and 1 s after the cluster has recovered.
# Every write that fails during a fault keeps an unknown outcome, and longer
# histories with both faults timed out the linearization check (#356).
round() {
  local fault=$1 dir=$RESULTS/$1 status=0 traffic_status=0
  mkdir -p "$dir"
  for n in 1 2 3; do start_node "$n" "$dir"; done
  RESULTS=$dir "$root/.robustness/traffic.test" -test.v -test.run '^TestExternalTraffic$' -test.timeout 8m > "$dir/test.log" 2>&1 &
  local traffic_pid=$!
  for attempt in $(seq 1 90); do
    [[ -f $dir/traffic-ready ]] && break
    kill -0 "$traffic_pid" 2>/dev/null || { wait "$traffic_pid"; exit 1; }
    sleep 1
  done
  [[ -f $dir/traffic-ready ]]
  sleep 1
  case $fault in
    pause)
      kill -STOP "${pids[1]}"
      printf '%s pause n1 pid=%s\n' "$(date -u +%FT%T.%N)" "${pids[1]}" >> "$dir/faults.log"
      wait_stopped "${pids[1]}" "$dir/faults.log"
      sleep 3
      kill -CONT "${pids[1]}"
      printf '%s resume n1\n' "$(date -u +%FT%T.%N)" >> "$dir/faults.log"
      ;;
    kill)
      kill -KILL "${pids[2]}"
      wait "${pids[2]}" || status=$?
      printf '%s kill n2 pid=%s wait=%s\n' "$(date -u +%FT%T.%N)" "${pids[2]}" "$status" >> "$dir/faults.log"
      [[ $status == 137 ]]
      sleep 3
      start_node 2 "$dir"
      ;;
  esac
  touch "$dir/fault-done"
  wait "$traffic_pid" || traffic_status=$?
  cat "$dir/test.log"
  (( traffic_status == 0 ))
  for n in 1 2 3; do
    kill -0 "${pids[$n]}"
  done
  for n in 1 2 3; do
    kill -TERM "${pids[$n]}"
    wait "${pids[$n]}" || echo "Server n$n exited nonzero at the end of round $fault"
  done
  pids=()
}
round pause
round kill
