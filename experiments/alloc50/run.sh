#!/usr/bin/env bash
set -euo pipefail
export GOFLAGS=-p=2 GOMEMLIMIT=2GiB
check_time() { [[ $(date -u +%s) -lt $(date -ud 2026-10-13T22:00:00Z +%s) ]]; }
check_time
[[ ${GITHUB_ACTIONS:-} == true && ${RUNNER_ARCH:-} == ARM64 ]]
[[ ${CLIENTS:?} == 64 || $CLIENTS == 256 ]]
[[ $(nproc) == 4 ]]
BASE=$(git rev-parse '0d406cb^{commit}')
[[ $BASE == 0d406cb09aa99dea7ef1488476d6ebc6b032a6b8 ]]
EXPERIMENT=$(git rev-parse HEAD)
python3 experiments/alloc50/verify-harness.py
[[ $(git branch --show-current) == exp/50-alloc || ${GITHUB_REF:-} == refs/heads/exp/50-alloc ]]
OUT=$PWD/evidence/alloc50/c$CLIENTS
PRIVATE=$RUNNER_TEMP/alloc50
[[ ! -e "$OUT" && ! -e "$PRIVATE" ]]
mkdir -p "$OUT/symbols" "$PRIVATE/bin" "$PRIVATE/base" "$PRIVATE/experiment"
trap 'rm -rf "$PRIVATE"' EXIT
cp experiments/alloc50/attempt-notes.md "$OUT/attempt-notes.md"
tar -czf "$OUT/harness-source.tar.gz" experiments/alloc50 experiments/bench75 .github/workflows/alloc50.yml
git diff ae0c2ecc639bb49c723dc1efd6260424a9028a82 "$EXPERIMENT" -- experiments/alloc50 .github/workflows/alloc50.yml internal/server/server_test.go internal/server/allocation_path_test.go > "$OUT/first-attempt-followup.patch"
for log in harness-followup harness-async; do
 if [[ -f evidence/local-parent/$log.log ]]; then
  cp "evidence/local-parent/$log.log" "$OUT/local-$log.log"
 fi
done
{
 date -u +%FT%TZ
 printf 'base %s\nexperiment %s\nharness_origin %s\n' "$BASE" "$EXPERIMENT" 0f82f687e5a21fa6265e508866f07dc3fffeaae9
 uname -a
 lscpu
 free -b
 lsblk -b -o NAME,SIZE,TYPE,ROTA,MODEL,MOUNTPOINTS
 df -B1 "$PWD" "$PRIVATE"
 findmnt -T "$PRIVATE"
 env | grep -E '^(ImageOS|ImageVersion|RUNNER_NAME|RUNNER_ARCH|RUNNER_OS)='
 go version
} > "$OUT/environment.txt"
sudo mount -o remount,barrier=1 /
findmnt -T "$PRIVATE" -o TARGET,SOURCE,FSTYPE,OPTIONS | tee "$OUT/mount.txt"
test "$(findmnt -T "$PRIVATE" -n -o FSTYPE)" = ext4
! findmnt -T "$PRIVATE" -n -o OPTIONS | grep -Eq 'nobarrier|barrier=0'
check_time
# etcdctl is only an independent TLS/fence client, never a timed server here.
curl --fail --location --retry 3 -o "$PRIVATE/etcd.tar.gz" https://github.com/etcd-io/etcd/releases/download/v3.7.2/etcd-v3.7.2-linux-arm64.tar.gz
tar -xzf "$PRIVATE/etcd.tar.gz" -C "$PRIVATE"
echo '17122293e6713e3d8b14b33964af8f6eae0a83af03e87487f34731ee0e28112d  '"$PRIVATE/etcd-v3.7.2-linux-arm64/etcd" | sha256sum -c -
cp "$PRIVATE/etcd-v3.7.2-linux-arm64/etcdctl" "$PRIVATE/bin/"
git diff --exit-code
git status --short > "$OUT/build-status.txt"
git archive "$BASE" | tar -x -C "$PRIVATE/base"
git archive "$EXPERIMENT" | tar -x -C "$PRIVATE/experiment"
# Test fixtures only: production tick is 100ms in both census archives. No
# candidate production file is overlaid onto the pristine base source.
cp "$PRIVATE/base/internal/server/server_test.go" "$OUT/base-server_test-pristine.go.txt"
cp "$PRIVATE/experiment/internal/server/server_test.go" "$PRIVATE/base/internal/server/server_test.go"
cp "$PRIVATE/experiment/internal/server/allocation_path_test.go" "$PRIVATE/base/internal/server/allocation_path_test.go"
diff -u "$OUT/base-server_test-pristine.go.txt" "$PRIVATE/base/internal/server/server_test.go" > "$OUT/base-server-fixture.patch" || [[ $? == 1 ]]
cp "$PRIVATE/base/internal/server/allocation_path_test.go" "$OUT/base-allocation_path_test.go.txt"
git diff "$BASE" "$EXPERIMENT" -- '*.go' go.mod go.sum > "$OUT/experiment-source.patch"
for source in base experiment; do
 check_time
 cp "$PRIVATE/$source/cmd/tigerstore/main.go" "$OUT/$source-main-pristine.go.txt"
 python3 experiments/alloc50/install-observe.py "$PRIVATE/$source"
 diff -u "$OUT/$source-main-pristine.go.txt" "$PRIVATE/$source/cmd/tigerstore/main.go" > "$OUT/$source-observe.patch" || [[ $? == 1 ]]
 (cd "$PRIVATE/$source" && find . -type f \( -name '*.go' -o -name go.mod -o -name go.sum \) -print0 | sort -z | xargs -0 sha256sum) > "$OUT/$source-source-manifest.txt"
 (cd "$PRIVATE/$source" && go build -trimpath -o "$PRIVATE/bin/$source" ./cmd/tigerstore)
done
cp experiments/alloc50/observe.go.txt "$OUT/observe.go.txt"
check_time
go build -trimpath -o "$PRIVATE/bin/load" ./experiments/bench75/load
check_time
go build -trimpath -o "$PRIVATE/bin/cases" ./experiments/alloc50/cases
check_time
go test -short -parallel 2 ./experiments/bench75/load ./experiments/alloc50/cases
printf 'base %s\nexperiment %s\nharness_origin %s\n' "$BASE" "$EXPERIMENT" 0f82f687e5a21fa6265e508866f07dc3fffeaae9 > "$OUT/sources.txt"
{
 "$PRIVATE/bin/etcdctl" version
 sha256sum "$PRIVATE/bin/"*
 for binary in base experiment load cases; do go version -m "$PRIVATE/bin/$binary"; done
} > "$OUT/versions.txt"
# Save matching symbol binaries before measurements, including if they fail.
for binary in base experiment load cases; do gzip -c "$PRIVATE/bin/$binary" > "$OUT/symbols/$binary.gz"; done
bash scripts/dev-certs.sh "$PRIVATE/certs"
# Targeted correctness gates only on Actions, sequential commands, no race or
# simulator campaign. The pristine base remains untouched except observe.
GATE='Test(Allocation(ValueLifetimes|Workspace.*|Shutdown.*)|RawFrames|LargeValues|CanceledCalls|DeadlinesLeaderAndUnknownMethods|NodeStopsUnderLoad|ThreeNodesOverTLS|ClientCertificateRequired|ManyStreamsOnOneConnection|SlowWatcherIsCut|WritesWaitForTheLeadersAppend|ByteCapSendsAnEntryEarly|HardStateAppendHoldsNothing|BatchRoundTrip|RequestSizeLimitMatchesEtcd|ParsePutMatchesProtobuf)$'
mkdir -p "$OUT/tests"
check_time
(cd "$PRIVATE/base" && env -u TIGER_ALLOC_STRATEGY -u ALLOC50_PROFILE_RATE go test -short -count=1 -parallel 2 -run "$GATE" ./internal/server ./internal/node ./internal/core) > "$OUT/tests/pristine-base.log" 2>&1
for mode in base ring arena; do
 check_time
 (cd "$PRIVATE/experiment" && TIGER_ALLOC_STRATEGY="$mode" go test -short -count=1 -parallel 2 -run "$GATE" ./internal/server ./internal/node ./internal/core) > "$OUT/tests/$mode.log" 2>&1
done
# Path changes the global memory profiler: run it only in fresh processes,
# after all ordinary gates. Preserve each archive's matching census symbols.
for source in base experiment; do
 check_time
 (cd "$PRIVATE/$source" && go test -c -o "$PRIVATE/bin/$source-census.test" ./internal/server)
 gzip -c "$PRIVATE/bin/$source-census.test" > "$OUT/symbols/$source-census.test.gz"
 sha256sum "$PRIVATE/bin/$source-census.test" >> "$OUT/versions.txt"
done
check_time
env -u TIGER_ALLOC_STRATEGY -u ALLOC50_PROFILE_RATE ALLOC50_TEST_OUT="$OUT/tests/pristine-base-census" "$PRIVATE/bin/base-census.test" -test.short -test.count=1 -test.parallel=2 -test.run='^TestAllocationPath$' > "$OUT/tests/pristine-base-census.log" 2>&1
for mode in base ring arena; do
 check_time
 TIGER_ALLOC_STRATEGY="$mode" ALLOC50_TEST_OUT="$OUT/tests/$mode-census" "$PRIVATE/bin/experiment-census.test" -test.short -test.count=1 -test.parallel=2 -test.run='^TestAllocationPath$' > "$OUT/tests/$mode-census.log" 2>&1
done
check_time
timeout --kill-after=5s 650s taskset -c 3 python3 experiments/alloc50/run.py
check_time
python3 experiments/alloc50/summarize.py
