#!/usr/bin/env bash
# Runner setup for the A+ prototype jobs (#133): etcd v3.7.2 for etcdctl,
# the generator, main at 76952348 as "baseline", this branch as
# "candidate", and "cand256" (this branch with a checkpoint every 256 MiB
# of log), certificates, and disk write barriers on.
set -euo pipefail
BASELINE=76952348
curl -fsSL -o "$RUNNER_TEMP/etcd.tgz" https://github.com/etcd-io/etcd/releases/download/v3.7.2/etcd-v3.7.2-linux-arm64.tar.gz
echo "5e3ff99d428fabfcefdc95348f6560d1efe835f7b2d1ab3334fe0831076966b7  $RUNNER_TEMP/etcd.tgz" | sha256sum -c
tar xzf "$RUNNER_TEMP/etcd.tgz" -C "$RUNNER_TEMP"
mkdir -p bin
cp "$RUNNER_TEMP/etcd-v3.7.2-linux-arm64/etcd" "$RUNNER_TEMP/etcd-v3.7.2-linux-arm64/etcdctl" bin/
go build -o bin/loadgen ./cmd/loadgen
go build -o bin/benchreport ./cmd/benchreport
go build -o bin/candidate ./cmd/tigerstore
git worktree add --detach "$RUNNER_TEMP/baseline" "$BASELINE"
(cd "$RUNNER_TEMP/baseline" && go build -o "$GITHUB_WORKSPACE/bin/baseline" ./cmd/tigerstore)
cat >bin/cand256 <<'W'
#!/usr/bin/env bash
APLUS_CKPT_EVERY=268435456 exec "$(dirname "$0")/candidate" "$@"
W
chmod +x bin/cand256
{
	echo "baseline $(git -C "$RUNNER_TEMP/baseline" rev-parse HEAD)"
	echo "candidate $(git rev-parse HEAD)"
} | tee commits.txt
scripts/dev-certs.sh certs
sudo mount -o remount,barrier=1 /
findmnt -T "$RUNNER_TEMP" -n -o SOURCE,FSTYPE,OPTIONS
if findmnt -T "$RUNNER_TEMP" -n -o OPTIONS | grep -Eq 'nobarrier|barrier=0'; then exit 1; fi
mkdir -p results
{
	lscpu | grep -E 'Model name|^CPU\(s\)'
	free -b | head -2
	findmnt -T "$RUNNER_TEMP" -n -o SOURCE,FSTYPE,OPTIONS
	uname -r
	go version
	cat commits.txt
	echo "runner: $RUNNER_NAME $ImageOS $ImageVersion"
} | tee results/runner.txt
