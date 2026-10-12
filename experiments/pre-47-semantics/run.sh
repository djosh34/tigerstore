#!/usr/bin/env bash
# All downloaded files, keys, data and build caches live under WORK.
set -euo pipefail
: "${WORK:?Set WORK to your own disposable directory}"
mkdir -p "$WORK"/{tmp,cache,mod,bin,results}
rm -rf "$WORK/tmp/data" "$WORK/tmp/no-leader-data" "$WORK/tmp/absent1-data" "$WORK/tmp/absent2-data"
export TMPDIR="$WORK/tmp" GOCACHE="$WORK/cache" GOMODCACHE="$WORK/mod"
export GOTOOLCHAIN=local
if [[ ! -x "$WORK/bin/etcd" ]]; then
  curl -fLsS "https://github.com/etcd-io/etcd/releases/download/v3.7.2/etcd-v3.7.2-linux-$(go env GOARCH).tar.gz" -o "$WORK/tmp/etcd.tgz"
  sha256sum "$WORK/tmp/etcd.tgz" | tee "$WORK/results/archive.sha256"
  tar -xzf "$WORK/tmp/etcd.tgz" -C "$WORK/tmp"
  cp "$WORK/tmp/etcd-v3.7.2-linux-$(go env GOARCH)/"{etcd,etcdctl} "$WORK/bin/"
fi
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -keyout "$WORK/tmp/key.pem" -out "$WORK/tmp/cert.pem" -subj '/CN=localhost' -addext 'subjectAltName=IP:127.0.0.1,DNS:localhost' -addext 'extendedKeyUsage=serverAuth,clientAuth' 2> "$WORK/results/openssl.log"
export ENDPOINT=https://127.0.0.1:32379 CERT="$WORK/tmp/cert.pem" CA="$WORK/tmp/cert.pem" KEY="$WORK/tmp/key.pem"
"$WORK/bin/etcd" --name probe --data-dir "$WORK/tmp/data" --listen-client-urls "$ENDPOINT" --advertise-client-urls "$ENDPOINT" --listen-peer-urls http://127.0.0.1:32380 --initial-advertise-peer-urls http://127.0.0.1:32380 --initial-cluster probe=http://127.0.0.1:32380 --cert-file "$CERT" --key-file "$KEY" --trusted-ca-file "$CA" --client-cert-auth --watch-progress-notify-interval 100ms --max-request-bytes 2097152 > "$WORK/results/server.log" 2>&1 &
pid=$!
others=()
trap 'kill "$pid" "${others[@]}" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; for child in "${others[@]}"; do wait "$child" 2>/dev/null || true; done; rm -rf "$WORK/tmp/data" "$WORK/tmp/no-leader-data" "$WORK/tmp/absent1-data" "$WORK/tmp/absent2-data" "$WORK/tmp/cert.pem" "$WORK/tmp/key.pem" "$WORK/tmp/probe"' EXIT
for i in {1..40}; do if curl --max-time 2 -sf --cacert "$CA" --cert "$CERT" --key "$KEY" "$ENDPOINT/health" >/dev/null; then break; fi; sleep .1; done
for p in health version; do
  curl -sS --cacert "$CA" --cert "$CERT" --key "$KEY" -D "$WORK/results/$p.headers" "$ENDPOINT/$p" -o "$WORK/results/$p.body"
done
"$WORK/bin/etcd" --version > "$WORK/results/version.txt"
go version >> "$WORK/results/version.txt"
go mod tidy
go build -o "$WORK/tmp/probe" .
"$WORK/tmp/probe" > "$WORK/results/probe.log"
export ETCDCTL_ENDPOINTS="$ENDPOINT" ETCDCTL_CACERT="$CA" ETCDCTL_CERT="$CERT" ETCDCTL_KEY="$KEY"
for format in table simple fields json; do "$WORK/bin/etcdctl" endpoint status -w "$format" > "$WORK/results/status-$format.txt"; done
"$WORK/bin/etcdctl" endpoint status --cluster -w table > "$WORK/results/status-cluster.txt"
# Basic debugging smoke over authenticated TLS.
"$WORK/bin/etcdctl" put ctl-key first > "$WORK/results/ctl-put.txt"
"$WORK/bin/etcdctl" get ctl-key > "$WORK/results/ctl-get.txt"
rev=$("$WORK/bin/etcdctl" get ctl-key -w json | jq -r .header.revision)
"$WORK/bin/etcdctl" watch ctl-key --rev="$rev" --prev-kv -w json > "$WORK/results/ctl-watch.txt" &
watchpid=$!
"$WORK/bin/etcdctl" put ctl-key second >> "$WORK/results/ctl-put.txt"
"$WORK/bin/etcdctl" del ctl-key > "$WORK/results/ctl-del.txt"
sleep .2
kill "$watchpid" 2>/dev/null || true
wait "$watchpid" 2>/dev/null || true
jq -s -e '[.[].Events[]] | length == 3' "$WORK/results/ctl-watch.txt" >/dev/null
# Bootstrap all three first: etcd waits for readiness before serving the client
# port, so starting a fresh minority alone cannot probe its HTTP handlers.
kill "$pid"
wait "$pid" || true # SIGTERM is the requested shutdown, not a probe failure.
cluster='probe=http://127.0.0.1:32380,absent1=http://127.0.0.1:32391,absent2=http://127.0.0.1:32392'
for name in probe absent1 absent2; do
  case "$name" in
    probe) client=32379; peer=32380; dir=no-leader-data;;
    absent1) client=32381; peer=32391; dir=absent1-data;;
    absent2) client=32382; peer=32392; dir=absent2-data;;
  esac
  "$WORK/bin/etcd" --name "$name" --data-dir "$WORK/tmp/$dir" --listen-client-urls "https://127.0.0.1:$client" --advertise-client-urls "https://127.0.0.1:$client" --listen-peer-urls "http://127.0.0.1:$peer" --initial-advertise-peer-urls "http://127.0.0.1:$peer" --initial-cluster "$cluster" --cert-file "$CERT" --key-file "$KEY" --trusted-ca-file "$CA" --client-cert-auth > "$WORK/results/$name-cluster-server.log" 2>&1 &
  if [[ "$name" == probe ]]; then pid=$!; else others+=("$!"); fi
 done
for i in {1..40}; do if curl --max-time 2 -sf --cacert "$CA" --cert "$CERT" --key "$KEY" "$ENDPOINT/health" >/dev/null; then break; fi; sleep .1; done
kill "${others[@]}"
for child in "${others[@]}"; do wait "$child" 2>/dev/null || true; done
others=()
sleep 3
for p in health version 'health?serializable=true'; do
  name=${p//\?/-}
  curl -sS --cacert "$CA" --cert "$CERT" --key "$KEY" -D "$WORK/results/no-leader-$name.headers" "$ENDPOINT/$p" -o "$WORK/results/no-leader-$name.body"
done
grep -Eq '^HTTP/[^ ]+ 503( |\r|$)' "$WORK/results/no-leader-health.headers"
jq -e '.health == "false" and .reason == "RAFT NO LEADER"' "$WORK/results/no-leader-health.body" >/dev/null
printf 'PASS\n'
