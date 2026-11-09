#!/usr/bin/env bash
set -euo pipefail
phase=$1
# All snapshots occur within roughly one HTTP round trip of loadgen's CPU
# boundary. Save timestamps so the skew can be checked, not assumed away.
date +%s.%N > "$OUT/boundary-$phase.begin"
read -ra eps <<< "$RESEARCH_EPS"
for i in 0 1; do
 ep=${eps[i]}
 for path in debug/vars metrics debug/pprof/allocs; do
  name=${path##*/}
  curl -fsS --cacert "$CERTS/ca.pem" --cert "$CERTS/client.pem" --key "$CERTS/client-key.pem" "https://$ep/$path" -o "$OUT/node-$i-$phase.$name" &
 done
done
wait
# These distinctive exec names gate BPF counters, without polling files in BPF.
if [ "$phase" = 0 ]; then "$PWD/bin/research573-start"; else "$PWD/bin/research573-stop"; fi
date +%s.%N > "$OUT/boundary-$phase.end"
touch "$OUT/phase-$phase"
