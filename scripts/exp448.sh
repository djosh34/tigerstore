#!/usr/bin/env bash
# Throwaway #448 work/floor measurements. Every process is bounded.
set -uo pipefail
root=$(pwd)
out=$root/exp448-results
mkdir -p "$out"
export GOSIMTOOL=$root/.gosim/bin/gosim SIM_SEEDS=1
for mode in default short long; do
 test=TestSeeds; seed=1; long=0
 if [[ $mode != default ]]; then test=TestKubernetesLoad; fi
 if [[ $mode == long ]]; then seed=7; long=1; fi
 for engine in gosim native; do
  dest=$out/$mode-$engine;mkdir -p "$dest"
  native=0; limit=12m; testtimeout=10m
  if [[ $engine == native ]];then native=1; limit=2m; testtimeout=90s;fi
  # The native .test driver launches one translated binary per gosim seed.
  (cd internal/sim; /usr/bin/time -v -o "$dest/time.txt" timeout --kill-after=10s "$limit" env SIM_SEED=$seed SIM_LONG=$long EXP441_MINUTES=10 EXP448_NATIVE=$native GOSIM_CPUPROFILE="$dest/cpu" "$root/.gosim/bin/sim.test" -test.run "^$test$" -test.v -test.timeout="$testtimeout") >"$dest/seed.log" 2>&1
  echo "$mode $engine exit=$?"
  grep -E 'REALMARK|TIMERS448' "$dest/seed.log" | cut -c1-1200 || true
  for profile in "$dest"/cpu.*; do
   [[ -f $profile ]] || continue
   go tool pprof -top -nodecount=120 "$profile" > "$profile.top.txt" 2>&1
   go tool pprof -top -cum -nodecount=120 "$profile" > "$profile.cum.txt" 2>&1
  done
 done
done
