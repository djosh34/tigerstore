#!/usr/bin/env bash
set -uo pipefail
mkdir -p results
run() {
  echo "CASE rep=$rep args=$*"
  taskset -c 0 bin/io576 "$@"
  local status=$?
  if ((status)); then echo "FAILED rep=$rep status=$status args=$*"; fi
}
for rep in 1 2 3; do
  {
    run config
    for size in 256 1024 4096 16384 65536; do
      for mode in 0 1 2 8 9 10; do run net "$size" "$mode" 1 0 0 0 unused; done
      for batch in 1 32; do
        for mode in 3 4 5 6; do run net "$size" "$mode" "$batch" 0 0 0 unused; done
      done
      run net "$size" 7 32 0 0 0 unused
      # Receiver comparison with identical send pattern; read capped to message, read64k, multishot64k.
      run net "$size" 3 32 0 0 1 unused
      run net "$size" 3 32 0 0 2 unused
      # Taskrun and SQPOLL; SQPOLL explicitly occupies core 2, receiver core 1.
      run net "$size" 3 32 1 0 0 unused
      run net "$size" 3 32 2 0 0 unused
      run net "$size" 3 32 1 0 2 unused
      for mode in 0 2 8; do run net "$size" "$mode" 1 0 1 0 unused; done
      run net "$size" 7 32 0 1 0 unused
      for mode in 3 4 5 6; do run net "$size" "$mode" 32 0 1 0 unused; done
    done
    for size in 4096 16384 65536 262144; do
      for direct in 0 1; do
        for mode in 0 1 2 3; do run disk "$RUNNER_TEMP/io576" "$size" "$mode" "$direct"; done
      done
    done
  } 2>&1 | tee "results/io-$rep.txt"
done
# Separate small traced runs: do not confuse traced wall/CPU with untraced measurements.
for spec in '0 1 0 0' '1 1 0 0' '3 1 0 0' '3 32 0 0' '4 32 0 0' '5 32 0 0' '7 32 0 0' '3 32 0 2' '3 32 1 2' '3 32 2 0'; do
  read -r mode batch flags rx <<< "$spec"
  SHORT=1 taskset -c 0 strace -ff -qq -e trace=write,writev,sendto,sendmsg,recvmsg,read,poll,ppoll,io_uring_enter,sendfile,splice -o "results/strace-$mode-$batch-$flags-$rx" bin/io576 net 1024 "$mode" "$batch" "$flags" 0 "$rx" unused > "results/trace-case-$mode-$batch-$flags-$rx.txt" 2>&1 || true
done
