# wait_stopped waits up to 5 s for process $1 to show state T after a
# SIGSTOP, and logs each look to file $2. A process in uninterruptible
# sleep (D) stops only once its call returns (#303).
wait_stopped() {
  local pid=$1 log=$2 state
  for _ in $(seq 1 50); do
    state=$(ps -p "$pid" -o stat= | tr -d ' ' || true)
    printf '   %s %s\n' "$pid" "$state" >> "$log"
    [[ $state == T* ]] && return 0
    sleep 0.1
  done
  printf '%s pid %s did not stop within 5s\n' "$(date -u +%FT%T.%N)" "$pid" >> "$log"
  return 1
}
