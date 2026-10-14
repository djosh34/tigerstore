#!/usr/bin/env bash
# Steady churn after the fleet exists: scale Deployments (Pod create and delete),
# patch ConfigMaps, client LISTs and GETs, and one kube-apiserver restart.
set -u
END=$((SECONDS + ${CHURN_SECONDS:-300}))
RESTART_AT=$((SECONDS + ${CHURN_SECONDS:-300} / 2))
restarted=0
i=0
while (( SECONDS < END )); do
  i=$((i + 1))
  ns=shop-$(printf %02d $((RANDOM % 12)))
  app=service-$(printf %02d $((1 + RANDOM % 9)))
  kubectl -n "$ns" scale deploy "$app" --replicas=$((1 + RANDOM % 3)) >/dev/null 2>&1 &
  kubectl -n "$ns" patch cm "$app-config-1" --type merge -p "{\"data\":{\"generation\":\"churn-$i\"}}" >/dev/null 2>&1 &
  kubectl -n "$ns" annotate deploy "$app" --overwrite "churn=$i" >/dev/null 2>&1 &
  if (( i % 3 == 0 )); then kubectl get pods -A >/dev/null 2>&1 & fi
  if (( i % 5 == 0 )); then kubectl get cm -A --chunk-size=50 >/dev/null 2>&1 & fi
  if (( i % 4 == 0 )); then kubectl -n "$ns" get secret "$app-credentials" -o name >/dev/null 2>&1 & fi
  if (( i % 7 == 0 )); then kubectl get pods -A --chunk-size=100 --resource-version=0 >/dev/null 2>&1 & fi
  if (( !restarted && SECONDS >= RESTART_AT )); then
    restarted=1
    echo "{\"apiserver_restart\": $(date +%s.%N)}"
    docker exec cache-control-plane sh -c 'crictl stop $(crictl ps --name kube-apiserver -q)' >/dev/null 2>&1
  fi
  wait
  sleep 1
done
echo "{\"churn_iterations\": $i, \"end\": $(date +%s.%N)}"
