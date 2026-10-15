Temporary scripts for issue #188, not for merge. Both expect binaries in
/tmp/b188/bin (tigerstore, loadgen, fakeetcd) and certificates from
`scripts/dev-certs.sh /tmp/b188/certs`.

- `once.sh <name> <gen cpus> <keys>`: one mixed-k8s max run against a real
  3-node tigerstore cluster (nodes on CPUs 0-1, 2-3, 4-5), with the loadgen
  flags `bench/run.sh` uses, `BENCH_GOMEMLIMIT=1GiB`.
- `fake-once.sh <name> <gen cpus> <keys> [loadgen flags]`: the same mixed
  run against `cmd/fakeetcd` on `FAKECPUS`, with `-preload-rate 0`,
  `-warmup 2s -duration 8s`. Runs here used `FGOGC=off FML=800MiB`.
