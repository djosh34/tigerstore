# Jepsen experiment for #49

Throwaway branch `exp/jepsen`. No product changes. See `REPORT.md` for measured results and `robustness.md` for the source-only audit of etcd's robustness tests.

## Versions and commands

- Tigerstore main at the start of this experiment: `90870876b6459334c7eddd67c0f9d184267f2eba`.
- Stock etcd v3.7.2: `68c065e562994b89e333e77b039ad066f933c586`.
- Latest `jepsen-io/etcd` at checkout: `b47cc401f27bfb7c28afd9ac088b5f0dca4501b8`, dated 2026-04-17. Its project uses Jepsen 0.3.11 and jetcd 0.8.5.
- Jepsen's linked community Docker setup: `nurturenature/jepsen-docker` at `4b5707813a0a24966082ae4fd2dd223da447e499`. `run.sh` pins the image digests used by the first successful control. The project dependency selects Jepsen 0.3.11, not the newer version preinstalled in the image.
- Go 1.27.2 builds tigerstore on the runner with `GOFLAGS=-p=2 GOMEMLIMIT=2GiB CGO_ENABLED=0`.

The runner has one control container and three database containers on its private Docker bridge. Run one case on a GitHub-hosted `ubuntu-24.04` runner, never on the agent host:

```sh
bash experiments/jepsen/run.sh etcd register partition 90
bash experiments/jepsen/run.sh tigerstore register partition 90
```

Run the etcd case before its tigerstore counterpart. Replace `register` with `set`, `append`, or `watch`; replace `partition` with `none`, `kill`, or `pause`. The script rejects starts after 2026-10-13T22:00:00Z. A maintained CI job must replace that experiment deadline with its own execution policy.

Inside the control container, each case uses:

```sh
lein run test --nodes n1,n2,n3 --username root \
  --ssh-private-key /root/.ssh/id_rsa --version 3.7.2 \
  --workload register --nemesis partition --concurrency 2n \
  --rate 30 --time-limit 90 --nemesis-interval 5
```

The `--version 3.7.2` argument also appears on tigerstore tests because this is upstream's test naming option. `STORE=tigerstore` selects the tigerstore binary; `server.sha256`, the workflow matrix, and the build SHA identify the actual server. This does not mean tigerstore is etcd v3.7.2.

## Changes to upstream

`patch-db.py` changes only `src/jepsen/etcd/db.clj` and the JVM heap setting in `project.clj`. Every job saves the exact diff as `upstream.patch`.

The DB adapter copies the chosen server into `/opt/etcd/etcd`. Keeping that process name preserves upstream's kill/pause targets. Both stores use plain client gRPC on port 2379 and mutually authenticated peer TLS on port 2380. Each fresh job generates its own test CA. Tigerstore cannot use plaintext peer links.

The adapter uses the supported flags for each binary. It omits upstream's obsolete `--enable-v2` flag for etcd 3.7.2. It resolves primary status through the client port, since upstream's `member-status` calls the peer port. Workloads, clients, checkers, fault generators, and nemesis implementations remain unchanged. The 72 GiB JVM heap limit becomes 4 GiB to fit the free runner.

The compose file drops n4/n5 and pins images. An observer samples process states and `iptables-save -c` once per loop on each DB container. `summarize.py` checks saved Jepsen verdicts and requires observed DROP rules, stopped processes, or replacement PIDs for the selected faults. It also requires corresponding completed fault and healing operations. Run it offline with `edn_format==0.7.5` installed:

```sh
python3 experiments/jepsen/summarize.py path/to/artifact
```

The observer verifies that faults happened, not that every randomly selected target changed state. For example, upstream can select an empty primary set during an election. A kill/restart test does not simulate power loss or loss of kernel page-cache contents.

## Unsupported cases

The [Docker setup explicitly excludes independent clock skew](https://github.com/nurturenature/jepsen-docker/blob/4b5707813a0a24966082ae4fd2dd223da447e499/README.md#containers-dont-have-real-clocks). Docker shares `CLOCK_REALTIME` across the database containers, the control, and the runner. The clock preflight records namespace identities and exits nonzero before invoking upstream's clock nemesis. A clock preflight failure is missing coverage, not a passing clock test or a product bug. Real clock-skew coverage needs independent VM kernels. `libfaketime` is not a substitute for the Go runtime's clock reads and would change the requested test.

Tigerstore does not implement the Lock service called by upstream `lock-set` and `lock-etcd-set`. These workloads cannot run against the pinned tigerstore. No lock workload is substituted. The older `lock` workload is already disabled upstream. Membership changes, etcd WAL bitflips, snapshot corruption, defragmentation, and LazyFS are outside this comparison.

## CI requirements

Keep one bounded case per job, upload histories and server logs even on failure, and gate on both a definite valid checker result and fault coverage. A timeout, `:unknown`, a missing result, or a blocked capability must fail the corresponding required check. Use stock etcd controls to separate setup failures from product failures. Keep the runtime dependencies and Docker digests pinned; cache downloads if repeated cold starts cost too much.

Use a separate VM-based job for clock skew. Adding a skip to a required clock check does not provide that coverage. Porting this experiment into maintained CI also requires removing the fixed product SHA, adding trigger policy, and retaining counterexamples outside expiring Actions artifacts. Nothing in this branch is intended for merge as product code.
