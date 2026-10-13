# Correctness round 1 runner

This is throwaway code for #49. It does not change the product. Run fault scenarios only on free GitHub-hosted runners. The controller rejects local execution, including self-hosted Actions runners.

```sh
sudo -E python3 experiments/correct-round1/runner.py \
  --target tigerstore --scenario signals --server "$PWD/tigerstore" \
  --client "$PWD/client-tool" --out "$PWD/artifacts/signals" \
  --seed 49 --repetition 1 --duration 65
```

The output directory must not exist. The runner needs Python 3, openssl, iproute2, util-linux and e2fsprogs. Run from the checkout whose commit should be recorded. Build stock tigerstore from pinned main without test tags. For the baseline supply a stock etcd v3.7.2 executable and `--target etcd`. Only etcd gets `--initial-advertise-peer-urls`, `--peer-client-cert-auth` and a high snapshot threshold.

The controller reexecs in private network and mount namespaces. It generates mutual-TLS certificates with every peer and client IP SAN. Peers listen on `127.77.0.N:2380`, clients on `127.78.0.N:2379`. No source-address binding assumption is made.

Scenarios:

- `signals` kills and restarts the current leader and a current follower, then stops and resumes each role. Leader discovery precedes every fault. Raw waitpid statuses prove signals. Stop evidence includes every task's `/proc` state and unchanged process CPU time during a 1.5-second pause.
- `netem` applies 25% loss, 5 ms delay and 10 ms delay with 100% reorder and gap 2, in separate periods. Four scoped flower filters match TCP or UDP with the node's destination IP and destination peer port, or its source IP and source peer port. The UDP canaries prove both cases, including an initiator from `127.0.0.1`. TCP action counters prove stock peer traffic entered both selectors. Negative UDP controls use the client port or client IP and must add no packets to either UDP action counter. Qdisc options, filters, packet counters, baseline, positive, negative and healed canaries are saved.
- `disk` puts one node on a 128 MiB ext4 loop image with no reserved blocks. The controller fills it during recorded writes, demands actual ENOSPC from a separate write, and then demands that the stock server exits nonzero with its own ENOSPC log. Filler exhaustion alone is not a fault hit. It frees space, proves write plus fsync works, restarts and checks readiness.
- `mixed` runs every listed fault. Use `--duration 90` or more. Too-short durations fail rather than omit a fault.

For longer faults use `--pause-seconds 5`, `--kill-seconds 5`, `--netem-role leader` and `--netem-hold-seconds 5`. Pause and netem hold default to 1.5 seconds, kill hold to 1.2 seconds, and the netem role to follower. Holds accept 0.1 through 30 seconds. Netem hold is in addition to installation and packet-canary time. Requested evidence records the actual hold and role. Longer schedules need longer workload durations.

Any unrequested server exit fails the run explicitly. The controller records raw waitpid status, the log tail and exit classification. A nonzero exit whose log names the core's disk-stall or commit-stall watchdog is classified as `designed-self-fence`. That remains a failed scenario, not an automatic product-bug claim. No self-fenced node restarts automatically. Later recovery policy can change that only with an explicit decision.

`--duration` is the workload's actual configured duration, not a maximum that the runner cuts short. The runner requires at least four seconds of healthy traffic after healing, then waits for natural workload finalization. Traffic is three sequential clients at five operations per second each. `--value-size` defaults to 128 bytes and passes to the client. `--disk-mib` accepts 96 through 256 MiB. The baseline etcd WAL preallocates 64 MiB. A small, short workload may not hit its server ENOSPC. That run must fail honestly; do not use unrecorded writes to force it.

Every required fault must have `requested`, `installed`, `exercised` and `healed` phases in `faults.json`, with monotonic times. The realized schedule, not only the seed, is evidence. Kernel packet scheduling is not deterministic.

After the workload ends, each original endpoint must match its terminal receipt. The controller kills and reaps all servers, copies their data directories before any restart, then performs three sequential cold recoveries. Each recovery starts only the two surviving nodes, using new copies of the original disks and unchanged three-member configuration. Each endpoint's full range, revision and MVCC metadata must independently match the pre-kill receipt. The missing member never starts. The recovery pair is then killed and reaped before the next case.

`oracles.json` is an array of `{name,data_dirs,stopped_file}`. Names are `original` and `cold-missing-n1` through `cold-missing-n3`. Paths are absolute. The workflow must run the Go client's offline `log` then `check` for every entry, with `out/workload/history.json` as the source history. Controller success alone is not checker success. A failed receipt inspection still retains the stopped pair for offline checks. A failure before cold recovery preserves original disks and their stopped marker where possible.

Retain the entire output directory even if the controller fails. It includes commands, fault evidence, process logs, raw client history, terminal receipt, original data directories, independent recovery directories, certificates, and the loop image for disk runs. Any missing fault or checker is a failed run. Do not report a tooling failure as a product bug.

Failure cleanup gives the workload 40 seconds to save its history. The normal-run alarm reserves 90 seconds for workload finalization, process reaping and mount cleanup. A separate hard deadline kills managed process sessions and terminates the controller at the earlier of 540 seconds or 2026-10-13T22:00:00Z. New commands, fault scenarios and cold cases check the UTC deadline before starting. Cleanup alone may run after it.

This experiment does not prove power-loss durability. SIGKILL leaves the kernel page cache intact. It also does not cover byte-proxy faults, CPU quotas, named recovery crash boundaries, snapshots or all of #49's acceptance criteria. Scoped netem and UDP-canary logic was adapted from the #49 prework `experiments/chaos49/network.py` on branch `exp/49-real-process-chaos`. These canaries are chaos verification, not a calibrated physical-network throughput model.

Local validation is syntax checking and pure mocked tests only:

```sh
python3 -m py_compile experiments/correct-round1/runner.py
python3 -m unittest discover -s experiments/correct-round1 -p test_runner.py -v
```

The unit tests create only temporary files inside the experiment directory. They never send signals, run commands, mount filesystems or transmit packets.
