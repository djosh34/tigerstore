# Runner contract

Controller command, on GitHub runners only:

```sh
sudo -E python3 experiments/correct-round1/runner.py \
  --target tigerstore --scenario signals --server /absolute/tigerstore \
  --client /absolute/chaos-client --out /absolute/artifacts/signals \
  --seed 49 --repetition 1 --duration 65
```

The controller creates a private network namespace and starts the servers and client inside it. Output must not exist before the run. It preserves certificates, server logs, client histories, original cold disks, each independent recovery copy, commands.jsonl and faults.json on failure. Each run has a 540-second wall limit. Scenarios are signals, netem, disk and mixed. Mixed includes every fault. Scenario-specific required faults must all have requested, installed, exercised and healed evidence.

Nodes are n1, n2, n3. Peer URLs are https://127.77.0.N:2380. Client URLs are https://127.78.0.N:2379. Both client and peer links use mutual TLS. Certificates include all these IP SANs. Source binding is not assumed. Node-specific netem selects destination node IP plus destination peer port, or source node IP plus source peer port.

Confirmed Go client CLI:

- Common online options: --endpoints comma-separated URLs, --cacert file, --cert file, --key file.
- `status`: outputs JSON with `nodes` array, each item has `endpoint`, `member_id` and `leader_id` as decimal strings. All live nodes must agree on a nonzero leader. Controller maps the leader member ID to an endpoint.
- `run`: --duration 65s --seed 49 --clients 3 --rate 5 --value-size 128 --out DIR --stop-file PATH. Runs concurrently with faults and saves DIR/history.json. At the end, obtains a full-range receipt with revision, values and all MVCC fields, saved as DIR/receipt.json. It must not exit early if faults cause transient RPC failures. The normal controller waits for natural duration completion. A stop marker is only used during failure cleanup. The parent runs the offline checkers separately.
- `inspect`: --receipt DIR/receipt.json --out FILE. Independently reads the full key range linearly from each supplied endpoint, compares revision and full KV metadata/value bytes with the receipt. Exits nonzero if any endpoint disagrees. Used after workload completion, and after each pair-only cold boot.
- Parent calls `log` then `check` for every set in OUT/oracles.json, using OUT/workload/history.json. The manifest is an array of `{name,data_dirs:[absolute paths],stopped_file:absolute path}`. Names are `original` and `cold-missing-n1` through `cold-missing-n3`. A fully reaped pair is recorded even if its receipt inspection failed.

Cold recovery kills all servers after the workload ends, copies their original data directories before any restart, and performs three sequential tests. Each uses new copies of only the two surviving original data directories and starts only that pair with unchanged three-member bootstrap configuration. Inspection of each surviving endpoint must match the pre-kill receipt exactly. No write traffic occurs during cold recovery.

Signals require current leader/follower selection before each fault. SIGKILL uses waitpid WIFSIGNALED/WTERMSIG evidence. SIGSTOP uses waitpid WIFSTOPPED/WSTOPSIG and all /proc/PID/task/*/status states T, plus unchanged process CPU counters; SIGCONT uses waitpid WIFCONTINUED and non-stopped task states. Restart must regain online inspection/status.

Netem has independent loss, delay and reorder periods. Each requires actual qdisc parameters, filter selectors, increasing affected-packet stats, UDP positive canaries on the same peer selectors, negative canaries on excluded client addresses and ports, and healed canaries after removal. The canaries cover unbound initiator source addresses too.

Disk uses one node's data directory on a small ext4 loop image with no reserved blocks. During concurrent writes, fill until an actual ENOSPC, prove no space for a separate write, demand node disk-error/exit evidence, then delete filler, prove write/fsync works, restart and inspect. A failed setup or missing exercised fault fails the run. No tooling failure is a product bug.
