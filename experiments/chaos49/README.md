# Real-process chaos feasibility probes

Question: which fault mechanisms can outcome #49 use on free Actions runners, and how can a job prove each fault took effect?

This is throwaway pre-work, not tigerstore implementation code. Do not merge this branch. The starting main commit was `91c4f974a6661ac02c2e7747781ea215353c643b`.

## Runner probes

The on-push workflow `.github/workflows/chaos49-probe.yml` runs isolated, short jobs on `ubuntu-24.04-arm` and `ubuntu-24.04`. Run the following only on disposable Linux runners with passwordless sudo:

```sh
sudo unshare --net python3 experiments/chaos49/network.py
sudo python3 experiments/chaos49/process.py "$PWD/results/process"
```

The network script creates a new network namespace, brings up its own loopback device, and puts netem behind a PRIO class. The host network and Actions control connection do not enter that namespace. No `ip addr add` is needed to bind distinct addresses within `127.0.0.0/8` on these Linux runners.

The peer canary binds source `127.77.0.1` and destination `127.77.0.2:2380`. Flower filters match both addresses, protocol and peer port. Unmatched traffic maps to class `1:1`, and only class `1:3` has netem. The all-zero `priomap` is intentional. An unmodified default PRIO mapping can send unrelated traffic into the impaired class.

The script reads qdisc and filter configuration as JSON, measures peer traffic, and checks that client traffic does not increase the netem packet counter. A successful `tc` exit alone is not a pass. UDP canaries expose loss, packet reordering and corruption. The corruption canary disables its own UDP checksum so that damaged payload bytes reach the checker. Production TCP/TLS must not disable integrity checks. The delay probe also measures a real TCP echo stream.

The process script starts a busy process under a transient systemd service with `CPUQuota=10%` and `CPUQuotaPeriodSec=100ms`. It reads the actual cgroup path, `cpu.max`, `cpu.stat` and process CPU time. It separately moves one gated worker into a direct cgroupfs child under `system.slice`. It does not move the runner agent or controller. SIGSTOP success requires a stopped wait status, stopped task states and unchanged CPU counters across the pause. SIGKILL success requires a reaped signal exit.

## Limits

These programs test mechanisms, not a tigerstore cluster. They do not prove linearizability, durability or liveness of tigerstore. SIGKILL does not discard the host page cache and is not power loss. A real-process seed does not reproduce kernel scheduling, socket read boundaries or reconnect order.

Loopback netem is useful for chaos and declared delay experiments, not an exact physical network throughput model. The upstream netem manual warns about TCP Small Queues and recommends receiver ingress placement for realistic TCP performance tests. Offloads, packet aggregation, TCP checksums and retransmissions affect the observed fault rate. Save configuration and measured traffic outcomes together.

Primary references:

- [netem behavior and limitations](https://github.com/iproute2/iproute2/blob/v6.14.0/man/man8/tc-netem.8)
- [PRIO classification](https://github.com/iproute2/iproute2/blob/v6.14.0/man/man8/tc-prio.8)
- [Flower selectors](https://github.com/iproute2/iproute2/blob/v6.14.0/man/man8/tc-flower.8)
- [cgroup v2 CPU controls](https://github.com/torvalds/linux/blob/v6.17/Documentation/admin-guide/cgroup-v2.rst)
- [systemd quota and period](https://github.com/systemd/systemd/blob/v255/man/systemd.resource-control.xml)
