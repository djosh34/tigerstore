# Round 2 clock, CPU and request-limit campaign

This branch is throwaway reliability testing for #49. Product binaries are built unchanged from `5d964228952d6983ba273bc2e6813770aad796c6`. Stock etcd v3.7.2 uses the same requests and fault script. Every fault runs only on free GitHub-hosted `ubuntu-24.04-arm` or `ubuntu-24.04` runners. No PR will be opened. Deadline is 2026-10-13T22:00:00Z.

## Clock mechanism

`clockwrap.c` is an external ptrace launcher. At exec it clears the child's `AT_SYSINFO_EHDR` auxiliary-vector value. Go then reads time through kernel syscalls rather than the vDSO. The launcher intercepts successful `clock_gettime(CLOCK_REALTIME)` results and offsets the returned seconds. It leaves monotonic time unchanged. It follows cloned threads. The server executable bytes are unchanged. This changes scheduling, so a zero-offset traced control is required before treating a traced failure as a clock-induced product failure.

All three members have separate offset files and traces. Only the selected member changes to +3600 seconds, heals to zero, then changes to -3600 seconds and heals again. Each member has an independent Go canary under the identical wrapper and offset file. Required proof consists of the server's syscall interception, the canary's realtime discontinuity, unchanged monotonic elapsed time, and no jump on the other two members. A command returning success is not proof. The host clock is never changed.

## CPU quota

A single native server PID moves into its own cgroup v2 child with `cpu.max` of `5000 100000` or `10000 100000`. Those quotas are 5% or 10% of one CPU across all server threads. The controller and clients remain outside. The job reads actual membership, quota, `cpu.stat` and process CPU counters. `nr_throttled` and `throttled_usec` must increase while the server consumes CPU. Missing hits fail the run. The fault lasts eight seconds, then the original cgroup membership is restored and cluster readiness must return within the base script's 18-second bound.

## Request sizes and histories

Exact 1.5 MiB value bytes exceed the default framed Raft request limit. Three member-pinned clients each send an exact-size Put and require a definite `request is too large` response. The workload then records admitted and rejected boundary probes around the independently computed protobuf limit. It uses the largest admitted framing allowance to fit every generated Put and CAS success-branch value, including the CAS framing. Full values, previous values and watch events remain in the repository history JSON. Hash-only values are not an oracle.

Near-limit workloads have three sequential clients at one operation per second each, six hot keys and a 40-second traffic duration. The mixed operations include Put, both CAS branches, range delete and Range. All three member-pinned watches request previous values. Coverage floors remain those of round 1. The controller cold-starts each surviving pair without the missing member and verifies full terminal values and revisions.

## Failed runs

Every matrix attempt has metadata and its complete Actions job log archived, including builds and setup failures. Attempts that reached the controller are counted separately. Every recorded client history invokes `Linearizable`, `Watches`, `Durable` and `Revisions`. If log conversion fails, the check command still runs on the enriched output if available, otherwise the raw history. Missing log, target or watches causes failed verdicts. A missing oracle manifest no longer returns before the check command. A checker timeout fails the run; synthetic missing verdicts are explicitly marked and never counted as executed passes.

Permanent archives use round 1's `manifest.json` format with path, original byte count and SHA-256. They stream compression and preserve full JSON histories without loading them into local memory. Original mutable server disks remain in Actions artifacts. Archived histories and server logs are permanent in this branch.

Known bug #88 is the commit-watchdog failure during near-limit writes. Equivalent failures add evidence to #88 rather than creating a duplicate. A distinct product failure needs two reproductions and an existing-bug check before filing a labelled bug sub-issue of #49.
