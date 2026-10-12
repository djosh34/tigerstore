# Disk fault layer for real-process chaos

## Recommendation

Use a test-only `faultfs` build plus an explicit control socket flag. Wrap the real disk adapter, not the simulated disk. Use ordinary files underneath, and return faults through the same storage interfaces that the real node uses. Add bounded ext4 loop ENOSPC as an independent kernel test. Keep dm-flakey/dm-delay for additional block/filesystem tests. FUSE works on both runners, but a complete passthrough filesystem is more code than this project needs initially.

This is a recommendation and a throwaway demonstration, not tigerstore integration or a completed #49 gate.

## What exists

I read issues [46](https://github.com/djosh34/tigerstore/issues/46), [1](https://github.com/djosh34/tigerstore/issues/1), [49](https://github.com/djosh34/tigerstore/issues/49), [33](https://github.com/djosh34/tigerstore/issues/33), their comments, and main's design.

At inspected main `91c4f974a6661ac02c2e7747781ea215353c643b`, the executable is a skeleton. The [design](https://github.com/djosh34/tigerstore/blob/91c4f974a6661ac02c2e7747781ea215353c643b/docs/design.md#testing) describes adapters and real-process faults, but main has no disk adapter to reuse.

The unmerged `work/47-storage` branch at `cbcdb7b6f7fb7db02c06cb22b4497b68235d8290` does have [wal.FS and wal.File](https://github.com/djosh34/tigerstore/blob/cbcdb7b6f7fb7db02c06cb22b4497b68235d8290/internal/wal/wal.go#L30-L59) and [osfs.Dir](https://github.com/djosh34/tigerstore/blob/cbcdb7b6f7fb7db02c06cb22b4497b68235d8290/internal/osfs/osfs.go). That is the candidate integration point, if its interfaces survive merge. Its real files use `O_APPEND`. Wrap `File.Write`, not this demonstration's `WriteAt`. Go rejects `WriteAt` on append-opened files. [Go 1.27.2 source](https://github.com/golang/go/blob/go1.27.2/src/os/file.go#L208-L260).

## Comparison

| Approach | What it tests | Limits and cost |
| --- | --- | --- |
| Wrapper over real adapter | Exact file/operation/Nth-call selection, actual prefix bytes, `EIO`/`ENOSPC` returns, indefinite sync stall with live controller | Needs complete call coverage in the real process. Injected errno is not a kernel writeback failure. Stall is before syscall entry, not a kernel task stuck inside fsync. |
| FUSE passthrough | Unmodified binary's filesystem syscalls, independent process control, kernel-visible fsync errors/stalls | Must implement and audit namespace, directory sync, truncate, file-handle and cache behavior. Cache mode changes which operations reach callbacks. Short writes need direct I/O mode under libfuse's documented contract. [FUSE I/O modes](https://docs.kernel.org/filesystems/fuse/fuse-io.html), [libfuse callbacks](https://github.com/libfuse/libfuse/blob/fuse-3.17.4/include/fuse.h#L501-L567). |
| dm-flakey/dm-delay over owned loop image | Actual block failures and delayed reads/writes/flushes under a normal filesystem | Runner root and loop/DM support. Block requests do not identify WAL records. Filesystem writeback and journaling affect where errors appear. Delay is finite milliseconds, not a convenient controllable forever-fsync stall. [flakey](https://docs.kernel.org/admin-guide/device-mapper/dm-flakey.html), [delay](https://docs.kernel.org/admin-guide/device-mapper/delay.html). |
| Small tmpfs | Fast, bounded, real kernel ENOSPC | Memory-backed, no durability or block-device behavior. [tmpfs sizing](https://docs.kernel.org/filesystems/tmpfs.html). |
| Small ext4 loop filesystem | Real allocation, writeback, fsync and ENOSPC under ext4 | Slower setup, runner root. It remains on the same runner host and backing filesystem, not an independent physical disk. |

`dm-flakey error_writes` reports errors during its down interval. `drop_writes` silently discards writes instead. Do not substitute the latter in a truthful-fsync durability gate. `dm-delay` accepts a 9-argument table with separate read, write and flush delays. The probes install 0/200/300 ms respectively. These are documented mechanisms, not evidence that a particular tigerstore fsync encountered them.

Linux permits partial writes and later writeback errors. Successful write does not prove persistence. Successful fsync also does not persist the file's directory entry without directory fsync. [write(2)](https://man7.org/linux/man-pages/man2/write.2.html), [fsync(2)](https://man7.org/linux/man-pages/man2/fsync.2.html). Go's [File.Sync](https://github.com/golang/go/blob/go1.27.2/src/os/file_posix.go#L160-L171) calls the underlying fsync implementation.

## Runnable demonstration

Copy `demo/` and `run-demo.sh` together, then:

```bash
bash run-demo.sh
```

The script runs `go vet` in both build configurations, builds normal and `-tags=faultfs` workers, and runs a separate Python controller over a Unix socket. It sets `TMPDIR`, `GOCACHE`, `GOMODCACHE`, and `XDG_CACHE_HOME` beneath this directory. It deletes worker binaries, temporary data and Go caches, retaining `results/demo.log` and `results/demo-events.json`. No third-party Go dependency is needed. The Go module accepts 1.24+, while the measured host used 1.27.2.

The normal binary rejects the control flag. The tagged worker requires opt-in. Rules name a file, operation, Nth match, action and unique ID. The controller requires an `installed` event and a separate `fired` event. Both include file/operation/action and sequence number; replies also identify PID and process incarnation. The controller remains responsive during a blocked sync. The test verifies:

- Wrong file scope and duplicate rule IDs are rejected. Nth selection does not fire early. One-shot error injection does not affect the next healthy sync.
- Fsync EIO is returned. A stalled sync has no reply until release; release then calls real `os.File.Sync` and records completion.
- ENOSPC writes three real prefix bytes before returning the injected errno. A torn-write rule writes five actual bytes from a 16-byte request and returns EIO.
- SIGKILL during a second stalled sync produces no successful reply or completion event. The file still contains exactly `BASELINEDISTORN-` when another process reads it.

Local result at 2026-10-12T22:22:35Z was PASS on ARM64, Linux `5.14.0-611.26.1.el9_7.aarch64`, Go `1.27.2`, on virtiofs. Two earlier attempts failed and led to fixes: chmod on a Unix socket returned EINVAL on virtiofs, so the worker now sets its own creation umask; Python's capitalized strerror did not match Go's error string, so the controller now checks numeric errno. A passing rerun does not erase those failures.

This tests real partial file contents and a process-level control path. It does not test a WAL parser, Raft, recovery, actual kernel-generated EIO/ENOSPC, or power-loss persistence. SIGKILL does not discard the kernel page cache. Do not emulate power loss by claiming that every unsynced byte must disappear after process death.

## Runner results

Parent ran the probes in [Actions run 38316232203](https://github.com/djosh34/tigerstore/actions/runs/38316232203), commit `064d4fc089e13b53753db12f6e85b03de881dc3a` on `exp/49-real-process-chaos`. I read the downloaded disk logs and versions and verified all eight disk jobs' successful conclusions through the GitHub API. Raw evidence is in `../results/runner-38316232203/` on this experiment branch. The linked Actions run contains the individual job URLs.

Both `ubuntu-24.04-arm` and `ubuntu-24.04` ran kernel `6.17.0-1022-azure`. Image versions were `20261004.142.1` and `20261004.327.1`. Recorded packages included fuse3/libfuse3-dev `3.14.0-5build1`, dmsetup `2:1.02.185-3ubuntu3.2`, util-linux `2.39.3-9ubuntu6.6`, and e2fsprogs `1.47.0-2.4~exp1ubuntu4.1`.

| Probe | ARM64 | AMD64 |
| --- | --- | --- |
| FUSE | Actual non-root mount, write/read and kernel-visible fsync EIO passed | Same |
| dm-flakey | Direct block write returned EIO; read remained healthy | Same |
| dm-delay | Direct write plus fsync took 0.514851 s | 0.514918 s |
| 1 MiB tmpfs | Real ENOSPC after 1,048,576 written bytes; write after freeing file passed | Same |
| 32 MiB ext4 loop | Real ENOSPC after 26,542,080 written bytes; write after freeing file passed | Same |

The delay measurement establishes a delayed write-plus-sync path and acceptance of the 9-argument table. It does not independently establish flush-only timing, a failing fsync, or an indefinitely stalled kernel fsync. The FUSE smoke filesystem stores bytes in its daemon's memory; it is not a complete faulting passthrough filesystem.

The standalone tagged wrapper also passed on ARM64 with Go 1.27.2 in [final Actions run 38316542472](https://github.com/djosh34/tigerstore/actions/runs/38316542472), commit `6d1bf2344d3d961c83c87ca0d7f1fa38065659da`. I read `wrapper.txt` and `versions.txt` from the parent's final artifact and verified the run's success through the API. The log records all five installed/fired rules and the final PASS. The table above uses the first successful run's disk measurements, not the final run's timings. No actual tigerstore integration was tested.

## Runner commands and integration obligations

Copy the entire `probes/` directory. In Actions, use explicit `shell: bash` and `set -euo pipefail`, including outer commands piped to `tee`:

```bash
bash probes/probe-fuse.sh
bash probes/probe-dm.sh
bash probes/probe-enospc.sh tmpfs
bash probes/probe-enospc.sh loop
```

FUSE needs a C compiler, pkg-config, fuse3 and libfuse3 development headers. The parent installed the FUSE packages only in disposable Actions jobs. DM and ENOSPC use runner-only noninteractive sudo and private mount namespaces. They create only owned backing images and new loop/DM devices, with cleanup traps. No host sudo, module commands, fuse.conf edits or sysctl changes were used. Exit 77 means unavailable and must fail a required chaos job, never count as coverage. Probe scripts reject non-Actions execution and starts at or after `2026-10-13T22:00:00Z`.

For actual tigerstore, create the controller before WAL recovery and install startup faults before allowing recovery to proceed. Wrap every storage call, including open/create, directory sync, truncate, remove, snapshot operations and any direct syscall/mmap path. Keep stalls outside controller locks and retain acknowledgments outside the node before killing it. Require the exact requested rule to fire, not merely a table or rule to exist. After healing, run the #49 linearizability, watch and durability checkers on real client histories. None of that integration exists in this demonstration.

No scripts changed after the parent's copies at 22:24:50Z. No comments were posted, no push was made, and no subagents were created.
