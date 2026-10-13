# Offline profile analysis

`analyze.py` reads the downloaded CPU and trace artifacts. It runs only `go version`, `go tool pprof -raw` and `go tool trace -d=parsed`. It never executes artifact binaries, collects profiles, starts servers, runs tests or benchmarks, or accesses the network. Every Go subprocess receives `GOFLAGS=-p=2 GOMEMLIMIT=2GiB`. Commands run sequentially.

## Reproduce

Use the original downloaded artifact tree and Go 1.27.2. Run from the observe worktree:

```sh
python3 profile-analysis/analyze.py \
  --results-root /home/joshazimullah.linux/work_mounts/scratch/pre-50-shape/results \
  --output-dir /tmp/tigerstore-profile-analysis
```

The script refuses to write inside the input artifact tree. Its output path is independent of the input path. To regenerate the committed archive, use `--output-dir profile-analysis/archive` instead.

## Archived evidence

- `archive/computed-tables.md` contains generated CPU, Go-state, completed-syscall and fenced-counter tables. These are calculations, not manually transcribed report numbers.
- `archive/computed.json.gz` contains the integer nanosecond totals, every CPU sample's count, location IDs, disjoint context and inclusive mechanism labels, plus trace state totals for each subject GoID. It also contains each completed file-write/fsync and directory-fsync span's entry stack ID, GoID, timestamps and duration. `completed_syscall_entry_stacks` resolves those IDs to full function stacks.
- `archive/raw/` contains all 27 CPU `pprof -raw` outputs and 12 leader trace-profile raw outputs, including inline frames, location tables and empty-stack samples. No trace capture or complete parsed-event dump is duplicated here; the supplied trace files remain inputs for independent Go-state reconstruction.
- `archive/input-manifest.json` records the analysis script's SHA-256, Go version, exact commands and hashes and sizes of every consumed input, including matching binaries. Input paths are relative to the results root; recorded command arguments retain the original absolute paths. A different local directory or timezone can change those command strings or raw header formatting without changing calculated nanoseconds.

Read the machine archive with Python's standard library:

```python
import gzip
import json

with gzip.open("profile-analysis/archive/computed.json.gz", "rt") as stream:
    data = json.load(stream)
for cell, result in data["trace"].items():
    print(cell, result["completed_syscall_summaries"])
```

## File versus directory sync

The original report counted all completed `os.File.Sync`/`syscall.Fsync` spans as file syncs. That included six directory syncs during WAL segment creation.

The corrected classifier checks `osfs.syncPath` and both value/pointer `Dir.SyncDir` stacks first and assigns them to `directory_fsync`. It requires `osfs.observedFile.Sync` for `observed_file_fsync`, which matches the file-only observation wrapper. A sync lacking either identifying stack goes into `unclassified_fsync`, not the file column. Writes similarly require the observed-file wrapper for the observed-file column. The script checks each classified completed-span count and integer duration sum against the runner's syscall profile.

| Leader trace cell | Excluded directory syncs | Directory span sum ns | Corrected file syncs |
| --- | ---: | ---: | ---: |
| Put 1 KiB, 256 | 1 | 768 | 1293 |
| Put 1 KiB, 4096 | 3 | 62016 | 141 |
| Put 10 KiB, 256 | 2 | 258880 | 151 |

There are no unclassified syncs in these captures. No directory-sync CPU sample occurs in the 27 CPU profiles, so the file-fsync CPU column is unchanged. The whole-cohort `files.fsync` observation counters already exclude directory syncs and remain unchanged. Total WAL syscall state includes directory syncs and other syscalls; it is not a file-only counter.

## Attribution rules

The CPU partition operates on full leaf-to-root stacks and counts each sample once. Inclusive columns count a sample once per matching mechanism and overlap. Source code defines the matching prefixes and priority. Normalization uses full six-second captured CPU divided by the five-second cohort's successful operations, not captured wall duration.

Go-state attribution uses target `GoID` and `TransitionStack`. Stable owner, WAL, frontend and peer roots take priority over temporary observation calls on those goroutines. It never identifies the blocked subject using the event's waker `G` or `Stack`. Intervals start at the subject's first observed state; final states close at the last trace event. Runtime-only or unidentified subjects remain `other`.

Completed syscall spans require a seen entry and exit and omit boundary-crossing spans. Their median is the upper middle sorted duration, and p99 is the zero-based sorted index `floor(0.99 * count)`. Syscall duration includes kernel execution and any descheduling. State intervals and completed spans are elapsed wall durations, not measured CPU or additive request critical paths.
