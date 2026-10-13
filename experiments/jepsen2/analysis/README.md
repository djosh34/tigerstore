# Register analysis

Run from the directory containing `analyze.py`. After integration, this is `experiments/jepsen2/analysis`. All outputs stay below that directory. Inputs are read-only. Python 3.10 or newer is sufficient; no packages are required.

The script refuses to start at or after `2026-10-13T22:00:00Z`, as requested for this task. It never runs a consistency checker, JVM, build, workload, or fault.

## Reproduce the existing evidence analysis

After integration into the experiment checkout:

```sh
cd experiments/jepsen2/analysis
PYTHONDONTWRITEBYTECODE=1 python3 analyze.py --evidence ../../jepsen/evidence
PYTHONDONTWRITEBYTECODE=1 python3 analyze.py \
  --evidence ../../jepsen/evidence --discover --output all-register
```

For a standalone copy, replace `../../jepsen/evidence` with the absolute path to the original archive directory. The `--evidence` parameter identifies inputs independently of the analysis directory's location.

The first command selects 28 histories. Its primary `core` scope is exactly eight unresolved tigerstore histories and eight matched etcd controls. The other twelve histories have explicit auxiliary scopes. They are four earlier 50-operation controls and eight bounded 10-operation histories.

The second command discovers all register histories with results in the existing archives. Its separate `all-register/` output contains 37 histories. The primary report does not mix these totals with the 16-history comparison.

## Analyze the fresh run

For downloaded and unzipped GitHub artifact folders, preserve artifact names such as `jepsen2-tigerstore-kill,pause-ops200-rep1`. The artifact content should include `command.txt` and `store/latest/history.edn`, `results.edn`, and `jepsen.log`. The scan also accepts a nested `results/` directory beneath the named artifact folder. Existing `store/latest` links can be read without changing them.

```sh
PYTHONDONTWRITEBYTECODE=1 python3 analyze.py \
  --artifacts /absolute/path/to/downloaded-artifacts \
  --run-id 38440292743 \
  --output fresh-38440292743
```

For packed `.tar.gz` archives with case directories, use discovery instead.

```sh
PYTHONDONTWRITEBYTECODE=1 python3 analyze.py \
  --discover \
  --evidence /absolute/path/to/archive-directory \
  --output fresh-38440292743
```

Archive names become run identifiers. If `SHA256SUMS` exists, the script verifies listed archive hashes before extraction. Without it, the script records the computed hashes but does not claim an external hash verification.

Discovery reads register settings from each command. It supports combined kill, partition, and pause faults, arbitrary operation budgets, old competition results, and explicit WGL results. Store and repetition come from artifact names, with `runner.log` as a fallback for store identification. Each case records its command, history hash, checker verdict, key verdicts, and resource changes found in the saved upstream patch.

Missing checker results appear in `skipped-cases.json` and do not count as passes. Discovery does not assert that controls pass or that candidates fail. A new `:unknown` result stays unknown. Incomplete client invocations stay separate from recorded completions. They are not converted to `:info`.

Do not place input artifacts beneath the chosen output directory. All output locations must remain below this analysis directory.

## Output files

This directory retains derived tables, source pins, fault-edge logs and checker excerpts. Full original histories and server evidence remain in `../../jepsen/evidence` archives. The commands above regenerate the extracted files and `operations.json`; those duplicates are not committed here. For fresh histories whose online results are missing, `../summarize-fresh.py` still counts the complete raw history and checks fault observations without inventing a checker verdict.

- `REPORT.md` is the written analysis of the original evidence.
- `tables.md` contains generated per-history totals, per-operation counts, latency summaries, and fault-edge summaries with links to every primary case's edge log.
- `summary.json` records case selection, original verdicts, independent-key counts, checker log measurements, commands, and hashes.
- `counts-by-history.csv` and `latencies-by-history.csv` group completed calls by history, function, and outcome.
- `counts-by-store.csv` and `latencies-by-store.csv` group them by explicit scope and store.
- `counts-by-configuration.csv`, `latencies-by-configuration.csv`, and `configuration-summary.csv` add fault and operation-budget groups.
- `errors.csv` groups exact recorded errors by history, function, and outcome, with latency summaries. Null error groups include normal completions.
- `no-leader.csv` isolates the exact `[:unavailable "etcdserver: no leader"]` error and records counts, quantiles, and counts within 10 and 100 ms.
- `keys.csv` records each independent key's original verdict, memory-abort cause, outcomes, ambiguous mutation counts, duration, and saved pending configuration sizes.
- `fault-events.csv` records every fault invocation and completion, including time, index, applied targets, open client calls, and prior ambiguous mutations by key.
- `histories/<run>/<artifact>/fault-edges.log` lists every fault edge and its open calls in chronological order. It distinguishes nemesis invocation from completion even though both have recorded type `:info`.
- Each extracted case also contains original histories and results, command, patch, available fault-observer logs, a derived `operations.json`, and `checker-excerpts.log`.
- `analysis-runtime.json` records this Python analysis's wall time and maximum resident memory. It is not checker runtime.
- `sources/manifest.json` lists saved upstream source URLs and hashes used in the written explanation.

## Counting and validation rules

The script pairs client operations by process, checks function and independent key agreement, and preserves recorded completion outcomes. It checks main-history indices and timestamp order. It checks the outcome and per-function counts against the unchanged saved Jepsen stats checker. Where independent histories and results are present, it cross-checks them against the main history and main results.

Latency is completion time minus invocation time, converted from nanoseconds to milliseconds. Quantiles use nearest rank. Empty groups have no latency values, not zero latency.

Open calls at an edge are client invocations already recorded without a completion already recorded. A completed `:info` call is no longer physically open, but its mutation can remain ambiguous for the consistency checker. The cumulative prior-ambiguity columns are counts of recorded unknown mutations, not counts of server requests still executing or exact Knossos search states.

Nemesis records are sequential invocation/completion pairs. The script pairs them and verifies matching functions. For partition starts, it identifies empty link sets rather than treating any nonempty node map as a nonempty partition. Empty kill or pause target maps also remain distinct from applied nonempty faults. Fault-observation coverage statuses come from the saved summaries; this script does not create new fault-observation certifications.

The EDN reader handles the forms in these saved register records, including keyword apostrophes, sets, exception maps, and tagged checker models. It never evaluates Clojure or EDN forms. Unsupported forms cause a parse error rather than a guessed interpretation.
