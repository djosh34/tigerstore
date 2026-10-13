# Jepsen round 2

Throwaway reliability experiment for tigerstore issue #49. Do not merge this branch. Product code is unchanged.

The prior run left eight versioned-register histories with an unknown verdict after Knossos ran out of memory. The original commands used 200 or 50 operations per key. Shorter histories do not resolve those results.

## Runner commands

Run only on a GitHub Actions runner before 2026-10-13T22:00:00Z:

```sh
bash experiments/jepsen2/run.sh etcd register kill 90 200
PRODUCT_SHA=2b072ac7e7acb4aeef1a6e794145914201607b8c \
  bash experiments/jepsen2/run.sh tigerstore register kill 90 200
```

The matrix uses `kill`, `partition`, `kill,pause`, `partition,pause`, and `kill,partition,pause`, with 200 and 50 operations per key and two repetitions on each store. Each case has its own fresh runner. All etcd controls finish before the candidate matrix starts. Fault schedules remain random and are not identical between products.

The client command retains upstream concurrency `2n`, rate 30, duration 90 seconds and nemesis interval 5 seconds. The first candidate matrix uses main commit `b5f2e28ef2edd94393d62952b1568763ebc12fe2`, the newest main when this round started. The full candidate matrix repeats at `2b072ac7e7acb4aeef1a6e794145914201607b8c`, newest main when the second matrix started. The baseline is stock etcd v3.7.2. Other version and container pins match `experiments/jepsen/REPORT.md`.

## Checker changes

Upstream already uses Jepsen's independent-key checker. This round retains that checker and its exact `VersionedRegister` model. It changes the JVM heap from 4 GiB to 10 GiB and wraps the per-key linearizability checker with `checker/concurrency-limit 1`. Knossos uses WGL instead of its default competition between WGL and the linear algorithm. These changes affect resource use and search strategy, not the model or input history.

The online checker receives every original operation. In particular, `:info` writes and CAS operations retain their uncertain outcomes. Elle is not a replacement for this versioned register and CAS specification. The larger heap still leaves unknown verdicts. The [report](REPORT.md) explains the independent certificate checks that resolve them without changing the model or classifying unknown calls as definite aborts.

Each job also runs the original independent fault-observation checks. An unknown or missing verdict, absent fault, or missing heal is a failure. The observer must see replacement PIDs for kills, stopped process states for pauses, and DROP rules with packet hits for partitions. These checks prove at least one observed application of each requested fault class, not every target of every random nemesis call.

Full upstream patches, commands, hashes, histories, results, logs and observer output are uploaded even on failure. The [report](REPORT.md), [original-history analysis](analysis/REPORT.md) and [permanent evidence](evidence/) identify every attempt and its outcome.
