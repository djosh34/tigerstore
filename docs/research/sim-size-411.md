# Simulator seeds at 20,000 to 50,000 objects (#411)

Nothing here is intended for main. No PR was opened. All simulations ran in GitHub Actions, not on the shared research host.

## Method

Run [40117050466](https://github.com/djosh34/tigerstore/actions/runs/40117050466), harness `2838c211e3b9e831427efdef1412b22282b88b3c`. Its simulator code equals the #401 primary harness `95fe1fc8` when `compact_preload` is false; later commits only add that option and documents.

Inputs as the #401 primary matrix: seed 1, full-size values (`value_divisor=1`, mean 1,458 B), `watch_after_preload=true`, `compact_preload=false`, usual `DefaultConfig(1)` faults, 12 GiB RSS guard. One change: `wall_seconds=19800` instead of 1,200, so four-hour cells could run past 20 minutes. Each cell had its own `ubuntu-24.04-arm` VM.

## Results

| Initial objects | Virtual churn | Wall s | Peak RSS GiB | Virtual s / wall s | Outcome |
|---:|---:|---:|---:|---:|---|
| 20,000 | 1 h | 466.71 | 8.633 | 7.93 | finished |
| 20,000 | 4 h | 1,357.59 | 10.813 | 10.68 | finished |
| 30,000 | 1 h | 718.51 | 9.871 | 5.22 | finished |
| 30,000 | 4 h | 2,210.02 | 11.369 | 6.58 | finished |
| 50,000 | 1 h | 90.61 | 12.747 | 2.54 | 12 GiB guard, 38.6 virtual s into churn |
| 50,000 | 4 h | 90.57 | 12.090 | 2.54 | 12 GiB guard, 38.6 virtual s into churn |

Peak is GNU time's maximum RSS, or the larger five-second sample. Preload took 80.3 / 129.4 / 191.5 virtual seconds at 20,000 / 30,000 / 50,000 objects.

Four-hour cells: 1,482,071 / 1,519,732 calls; 68,860 / 74,518 succeeded; 40 / 40 compactions; 3,590 / 3,774 kills (20,000 / 30,000 objects).

At 50,000 objects RSS swung between 7 and 12.7 GiB in the first 40 virtual seconds of churn. At the guard, the running goroutine was a watch creation replaying history: `core.(*Node).createWatch` (`internal/core/watch.go:323`) → `watcher.event` → `mvcc.Change.Event` → `Store.kv`/`fill` (`internal/mvcc/store.go:179,193,454`). This matches the gray-client replay limit found in #401.

Not measured: more seeds, a kernel OOM above the 12 GiB guard, 40,000 objects, a smaller-value or compact-preload control at 50,000.
