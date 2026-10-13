# Wholebatch runner medians

Five repetitions per variant in the same job. 16 connections, 1 KiB Put, 4,096 keys, leader-only.
Leader CPU 0, followers 1 and 2, generator 3. Servers GOMAXPROCS=2. Generator GOMAXPROCS=1.
These are closed-loop capacity runs with counting-only instrumentation, not profiles.

| Clients | Variant | Put/s | p50 ms | p95 ms | p99 ms | Total server CPU us/op |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 16 | admit | 8798.7 | 1.771 | 2.493 | 3.329 | 186.333 |
| 16 | etcd | 3444.6 | 4.196 | 6.922 | 11.127 | 420.000 |
| 16 | full | 9751.5 | 1.597 | 2.242 | 2.916 | 165.000 |
| 16 | main | 5708.1 | 2.695 | 3.815 | 5.728 | 245.333 |
| 16 | pool | 9051.9 | 1.733 | 2.367 | 2.829 | 177.667 |
| 16 | ring | 9077.1 | 1.738 | 2.367 | 2.838 | 177.667 |
| 16 | rpc | 9540.4 | 1.634 | 2.267 | 3.025 | 167.667 |
| 64 | admit | 25579.0 | 2.329 | 4.064 | 5.940 | 66.000 |
| 64 | etcd | 5399.2 | 11.186 | 19.291 | 22.992 | 267.667 |
| 64 | full | 26685.1 | 2.235 | 3.815 | 5.486 | 61.333 |
| 64 | main | 9058.4 | 6.600 | 10.408 | 15.813 | 137.000 |
| 64 | pool | 24450.3 | 2.460 | 3.943 | 5.562 | 65.333 |
| 64 | ring | 23847.7 | 2.476 | 4.119 | 5.633 | 66.667 |
| 64 | rpc | 22687.0 | 2.642 | 4.373 | 7.014 | 71.000 |
| 256 | admit | 40757.5 | 5.550 | 11.134 | 13.011 | 41.000 |
| 256 | etcd | 5919.3 | 42.080 | 63.639 | 78.338 | 230.667 |
| 256 | full | 42030.0 | 5.508 | 10.700 | 12.918 | 40.000 |
| 256 | main | 9569.7 | 24.869 | 42.523 | 54.652 | 120.000 |
| 256 | pool | 38353.9 | 6.014 | 11.667 | 14.069 | 40.667 |
| 256 | ring | 39953.3 | 5.858 | 10.965 | 13.262 | 39.333 |
| 256 | rpc | 34743.3 | 6.599 | 13.022 | 17.170 | 44.000 |

| Clients | Variant | Role | CPU us/op | Allocs/op | Bytes/op | Puts/file-fsync |
| ---: | --- | --- | ---: | ---: | ---: | ---: |
| 16 | admit | leader | 92.333 | 62.162 | 13238.6 | 3.837 |
| 16 | admit | follower1 | 51.000 | 20.269 | 3267.4 | 3.788 |
| 16 | admit | follower2 | 43.000 | 20.146 | 3262.6 | 3.864 |
| 16 | etcd | leader | 260.667 | 299.550 | 36101.3 | 4.541 |
| 16 | etcd | follower1 | 83.000 | 105.835 | 16293.6 | 4.652 |
| 16 | etcd | follower2 | 76.333 | 105.822 | 16349.6 | 4.623 |
| 16 | full | leader | 81.667 | 56.609 | 7587.2 | 3.911 |
| 16 | full | follower1 | 45.000 | 18.214 | 2194.6 | 3.848 |
| 16 | full | follower2 | 38.333 | 18.110 | 2191.3 | 3.894 |
| 16 | main | leader | 161.333 | 160.373 | 21473.6 | 4.351 |
| 16 | main | follower1 | 44.667 | 18.267 | 3189.4 | 3.909 |
| 16 | main | follower2 | 39.667 | 18.240 | 3188.3 | 3.936 |
| 16 | pool | leader | 93.667 | 47.575 | 5768.7 | 3.876 |
| 16 | pool | follower1 | 45.333 | 17.926 | 2182.5 | 3.717 |
| 16 | pool | follower2 | 38.667 | 17.807 | 2177.8 | 3.820 |
| 16 | ring | leader | 93.667 | 47.347 | 5759.1 | 3.965 |
| 16 | ring | follower1 | 45.000 | 17.679 | 2174.1 | 3.778 |
| 16 | ring | follower2 | 39.000 | 17.590 | 2170.7 | 3.943 |
| 16 | rpc | leader | 85.667 | 60.476 | 13195.2 | 4.023 |
| 16 | rpc | follower1 | 44.333 | 19.466 | 3237.7 | 3.952 |
| 16 | rpc | follower2 | 37.667 | 19.396 | 3235.1 | 4.030 |
| 64 | admit | leader | 35.000 | 42.049 | 13515.8 | 14.479 |
| 64 | admit | follower1 | 16.667 | 10.770 | 2954.4 | 13.908 |
| 64 | admit | follower2 | 14.333 | 10.736 | 2952.5 | 14.313 |
| 64 | etcd | leader | 182.667 | 275.751 | 36107.6 | 12.376 |
| 64 | etcd | follower1 | 43.667 | 92.116 | 16201.3 | 16.456 |
| 64 | etcd | follower2 | 41.000 | 92.626 | 16236.6 | 16.304 |
| 64 | full | leader | 32.333 | 36.732 | 6953.4 | 14.599 |
| 64 | full | follower1 | 15.333 | 8.853 | 1883.1 | 14.409 |
| 64 | full | follower2 | 13.333 | 8.831 | 1883.9 | 15.198 |
| 64 | main | leader | 105.667 | 143.894 | 21980.2 | 16.120 |
| 64 | main | follower1 | 16.667 | 10.275 | 2932.7 | 14.486 |
| 64 | main | follower2 | 14.667 | 10.275 | 2936.8 | 14.521 |
| 64 | pool | leader | 36.000 | 28.690 | 5047.3 | 14.528 |
| 64 | pool | follower1 | 16.000 | 8.626 | 1874.5 | 13.819 |
| 64 | pool | follower2 | 13.667 | 8.615 | 1874.0 | 14.171 |
| 64 | ring | leader | 36.333 | 28.783 | 5052.5 | 14.423 |
| 64 | ring | follower1 | 16.333 | 8.681 | 1876.1 | 13.787 |
| 64 | ring | follower2 | 14.000 | 8.652 | 1874.2 | 14.012 |
| 64 | rpc | leader | 40.333 | 43.438 | 13556.5 | 14.963 |
| 64 | rpc | follower1 | 16.333 | 10.530 | 2943.5 | 14.025 |
| 64 | rpc | follower2 | 14.333 | 10.521 | 2942.3 | 14.131 |
| 256 | admit | leader | 23.333 | 36.974 | 13661.8 | 37.783 |
| 256 | admit | follower1 | 9.000 | 8.639 | 2848.7 | 34.522 |
| 256 | admit | follower2 | 8.333 | 8.644 | 2848.8 | 35.377 |
| 256 | etcd | leader | 168.333 | 264.778 | 34829.2 | 14.500 |
| 256 | etcd | follower1 | 31.333 | 82.543 | 14849.6 | 44.577 |
| 256 | etcd | follower2 | 30.667 | 81.763 | 14605.4 | 45.732 |
| 256 | full | leader | 22.333 | 32.790 | 6870.8 | 32.644 |
| 256 | full | follower1 | 9.333 | 6.824 | 1793.5 | 30.864 |
| 256 | full | follower2 | 8.333 | 6.823 | 1793.6 | 31.813 |
| 256 | main | leader | 102.000 | 144.774 | 21902.1 | 13.089 |
| 256 | main | follower1 | 9.333 | 10.181 | 2929.3 | 41.152 |
| 256 | main | follower2 | 8.667 | 10.185 | 2922.3 | 40.872 |
| 256 | pool | leader | 24.667 | 24.792 | 4936.5 | 42.076 |
| 256 | pool | follower1 | 8.333 | 6.490 | 1769.2 | 37.453 |
| 256 | pool | follower2 | 7.333 | 6.488 | 1764.3 | 38.314 |
| 256 | ring | leader | 24.000 | 24.764 | 4937.2 | 42.614 |
| 256 | ring | follower1 | 8.000 | 6.473 | 1769.8 | 38.119 |
| 256 | ring | follower2 | 7.667 | 6.473 | 1766.3 | 39.012 |
| 256 | rpc | leader | 28.000 | 38.744 | 13601.4 | 46.584 |
| 256 | rpc | follower1 | 8.333 | 8.355 | 2834.6 | 42.614 |
| 256 | rpc | follower2 | 8.000 | 8.354 | 2824.4 | 42.918 |

Stage ratios divide the later stage by the earlier stage within each repetition, then take the median ratio.
| Clients | Stages | Put/s ratio | Total CPU/op ratio | Allocs/op ratio | Bytes/op ratio | p95 ratio | p99 ratio |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 16 | admit/rpc | 0.989 | 1.045 | 1.032 | 1.005 | 1.023 | 1.052 |
| 16 | full/admit | 1.086 | 0.927 | 0.901 | 0.605 | 0.912 | 0.872 |
| 64 | admit/rpc | 1.146 | 0.924 | 0.986 | 1.000 | 0.929 | 0.892 |
| 64 | full/admit | 1.071 | 0.923 | 0.853 | 0.552 | 0.952 | 0.934 |
| 256 | admit/rpc | 1.181 | 0.931 | 0.974 | 1.005 | 0.855 | 0.771 |
| 256 | full/admit | 1.027 | 0.967 | 0.855 | 0.540 | 0.958 | 0.993 |

| Clients | Variant | Leader GC accounting us/Put | Leader retained heap MiB | Core handoff sends/Put | Frontend controls/Put |
| ---: | --- | ---: | ---: | ---: | ---: |
| 16 | admit | 1.537 | 96.38 | 2.000 | 3.033 |
| 16 | etcd | nan | nan | nan | nan |
| 16 | full | 1.022 | 54.28 | 2.000 | 3.033 |
| 16 | main | 3.005 | 96.69 | 2.000 | nan |
| 16 | pool | 0.823 | 114.38 | 2.000 | 3.033 |
| 16 | ring | 0.481 | 242.29 | 2.000 | 3.033 |
| 16 | rpc | 1.415 | 96.75 | 2.000 | 3.033 |
| 64 | admit | 1.276 | 99.01 | 1.018 | 1.559 |
| 64 | etcd | nan | nan | nan | nan |
| 64 | full | 0.783 | 55.44 | 1.018 | 1.563 |
| 64 | main | 2.992 | 98.98 | 2.000 | nan |
| 64 | pool | 0.810 | 115.44 | 1.012 | 1.557 |
| 64 | ring | 0.374 | 243.47 | 1.014 | 1.554 |
| 64 | rpc | 1.292 | 97.93 | 2.000 | 1.708 |
| 256 | admit | 1.278 | 101.59 | 1.013 | 1.171 |
| 256 | etcd | nan | nan | nan | nan |
| 256 | full | 1.009 | 54.97 | 1.017 | 1.165 |
| 256 | main | 4.450 | 100.87 | 2.000 | nan |
| 256 | pool | 1.178 | 115.00 | 1.011 | 1.173 |
| 256 | ring | 0.395 | 243.12 | 1.010 | 1.172 |
| 256 | rpc | 1.611 | 98.75 | 2.000 | 1.211 |

GC CPU is runtime /cpu/classes/gc/total accounting, not sampled process CPU. Stock etcd does not expose this counter here.
Retained heap is after eight quiet seconds and forced GC, outside headline windows. It includes the retained log and history, plus reserved ring bytes.
Channel figures count specified successful sends, not runtime-wide channel operations. Core handoffs are admission plus reply delivery; frontend control sends are separate. grpc-go internals are not counted.
CPU is process user+system time from /proc, not owner-goroutine CPU.
CPU and allocation windows do not wait for follower catch-up. Follower cost can fall outside those windows.
Allocation counters bracket the CPU window and include small debug scrape overhead. They count all process allocations, not retained bytes.
Fsync deltas enclose the fully applied cohort on all nodes. etcd also fences on its post-WAL.Save committed gauge.
Tigerstore file syncs exclude directory syncs. etcd counts WAL fsync observations; backend commits are separate in runs.csv.
Medians are computed independently. paired.csv contains per-repetition candidate/baseline ratios.
