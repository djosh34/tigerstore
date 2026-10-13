# Per-job results

Durations are Actions job start to finish, including setup and artifact upload, excluding queue time. A pass includes the upstream checker verdict and the offline fault-observation checks. Inconclusive means `:valid? :unknown`; it is a failed gate, not a pass. Blocked clock cases never ran their workloads.

| Store | Outcome | Jobs |
| --- | --- | --- |
| etcd | blocked | 1 |
| etcd | pass | 37 |
| tigerstore | blocked | 1 |
| tigerstore | execution-failure | 4 |
| tigerstore | inconclusive | 8 |
| tigerstore | pass | 20 |

| Run | Jobs | Job duration range |
| --- | --- | --- |
| [38426936461](https://github.com/djosh34/tigerstore/actions/runs/38426936461) | 1 | 2m52s to 2m52s |
| [38427801610](https://github.com/djosh34/tigerstore/actions/runs/38427801610) | 17 | 1m08s to 3m33s |
| [38428716698](https://github.com/djosh34/tigerstore/actions/runs/38428716698) | 5 | 1m01s to 4m31s |
| [38429841266](https://github.com/djosh34/tigerstore/actions/runs/38429841266) | 12 | 1m35s to 3m43s |
| [38430056885](https://github.com/djosh34/tigerstore/actions/runs/38430056885) | 8 | 3m11s to 5m20s |
| [38430224306](https://github.com/djosh34/tigerstore/actions/runs/38430224306) | 4 | 3m21s to 3m47s |
| [38430904489](https://github.com/djosh34/tigerstore/actions/runs/38430904489) | 8 | 3m17s to 3m42s |
| [38432262482](https://github.com/djosh34/tigerstore/actions/runs/38432262482) | 8 | 3m27s to 6m17s |
| [38433448661](https://github.com/djosh34/tigerstore/actions/runs/38433448661) | 4 | 3m21s to 3m33s |
| [38434478844](https://github.com/djosh34/tigerstore/actions/runs/38434478844) | 4 | 3m35s to 4m11s |

| Run/job | Store | Workload | Fault | Ops/key | Repetition | Duration | Outcome | Operations |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| [38426936461](https://github.com/djosh34/tigerstore/actions/runs/38426936461/job/115398670256) | etcd | register | none | 200 | 1 | 2m52s | pass | 1772 |
| [38427801610](https://github.com/djosh34/tigerstore/actions/runs/38427801610/job/115401685574) | etcd | append | kill | 200 | 1 | 3m12s | pass | 1911 |
| [38427801610](https://github.com/djosh34/tigerstore/actions/runs/38427801610/job/115401685834) | etcd | append | none | 200 | 1 | 3m05s | pass | 2728 |
| [38427801610](https://github.com/djosh34/tigerstore/actions/runs/38427801610/job/115401685842) | etcd | append | partition | 200 | 1 | 3m18s | pass | 2567 |
| [38427801610](https://github.com/djosh34/tigerstore/actions/runs/38427801610/job/115401685912) | etcd | append | pause | 200 | 1 | 3m24s | pass | 1927 |
| [38427801610](https://github.com/djosh34/tigerstore/actions/runs/38427801610/job/115401689669) | etcd | register | clock |  | 1 | 1m08s | blocked |  |
| [38427801610](https://github.com/djosh34/tigerstore/actions/runs/38427801610/job/115401685910) | etcd | register | kill | 200 | 1 | 3m14s | pass | 1956 |
| [38427801610](https://github.com/djosh34/tigerstore/actions/runs/38427801610/job/115401686010) | etcd | register | none | 200 | 1 | 3m16s | pass | 2731 |
| [38427801610](https://github.com/djosh34/tigerstore/actions/runs/38427801610/job/115401685976) | etcd | register | partition | 200 | 1 | 3m32s | pass | 1799 |
| [38427801610](https://github.com/djosh34/tigerstore/actions/runs/38427801610/job/115401686104) | etcd | register | pause | 200 | 1 | 3m26s | pass | 2003 |
| [38427801610](https://github.com/djosh34/tigerstore/actions/runs/38427801610/job/115401686085) | etcd | set | kill | 200 | 1 | 3m16s | pass | 2289 |
| [38427801610](https://github.com/djosh34/tigerstore/actions/runs/38427801610/job/115401685782) | etcd | set | none | 200 | 1 | 3m19s | pass | 2637 |
| [38427801610](https://github.com/djosh34/tigerstore/actions/runs/38427801610/job/115401686037) | etcd | set | partition | 200 | 1 | 3m25s | pass | 1626 |
| [38427801610](https://github.com/djosh34/tigerstore/actions/runs/38427801610/job/115401686399) | etcd | set | pause | 200 | 1 | 3m15s | pass | 2004 |
| [38427801610](https://github.com/djosh34/tigerstore/actions/runs/38427801610/job/115401689491) | etcd | watch | kill | 200 | 1 | 3m20s | pass | 1284 |
| [38427801610](https://github.com/djosh34/tigerstore/actions/runs/38427801610/job/115401685951) | etcd | watch | none | 200 | 1 | 3m33s | pass | 2725 |
| [38427801610](https://github.com/djosh34/tigerstore/actions/runs/38427801610/job/115401685933) | etcd | watch | partition | 200 | 1 | 3m28s | pass | 1751 |
| [38427801610](https://github.com/djosh34/tigerstore/actions/runs/38427801610/job/115401689808) | etcd | watch | pause | 200 | 1 | 3m17s | pass | 1807 |
| [38428716698](https://github.com/djosh34/tigerstore/actions/runs/38428716698/job/115404904190) | tigerstore | register | clock |  | 1 | 1m01s | blocked |  |
| [38428716698](https://github.com/djosh34/tigerstore/actions/runs/38428716698/job/115404904262) | tigerstore | register | kill | 200 | 1 | 4m15s | inconclusive | 1636 |
| [38428716698](https://github.com/djosh34/tigerstore/actions/runs/38428716698/job/115404904008) | tigerstore | register | none | 200 | 1 | 3m57s | pass | 2659 |
| [38428716698](https://github.com/djosh34/tigerstore/actions/runs/38428716698/job/115404905586) | tigerstore | register | partition | 200 | 1 | 4m31s | inconclusive | 2389 |
| [38428716698](https://github.com/djosh34/tigerstore/actions/runs/38428716698/job/115404904318) | tigerstore | register | pause | 200 | 1 | 3m56s | pass | 2138 |
| [38429841266](https://github.com/djosh34/tigerstore/actions/runs/38429841266/job/115408816728) | tigerstore | append | kill | 200 | 1 | 3m27s | pass | 2355 |
| [38429841266](https://github.com/djosh34/tigerstore/actions/runs/38429841266/job/115408816492) | tigerstore | append | none | 200 | 1 | 3m18s | pass | 2666 |
| [38429841266](https://github.com/djosh34/tigerstore/actions/runs/38429841266/job/115408816618) | tigerstore | append | partition | 200 | 1 | 3m43s | pass | 1375 |
| [38429841266](https://github.com/djosh34/tigerstore/actions/runs/38429841266/job/115408816537) | tigerstore | append | pause | 200 | 1 | 3m19s | pass | 2012 |
| [38429841266](https://github.com/djosh34/tigerstore/actions/runs/38429841266/job/115408816585) | tigerstore | set | kill | 200 | 1 | 2m01s | execution-failure |  |
| [38429841266](https://github.com/djosh34/tigerstore/actions/runs/38429841266/job/115408816650) | tigerstore | set | none | 200 | 1 | 1m50s | execution-failure |  |
| [38429841266](https://github.com/djosh34/tigerstore/actions/runs/38429841266/job/115408816612) | tigerstore | set | partition | 200 | 1 | 1m42s | execution-failure |  |
| [38429841266](https://github.com/djosh34/tigerstore/actions/runs/38429841266/job/115408816117) | tigerstore | set | pause | 200 | 1 | 1m35s | execution-failure |  |
| [38429841266](https://github.com/djosh34/tigerstore/actions/runs/38429841266/job/115408816866) | tigerstore | watch | kill | 200 | 1 | 3m29s | pass | 1286 |
| [38429841266](https://github.com/djosh34/tigerstore/actions/runs/38429841266/job/115408816599) | tigerstore | watch | none | 200 | 1 | 3m27s | pass | 2660 |
| [38429841266](https://github.com/djosh34/tigerstore/actions/runs/38429841266/job/115408816691) | tigerstore | watch | partition | 200 | 1 | 3m43s | pass | 2203 |
| [38429841266](https://github.com/djosh34/tigerstore/actions/runs/38429841266/job/115408816522) | tigerstore | watch | pause | 200 | 1 | 3m32s | pass | 1523 |
| [38430056885](https://github.com/djosh34/tigerstore/actions/runs/38430056885/job/115409571923) | etcd | register | kill | 200 | 2 | 3m11s | pass | 1429 |
| [38430056885](https://github.com/djosh34/tigerstore/actions/runs/38430056885/job/115409571681) | etcd | register | kill | 200 | 3 | 3m50s | pass | 1656 |
| [38430056885](https://github.com/djosh34/tigerstore/actions/runs/38430056885/job/115409571574) | etcd | register | partition | 200 | 2 | 3m17s | pass | 2253 |
| [38430056885](https://github.com/djosh34/tigerstore/actions/runs/38430056885/job/115409571662) | etcd | register | partition | 200 | 3 | 3m16s | pass | 2188 |
| [38430056885](https://github.com/djosh34/tigerstore/actions/runs/38430056885/job/115409571644) | tigerstore | register | kill | 200 | 2 | 3m55s | inconclusive | 1888 |
| [38430056885](https://github.com/djosh34/tigerstore/actions/runs/38430056885/job/115409571090) | tigerstore | register | kill | 200 | 3 | 4m32s | inconclusive | 1774 |
| [38430056885](https://github.com/djosh34/tigerstore/actions/runs/38430056885/job/115409571578) | tigerstore | register | partition | 200 | 2 | 3m36s | inconclusive | 2140 |
| [38430056885](https://github.com/djosh34/tigerstore/actions/runs/38430056885/job/115409571712) | tigerstore | register | partition | 200 | 3 | 5m20s | inconclusive | 2159 |
| [38430224306](https://github.com/djosh34/tigerstore/actions/runs/38430224306/job/115410153952) | etcd | register | kill | 50 | 1 | 3m32s | pass | 1703 |
| [38430224306](https://github.com/djosh34/tigerstore/actions/runs/38430224306/job/115410154602) | etcd | register | kill | 50 | 2 | 3m47s | pass | 1675 |
| [38430224306](https://github.com/djosh34/tigerstore/actions/runs/38430224306/job/115410154424) | etcd | register | partition | 50 | 1 | 3m22s | pass | 2250 |
| [38430224306](https://github.com/djosh34/tigerstore/actions/runs/38430224306/job/115410154234) | etcd | register | partition | 50 | 2 | 3m21s | pass | 1555 |
| [38430904489](https://github.com/djosh34/tigerstore/actions/runs/38430904489/job/115412516354) | etcd | register | kill | 50 | 1 | 3m42s | pass | 1607 |
| [38430904489](https://github.com/djosh34/tigerstore/actions/runs/38430904489/job/115412515799) | etcd | register | none | 50 | 1 | 3m18s | pass | 2704 |
| [38430904489](https://github.com/djosh34/tigerstore/actions/runs/38430904489/job/115412516038) | etcd | register | partition | 50 | 1 | 3m31s | pass | 1744 |
| [38430904489](https://github.com/djosh34/tigerstore/actions/runs/38430904489/job/115412516259) | etcd | register | pause | 50 | 1 | 3m21s | pass | 1818 |
| [38430904489](https://github.com/djosh34/tigerstore/actions/runs/38430904489/job/115412516291) | etcd | set | kill | 200 | 1 | 3m17s | pass | 1793 |
| [38430904489](https://github.com/djosh34/tigerstore/actions/runs/38430904489/job/115412516257) | etcd | set | none | 200 | 1 | 3m41s | pass | 2698 |
| [38430904489](https://github.com/djosh34/tigerstore/actions/runs/38430904489/job/115412516185) | etcd | set | partition | 200 | 1 | 3m27s | pass | 1686 |
| [38430904489](https://github.com/djosh34/tigerstore/actions/runs/38430904489/job/115412516384) | etcd | set | pause | 200 | 1 | 3m30s | pass | 2093 |
| [38432262482](https://github.com/djosh34/tigerstore/actions/runs/38432262482/job/115417205014) | tigerstore | register | kill | 50 | 1 | 6m17s | inconclusive | 1653 |
| [38432262482](https://github.com/djosh34/tigerstore/actions/runs/38432262482/job/115417204859) | tigerstore | register | none | 50 | 1 | 3m30s | pass | 2670 |
| [38432262482](https://github.com/djosh34/tigerstore/actions/runs/38432262482/job/115417205091) | tigerstore | register | partition | 50 | 1 | 5m27s | inconclusive | 2348 |
| [38432262482](https://github.com/djosh34/tigerstore/actions/runs/38432262482/job/115417204358) | tigerstore | register | pause | 50 | 1 | 3m34s | pass | 2117 |
| [38432262482](https://github.com/djosh34/tigerstore/actions/runs/38432262482/job/115417204782) | tigerstore | set | kill | 200 | 1 | 3m28s | pass | 1620 |
| [38432262482](https://github.com/djosh34/tigerstore/actions/runs/38432262482/job/115417204775) | tigerstore | set | none | 200 | 1 | 5m52s | pass | 2749 |
| [38432262482](https://github.com/djosh34/tigerstore/actions/runs/38432262482/job/115417205022) | tigerstore | set | partition | 200 | 1 | 3m27s | pass | 1394 |
| [38432262482](https://github.com/djosh34/tigerstore/actions/runs/38432262482/job/115417204979) | tigerstore | set | pause | 200 | 1 | 3m55s | pass | 1868 |
| [38433448661](https://github.com/djosh34/tigerstore/actions/runs/38433448661/job/115421280132) | etcd | register | kill | 10 | 1 | 3m23s | pass | 1782 |
| [38433448661](https://github.com/djosh34/tigerstore/actions/runs/38433448661/job/115421279861) | etcd | register | kill | 10 | 2 | 3m21s | pass | 2065 |
| [38433448661](https://github.com/djosh34/tigerstore/actions/runs/38433448661/job/115421280347) | etcd | register | partition | 10 | 1 | 3m32s | pass | 2056 |
| [38433448661](https://github.com/djosh34/tigerstore/actions/runs/38433448661/job/115421280365) | etcd | register | partition | 10 | 2 | 3m33s | pass | 2132 |
| [38434478844](https://github.com/djosh34/tigerstore/actions/runs/38434478844/job/115424787334) | tigerstore | register | kill | 10 | 1 | 3m42s | pass | 2132 |
| [38434478844](https://github.com/djosh34/tigerstore/actions/runs/38434478844/job/115424787549) | tigerstore | register | kill | 10 | 2 | 3m35s | pass | 1906 |
| [38434478844](https://github.com/djosh34/tigerstore/actions/runs/38434478844/job/115424787410) | tigerstore | register | partition | 10 | 1 | 4m09s | pass | 2159 |
| [38434478844](https://github.com/djosh34/tigerstore/actions/runs/38434478844/job/115424787531) | tigerstore | register | partition | 10 | 2 | 4m11s | pass | 2327 |
