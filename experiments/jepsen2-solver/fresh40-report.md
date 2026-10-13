# fresh40 register certificate report

Run https://github.com/djosh34/tigerstore/actions/runs/38448351446 completed successfully at solver commit `a0d1b2c19aed096560050a2930b09cc8de149914`.
All 40 histories and 862 keys have SAT certificates accepted by both the independent Python verifier and the exact upstream Clojure model.
All 67,489 client calls remain in the decision records. The witnesses include 134 info operations. The runner generated 46 missing saved projections from complete full histories and checked them in both validators.

Parent input commit is `4b88f1226647df31f394f5453739b3345ad9e3ce`.
Input archive is `experiments/jepsen2/evidence/38440292743.tar.gz`.
Its enforced SHA-256 is `bdd0e6efba7c45fa22cc92b20be28ac1c853fef4f502bc7890af420717679857`.

| Product | Histories | Keys | Client calls | Included infos | Generated projections |
| --- | --- | --- | --- | --- | --- |
| etcd | 20 | 410 | 31,522 | 73 | 0 |
| tigerstore | 20 | 452 | 35,967 | 61 | 46 |

This is a register-model verdict only. Original checker results, crashes, missing online results, and fault-coverage failures remain separate evidence. A SAT certificate does not turn a failed fault-coverage check into a pass. No info was relabelled as fail, and no client history was truncated.

`fresh40-verdicts.json` contains per-key source and witness hashes, artifact URLs, and saved-versus-generated projection flags. Every archive/member hash is in `evidence/manifest.json` and its sidecars. The exact executed source snapshot is retained with the runner artifacts.

| Case ID | Original case | Validated keys | Artifact |
| --- | --- | --- | --- |
| 0 | jepsen2-etcd-kill,partition,pause-ops200-rep1 | 4 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38448351446/artifacts/11839147313) |
| 1 | jepsen2-etcd-kill,partition,pause-ops200-rep2 | 6 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38448351446/artifacts/11838783443) |
| 2 | jepsen2-etcd-kill,partition,pause-ops50-rep1 | 23 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38448351446/artifacts/11839722043) |
| 3 | jepsen2-etcd-kill,partition,pause-ops50-rep2 | 20 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38448351446/artifacts/11840215548) |
| 4 | jepsen2-etcd-kill,pause-ops200-rep1 | 9 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38448351446/artifacts/11838728458) |
| 5 | jepsen2-etcd-kill,pause-ops200-rep2 | 10 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38448351446/artifacts/11839980927) |
| 6 | jepsen2-etcd-kill,pause-ops50-rep1 | 39 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38448351446/artifacts/11839087775) |
| 7 | jepsen2-etcd-kill,pause-ops50-rep2 | 32 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38448351446/artifacts/11838972714) |
| 8 | jepsen2-etcd-kill-ops200-rep1 | 10 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38448351446/artifacts/11839702233) |
| 9 | jepsen2-etcd-kill-ops200-rep2 | 10 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38448351446/artifacts/11839995681) |
| 10 | jepsen2-etcd-kill-ops50-rep1 | 38 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38448351446/artifacts/11838788748) |
| 11 | jepsen2-etcd-kill-ops50-rep2 | 39 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38448351446/artifacts/11838788610) |
| 12 | jepsen2-etcd-partition,pause-ops200-rep1 | 7 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38448351446/artifacts/11839042430) |
| 13 | jepsen2-etcd-partition,pause-ops200-rep2 | 9 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38448351446/artifacts/11839795867) |
| 14 | jepsen2-etcd-partition,pause-ops50-rep1 | 35 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38448351446/artifacts/11840011001) |
| 15 | jepsen2-etcd-partition,pause-ops50-rep2 | 30 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38448351446/artifacts/11839677309) |
| 16 | jepsen2-etcd-partition-ops200-rep1 | 10 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38448351446/artifacts/11840065570) |
| 17 | jepsen2-etcd-partition-ops200-rep2 | 6 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38448351446/artifacts/11838583803) |
| 18 | jepsen2-etcd-partition-ops50-rep1 | 44 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38448351446/artifacts/11839881268) |
| 19 | jepsen2-etcd-partition-ops50-rep2 | 29 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38448351446/artifacts/11838783442) |
| 20 | jepsen2-tigerstore-kill,partition,pause-ops200-rep1 | 7 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38448351446/artifacts/11839387213) |
| 21 | jepsen2-tigerstore-kill,partition,pause-ops200-rep2 | 9 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38448351446/artifacts/11838822875) |
| 22 | jepsen2-tigerstore-kill,partition,pause-ops50-rep1 | 9 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38448351446/artifacts/11838703531) |
| 23 | jepsen2-tigerstore-kill,partition,pause-ops50-rep2 | 36 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38448351446/artifacts/11839886075) |
| 24 | jepsen2-tigerstore-kill,pause-ops200-rep1 | 5 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38448351446/artifacts/11840085425) |
| 25 | jepsen2-tigerstore-kill,pause-ops200-rep2 | 10 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38448351446/artifacts/11840250298) |
| 26 | jepsen2-tigerstore-kill,pause-ops50-rep1 | 32 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38448351446/artifacts/11839472612) |
| 27 | jepsen2-tigerstore-kill,pause-ops50-rep2 | 39 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38448351446/artifacts/11838947952) |
| 28 | jepsen2-tigerstore-kill-ops200-rep1 | 9 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38448351446/artifacts/11839022492) |
| 29 | jepsen2-tigerstore-kill-ops200-rep2 | 11 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38448351446/artifacts/11838778429) |
| 30 | jepsen2-tigerstore-kill-ops50-rep1 | 44 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38448351446/artifacts/11839112944) |
| 31 | jepsen2-tigerstore-kill-ops50-rep2 | 25 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38448351446/artifacts/11839810775) |
| 32 | jepsen2-tigerstore-partition,pause-ops200-rep1 | 10 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38448351446/artifacts/11839237619) |
| 33 | jepsen2-tigerstore-partition,pause-ops200-rep2 | 11 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38448351446/artifacts/11838673509) |
| 34 | jepsen2-tigerstore-partition,pause-ops50-rep1 | 33 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38448351446/artifacts/11840206138) |
| 35 | jepsen2-tigerstore-partition,pause-ops50-rep2 | 44 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38448351446/artifacts/11839462795) |
| 36 | jepsen2-tigerstore-partition-ops200-rep1 | 13 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38448351446/artifacts/11840105483) |
| 37 | jepsen2-tigerstore-partition-ops200-rep2 | 13 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38448351446/artifacts/11840170986) |
| 38 | jepsen2-tigerstore-partition-ops50-rep1 | 47 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38448351446/artifacts/11839692667) |
| 39 | jepsen2-tigerstore-partition-ops50-rep2 | 45 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38448351446/artifacts/11839931516) |

All numbers above come from `fresh40-verdicts.json`. Its SHA-256 is `025640c548f5270c455c41d712e2e5ca2d35d028f0d5cd2571b23010795af7d4`.
