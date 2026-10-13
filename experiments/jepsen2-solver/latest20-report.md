# latest20 register certificate report

Run https://github.com/djosh34/tigerstore/actions/runs/38454147853 completed successfully at solver commit `12c8945aa3eb922f2017e74832b9d058b516b303`.
All 20 histories and 432 keys have SAT certificates accepted by both the independent Python verifier and the exact upstream Clojure model.
All 34,436 client calls remain in the decision records. The witnesses include 66 info operations. The runner generated 0 missing saved projections from complete full histories and checked them in both validators.

Original client outcomes are 23,626 ok, 5,489 fail, 5,321 info, and 0 unfinished. These counts were independently reconstructed from the complete retained operations and matched against the parent input totals.

The parent identified tested product commit `2b072ac7e7acb4aeef1a6e794145914201607b8c` for this input archive.

Parent input commit is `06e0f1c5ed072488cfd61eeecf840cbc455eb828`.
Input archive is `experiments/jepsen2/evidence/38448413416.tar.gz`.
Its enforced SHA-256 is `37118bff52903f9899bcf0ee3ccc760cb08b3f1f0159ca5d35e6750f33e40b78`.

| Product | Histories | Keys | Client calls | Included infos | Generated projections |
| --- | --- | --- | --- | --- | --- |
| tigerstore | 20 | 432 | 34,436 | 66 | 0 |

This is a register-model verdict only. Original checker results, crashes, missing online results, and fault-coverage failures remain separate evidence. A SAT certificate does not turn a failed fault-coverage check into a pass. No info was relabelled as fail, and no client history was truncated.

`latest20-verdicts.json` contains per-key source and witness hashes, artifact URLs, and saved-versus-generated projection flags. Every archive/member hash is in `evidence/manifest.json` and its sidecars. The exact executed source snapshot is retained with the runner artifacts.

| Case ID | Original case | Validated keys | Artifact |
| --- | --- | --- | --- |
| 0 | jepsen2-latest-tigerstore-kill,partition,pause-ops200-rep1 | 8 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38454147853/artifacts/11841368742) |
| 1 | jepsen2-latest-tigerstore-kill,partition,pause-ops200-rep2 | 8 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38454147853/artifacts/11840814155) |
| 2 | jepsen2-latest-tigerstore-kill,partition,pause-ops50-rep1 | 28 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38454147853/artifacts/11840979177) |
| 3 | jepsen2-latest-tigerstore-kill,partition,pause-ops50-rep2 | 28 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38454147853/artifacts/11840949139) |
| 4 | jepsen2-latest-tigerstore-kill,pause-ops200-rep1 | 8 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38454147853/artifacts/11842036716) |
| 5 | jepsen2-latest-tigerstore-kill,pause-ops200-rep2 | 7 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38454147853/artifacts/11841913241) |
| 6 | jepsen2-latest-tigerstore-kill,pause-ops50-rep1 | 22 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38454147853/artifacts/11841494033) |
| 7 | jepsen2-latest-tigerstore-kill,pause-ops50-rep2 | 34 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38454147853/artifacts/11842037045) |
| 8 | jepsen2-latest-tigerstore-kill-ops200-rep1 | 9 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38454147853/artifacts/11841348826) |
| 9 | jepsen2-latest-tigerstore-kill-ops200-rep2 | 9 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38454147853/artifacts/11841931903) |
| 10 | jepsen2-latest-tigerstore-kill-ops50-rep1 | 35 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38454147853/artifacts/11842047042) |
| 11 | jepsen2-latest-tigerstore-kill-ops50-rep2 | 33 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38454147853/artifacts/11841134158) |
| 12 | jepsen2-latest-tigerstore-partition,pause-ops200-rep1 | 8 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38454147853/artifacts/11840779157) |
| 13 | jepsen2-latest-tigerstore-partition,pause-ops200-rep2 | 12 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38454147853/artifacts/11841278967) |
| 14 | jepsen2-latest-tigerstore-partition,pause-ops50-rep1 | 31 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38454147853/artifacts/11841484057) |
| 15 | jepsen2-latest-tigerstore-partition,pause-ops50-rep2 | 34 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38454147853/artifacts/11841883819) |
| 16 | jepsen2-latest-tigerstore-partition-ops200-rep1 | 12 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38454147853/artifacts/11840579448) |
| 17 | jepsen2-latest-tigerstore-partition-ops200-rep2 | 11 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38454147853/artifacts/11840569498) |
| 18 | jepsen2-latest-tigerstore-partition-ops50-rep1 | 46 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38454147853/artifacts/11842037142) |
| 19 | jepsen2-latest-tigerstore-partition-ops50-rep2 | 49 | [certificate archive](https://github.com/djosh34/tigerstore/actions/runs/38454147853/artifacts/11841504325) |

All numbers above come from `latest20-verdicts.json`. Its SHA-256 is `8cc2fc02efe53230ee940889c61f789542a7ebad61a34419b348ff9ff224aab3`.
