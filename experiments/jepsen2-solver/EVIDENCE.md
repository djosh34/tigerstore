# Solver evidence

## All eight original histories resolved

Run https://github.com/djosh34/tigerstore/actions/runs/38444996427 completed successfully at commit `89a97a0487a5a904f6b27dc9d9e1b990590cabff`. All 143 keys in all eight original OOM histories have SAT witnesses validated independently by Python and the exact upstream Clojure model. All 15,987 client calls remain in the per-operation decision records. Sixteen info mutations are included, and two linearize after their info responses. Failed calls retain upstream Knossos removal semantics. There was no history truncation or info-to-fail rewrite.

`old-verdicts.json` is the integration-ready machine summary. Its audit checks original archive/full/independent hashes, complete expected key coverage, both validator exits and output files, and JSON/EDN witness hashes. Per-case artifacts are:

| Case | History | Validated keys | Artifact |
| --- | --- | --- | --- |
| 0 | first kill | 9 | https://github.com/djosh34/tigerstore/actions/runs/38444996427/artifacts/11837183532 |
| 1 | first partition | 12 | https://github.com/djosh34/tigerstore/actions/runs/38444996427/artifacts/11838003304 |
| 2 | kill rep2 | 10 | https://github.com/djosh34/tigerstore/actions/runs/38444996427/artifacts/11838155844 |
| 3 | kill rep3 | 9 | https://github.com/djosh34/tigerstore/actions/runs/38444996427/artifacts/11838385703 |
| 4 | partition rep2 | 11 | https://github.com/djosh34/tigerstore/actions/runs/38444996427/artifacts/11837143521 |
| 5 | partition rep3 | 11 | https://github.com/djosh34/tigerstore/actions/runs/38444996427/artifacts/11838140808 |
| 6 | 50/key kill | 34 | https://github.com/djosh34/tigerstore/actions/runs/38444996427/artifacts/11837848359 |
| 7 | 50/key partition | 47 | https://github.com/djosh34/tigerstore/actions/runs/38444996427/artifacts/11837423725 |

The successful run also passed 19 fixed tests, 80 exhaustive differential cases, exact upstream replay of every SAT selftest certificate, and six invalid-witness rejection tests in both validators. None of these solves or JVMs ran on the host.

## First fresh unknown history

The supplied original artifact `38440292743/jepsen2-tigerstore-kill-ops200-rep2` is retained without changes in `inputs/first-fresh-38440292743-kill-ops200-rep2.tar.gz`. Its SHA-256 is `7651fd001ac6f9e899e13ff1cf7202f10baa9743e64f282e1a9e2d5a6aea1afa`. The adjacent manifest records every original member's hash. Only full/independent histories and original results are archived. Run https://github.com/djosh34/tigerstore/actions/runs/38445539009 completed successfully at commit `82989021aabe01bd06a9af26b291e04fe772ed8e`. It validated every key in the first fresh unknown history before the parent's complete 40-history archive. All 11 keys and 2,002 client calls passed both validators. One info mutation is included. `fresh-verdicts.json` records hashes and per-key decisions. Its retained artifact is https://github.com/djosh34/tigerstore/actions/runs/38445539009/artifacts/11838556178 .

## Permanent branch evidence

The `evidence` directory contains compressed complete runner artifacts and logs for the failed parser attempt, the successful eight-history run, and the first fresh success. Each archive includes the exact executed source/workflow snapshot and its Git blob/SHA-256 manifest. `evidence/manifest.json` records archive hashes, and each `*.members.json` records every retained member's SHA-256. These files do not depend on expiring Actions artifact retention. Repeated selftests are retained as executed, not reconstructed. Original full and independent histories, all certificates, both replay outputs, and unsuccessful parser logs remain available.

## First validated witnesses

Commit `29e1e7fcdc1d7c87acb3f805ef1d120a3c87aa8f` ran on GitHub Actions:
https://github.com/djosh34/tigerstore/actions/runs/38444408855

Cases 0, 2, and 3 passed every key. The overall run failed because the other five jobs stopped on unsupported EDN metadata sets/symbols before solving. These are tooling failures, not invalid histories. Commit `89a97a0487a5a904f6b27dc9d9e1b990590cabff` fixes metadata parsing and adds set/symbol regression and negative-verifier tests. Its successful all-eight recheck is:
https://github.com/djosh34/tigerstore/actions/runs/38444996427

| Case | History | Keys validated | Artifact |
| --- | --- | --- | --- |
| 0 | 38428716698 kill | 9/9 | https://github.com/djosh34/tigerstore/actions/runs/38444408855/artifacts/11837027688 |
| 2 | 38430056885 kill rep2 | 10/10 | https://github.com/djosh34/tigerstore/actions/runs/38444408855/artifacts/11837097786 |
| 3 | 38430056885 kill rep3 | 9/9 | https://github.com/djosh34/tigerstore/actions/runs/38444408855/artifacts/11837817530 |

Old first kill key 3 retained 200 client operations. Its validated witness includes 117 operations, omits 51 optional infos, and excludes 32 failures under upstream Knossos rules. Python checked 6,781 real-time edges. The final version is 60. Its JSON witness SHA-256 is `c51e31a5bd52575e1e132fc25257148eccff6666defc654b69bf29664cc70c5f`; its independently replayed EDN witness SHA-256 is `081a276874ebc6881ccfeb54456f3c168f0b3ca391b426d99aa495b3262a63db`. Local files are `evidence-first/case0/case/3/`.

Case 3/key 6 also retained 200 client operations. Its witness includes two info mutations, omits 29 optional infos, and excludes 30 upstream failures. Python checked 9,452 real-time edges. Its final version is 50. JSON witness SHA-256 is `8400de35176380442b628f1f39d312bcb1a457532dcd29bc3ec4123e4d797d3a`. Local files are `evidence-first/case3/case/6/`.

The first run passed 18 fixed tests and 80 exhaustive differential cases. Every SAT selftest witness passed the exact upstream Clojure replay. The second run adds metadata coverage and six tampered-witness rejection tests in both validators.

Every artifact contains complete original full and independent histories under `case/input`, per-operation decisions in `case/KEY/witness.json` and `witness.edn`, both independent validation outputs, solver attempts/statistics, and `case/summary.json`. A solve's own `solve.json` remains a SAT-candidate record with `valid: unknown`; only `result.json` and the case summary can state a validated pass.

No product changes, comments, or issues were made. All actual solves and JVMs ran on GitHub runners. A model-level register pass does not replace fault coverage or other checker components.

## Full fresh matrix

Run https://github.com/djosh34/tigerstore/actions/runs/38448351446 completed successfully. Its exact executed source/workflow commit is `a0d1b2c19aed096560050a2930b09cc8de149914`. Parent input commit is `4b88f1226647df31f394f5453739b3345ad9e3ce`, and the enforced source archive SHA-256 is `bdd0e6efba7c45fa22cc92b20be28ac1c853fef4f502bc7890af420717679857`.

`fresh40-report.md` is generated from the audited `fresh40-verdicts.json`. Its totals, product subtotals, case rows, and artifact links come directly from that JSON. `fresh40-product-summary.json` retains the derived product totals. The original fault-coverage failure remains separate and does not become a pass through these register certificates.

`evidence/fresh40-success-38448351446.tar.gz` permanently retains all runner inputs, certificates, verifier outputs, exact executed source snapshots, regression/differential/rejection tests, and complete logs. Its archive/member hashes are in `evidence/manifest.json` and the matching sidecar. Older permanent archives remain unchanged.

## Source provenance clarification

`MODEL_PROVENANCE.md` and `upstream-source-verification.json` retain the independent comparison with original GitHub blobs. The supplied register source differs from the whole original upstream file only in the existing WGL/concurrency checker wrapper. Its exact `VersionedRegister` record is byte-identical to both original upstream and the Clojure replay. The parent independently confirmed the same record hash.

Legacy `model-source.json` outputs called the supplied checker-configured file hash `upstream_source_sha256`. These original outputs remain unchanged. Later runs record original source, copied checker-source, and model-record hashes separately. This is a provenance-label correction, not a model or certificate change. The original pinned source is retained as `sources/pinned_register.clj`.
