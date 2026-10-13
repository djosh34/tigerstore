# Solver evidence

## First validated witnesses

Commit `29e1e7fcdc1d7c87acb3f805ef1d120a3c87aa8f` ran on GitHub Actions:
https://github.com/djosh34/tigerstore/actions/runs/38444408855

Cases 0, 2, and 3 passed every key. The overall run failed because the other five jobs stopped on unsupported EDN metadata sets/symbols before solving. These are tooling failures, not invalid histories. Commit `89a97a0487a5a904f6b27dc9d9e1b990590cabff` fixes metadata parsing and adds set/symbol regression and negative-verifier tests. Its all-eight recheck is:
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
