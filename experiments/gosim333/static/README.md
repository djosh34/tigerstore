# Static inventory of what gosim lacks on Go 1.27.2 (#333)

`enumerate.go` parses every file that `go list -tags=sim -deps -test ./internal/sim ./internal/node`
selects for linux/arm64 or linux/amd64 (CGO off), records every bodyless function and every
`//go:linkname`, and subtracts what gosim ffd3a613 already handles (its hook tables, skipped
packages, keep-asm allowlist, linkname exceptions). It also checks the same names in Go 1.23.12.
`run.sh` gives the commands; the paths in it are the helper's local paths.

- `missing-{arm64,amd64}.tsv`: the difference, with file:line and signature for each entry.
- `missing-grouped-*.txt`: the same names, one line per package.
- `inventory-*.tsv`: everything found, including rows the subtraction treated as handled.
- `summary.txt`: counts and the comparison with #291's `repro-audit.log`.
- `REPORT.md`: helper A's report.

Not covered by the subtraction: bodyless functions whose own linkname names them and whose body
the untranslated `runtime` or `testing` package supplies (`crypto/internal/fips140.{fatal,getIndicator,setIndicator}`,
`testing/synctest.testingSynctestTest`). The translator accepts these, and they fail at link time.
