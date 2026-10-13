# One fix check of PR 97

Reviewed the fixes to the four original findings at `6493ddca178ebe272b98c39f640b97ea16e94bcf`. No new review of main's merged changes or the folded PR 96.

All four findings are addressed. The server-stream error path preserves queued message bytes, internal chunk sizing matches etcd's header and count, the PR body calls the original TestCompact cause unresolved and cites the stock comparison, and response sizing happens before Send without later use of the log-backed response.

In a detached worktree of that head, copied only the two original review probe files into internal/server and ran:

```
REVIEW_ETCD=/path/to/stock-etcd-v3.7.2 \
  GOFLAGS=-p=2 GOMEMLIMIT=2GiB \
  go test ./internal/server -parallel 2 \
  -run '^(TestReview97.*|TestRangeStream.*)$' -count=3 -v
```

Passed, 28.248 seconds. Both servers now emit 10+20 at the boundary and return the compacted OutOfRange status after earlier chunks. Concurrent-write snapshot/count and cancellation probes also pass on both servers. The implementation's three RangeStream tests pass in all three repetitions.

Then ran scripts/difftest.sh against stock etcd and tigerstore sequentially, with GOFLAGS=-p=2 and GOMEMLIMIT=2GiB. The only script change in the detached test checkout was adding `-parallel 2` to its test arguments. Both suites passed, including the newly recorded RangeStream cases. Etcd test time was 3.960 seconds; tigerstore was 3.058 seconds. No golden files were recorded or changed. Output is retained in this directory.
