# #277 diagnostic fixtures (reference only)

These are saved histories, not regenerated simulator seeds. Every retained
request, response and timestamp is unchanged; effects and watches are removed.

- `8143538`: run [39170367649](https://github.com/djosh34/tigerstore/actions/runs/39170367649),
  artifact `sim-campaign-8`, `sim-out/TestSeeds_8143538/history.json`.
  Retain ops 54,55,56,63,66,67,70,71,72,75,76,81,82,84 and log `[1:13]`
  (14 ops). This is the earlier diag reduction from `975f6509`.
- `8200093`: run [39185668816](https://github.com/djosh34/tigerstore/actions/runs/39185668816),
  artifact `sim-campaign-0`, `sim-out/TestSeeds_8200093/history.json`.
  Retain 46 ops, including original successful read 2800 at header 28,
  preceding grant 2797, ambiguous compaction 1705 and log `[:42]`.
- `8200334`: same run and artifact, `sim-out/TestSeeds_8200334/history.json`.
  Retain 217 ops, including original successful read 2241 at header 140,
  preceding grant 2239, ambiguous compaction 2066, ambiguous grant 2207
  (its assigned ID exists only in the log), and log `[:213]`.

For the latter two, retain all non-range successful requests with header no
higher than the target read's; ambiguous mutations whose requests occur in
the log prefix and whose calls precede the next writer's call; the ambiguous
compaction above the target read; compacted rejections returning before the
target read's call; and the target read. `8200334` additionally needs ambiguous
grant 2207 to account for the log's allocated lease ID.

Run `go test -v -count=1 ./check -run '^TestDiag277'` **on Actions**, not the host.
The temporary workflow checks both witnesses on all 29 original histories
(28 from the new campaign's failed-shard artifacts plus 8143538), and the
reductions against Porcupine as well. No simulator replay or campaign is needed.
