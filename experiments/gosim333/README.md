# #333 B: translator experiment (Go 1.27.2, linux/arm64)

Inputs: tigerstore `b8e39bba6fe200efe7f1828f68146ca18337e591`, gosim
`ffd3a613542675755e4cbf8186b5edaf404ed95c`, prototype
`0aab36927729c6c717f2a784118d4dec490de237`.
Own worktree: `/tmp/r333-translator`; own gosim clone:
`/tmp/r333-gosim-translator`. No main changes, PR, issue comment, or subagents.

## Result

Translation **and compilation finish** for `internal/sim` and `internal/node`
with diagnostic patches; the real simulator still **does not run a seed**.
`TestSeeds`, `TestMembershipFaults`, cleaner simulation, and a one-trial B01a
`TestPlanted` wrapper all hit `gosim: missing hook internal/synctest.Run`.
See `results/final-TestSeeds.log`, `final-TestMembershipFaults.log`,
`final-cleaner.log`, `planted-run.log`; `final-compile.log` precedes the runs.
Do not replace Run with simply calling f: this experiment does not implement
bubble membership, durable blocking, or bubble clocks.

With small adaptations, upstream `TestGosim` and #291's real-driver/simfs
`TestDriverGate` **pass seeds 1,1,2** (`example6.log`, `probe4.log`). Each pair of
seed-1 JSON event lists is byte-identical; seed 2 differs (`summary.txt`). This
is only 2 example events / 6 probe events, **not proof of exact simulator replay**.
The probe has no TLS peers, power cuts, or synctest.Test.

## Complete static diagnostic lists

* Reproduced setup exit 0, tools exit 1, audit exit 1 (`setup.log`, `tools.log`,
  `audit.log`). Baseline audit: **80 missing bodies, 35 unknown linknames**,
  stops at jsontext.export exactly as #291.
* Added `PublicExportHacks["encoding/json/jsontext"]={"export"}`. No additional
  public export hacks were needed. Skipped base-file InitOrder entries absent
  from the same-package test AST. Translation then exits 0 (`translate4.log`).
* Completed audit: **90 missing bodies + 37 unknown linkname diagnostics**;
  full sorted lists `missing-bodies.txt`, `unknown-linknames.txt`.
* Fail-closed compilation additionally exposed **4 self-linkname declarations**
  silently rewritten by upstream: FIPS fatal/getIndicator/setIndicator and
  testing/synctest.testingSynctestTest (`self-linknames.txt`).
* **128 generated fail-closed bodies** at the first successful sim compilation:
  complete `compile-stub-manifest.txt`, `translate-stubs9.log`, `compile9.log`.
  Unknown no-body linknames are also stubbed rather than allowed into the host.
  Two unknown HTTP test-export directives have bodies, so only their directives
  are removed. crypto/internal/fips140hash.sha3Unwrap must NOT be stubbed: its
  implementation is translated crypto/sha3.fips140hash_sha3Unwrap. Stubbing it
  produces the duplicate symbol in `compile7.log`.
* Thus these diagnostics are a static **package/build-configuration inventory**,
  not a reachable-call proof or a list of 127/128 independent runtime failures.
  The nonempty intrinsic body below is missed by a missing-body scan.

## Compile errors and narrow workarounds

Logs `compile1.log` through `compile9.log`, `node-compile1.log`:

1. Missing reflect.TypeAssert, Value.Seq/Seq2, Type.CanSeq/CanSeq2. Added interface
   assertion and capability forwarding; reflected iteration stays loud stubs.
2. Named-map generics: maps.Copy(FuncMap), maps.Keys(MIMEHeader). Inserted
   ExtractMap conversions on maps APIs (not a general ~map constraint fix).
3. Local `G` in ML-KEM shadows generated globals getter: renamed getter.
4. New crypto/subtle DIT linknames were incorrectly treated as retained asm;
   now unknown linknames are always stubbed, including in kept-asm packages.
5. runtime/pprof host context vs translated context in gRPC: two loud wrappers.
6. Taking address of translated map literals: use generic AddressOf helper.
7. Same-package test globals' ShouldShare merge and references to renamed tests:
   merged metadata and rewrote test function references.
8. sha3 duplicate symbol, then FIPS unresolved relocations: preserve the known
   translated alias, stub self-linknames. (These were not 90-body diagnostics.)
9. node tests require testing.T.Context: loud stub, not reached by cleaner gate.
10. Own example's translated stdlib needed module go directive 1.27.2 instead
    of tooling patch's 1.26 (`example1.log`).

## Runtime observations, in discovery order

Each row is an observed panic, not a conjectured call:

| Log | First new blocker | Adaptation before following attempt |
| --- | --- | --- |
| seed2 | internal/runtime/atomic.LoadAcquintptr | delegate old gosim pool-load hook |
| seed3 | internal/runtime/atomic.StoreReluintptr | delegate old gosim pool-store hook |
| example2, seed4 | nil dereference in HashTrieMap.initSlow | use native map solely for ABI metadata, not rewritten map struct |
| seed5 | internal/sync.runtime_rand | gosimruntime.Fastrand64 |
| seed6 | constanttime.boolToUint8: unreachable; must be intrinsicified | explicit bool conversion hook |
| seed7, seed8 | protobuf unsupported edition: 998 | preserve shared immutable embedded Defaults bytes (narrow workaround) |
| seed10 | time.runtimeNow (gosim's network, before test) | delegate Time_now |
| example4, seed14 | time.runtimeIsBubbled | false while Run remains a failing stub |
| seed16 / final target logs | internal/synctest.Run | **not implemented** |
| probe1 | internal/synctest.IsInBubble (WaitGroup.Go) | false outside bubbles; Run still fails |
| probe2 | sync.runtime_SemacquireWaitGroup (shutdown) | existing simulated semaphore if !durable; durable case still fails loudly |
| probe4 | none; real put/read and shutdown pass | stop |

CPU getisar1/getpfr0, DIT asm, and crypto asm **did not fire** in these runs;
that does NOT establish they are unreachable after implementing synctest.Run.
No purego shortcut was needed to reach the gates above. No claimed safe-to-stub
set for full tigerstore simulation follows from this prefix. Native ABI hashing,
new reflection APIs, and other adaptations have not been validated for replay.

## Reproduce

All heavy commands are serialized by `/tmp/r333-heavy.lock`, GOMAXPROCS=2,
GOFLAGS=-p=2 (plus -mod=mod). No long seed campaign or Actions job was run.

Baseline (from repository root):

```sh
flock /tmp/r333-heavy.lock bash experiments/gosim291/run.sh setup
flock /tmp/r333-heavy.lock bash experiments/gosim291/run.sh tools
flock /tmp/r333-heavy.lock bash experiments/gosim291/run.sh audit
```

Clean checkout of this branch; remove/rename the existing own clone first:

```sh
bash experiments/gosim333/run.sh prepare
bash experiments/gosim333/run.sh translate
bash experiments/gosim333/run.sh compile
bash experiments/gosim333/run.sh example
bash experiments/gosim333/run.sh probe
bash experiments/gosim333/run.sh seed        # expected exit 1: Run
bash experiments/gosim333/run.sh membership  # expected exit 1: Run
bash experiments/gosim333/run.sh cleaner     # expected exit 1: Run
bash experiments/gosim333/run.sh planted     # expected exit 1: Run
```

Each patch is **cumulative from pinned gosim**, not sequential:
`translate-audit.patch` stops after completed translation;
`compile-stubs.patch` is first sim compilation checkpoint;
`runtime-explore.patch` is final exploration including planted tags support.
Use `PATCH=translate-audit.patch` or `PATCH=compile-stubs.patch` with prepare.
All generated missing bodies panic via `gosimruntime.MissingHook(name)`.

`commands.log` archives exact shell calls during exploration, including failed
attempts. Root module graph delta is `root-module.patch`: required gosim and
its #291 tooling patch raise x/crypto .55->.58, x/net .58->.61, x/sys .47->.49,
x/text .41->.43. Root production files are restored on this branch; prepare
recreates that experimental module graph. This is not a byte-identical main
module graph. No production Go code changes are needed for the gates above.

Some intermediate output files are empty/truncated because disk filled.
In particular seed11/12 used a stale binary, seed13 and seed15/probe3 failed
builds due disk space; do not treat them as progress. Only the later explicitly
successful builds/runs support the final claims. Removed only Go cache entries
containing my unique paths, then reran (`seed16`, `probe4`, `final-compile`).
The zero-exit `final-TestPlanted.log` selected nothing without the planted tag;
use `planted-run.log`, which really calls TestPlanted via the one-trial wrapper.
