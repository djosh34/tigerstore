# #333 part C — tigerstore reachability / TLS / harness

Source keys used below:

* **T**: tigerstore `b8e39bba6fe200efe7f1828f68146ca18337e591`.
* **G**: gosim `ffd3a613542675755e4cbf8186b5edaf404ed95c`.
* **Go**: installed Go `go1.27.2`, `/home/joshazimullah.linux/.local/opt/go1.27.2/src`.
* **R**: raft module `go.etcd.io/raft/v3@v3.7.0`, pinned by T `go.mod:21`.

No gosim seed was run. These are source findings and native-source call-graph experiments, **not evidence that the unported translator works**. Worktree: `/tmp/r333-c` (detached at T). No GitHub write, PR, merge, or main checkout.

## Executive findings

1. Keeping the existing simulated disk and custom connections in **one gosim machine per independent run** is a good fit (inference). The process/node crash model is already implemented by contexts, connection resets and `simfs.Crash`, not OS process death. Gosim must provide deterministic goroutines, synchronization, maps/selects, clock/timers and entropy; neither an extra fake network nor gosim machine-crash support is necessary for these scenarios. Sources: T `internal/sim/sim.go:332-344,1062-1081,1381-1426,1611-1621`; G `docs/design.md:21-46,57-79,83-110`.
2. **Scheduler and time alone are insufficient for exact replay**: Go 1.27 crypto's implicit RNG must be controlled; TLS now reaches weak pointers/cleanup; and the cleaner test currently reads native GC/memory metrics. Sources detailed below.
3. The 115-symbol audit is not the complete port inventory. Our graph additionally reaches FIPS `getIndicator`/`setIndicator` and the moved P-256 assembly, which the old partial audit did not report. JSON's `export` failure is a **translation/type-export barrier**, not a missing callable hook. Sources: Go `crypto/internal/fips140/indicator.go:9-23,52-61`; `crypto/internal/fips140/ecdsa/ecdsa.go:164,201`; `encoding/json/jsontext/export.go:31-58`; experiment `callgraph/results.md`.
4. A static list of declarations requiring bodies/linkname decisions is possible; a static **exact dynamically exercised set** is not established by RTA/VTA. Native-runtime internals, FIPS branches, reflection/unsafe, error-only branches and replaced packages make a raw graph a conservative candidate list, not a port specification. The tool and complete per-audit-symbol paths/statuses are included.

## Actual harness surface

* `TestSeeds`, membership and planted sim cases enter through `runWith -> synctest.Test -> runInspect`; the cleaner has its own `synctest.Test`. There is **no explicit `synctest.Wait`** in either requested harness. Commands: `rg -n 'synctest.Wait' internal/sim internal/node/clean_sim_test.go` (no matches). Sources: T `internal/sim/sim_test.go:46-70,225-283`; `membership_test.go:12-45`; `planted_test.go:202-245`; `internal/node/clean_sim_test.go:178-191`.
* Synchronization directly in sim/peer/node: Mutex, RWMutex, WaitGroup Add/Done/Wait/**Go**, sync.Map (test failure bookkeeping), atomic typed integers/bools/pointers/Value; context cancellation and deadline timers; channel close/wake and selects. `Once`/`Cond` also appear in the conservative dependency graph (not directly in sim/peer/simfs production). Sources: T `internal/sim/sim.go:366-379,688-707,819-900,1077-1081`; `sim_test.go:43-68`; `internal/peer/peer.go:221-242,839-843`; `internal/node/node.go:377-420,959-1030`; Go `sync/waitgroup.go:77-110,175-241`; experiment `callgraph/results.md`.
* Time includes Now/Since/Until, Sleep, After, NewTimer/Stop and node/cleaner tickers; TLS uses handshake deadlines and context cancellation. Sources: T `internal/sim/sim.go:633-652,750,819-830`; `internal/sim/net.go:318-338`; `internal/node/node.go:1025-1027,1072`; `internal/node/clean.go:129-130,537-538`; `internal/peer/peer.go:1083-1110`.
* Network is **custom**, not `net.Pipe`: paired `conn` objects and chunk queues with seeded delivery times, channels and a shared Mutex; injected Listener/Dial carry real TLS. Normal OS sockets/DNS are not needed by the configured sim path. The default transport dial and optional SAN/CRL checks remain compiled but are not selected. Sources: T `internal/sim/net.go:72-145,244-269,285-398`; `internal/sim/sim.go:1062-1070`; `internal/peer/peer.go:263-280,650-659,1043-1052`; `internal/peer/cert.go:67-121`.
* `runtime.Gosched` is deliberate: 1–3 yields before file/network operations; cleaner similarly yields from a seeded call counter. It needs real gosim Yield semantics, not a no-op. No actual parallel CPUs are required by this design (inference from shared-lock/channel interleaving). Sources: T `internal/sim/disk.go:15-39`; `internal/sim/net.go:287-289,341-344`; `internal/node/clean_sim_test.go:26-46`; G `internal/translate/main.go:89-93`; `docs/design.md:57-65`.
* The real node/core, protobuf request/response machinery and raft run; gRPC and HTTP serving do not. Dependency imports still pull in more code. Reflection/unsafe are used by dependencies; they are not direct production imports in sim, peer or simfs. Protobuf reflection is explicitly used by the core. Sources: T `internal/sim/sim.go:1-18,32-40`; `internal/core/node.go:13-33`; `internal/core/alias.go:11,35-67`; command `rg -n 'unsafe|reflect' internal/{sim,peer,simfs} --glob '!**/*test.go'` (no matches).
* `osfs`/mmap **are compiled dependencies**, not build-tagged away. `NewOnFS` and cleaner's `newOnDisk(...,"",disk)` disable the mapping branch at runtime. The nonconstant `l.dir != ""` condition means a static graph can retain mmap anyway. `saveRun` additionally uses real-looking `os.MkdirAll/WriteFile` only if failure output is enabled. Sources: T `internal/node/node.go:35,480-484,528-549,604-632`; `internal/node/clean_sim_test.go:205`; `internal/sim/sim_test.go:87-90,104-106,171-180`.
* `runtime/debug` is compiled through memory helpers/dependencies; the requested graph reaches `runtime/debug.Stack` but **not** audit `runtime_setCrashFD`. Native memory-limit configuration is not the configured sim Memory function. Sources: T `internal/node/memory.go:10,20-29`; `internal/sim/sim.go:955-960`; experiment `callgraph/results.md`.

## TLS, crypto, and Go 1.27

* Sim creates one ECDSA **P-256 CA** and one P-256 certificate shared by all nodes, serials 1 and 2, DNS SANs `node1,node2,node3,member,updated-member`; validity is virtual `time.Now()-1h` through `+24h`. It uses `crypto/rand.Reader`, X.509 signing/parsing, PEM and EC-private-key marshal. Sources: T `internal/sim/net.go:33-44`; `internal/testcert/testcert.go:29-55,65-93`.
* Both peer TLS configs require **TLS >= 1.3**; mutual client certificate verification is enabled; no explicit cipher suites, CurvePreferences, Rand or Time overrides are set. Therefore the port must support Go's TLS-1.3 defaults, not merely P-256 certificate generation. Default TLS-1.3 suites include AES-128-GCM/SHA256, AES-256-GCM/SHA384 and ChaCha20-Poly1305/SHA256. Default key exchanges include X25519/ML-KEM hybrids (not only P-256). The exact negotiated suite is configuration/CPU dependent, **not measured here**. Sources: T `internal/peer/peer.go:263-271,1052-1063`; Go `crypto/tls/cipher_suites.go:213-217`; `crypto/tls/defaults.go:23-42,98-117`; `crypto/tls/common.go:1179-1193`.
* Raft election jitter also uses **crypto/rand** in this pinned raft version: `lockedRand.Intn -> rand.Int(rand.Reader,...)`, then `resetRandomizedElectionTimeout`. Node boot IDs additionally use package-global `math/rand/v2.Uint64`. Thus controlling only sim's explicit seeded PCG is insufficient. Sources: R `raft.go:19,90-104,2053-2054`; T `internal/node/node.go:713`; `internal/sim/sim.go:579-590`.
* Gosim's design translates the stdlib and keeps selected crypto assembly; it does not replace TLS with a fake protocol. Existing `keepAsmPackagesGo123` covers old crypto paths and `crypto/rand` ultimately uses simulated GetRandom: `InternalSyscallUnix_GetRandom -> SysGetrandom -> gosimruntime.Fastrand64`. Sources: G `docs/design.md:166-197`; `internal/translate/main.go:59-75`; `internal/hooks/go123/internal_syscall_unix.go:11-13`; `internal/simulation/os_linux.go:1689-1710`.
* Go 1.27 moved algorithms to `crypto/internal/fips140/...`: ordinary crypto still calls those implementations when FIPS mode is off. P-256, SHA256/512/3, AES/GCM, subtle XOR and bigmod paths need either retained compatible assembly plus correct rewritten references, or `purego` selection. **purego removes 19 bodyless audit candidates in this experiment**: 15 assembly symbols disappear, and four bigmod/XOR symbols acquire Go bodies (45 projected bodyless audit candidates -> 26). It does not fix runtime/weak/JSON hooks. Sources: experiment commands/results below; Go `crypto/internal/fips140/bigmod/nat_noasm.go:5-20`; `crypto/internal/fips140/indicator.go:52-61`.
* `crypto/internal/fips140.getIndicator/setIndicator` are **not optional even with FIPS off**: RecordApproved/RecordNonApproved call them without an Enabled guard, and ECDSA/SHA256/MLKEM invoke those functions. Preserve per-simulated-goroutine indicator state, or deliberately replace the indicator API as a documented non-FIPS simulation policy. A fail-loud stub on those reached paths cannot run TLS. Sources: Go `crypto/internal/fips140/indicator.go:9-23,52-61`; `crypto/internal/fips140/ecdsa/ecdsa.go:164,201`; `crypto/internal/fips140/sha256/sha256.go:201`; `crypto/internal/fips140/mlkem/mlkem768.go:182`.
* Conversely, with an **explicit FIPS-disabled contract**, bypass setters/getter and entropy-jitter monoTime can remain fail-loud unreachable stubs: bypass is guarded by Enabled/Enforced; DRBG uses sysrand directly outside FIPS. RTA/VTA retain those guarded paths. FIPS self-test fatal is likewise error-only. Sources: Go `crypto/fips140/enforcement.go:21-37`; `crypto/internal/fips140/drbg/rand.go:30-45`; `crypto/internal/fips140/cast.go:37-50`. Supporting real FIPS would require more; do not describe a deterministic test RNG as cryptographically secure/FIPS compliant.
* Go 1.27's ECDSA APIs can ignore the supplied Reader. Merely replacing public `crypto/rand.Reader` is therefore **not a complete deterministic-crypto fix**. The runtime's new supported testing API is useful: `testing/cryptotest.SetGlobalRandom` covers public and implicit crypto RNGs through `crypto/internal/rand.SetTestingReader -> drbg.SetTestingReader`. Port its linknames/testing compatibility or hook that internal boundary. This API is process-global, disallows parallel tests, and has a v1.0.0 module restriction. Sources: Go `crypto/ecdsa/ecdsa.go:340-355`; `crypto/internal/rand/rand.go:40-72`; `crypto/internal/fips140/drbg/rand.go:33-45,81-89`; `testing/cryptotest/rand.go:22-63`. This is a proposed solution, **not tested under gosim**.
* TLS now directly uses `weak.Pointer` and `runtime.AddCleanup` in `weakCertCache.newCert`, so weak is **not only a unique/netip false positive**. Implement deterministic weak/cleanup policy (e.g. a strong non-evicting simulation cache) or port these runtime semantics; retaining host GC-driven cleanup is not exact scheduling replay. Sources: Go `crypto/tls/cache.go:9-44`; G only replaces old `SetFinalizer` (`internal/translate/main.go:89-93`) and that implementation is a no-op (`gosimruntime/runtime.go:937-942`).

## Harness changes and replay contract

* Replace/remove the **outer synctest.Test** for gosim builds, retaining test callback execution and goroutine-leak/termination checking; there is no need to reimplement synctest.Wait. Let ordinary WaitGroup blocking use a gosim semaphore. A false `internal/synctest.IsInBubble` is coherent with this mode; Associate/Disassociate/IsAssociated become unreachable because no bubble flag is set. Sources: Go `sync/waitgroup.go:86-108,138-145,188-225`; T `internal/sim/sim_test.go:55-60`; `internal/node/clean_sim_test.go:181-185`.
* The old durable-block restriction is **not a required semantics to reproduce**: the sim deliberately avoids timed sleeps while holding production read locks because synctest will not advance time through mutex contention (`sim.go:951-955`). Gosim advances when all goroutines are blocked, including semaphore waits (G `docs/design.md:83-110`). Keep these conservative non-sleeping yields; do not layer native synctest on the gosim scheduler.
* Port old synchronization hooks to new `internal/sync`, add WaitGroup semaphore signature, time runtimeNow/runtimeIsBubbled/runtimeNano, the two newly direct runtime-atomic acquire/release operations from `sync.Pool`, and hashing randomness used by `internal/sync.HashTrieMap`. `runtime_canSpin=false` makes `runtime_doSpin` a valid fail-loud stub. Fatal/throw should terminate through one diagnostic hook; do not silently return. Sources: Go `sync/pool.go:163,211-244`; G `internal/hooks/go123/sync.go:7-67`; experiment paths in `callgraph/results.md`.
* **Cleaner replay needs an extra harness patch**: unlike `Sim.nodeConfig`, concurrentCleaning leaves Config.Memory nil while setting Core.MemoryBytes. `newOnDisk` installs a native runtime/metrics memory gauge; `measureMemory` uses it. Gosim explicitly leaves `runtime/metrics` native. Supply the same deterministic/no-op Memory function as sim, or explicitly model these metrics, otherwise GC timing can alter cache behavior. Sources: T `internal/node/clean_sim_test.go:192-211`; `internal/node/node.go:630-632,1233-1239`; `internal/node/memory.go:106-120`; `internal/sim/sim.go:957-960`; G `internal/translate/main.go:40-42`.
* Exact replay must record **both workload seed and gosim scheduler/entropy seed**, build/toolchain/CPU-feature configuration, test filter and run prefix. Existing replayCommand only prints workload flags and native go test; it must change. Gosim initializes a machine per top-level test, but TestSeeds iterates multiple subtests, so replaying only seed N after skipping earlier seeds does not automatically reproduce the scheduler/RNG state. Inference: use a fresh one-machine simulation per run, or replay the full prefix; do not promise standalone seed replay otherwise. Sources: T `internal/sim/sim_test.go:181-214,225-242`; G `docs/design.md:346-356`; `gosimruntime/runtime.go:124-134,268-284`.
* Logical outcomes, history, disk contents, network bytes and scheduling can replay with those conditions; **pointer addresses do not**. Map iteration and select choice are explicitly deterministic-randomized by gosim, not made sorted/fair; no stronger fairness claim follows. Pools need deterministic pin/cleanup policy, not host GC eviction. `maphash` seeds alone are insufficient if native runtime.memhash is retained: Go's hasher also uses process-random hashkey/AES keys; hash-dependent externally visible order requires deterministic hashing or a proven-irrelevant cache boundary. Sources: G `docs/design.md:57-79`; `gosimruntime/map.go:17,356`; `gosimruntime/chan.go:490-522`; `internal/hooks/go123/sync.go:27-39`; `internal/translate/hooks_go123.go:210`; Go `hash/maphash/maphash.go:318-331,350-365`; `internal/runtime/maps/runtime_alg.go:22-32,66-69`. No direct simulator maphash call was found; this is an audit requirement for dependencies, not a demonstrated tigerstore failure.
* Checkers currently run **outside the synctest bubble**, hence their CPU-search deadlines use real time. Translating the entire test moves their time.Now/timeout clocks into gosim; a CPU-bound runnable search need not advance virtual time. Inference: keep offline checking outside the machine or add a deterministic work/step bound and an external host watchdog. Sources: T `internal/sim/sim_test.go:60,349-385`; `check/witness.go:67-74,203-207`; G `docs/design.md:83-87`.
* The pinned translator hardcodes only the `sim` (plus optional race) build tag, so supporting **planted** and a purego path requires explicit tag plumbing. Sources: G `internal/translate/main.go:128-136`; T `internal/sim/planted_test.go:1`, `internal/planted/on.go:1`.

## Static experiment: what the lists do and do not establish

`callgraph/main.go` uses x/tools v0.52.0 SSA/RTA, optionally VTA refinement. It roots exactly sim.TestSeeds, sim.TestMembershipFaults, sim.TestPlanted and node.TestConcurrentCleanerSimulation, plus their package dependency initialization. It models direct testing Run/Cleanup callbacks and the opaque synctest.Test callback; Go runtime's linknamed Run has no SSA body. It excludes node.TestPlanted as a root (that is a separate planted suite).

Gosim does not wholesale replace sync/time/TLS: most packages are translated, runtime/errors/strconv/math/math-big/unique/runtime-metrics/etc are skipped, reflect and internal/reflectlite become its reflect wrapper, testing becomes its testing implementation, and net is translated under its renamed path. See G `internal/translate/main.go:27-56,361-385`. In particular native runtime-init paths to most runtime/atomic symbols are not simulator requirements; unique's netip path is cut, but the independent TLS weak-cache path survives.

The source graph's `Path` is an example candidate path, not proof of execution. `FrontierPath` projects through gosim's skipped/swapped package list and old hook boundaries, with modeled testing callbacks. `OnlyViaNativeOrReplaced` means the candidate was not reached by that projected traversal, **not a proof it can be stubbed**. It also includes cuts at existing hooks. The projection does not analyze replacement implementations or translation output. Table/paths for **every one of the 115 audit symbols**, additional bodyless hooks, and JSON names are in `callgraph/results.md`.

Measured linux/arm64, CGO_ENABLED=0:

| Analysis | Audit symbols reachable | Projected audit candidates | Bodyless projected candidates |
|---|---:|---:|---:|
| RTA, planted+sim | 94 | 46 | 46 |
| VTA-refined, planted+sim | 86 | 45 | 45 |
| VTA-refined, planted+sim+purego | 71 | 30 | 26 |

These numbers **must not be advertised as an exact minimum hook count**. Example false positives: a generic io.Writer edge connects fmt.Fprintf to tls.Conn.Write; a Pool.New field conflates unrelated pool constructors; the graph does not evaluate FIPS Enabled or mutex spinning guards. VTA's own contract is sound only modulo reflection/unsafe and a sound initial graph; RTA adds conservative reflection/address-taken targets. Sources: x/tools v0.52.0 `go/callgraph/rta/rta.go:14-40,174-211`; `go/callgraph/vta/vta.go:67-78`.

Confirmed notable native-source paths independent of those graph imprecisions:

* sim.runWith -> synctest.Test -> internal/synctest.Run.
* sim.schedule -> WaitGroup.Go/Add -> IsInBubble/associate; Sim.run/shutdown -> WaitGroup.Wait -> runtime_SemacquireWaitGroup.
* Sim.newSim -> time.Now -> runtimeNow; node/transport timing -> Since -> runtimeIsBubbled.
* fmt/TLS record writing -> sync.Pool -> runtime/atomic.LoadAcquintptr,StoreReluintptr.
* sync.Map -> internal/sync.HashTrieMap.initSlow -> runtime_rand.
* peer.sender.handshake -> TLS-1.3 verifyServerCertificate -> weakCertCache.newCert -> weak Make/Value and runtime.AddCleanup.
* testcert.NewCA -> ECDSA GenerateKey -> FIPS implementation -> service indicator and P-256 code.
* run failure -> explain/saveRun -> json.MarshalIndent -> JSON/jsontext internals.

Sources: T and Go locations above; full per-function paths are in results.md.

For the FIPS-disabled, no-synctest-bubble adapter, plausible fail-loud stubs include synctest.Wait/acquire/release/inBubble/association functions, internal/cpu probes not reached by the selected feature-init policy, FIPS bypass/entropy-jitter/self-test-only hooks, runtime_setCrashFD/runtimeClearenv and unsupported optional SAN/CRL/default-OS-network operations. **Conditional recommendation**, not a safe list proven by a native graph. IsInBubble=false, canSpin=false and a working fatal hook are real implemented policy, not fail-loud stubs. Crypto weak/indicator, scheduler/clock and runtime-atomic pool operations are not eligible merely because old package names were skipped.

Implementation recommendation (not implemented here): generate typed stubs at every otherwise missing declaration/linkname, each calling one `MissingHook(package,symbol)` abort routine. Emit a machine-readable manifest with reason, signature, GOOS/GOARCH/tags and graph classification before type-export/translation exits; fail the audit on newly unclassified symbols. Keep build-time barriers (JSON export, changed APIs/types/ABI/asm) separate. Validate observed hooks by running fixed seeds twice and comparing scheduler checksum plus canonical history/disk/trace, not raw pointer-bearing output. Sources for the relevant translator control points: G `internal/translate/rewrites.go:125-178,204-241`; `main.go:78-82`; design `docs/design.md:74-79`.

## Exact commands and output

Preparation:

```sh
git -C /home/joshazimullah.linux/work_mounts/agent_tools/paseo_host/state/worktrees/1fubxbmj/research-333 worktree add /tmp/r333-c origin/main --detach
gh issue view 333 -R djosh34/tigerstore
cd /tmp/r333-c
git rev-parse origin/main
# b8e39bba6fe200efe7f1828f68146ca18337e591
git show origin/exp/291-gosim:experiments/gosim291/results/repro-audit.log > /tmp/r333-c-audit.log
git clone https://github.com/jellevandenhooff/gosim /tmp/r333-gosim-c
git -C /tmp/r333-gosim-c checkout ffd3a613542675755e4cbf8186b5edaf404ed95c
go env GOROOT GOARCH GOOS CGO_ENABLED
# /home/joshazimullah.linux/.local/opt/go1.27.2 ; arm64 ; linux ; 1
```

Tool build:

```sh
cd /tmp/r333-c/experiments/gosim333/callgraph
gofmt -w main.go
GOFLAGS=-p=2 GOMAXPROCS=2 flock /tmp/r333-heavy.lock go mod tidy
GOFLAGS=-p=2 GOMAXPROCS=2 flock /tmp/r333-heavy.lock go build -o /tmp/r333-c-callgraph .
```

Final experiments (same executable, matching gosim's no-cgo package load):

```sh
GOFLAGS=-p=2 GOMAXPROCS=2 CGO_ENABLED=0 flock /tmp/r333-heavy.lock /tmp/r333-c-callgraph -tags planted,sim -hooks /tmp/r333-gosim-c/internal/translate/hooks_go123.go -dir /tmp/r333-c -audit /tmp/r333-c-audit.log -out /tmp/r333-c/experiments/gosim333/callgraph/rta.json > /tmp/r333-c-rta.log 2>&1
GOFLAGS=-p=2 GOMAXPROCS=2 CGO_ENABLED=0 flock /tmp/r333-heavy.lock /tmp/r333-c-callgraph -vta -tags planted,sim -hooks /tmp/r333-gosim-c/internal/translate/hooks_go123.go -dir /tmp/r333-c -audit /tmp/r333-c-audit.log -out /tmp/r333-c/experiments/gosim333/callgraph/vta.json > /tmp/r333-c-vta.log 2>&1
GOFLAGS=-p=2 GOMAXPROCS=2 CGO_ENABLED=0 flock /tmp/r333-heavy.lock /tmp/r333-c-callgraph -vta -tags planted,sim,purego -hooks /tmp/r333-gosim-c/internal/translate/hooks_go123.go -dir /tmp/r333-c -audit /tmp/r333-c-audit.log -out /tmp/r333-c/experiments/gosim333/callgraph/purego.json > /tmp/r333-c-purego.log 2>&1
python3 summarize.py /tmp/r333-c-audit.log
```

Earlier exploratory invocations used the same tool with default tags `planted`, with/without `-vta`, and without CGO_ENABLED=0; their counts are deliberately not used as the final results. No Go 1.23 comparison or full gosim translation/test was run. Cleanup command: `git -C /home/joshazimullah.linux/work_mounts/agent_tools/paseo_host/state/worktrees/1fubxbmj/research-333 worktree remove --force /tmp/r333-c`.
