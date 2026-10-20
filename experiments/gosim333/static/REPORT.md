# A — gosim internals and static hook inventory

## Scope and evidence

Worktree used: `/tmp/r333-internals`, detached at tigerstore `b8e39bba6fe200efe7f1828f68146ca18337e591`; removed after saving these artifacts. Gosim inspected at `ffd3a613542675755e4cbf8186b5edaf404ed95c` in `/tmp/r333-gosim-internals`. `G:` citations below mean files at that gosim commit; `Go:` means the local Go `go1.27.2` source tree. Go 1.23.12 source was downloaded from go.dev. No tests or translations were executed by this helper, and no runtime reachability/replay claim follows from this inventory.

Artifacts in this directory:
- `enumerate.go`: self-contained standard-library-only AST inventory program.
- `deps-{arm64,amd64}.json`: exact `go list -tags=sim -deps -test -json ./internal/sim ./internal/node` output (CGO disabled, matching gosim's loader).
- `inventory-{arm64,amd64}.tsv`: every selected bodyless function and every linkname, including skipped packages, dispositions, source file:line, and declaration signatures; also Go 1.23.12 stdlib comparison at the same package paths.
- `missing-{arm64,amd64}.tsv`: complete declaration/directive difference after gosim tables; every entry has its primary-source file:line. `missing-grouped-*.txt` gives the same names compactly.
- `summary.txt`: exact counts and difference against #291's checked-in audit.
- `run.sh`, `summarize.py`: reproduction commands and postprocessing.

## How the standard library is handled

This is **AST translation plus a simulated runtime/OS**, not replacement of all stdlib packages with mocks, and not a custom compiler. Channels, maps, goroutines, selects and per-machine globals are rewritten, then compiled normally. Scheduling is cooperative, one simulated goroutine at a time; seeded choices and simulated time provide the deterministic substrate. Pointer allocation is explicitly outside the determinism guarantee. [G: `docs/design.md:19-43,54-77,113-170,202-245`; `internal/translate/translate.go:529-618`; `internal/translate/go.go:13-55`]

Actual skip boundaries (not a blanket promise that all stdlib is translated): `runtime`, `errors`, `reflect`, `strconv`, `embed`, `math`, `math/big`, `unsafe`, `runtime/{coverage,metrics,pprof}`, `vendor/golang.org/x/sys/cpu`, `unique`, `testing`, `testing/internal/testdeps`, `internal/reflectlite`, and gosimruntime. `collectImports` stops traversing at these packages. `reflect` and `internal/reflectlite` imports are redirected to gosim's reflect wrapper; `testing` to gosim's testing wrapper; normal translated packages are placed under `translated/`, with special-package path escaping. `runtime.SetFinalizer`, `runtime.GOOS`, `runtime.Gosched` have selector rewrites; `//gosim:notranslate` provides an escape hatch. [G: `internal/translate/main.go:27-56,89-93,253-273,361-386`; `internal/translate/translate.go:579-609`]

Keep-asm is an explicit package allowlist: `crypto/{aes,md5,sha1,sha256,sha512,subtle}`, `crypto/internal/{boring/sig,nistec,bigmod,edwards25519/field}`, vendored x/crypto `{chacha20,internal/poly1305,chacha20poly1305,sha3}`, `hash/crc32`, `net/url`, `net/http`, cespare xxhash/v2. For these packages, a bodyless header may remain and selected OtherFiles are copied verbatim. New FIPS package paths are **not** automatically covered. [G: `internal/translate/main.go:59-80`; `internal/translate/translate.go:368-383`; `internal/translate/rewrites.go:209-220`]

Hook lookup combines `hooksGo123` with `hooksGensyscallGo123ByArch[GOARCH]`, even on Go 1.27: there is no new-version adapter here. A hook may remove an existing Go body; a generated linkname points the declaration to the hook, avoiding import cycles. Unhooked bodyless declarations are discovered while visiting FuncDecl nodes (`Body == nil`), not by linker reachability; absent linkname/keep-asm/newUnixFile exception they log and `os.Exit(1)`. [G: `internal/translate/translate.go:309-329`; `internal/translate/hooks_go123.go:3-205`; `internal/translate/hooks_go123_{arm64,amd64}_gen.go:5`; `internal/translate/rewrites.go:115-220`; `docs/design.md:258-313`]

Linkname handling: hooked local symbols have original directives removed; targets in translated packages are renamed; `hash/maphash.runtime_memhash -> runtime.memhash` is explicitly accepted; `gosimnotranslate/` prefixes are stripped. Other links log errors but do not immediately fail. One-argument bodyless linknames also log unknown. The implementation acknowledges it only recognizes directives attached to FuncDecl decorations and can miss directives elsewhere; dangling/GenDecl directives are removed. The inventory intentionally scans **every comment**, not only FuncDecl.Doc. [G: `internal/translate/rewrites.go:115-175,234-263`; `internal/translate/hooks_go123.go:209-211`]

The public-export hack is **type naming**, not a missing runtime function: when a generated type expression needs another package's unexported Named/Alias, `makeTypeExpr` terminates unless PublicExportHacks permits it. Permitted names receive exported type aliases. Existing entries are binary.littleEndian, poll.errNetClosing and protobuf/proto.enumsByName. Go 1.27's `encoding/json` and `encoding/json/v2` infer a global of unexported `jsontext.export` type; this requires an added alias/general solution. That cannot be found by enumerating bodyless functions/linknames alone. [G: `internal/translate/types.go:24-104`; `internal/translate/main.go:82-86`; `internal/translate/translate.go:386-400,660-676`; Go: `encoding/json/v2_scanner.go:20`; `encoding/json/v2/arshal.go:65`; `encoding/json/jsontext/export.go:31-52`]

Runtime surface: gosimruntime owns scheduler/coroutines, channels/maps/globals, semaphores/notify lists, clock/timers and seeded random state; hooks adapt stdlib internals to that surface. Poll hooks connect netpoll operations to simulation poll descriptors. The translated `internal/simulation` runs an OS model, with syscall RPC structs per goroutine, a dispatcher, semaphore completion, and byte/value views across the userspace boundary; generated syscall/x/sys wrappers call LinuxOS methods, not host syscalls. The generic RawSyscall6 fallback returns ENOSYS for unsupported traps (and panics if an already-handled syscall escaped rewriting). [G: `docs/design.md:26-43,54-111,258-326`; `internal/hooks/go123/{sync.go:7-25,time.go:11-27,internal_poll.go:11-62}`; `internal/simulation/syscallabi/syscall.go:20-143`; `internal/simulation/os_linux.go:27-44,161-173`; `internal/simulation/gensyscall/main.go:405-467`]

## Static results

Experiment: run `run.sh`; output is `summary.txt` and TSVs. Config: linux, each arch, CGO_ENABLED=0, tags=sim; combined closure of both requested test packages. Results:

| Architecture | unhooked bodyless declarations without retained linkname | unknown linkname directives | total rows |
| --- | ---: | ---: | ---: |
| arm64 | 90 | 37 | 127 |
| amd64 | 80 | 37 | 117 |

These are **diagnostic obligations, not counts of runtime failures or hooks that must all be implemented**. Two directives on each arch are Go-bodied net/http test exports (`protocolSetHTTP3`, `transportFromH1Transport`), not absent implementations; strip/allow those export pragmas rather than replacing their bodies with failure stubs. [Experiment: `summary.txt`; Go: `net/http/http.go:64-65`, `net/http/http2.go:554-558`]

The arm64 set contains **all 115 #291 diagnostics** and adds ten `crypto/internal/fips140/nistec` functions and those two net/http directives. It therefore does not stop at jsontext.export. The type-export problem is separate from the 127. This helper has **not** exhaustively enumerated subsequent type/global/reflect rewrite failures; that needs a typed translation dry-run, not this AST hook inventory. [Experiment: `summary.txt`; copied `repro-audit.log`; G: `internal/translate/types.go:24-104`]

Go 1.23 comparison at identical stdlib package paths: of the arm64 difference, 51 declarations/directives already exist in Go 1.23, 74 are absent there; amd64 44 already exist, 71 are absent. Two x/sys entries per architecture are external-module sources and were not compared to a historical module. **Do not call all the absent same-path entries newly invented runtime APIs:** FIPS packages moved, runtime syscall moved, weak became public, mutex support moved. Conversely, an old function can be newly exposed to translation by a changed import graph. The 1.23 rows deliberately use the same Go 1.27 package universe and dispositions; they are not a runnable Go 1.23 tigerstore build or proof of its old translated closure. [Experiment: `summary.txt`, TSV `go123_comparison`; G: skip/keep-asm maps above; Go 1.23: `internal/weak/pointer.go:56-83`, `runtime/stubs.go:337-361`]

## Groups and port requirements (recommendations, not execution findings)

- **Mutex/semaphores:** internal/sync's eight missing names (including runtime_rand) moved outside old `sync` hooks. Rebind semaphore, monotonic-clock, fatal/throw and spin entry points to equivalent gosim primitives; seed runtime_rand. Old implementation uses gosim Semacquire/Semrelease, false canSpin, panic doSpin, Nanotime, Fastrandn. [G: `internal/hooks/go123/sync.go:7-70,99-109`; Go: `internal/sync/runtime.go:11-54`, `internal/sync/hashtriemap.go:723-724`]
- **WaitGroup:** new `sync.runtime_SemacquireWaitGroup(s, synctestDurable)` must use gosim waiting, preserving the bubble/durable-block contract if retaining synctest. Old WaitGroup called runtime_Semacquire already hooked to gosim. WaitGroup.Go itself is ordinary Go; translating its goroutine is not a new low-level hook. [Go: `sync/runtime.go:11-17`, `sync/waitgroup.go:159-226,251-279`; Go 1.23: `sync/waitgroup.go:118`; G: `internal/hooks/go123/sync.go:7-9`]
- **Clock/time:** implement runtimeNow with gosim time, runtimeIsBubbled with chosen bubble semantics; retain/revalidate timers and Sleep. FIPS monoTime must not use a host clock. Old now/runtimeNano/Sleep/timer hooks are in `time.go`. [Go: `time/time.go:1305-1348`, `crypto/internal/fips140deps/time/time.go:19-24`; G: `internal/hooks/go123/time.go:11-27,60-113`]
- **Synctest:** all nine internal/synctest bodyless linknames are unhandled; Go 1.23 had no corresponding package. Keeping the existing tests means implementing compatible bubble lifecycle, Wait/quiescence, object association and bubble handles, or explicitly translating away/replacing the bubble API in a validated adapter. A bodyless declaration's presence alone cannot justify a silent no-op. [Go: `internal/synctest/synctest.go:15-78,88-118`; inventory old/new rows]
- **Atomics:** 51 arm64 / 44 amd64 missing internal/runtime/atomic declarations. Old gosim already hooks **sync/atomic**, with optional yielding and race-mode host atomic operations; there was no internal/runtime/atomic hook table. New internal atomics need equivalent deterministic/race-correct operations or proven-unreached traps, not indiscriminate assembly retention. Many existed in 1.23 but were behind different dependency boundaries. [G: `internal/translate/hooks_go123.go:130-174`, `internal/hooks/go123/sync_atomic.go:11-32,80-95`; `gosimruntime/sema.go:153`; inventory per-function old/current sources]
- **Weak/unique:** unique remains on the skip list, but new public weak is reached independently and its two runtime hooks are absent. Old internal/weak had the equivalent declarations; gosim did not implement them, and skipped unique. Choose deterministic weak-lifetime semantics or explicitly limit/fail usages; forwarding to host GC is not an exact-replay argument. Old SetFinalizer intentionally does nothing. [G: `internal/translate/main.go:48`, `gosimruntime/runtime.go:937-942`; Go: `weak/pointer.go:43-48,70-108`; Go 1.23: `internal/weak/pointer.go:56-83`]
- **CPU:** arm64 adds getisar1/getpfr0; amd64's CPU declarations are already table-covered. Existing cpu hook bodies **already panic unimplemented**, so table coverage is not semantic support. A fixed supported feature policy or generic implementations plus unreachable traps is a port choice requiring testing. [G: `internal/hooks/go123/internal_cpu.go:3-20`; Go: `internal/cpu/cpu_arm64.go:30-36`]
- **runtime/sys:** GetCallerPC/SP/ClosurePtr on both; arm64 also EnableDIT/DITEnabled/DisableDIT. Earlier caller intrinsics lived in skipped runtime; they now need correctly modeled implementations or traps if unreachable. DIT/crypto entry points need a consistent CPU policy. [Go: `internal/runtime/sys/intrinsics.go:233-256`, `dit_arm64.go:18-20`, `no_dit.go:11-13`; Go 1.23: `runtime/stubs.go:337-361`; G skip list]
- **FIPS/crypto:** old keep-asm covered pre-FIPS crypto paths; new FIPS paths are absent. Preserve vetted pure-computation assembly, or select existing generic code with a validated fixed policy; do not conclude all these functions need simulator scheduling logic. Remaining linknames include FIPS bypass state, sha3Unwrap and fatal paths; bypass is goroutine-local runtime state in the new API and needs semantics if exercised. [G keep-asm map; Go: `crypto/fips140/enforcement.go:12-47`; per-symbol TSV citations]
- **Random/getrandom:** vgetrandom in internal/syscall/unix and x/sys/unix is new to the hook table; bind it to seeded simulation or return unsupported so the **simulated** syscall fallback handles entropy. Old GetRandom calls simulation.SyscallSysGetrandom, which fills bytes using gosimruntime.Fastrand64. Also revalidate hooked function signatures/types (e.g. new named GetRandomFlag vs old uintptr); AST missing-declaration subtraction does not do ABI/type compatibility validation. [G: `internal/hooks/go123/internal_syscall_unix.go:11-13`, `internal/simulation/os_linux.go:1689-1710`; Go: `internal/syscall/unix/getrandom.go:15-26`; x/sys@v0.47.0: `unix/vgetrandom_linux.go:11-13`, `unix/syscall_linux.go:2007-2011`]
- **syscall/x/sys:** moved internal/runtime/syscall/linux.Syscall6, syscall.runtimeClearenv and x/sys runtime_getAuxv need adapters or traps according to reachability. Regenerate version/arch syscall wrappers and validate generated APIs; old runtime syscall name is table-covered but its new path is not. Existing generic raw fallback is ENOSYS, not host Linux. [G: `internal/translate/hooks_go123.go:66,176-213`, generated hook files, `internal/simulation/os_linux.go:161-173`; Go: `internal/runtime/syscall/linux/asm_linux_arm64.s` plus TSV declaration lines; x/sys@v0.47.0: `unix/auxv.go:14-22`]
- **Export hacks/other:** jsontext.export is a type visibility adaptation, not a runtime stub. runtime/debug.runtime_setCrashFD is also unknown (already present in Go 1.23); net/http's two unknown directives are bodyful test aliases. [G types/main sources above; Go: `runtime/debug/stack.go:92-93`; Go 1.23 same `runtime/debug/stack.go:92-93`; net/http sources above]

## What can be static; what cannot

For a **fixed source tree, GOOS/GOARCH, toolchain, CGO, build tags and dependency graph**, the declaration/directive obligation set is finite and statically enumerable. This enumerator parses every selected file, subtracts literal common+arch hook maps, the skip graph, keep-asm and explicit linkname exceptions, and emits a machine-readable allowlist/trap manifest. Go 1.23 files are chosen by go/build with old release/experiment tags. The result is a conservative inventory of compiled declarations, including dead functions, not a call graph. [Experiment: `enumerate.go`, `run.sh`; G translator code cited above]

Reflection, interfaces and func values do not hide *declarations* from that inventory, but prevent simple syntactic call scans from proving which hooks execute. Build tags select a different inventory. Assembly-only symbols/calls and compiler intrinsic semantics require scanning .s files and compiler/runtime ABI knowledge; retained assembly may reference symbols absent from Go FuncDecls. Non-bodyless runtime calls, package/type/global rewrites, reflection/unsafe layout assumptions, and mismatched signatures remain separate port work. Thus **a complete static declaration list is achievable; a complete semantic port/replay proof is not provided by that list**. [G: `docs/design.md:72-77,158-165`; `internal/translate/translate.go:368-383`; `internal/translate/types.go:24-104`; listed AST method's scope]

Recommendation: generate ABI-correct wrappers for still-unimplemented functions which call one named `UnimplementedHook(pkg, symbol)` terminal test-abort hook. Preserve labels/signatures in a static manifest; record/abort rather than return zero or silently fall through to host runtime. A plain panic can be caught, so a terminal simulation failure is preferable. Gosim's existing SetAbortError only accepts predefined constErr values, so adding this central failure path requires a deliberate new error/metadata design. **Which wrappers may remain traps cannot be concluded here**: validate concrete test paths/seeds, initialize-time paths and replay traces after translation works. [G: `gosimruntime/runtime.go:944-960`; recommendation/inference]

## Complete names (source file:line and signatures are in the TSVs)

### linux/arm64

```text
crypto/fips140: isBypassed, setBypass, unsetBypass
crypto/internal/fips140/aes: ctrBlocks1Asm, ctrBlocks2Asm, ctrBlocks4Asm, ctrBlocks8Asm, decryptBlockAsm, encryptBlockAsm, expandKeyAsm
crypto/internal/fips140/aes/gcm: gcmAesData, gcmAesDec, gcmAesEnc, gcmAesFinish, gcmAesInit
crypto/internal/fips140/bigmod: addMulVVW1024, addMulVVW1536, addMulVVW2048
crypto/internal/fips140/nistec: p256FromMont, p256MovCond, p256Mul, p256NegCond, p256PointAddAffineAsm, p256PointAddAsm, p256PointDoubleAsm, p256Select, p256SelectAffine, p256Sqr
crypto/internal/fips140/sha256: blockSHA2
crypto/internal/fips140/sha3: keccakF1600NEON
crypto/internal/fips140/sha512: blockSHA512
crypto/internal/fips140/subtle: xorBytes
crypto/internal/fips140deps/time: monoTime
crypto/internal/fips140hash: sha3Unwrap
crypto/internal/sysrand: fatal
crypto/rand: fatal
crypto/subtle: setDITDisabled, setDITEnabled
golang.org/x/sys/unix: runtime_getAuxv, vgetrandom
internal/cpu: getisar1, getpfr0
internal/runtime/atomic: And, And32, And64, And8, Anduintptr, Cas, Cas64, CasRel, Casint32, Casint64, Casp1, Casuintptr, Load, Load64, Load8, LoadAcq, LoadAcq64, LoadAcquintptr, Loadint32, Loadint64, Loadp, Loaduint, Loaduintptr, Or, Or32, Or64, Or8, Oruintptr, Store, Store64, Store8, StoreRel, StoreRel64, StoreReluintptr, Storeint32, Storeint64, StorepNoWB, Storeuintptr, Xadd, Xadd64, Xaddint32, Xaddint64, Xadduintptr, Xchg, Xchg64, Xchg8, Xchgint32, Xchgint64, Xchguintptr, casPointer, storePointer
internal/runtime/sys: DITEnabled, DisableDIT, EnableDIT, GetCallerPC, GetCallerSP, GetClosurePtr
internal/runtime/syscall/linux: Syscall6
internal/sync: fatal, runtime_SemacquireMutex, runtime_Semrelease, runtime_canSpin, runtime_doSpin, runtime_nanotime, runtime_rand, throw
internal/synctest: IsInBubble, Run, Wait, acquire, associate, disassociate, inBubble, isAssociated, release
internal/syscall/unix: vgetrandom
net/http: protocolSetHTTP3, transportFromH1Transport
runtime/debug: runtime_setCrashFD
sync: runtime_SemacquireWaitGroup
syscall: runtimeClearenv
time: runtimeIsBubbled, runtimeNow
weak: runtime_makeStrongFromWeak, runtime_registerWeakPointer
```

### linux/amd64

```text
crypto/fips140: isBypassed, setBypass, unsetBypass
crypto/internal/fips140/aes: ctrBlocks1Asm, ctrBlocks2Asm, ctrBlocks4Asm, ctrBlocks8Asm, decryptBlockAsm, encryptBlockAsm, expandKeyAsm
crypto/internal/fips140/aes/gcm: gcmAesData, gcmAesDec, gcmAesEnc, gcmAesFinish, gcmAesInit
crypto/internal/fips140/bigmod: addMulVVW1024, addMulVVW1536, addMulVVW2048
crypto/internal/fips140/edwards25519/field: feMul
crypto/internal/fips140/nistec: p256FromMont, p256MovCond, p256Mul, p256NegCond, p256PointAddAffineAsm, p256PointAddAsm, p256PointDoubleAsm, p256Select, p256SelectAffine, p256Sqr
crypto/internal/fips140/sha256: blockAVX2, blockSHANI
crypto/internal/fips140/sha3: keccakF1600
crypto/internal/fips140/sha512: blockAVX2
crypto/internal/fips140/subtle: xorBytes
crypto/internal/fips140deps/time: monoTime
crypto/internal/fips140hash: sha3Unwrap
crypto/internal/sysrand: fatal
crypto/rand: fatal
crypto/subtle: setDITDisabled, setDITEnabled
golang.org/x/sys/unix: runtime_getAuxv, vgetrandom
internal/runtime/atomic: And, And32, And64, And8, Anduintptr, Cas, Cas64, CasRel, Casint32, Casint64, Casp1, Casuintptr, Loadint32, Loadint64, Loaduint, Loaduintptr, Or, Or32, Or64, Or8, Oruintptr, Store, Store64, Store8, StoreRel, StoreRel64, StoreReluintptr, Storeint32, Storeint64, StorepNoWB, Storeuintptr, Xadd, Xadd64, Xaddint32, Xaddint64, Xadduintptr, Xchg, Xchg64, Xchg8, Xchgint32, Xchgint64, Xchguintptr, casPointer, storePointer
internal/runtime/sys: GetCallerPC, GetCallerSP, GetClosurePtr
internal/runtime/syscall/linux: Syscall6
internal/sync: fatal, runtime_SemacquireMutex, runtime_Semrelease, runtime_canSpin, runtime_doSpin, runtime_nanotime, runtime_rand, throw
internal/synctest: IsInBubble, Run, Wait, acquire, associate, disassociate, inBubble, isAssociated, release
internal/syscall/unix: vgetrandom
net/http: protocolSetHTTP3, transportFromH1Transport
runtime/debug: runtime_setCrashFD
sync: runtime_SemacquireWaitGroup
syscall: runtimeClearenv
time: runtimeIsBubbled, runtimeNow
weak: runtime_makeStrongFromWeak, runtime_registerWeakPointer
```
