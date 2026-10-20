# Native-source call-graph experiment results

Configuration: Go 1.27.2 linux/arm64, CGO_ENABLED=0, x/tools v0.52.0.
See ../README.md for exact commands, pins and interpretation. This is NOT an observed-hook trace.

## Counts

| Analysis | Audit reachable | Projected | Projected bodyless |
|---|---:|---:|---:|
| rta | 94 | 46 | 46 |
| vta | 86 | 45 | 45 |
| purego | 71 | 30 | 26 |

## All audit symbols

Status: frontier = candidate before a native/swapped-package or old-hook cut; cut = only reachable beyond such cuts in this projection; absent = not in this rooted source graph. Go-body = same old audit symbol now has a Go implementation under these tags.

| Symbol | RTA | VTA | VTA purego |
|---|---|---|---|
| `crypto/fips140.isBypassed` | frontier | frontier | frontier |
| `crypto/fips140.setBypass` | frontier | frontier | frontier |
| `crypto/fips140.unsetBypass` | frontier | frontier | frontier |
| `crypto/internal/fips140/aes.ctrBlocks1Asm` | frontier | frontier | absent |
| `crypto/internal/fips140/aes.ctrBlocks2Asm` | frontier | frontier | absent |
| `crypto/internal/fips140/aes.ctrBlocks4Asm` | frontier | frontier | absent |
| `crypto/internal/fips140/aes.ctrBlocks8Asm` | frontier | frontier | absent |
| `crypto/internal/fips140/aes.decryptBlockAsm` | frontier | frontier | absent |
| `crypto/internal/fips140/aes.encryptBlockAsm` | frontier | frontier | absent |
| `crypto/internal/fips140/aes.expandKeyAsm` | frontier | frontier | absent |
| `crypto/internal/fips140/aes/gcm.gcmAesData` | frontier | frontier | absent |
| `crypto/internal/fips140/aes/gcm.gcmAesDec` | frontier | frontier | absent |
| `crypto/internal/fips140/aes/gcm.gcmAesEnc` | frontier | frontier | absent |
| `crypto/internal/fips140/aes/gcm.gcmAesFinish` | frontier | frontier | absent |
| `crypto/internal/fips140/aes/gcm.gcmAesInit` | frontier | frontier | absent |
| `crypto/internal/fips140/bigmod.addMulVVW1024` | frontier | frontier | frontier / Go-body |
| `crypto/internal/fips140/bigmod.addMulVVW1536` | frontier | frontier | frontier / Go-body |
| `crypto/internal/fips140/bigmod.addMulVVW2048` | frontier | frontier | frontier / Go-body |
| `crypto/internal/fips140/sha256.blockSHA2` | frontier | frontier | absent |
| `crypto/internal/fips140/sha3.keccakF1600NEON` | frontier | frontier | absent |
| `crypto/internal/fips140/sha512.blockSHA512` | frontier | frontier | absent |
| `crypto/internal/fips140/subtle.xorBytes` | frontier | frontier | frontier / Go-body |
| `crypto/internal/fips140deps/time.monoTime` | frontier | frontier | frontier |
| `crypto/internal/fips140hash.sha3Unwrap` | frontier | frontier | frontier |
| `crypto/internal/sysrand.fatal` | frontier | frontier | frontier |
| `crypto/rand.fatal` | frontier | absent | absent |
| `crypto/subtle.setDITDisabled` | absent | absent | absent |
| `crypto/subtle.setDITEnabled` | absent | absent | absent |
| `golang.org/x/sys/unix.runtime_getAuxv` | absent | absent | absent |
| `golang.org/x/sys/unix.vgetrandom` | absent | absent | absent |
| `internal/cpu.getisar1` | absent | absent | absent |
| `internal/cpu.getpfr0` | absent | absent | absent |
| `internal/runtime/atomic.And` | cut | cut | cut |
| `internal/runtime/atomic.And32` | absent | absent | absent |
| `internal/runtime/atomic.And64` | absent | absent | absent |
| `internal/runtime/atomic.And8` | cut | cut | cut |
| `internal/runtime/atomic.Anduintptr` | absent | absent | absent |
| `internal/runtime/atomic.Cas` | cut | cut | cut |
| `internal/runtime/atomic.Cas64` | cut | cut | cut |
| `internal/runtime/atomic.CasRel` | cut | absent | absent |
| `internal/runtime/atomic.Casint32` | cut | cut | cut |
| `internal/runtime/atomic.Casint64` | cut | absent | absent |
| `internal/runtime/atomic.Casp1` | cut | cut | cut |
| `internal/runtime/atomic.Casuintptr` | cut | cut | cut |
| `internal/runtime/atomic.Load` | cut | cut | cut |
| `internal/runtime/atomic.Load64` | cut | cut | cut |
| `internal/runtime/atomic.Load8` | cut | cut | cut |
| `internal/runtime/atomic.LoadAcq` | cut | cut | cut |
| `internal/runtime/atomic.LoadAcq64` | absent | absent | absent |
| `internal/runtime/atomic.LoadAcquintptr` | frontier | frontier | frontier |
| `internal/runtime/atomic.Loadint32` | cut | cut | cut |
| `internal/runtime/atomic.Loadint64` | cut | cut | cut |
| `internal/runtime/atomic.Loadp` | cut | cut | cut |
| `internal/runtime/atomic.Loaduint` | cut | absent | absent |
| `internal/runtime/atomic.Loaduintptr` | cut | cut | cut |
| `internal/runtime/atomic.Or` | cut | cut | cut |
| `internal/runtime/atomic.Or32` | cut | cut | cut |
| `internal/runtime/atomic.Or64` | cut | absent | absent |
| `internal/runtime/atomic.Or8` | cut | cut | cut |
| `internal/runtime/atomic.Oruintptr` | absent | absent | absent |
| `internal/runtime/atomic.Store` | cut | cut | cut |
| `internal/runtime/atomic.Store64` | cut | cut | cut |
| `internal/runtime/atomic.Store8` | cut | cut | cut |
| `internal/runtime/atomic.StoreRel` | cut | cut | cut |
| `internal/runtime/atomic.StoreRel64` | absent | absent | absent |
| `internal/runtime/atomic.StoreReluintptr` | frontier | frontier | frontier |
| `internal/runtime/atomic.Storeint32` | cut | absent | absent |
| `internal/runtime/atomic.Storeint64` | cut | cut | cut |
| `internal/runtime/atomic.StorepNoWB` | cut | cut | cut |
| `internal/runtime/atomic.Storeuintptr` | cut | cut | cut |
| `internal/runtime/atomic.Xadd` | cut | cut | cut |
| `internal/runtime/atomic.Xadd64` | cut | cut | cut |
| `internal/runtime/atomic.Xaddint32` | cut | cut | cut |
| `internal/runtime/atomic.Xaddint64` | cut | cut | cut |
| `internal/runtime/atomic.Xadduintptr` | cut | cut | cut |
| `internal/runtime/atomic.Xchg` | cut | cut | cut |
| `internal/runtime/atomic.Xchg64` | absent | absent | absent |
| `internal/runtime/atomic.Xchg8` | cut | cut | cut |
| `internal/runtime/atomic.Xchgint32` | cut | absent | absent |
| `internal/runtime/atomic.Xchgint64` | cut | absent | absent |
| `internal/runtime/atomic.Xchguintptr` | absent | absent | absent |
| `internal/runtime/atomic.casPointer` | cut | cut | cut |
| `internal/runtime/atomic.storePointer` | cut | cut | cut |
| `internal/runtime/sys.DITEnabled` | cut | cut | cut |
| `internal/runtime/sys.DisableDIT` | cut | cut | cut |
| `internal/runtime/sys.EnableDIT` | cut | cut | cut |
| `internal/runtime/sys.GetCallerPC` | cut | cut | cut |
| `internal/runtime/sys.GetCallerSP` | cut | cut | cut |
| `internal/runtime/sys.GetClosurePtr` | absent | absent | absent |
| `internal/runtime/syscall/linux.Syscall6` | cut | cut | cut |
| `internal/sync.fatal` | frontier | frontier | frontier |
| `internal/sync.runtime_SemacquireMutex` | frontier | frontier | frontier |
| `internal/sync.runtime_Semrelease` | frontier | frontier | frontier |
| `internal/sync.runtime_canSpin` | frontier | frontier | frontier |
| `internal/sync.runtime_doSpin` | frontier | frontier | frontier |
| `internal/sync.runtime_nanotime` | frontier | frontier | frontier |
| `internal/sync.runtime_rand` | frontier | frontier | frontier |
| `internal/sync.throw` | frontier | frontier | frontier |
| `internal/synctest.IsInBubble` | frontier | frontier | frontier |
| `internal/synctest.Run` | frontier | frontier | frontier |
| `internal/synctest.Wait` | absent | absent | absent |
| `internal/synctest.acquire` | absent | absent | absent |
| `internal/synctest.associate` | frontier | frontier | frontier |
| `internal/synctest.disassociate` | frontier | frontier | frontier |
| `internal/synctest.inBubble` | absent | absent | absent |
| `internal/synctest.isAssociated` | frontier | frontier | frontier |
| `internal/synctest.release` | absent | absent | absent |
| `internal/syscall/unix.vgetrandom` | cut | cut | cut |
| `runtime/debug.runtime_setCrashFD` | absent | absent | absent |
| `sync.runtime_SemacquireWaitGroup` | frontier | frontier | frontier |
| `syscall.runtimeClearenv` | absent | absent | absent |
| `time.runtimeIsBubbled` | frontier | frontier | frontier |
| `time.runtimeNow` | frontier | frontier | frontier |
| `weak.runtime_makeStrongFromWeak` | frontier | frontier | frontier |
| `weak.runtime_registerWeakPointer` | frontier | frontier | frontier |

## One source-level candidate path per reachable audit symbol

Prefer a VTA projected path; otherwise show its native-source path, or RTA-only path. Package init is an explicit root. Some interface/callback paths are infeasible: see README cautions.

### crypto/fips140.isBypassed

crypto/internal/fips140/check.init -> crypto/internal/fips140/check.init#1 -> crypto/internal/fips140/hmac.New[*crypto/internal/fips140/sha256.Digest] -> (*crypto/md5.digest).Write -> crypto/internal/fips140only.Enforced -> crypto/fips140.Enforced -> crypto/fips140.isBypassed

### crypto/fips140.setBypass

google.golang.org/protobuf/reflect/protodesc.init -> google.golang.org/protobuf/reflect/protodesc.init#1 -> fmt.Fprintf -> (*crypto/tls.Conn).Write -> (*crypto/tls.Conn).Handshake -> (*crypto/tls.Conn).HandshakeContext -> (*crypto/tls.Conn).handshakeContext -> (*crypto/tls.Conn).clientHandshake$bound -> (*crypto/tls.Conn).clientHandshake -> (*crypto/tls.Conn).makeClientHello -> (*crypto/tls.hybridKeyExchange).keyShares -> crypto/fips140.WithoutEnforcement -> crypto/fips140.setBypass

### crypto/fips140.unsetBypass

google.golang.org/protobuf/reflect/protodesc.init -> google.golang.org/protobuf/reflect/protodesc.init#1 -> fmt.Fprintf -> (*crypto/tls.Conn).Write -> (*crypto/tls.Conn).Handshake -> (*crypto/tls.Conn).HandshakeContext -> (*crypto/tls.Conn).handshakeContext -> (*crypto/tls.Conn).clientHandshake$bound -> (*crypto/tls.Conn).clientHandshake -> (*crypto/tls.Conn).makeClientHello -> (*crypto/tls.hybridKeyExchange).keyShares -> crypto/fips140.WithoutEnforcement -> crypto/fips140.unsetBypass

### crypto/internal/fips140/aes.ctrBlocks1Asm

crypto/internal/fips140/aes.init -> crypto/internal/fips140/aes.init#2 -> crypto/internal/fips140.CAST -> crypto/internal/fips140/drbg.init#1$1 -> (*crypto/internal/fips140/drbg.Counter).Generate -> (*crypto/internal/fips140/aes.CTR).XORKeyStream -> (*crypto/internal/fips140/aes.CTR).XORKeyStreamAt -> crypto/internal/fips140/aes.ctrBlocks1 -> crypto/internal/fips140/aes.ctrBlocks1Asm

### crypto/internal/fips140/aes.ctrBlocks2Asm

crypto/internal/fips140/aes.init -> crypto/internal/fips140/aes.init#2 -> crypto/internal/fips140.CAST -> crypto/internal/fips140/drbg.init#1$1 -> (*crypto/internal/fips140/drbg.Counter).Generate -> (*crypto/internal/fips140/aes.CTR).XORKeyStream -> (*crypto/internal/fips140/aes.CTR).XORKeyStreamAt -> crypto/internal/fips140/aes.ctrBlocks2 -> crypto/internal/fips140/aes.ctrBlocks2Asm

### crypto/internal/fips140/aes.ctrBlocks4Asm

crypto/internal/fips140/aes.init -> crypto/internal/fips140/aes.init#2 -> crypto/internal/fips140.CAST -> crypto/internal/fips140/drbg.init#1$1 -> (*crypto/internal/fips140/drbg.Counter).Generate -> (*crypto/internal/fips140/aes.CTR).XORKeyStream -> (*crypto/internal/fips140/aes.CTR).XORKeyStreamAt -> crypto/internal/fips140/aes.ctrBlocks4 -> crypto/internal/fips140/aes.ctrBlocks4Asm

### crypto/internal/fips140/aes.ctrBlocks8Asm

crypto/internal/fips140/aes.init -> crypto/internal/fips140/aes.init#2 -> crypto/internal/fips140.CAST -> crypto/internal/fips140/drbg.init#1$1 -> (*crypto/internal/fips140/drbg.Counter).Generate -> (*crypto/internal/fips140/aes.CTR).XORKeyStream -> (*crypto/internal/fips140/aes.CTR).XORKeyStreamAt -> crypto/internal/fips140/aes.ctrBlocks8 -> crypto/internal/fips140/aes.ctrBlocks8Asm

### crypto/internal/fips140/aes.decryptBlockAsm

crypto/internal/fips140/aes.init -> crypto/internal/fips140/aes.init#2 -> crypto/internal/fips140.CAST -> crypto/internal/fips140/aes.init#2$1 -> (*crypto/internal/fips140/aes.CBCDecrypter).CryptBlocks -> crypto/internal/fips140/aes.cryptBlocksDec -> crypto/internal/fips140/aes.cryptBlocksDecGeneric -> crypto/internal/fips140/aes.decryptBlock -> crypto/internal/fips140/aes.decryptBlockAsm

### crypto/internal/fips140/aes.encryptBlockAsm

crypto/internal/fips140/aes.init -> crypto/internal/fips140/aes.init#2 -> crypto/internal/fips140.CAST -> crypto/internal/fips140/aes/gcm.init#1$1 -> (*crypto/internal/fips140/aes/gcm.CounterKDF).DeriveKey -> (*crypto/internal/fips140/aes/gcm.CMAC).MAC -> crypto/internal/fips140/aes.EncryptBlockInternal -> crypto/internal/fips140/aes.encryptBlock -> crypto/internal/fips140/aes.encryptBlockAsm

### crypto/internal/fips140/aes.expandKeyAsm

crypto/internal/fips140/aes.init -> crypto/internal/fips140/aes.init#2 -> crypto/internal/fips140.CAST -> crypto/internal/fips140/aes/gcm.init#1$1 -> crypto/internal/fips140/aes.New -> crypto/internal/fips140/aes.newOutlined -> crypto/internal/fips140/aes.newBlock -> crypto/internal/fips140/aes.expandKeyAsm

### crypto/internal/fips140/aes/gcm.gcmAesData

google.golang.org/protobuf/reflect/protodesc.init -> google.golang.org/protobuf/reflect/protodesc.init#1 -> fmt.Fprintf -> (*crypto/tls.Conn).Write -> (*crypto/tls.Conn).writeRecordLocked -> (*crypto/tls.halfConn).encrypt -> (*crypto/tls.xorNonceAEAD).Seal -> (*crypto/internal/fips140/aes/gcm.GCMWithXORCounterNonce).Seal -> (*crypto/internal/fips140/aes/gcm.GCM).sealAfterIndicator -> crypto/internal/fips140/aes/gcm.seal -> crypto/internal/fips140/aes/gcm.gcmAesData

### crypto/internal/fips140/aes/gcm.gcmAesDec

crypto/fips140.init -> (*internal/godebug.Setting).Value -> (*sync.Once).Do -> (*sync.Once).doSlow -> net.readServices -> (*net.file).readLine -> io.ReadFull -> io.ReadAtLeast -> (*crypto/tls.Conn).Read -> (*crypto/tls.Conn).readRecord -> (*crypto/tls.Conn).readRecordOrCCS -> (*crypto/tls.halfConn).decrypt -> (*crypto/tls.xorNonceAEAD).Open -> (*crypto/internal/fips140/aes/gcm.GCMWithXORCounterNonce).Open -> (*crypto/internal/fips140/aes/gcm.GCM).Open -> crypto/internal/fips140/aes/gcm.open -> crypto/internal/fips140/aes/gcm.gcmAesDec

### crypto/internal/fips140/aes/gcm.gcmAesEnc

google.golang.org/protobuf/reflect/protodesc.init -> google.golang.org/protobuf/reflect/protodesc.init#1 -> fmt.Fprintf -> (*crypto/tls.Conn).Write -> (*crypto/tls.Conn).writeRecordLocked -> (*crypto/tls.halfConn).encrypt -> (*crypto/tls.xorNonceAEAD).Seal -> (*crypto/internal/fips140/aes/gcm.GCMWithXORCounterNonce).Seal -> (*crypto/internal/fips140/aes/gcm.GCM).sealAfterIndicator -> crypto/internal/fips140/aes/gcm.seal -> crypto/internal/fips140/aes/gcm.gcmAesEnc

### crypto/internal/fips140/aes/gcm.gcmAesFinish

google.golang.org/protobuf/reflect/protodesc.init -> google.golang.org/protobuf/reflect/protodesc.init#1 -> fmt.Fprintf -> (*crypto/tls.Conn).Write -> (*crypto/tls.Conn).writeRecordLocked -> (*crypto/tls.halfConn).encrypt -> (*crypto/tls.xorNonceAEAD).Seal -> (*crypto/internal/fips140/aes/gcm.GCMWithXORCounterNonce).Seal -> (*crypto/internal/fips140/aes/gcm.GCM).sealAfterIndicator -> crypto/internal/fips140/aes/gcm.seal -> crypto/internal/fips140/aes/gcm.gcmAesFinish

### crypto/internal/fips140/aes/gcm.gcmAesInit

google.golang.org/protobuf/reflect/protodesc.init -> google.golang.org/protobuf/reflect/protodesc.init#1 -> fmt.Fprintf -> (*crypto/tls.Conn).Write -> (*crypto/tls.Conn).Handshake -> (*crypto/tls.Conn).HandshakeContext -> (*crypto/tls.Conn).handshakeContext -> (*crypto/tls.Conn).clientHandshake$bound -> (*crypto/tls.Conn).clientHandshake -> (*crypto/tls.clientHandshakeState).handshake -> (*crypto/tls.clientHandshakeState).establishKeys -> crypto/tls.aeadAESGCM -> crypto/internal/fips140/aes/gcm.NewGCMForTLS12 -> crypto/internal/fips140/aes/gcm.newGCM -> crypto/internal/fips140/aes/gcm.initGCM -> crypto/internal/fips140/aes/gcm.gcmAesInit

### crypto/internal/fips140/bigmod.addMulVVW1024

crypto/internal/fips140/aes.init -> crypto/internal/fips140/aes.init#2 -> crypto/internal/fips140.CAST -> crypto/internal/fips140/ecdsa.init$6$1 -> crypto/internal/fips140/ecdsa.sign[*crypto/internal/fips140/nistec.P256Point] -> crypto/internal/fips140/ecdsa.signGeneric[*crypto/internal/fips140/nistec.P256Point] -> (*crypto/internal/fips140/bigmod.Nat).Mul -> crypto/internal/fips140/bigmod.addMulVVW1024

### crypto/internal/fips140/bigmod.addMulVVW1536

crypto/internal/fips140/aes.init -> crypto/internal/fips140/aes.init#2 -> crypto/internal/fips140.CAST -> crypto/internal/fips140/ecdsa.init$6$1 -> crypto/internal/fips140/ecdsa.sign[*crypto/internal/fips140/nistec.P256Point] -> crypto/internal/fips140/ecdsa.signGeneric[*crypto/internal/fips140/nistec.P256Point] -> (*crypto/internal/fips140/bigmod.Nat).Mul -> crypto/internal/fips140/bigmod.addMulVVW1536

### crypto/internal/fips140/bigmod.addMulVVW2048

crypto/internal/fips140/aes.init -> crypto/internal/fips140/aes.init#2 -> crypto/internal/fips140.CAST -> crypto/internal/fips140/ecdsa.init$6$1 -> crypto/internal/fips140/ecdsa.sign[*crypto/internal/fips140/nistec.P256Point] -> crypto/internal/fips140/ecdsa.signGeneric[*crypto/internal/fips140/nistec.P256Point] -> (*crypto/internal/fips140/bigmod.Nat).Mul -> crypto/internal/fips140/bigmod.addMulVVW2048

### crypto/internal/fips140/sha256.blockSHA2

crypto/internal/fips140/check.init -> crypto/internal/fips140/check.init#1 -> crypto/internal/fips140/hmac.New[*crypto/internal/fips140/sha256.Digest] -> (*crypto/internal/fips140/sha256.Digest).Write -> crypto/internal/fips140/sha256.block -> crypto/internal/fips140/sha256.blockSHA2

### crypto/internal/fips140/sha3.keccakF1600NEON

crypto/internal/fips140/check.init -> crypto/internal/fips140/check.init#1 -> crypto/internal/fips140/hmac.New[*crypto/internal/fips140/sha256.Digest] -> (*crypto/internal/fips140/sha3.Digest).Write -> (*crypto/internal/fips140/sha3.Digest).write -> (*crypto/internal/fips140/sha3.Digest).writeGeneric -> (*crypto/internal/fips140/sha3.Digest).permute -> crypto/internal/fips140/sha3.keccakF1600 -> crypto/internal/fips140/sha3.keccakF1600NEON

### crypto/internal/fips140/sha512.blockSHA512

crypto/internal/fips140/check.init -> crypto/internal/fips140/check.init#1 -> crypto/internal/fips140/hmac.New[*crypto/internal/fips140/sha256.Digest] -> (*crypto/internal/fips140/sha512.Digest).Write -> crypto/internal/fips140/sha512.block -> crypto/internal/fips140/sha512.blockSHA512

### crypto/internal/fips140/subtle.xorBytes

crypto/internal/fips140/aes.init -> crypto/internal/fips140/aes.init#2 -> crypto/internal/fips140.CAST -> crypto/internal/fips140/drbg.init#1$1 -> (*crypto/internal/fips140/drbg.Counter).Reseed -> crypto/internal/fips140/subtle.XORBytes -> crypto/internal/fips140/subtle.xorBytes

### crypto/internal/fips140deps/time.monoTime

github.com/djosh34/tigerstore/internal/sim.TestMembershipFaults -> (*github.com/djosh34/tigerstore/internal/sim.effects).check -> fmt.Sprintf -> fmt.newPrinter -> (*sync.Pool).Get -> crypto/internal/fips140/drbg.init$1 -> crypto/internal/fips140/drbg.getEntropy -> crypto/internal/entropy/v1.0.0.Seed -> crypto/internal/entropy/v1.0.0.Samples -> crypto/internal/entropy/v1.0.0.newSource -> crypto/internal/fips140deps/time.HighPrecisionNow -> crypto/internal/fips140deps/time.monoTime

### crypto/internal/fips140hash.sha3Unwrap

crypto/internal/fips140/aes.init -> crypto/internal/fips140/aes.init#2 -> crypto/internal/fips140.CAST -> crypto/internal/fips140/tls13.init#1$1 -> (*crypto/internal/fips140/tls13.EarlySecret).HandshakeSecret -> crypto/internal/fips140/tls13.extract[hash.Hash] -> crypto/internal/fips140/hkdf.Extract[hash.Hash] -> crypto/internal/fips140hash.UnwrapNew[hash.Hash]$1 -> crypto/internal/fips140hash.Unwrap -> crypto/internal/fips140hash.sha3Unwrap

### crypto/internal/sysrand.fatal

crypto/fips140.init -> (*internal/godebug.Setting).Value -> (*sync.Once).Do -> (*sync.Once).doSlow -> net.readServices -> (*net.file).readLine -> io.ReadFull -> io.ReadAtLeast -> (crypto/internal/rand.reader).Read -> crypto/internal/fips140/drbg.Read -> crypto/internal/sysrand.Read -> crypto/internal/sysrand.fatal

### crypto/rand.fatal

github.com/djosh34/tigerstore/internal/core.init -> hash/crc32.MakeTable -> (*net/http/internal/http2.ClientConn).healthCheck$bound -> (*net/http/internal/http2.ClientConn).healthCheck -> (*net/http/internal/http2.ClientConn).Ping -> crypto/rand.Read -> crypto/rand.fatal

### internal/runtime/atomic.And

google.golang.org/protobuf/internal/detrand.init -> google.golang.org/protobuf/internal/detrand.binaryHash -> (*os.File).Close -> (*os.file).close -> runtime.SetFinalizer -> runtime.createfing -> runtime.runFinalizers -> (*internal/runtime/atomic.Uint32).And -> internal/runtime/atomic.And

### internal/runtime/atomic.And8

runtime.init -> runtime.init#8 -> runtime.forcegchelper -> runtime.gcStart -> runtime.sweepone -> (*runtime.sweepLocked).sweep -> runtime.spanHasNoSpecials -> internal/runtime/atomic.And8

### internal/runtime/atomic.Cas

runtime.init -> runtime.init#8 -> runtime.forcegchelper -> runtime.gcStart -> runtime.sweepone -> (*runtime.sweepLocker).tryAcquire -> internal/runtime/atomic.Cas

### internal/runtime/atomic.Cas64

net/netip.init -> unique.Make[net/netip.addrDetail] -> (*unique.canonMap[net/netip.addrDetail]).LoadOrStore -> runtime.AddCleanup[net/netip.addrDetail, struct{}] -> (*runtime.cleanupQueue).createGs -> runtime.runCleanups -> (*runtime.lfstack).push -> internal/runtime/atomic.Cas64

### internal/runtime/atomic.CasRel

github.com/djosh34/tigerstore/internal/core.init -> hash/crc32.MakeTable -> runtime.gcBgMarkWorker$2 -> runtime.runqdrain -> internal/runtime/atomic.CasRel

### internal/runtime/atomic.Casint32

runtime.init -> runtime.init#8 -> runtime.forcegchelper -> runtime.gcStart -> runtime.gcMarkTinyAllocs -> runtime.greyobject -> (*runtime.gcWork).putObj -> (*runtime.gcControllerState).enlistWorker -> runtime.wakep -> (*internal/runtime/atomic.Int32).CompareAndSwap -> internal/runtime/atomic.Casint32

### internal/runtime/atomic.Casint64

github.com/djosh34/tigerstore/internal/core.init -> hash/crc32.MakeTable -> (*runtime.mheap).alloc$1 -> (*runtime.mheap).allocSpan -> (*runtime.pageAlloc).scavenge -> (*runtime.scavengeIndex).find -> (*runtime.atomicOffAddr).Clear -> (*internal/runtime/atomic.Int64).CompareAndSwap -> internal/runtime/atomic.Casint64

### internal/runtime/atomic.Casp1

net/netip.init -> unique.Make[net/netip.addrDetail] -> (*unique.canonMap[net/netip.addrDetail]).LoadOrStore -> runtime.AddCleanup[net/netip.addrDetail, struct{}] -> (*runtime.mspan).typePointersOfUnchecked -> runtime.getGCMask -> runtime.getGCMaskOnDemand -> internal/runtime/atomic.Casp1

### internal/runtime/atomic.Casuintptr

runtime.init -> runtime.init#8 -> runtime.forcegchelper -> runtime.lock -> runtime.lockWithRank -> runtime.lock2 -> internal/runtime/atomic.Casuintptr

### internal/runtime/atomic.Load

runtime.init -> runtime.init#8 -> runtime.forcegchelper -> runtime.gcStart -> (*internal/runtime/atomic.Uint32).Load -> internal/runtime/atomic.Load

### internal/runtime/atomic.Load64

runtime.init -> runtime.init#8 -> runtime.forcegchelper -> runtime.gcStart -> (*internal/runtime/atomic.Uint64).Load -> internal/runtime/atomic.Load64

### internal/runtime/atomic.Load8

runtime.init -> runtime.init#8 -> runtime.forcegchelper -> (*internal/runtime/atomic.Bool).Load -> (*internal/runtime/atomic.Uint8).Load -> internal/runtime/atomic.Load8

### internal/runtime/atomic.LoadAcq

runtime.init -> runtime.init#8 -> runtime.forcegchelper -> runtime.gcStart -> runtime.gcBgMarkStartWorkers -> runtime.gcBgMarkWorker -> runtime.gcMarkDone -> runtime.gcMarkTermination -> runtime.injectglist -> runtime.runqputbatch -> internal/runtime/atomic.LoadAcq

### internal/runtime/atomic.LoadAcquintptr

github.com/djosh34/tigerstore/internal/sim.TestMembershipFaults -> (*github.com/djosh34/tigerstore/internal/sim.effects).check -> fmt.Sprintf -> fmt.newPrinter -> (*sync.Pool).Get -> (*sync.Pool).pin -> internal/runtime/atomic.LoadAcquintptr

### internal/runtime/atomic.Loadint32

runtime.init -> runtime.init#8 -> runtime.forcegchelper -> runtime.gcStart -> (runtime.gcTrigger).test -> (*internal/runtime/atomic.Int32).Load -> internal/runtime/atomic.Loadint32

### internal/runtime/atomic.Loadint64

runtime.init -> runtime.init#8 -> runtime.forcegchelper -> runtime.gcStart -> (*runtime.gcCPULimiterState).startGCTransition -> (*runtime.gcCPULimiterState).updateLocked -> (*internal/runtime/atomic.Int64).Load -> internal/runtime/atomic.Loadint64

### internal/runtime/atomic.Loadp

net/netip.init -> unique.Make[net/netip.addrDetail] -> (*unique.canonMap[net/netip.addrDetail]).LoadOrStore -> runtime.AddCleanup[net/netip.addrDetail, struct{}] -> (*runtime.mspan).typePointersOfUnchecked -> internal/runtime/atomic.Loadp

### internal/runtime/atomic.Loaduint

github.com/djosh34/tigerstore/internal/sim.TestMembershipFaults -> (*github.com/djosh34/tigerstore/internal/sim.effects).check -> fmt.Sprintf -> (*fmt.pp).doPrintf -> (*fmt.pp).printArg -> (*fmt.pp).handleMethods -> (*google.golang.org/protobuf/internal/filedesc.EnumRanges).Format -> google.golang.org/protobuf/internal/descfmt.FormatList -> google.golang.org/protobuf/internal/descfmt.formatListOpt -> (reflect.Value).Call -> runtime.chanrecv -> runtime.empty -> internal/runtime/atomic.Loaduint

### internal/runtime/atomic.Loaduintptr

runtime.init -> runtime.init#8 -> runtime.forcegchelper -> runtime.lock -> runtime.lockWithRank -> runtime.lock2 -> internal/runtime/atomic.Loaduintptr

### internal/runtime/atomic.Or

runtime.init -> runtime.init#8 -> runtime.forcegchelper -> runtime.gcStart -> runtime.gcMarkTinyAllocs -> runtime.tryDeferToSpanScan -> (runtime.pMask).set -> internal/runtime/atomic.Or

### internal/runtime/atomic.Or32

runtime.init -> runtime.init#8 -> runtime.forcegchelper -> runtime.gcStart -> runtime.gcMarkTinyAllocs -> runtime.tryDeferToSpanScan -> (*runtime.spanInlineMarkBits).tryAcquire -> (*runtime.spanScanOwnership).or -> internal/runtime/atomic.Or32

### internal/runtime/atomic.Or64

github.com/djosh34/tigerstore/internal/core.init -> hash/crc32.MakeTable -> runtime.gcAssistAlloc$2 -> runtime.gcAssistAlloc1 -> runtime.gcDrainN -> runtime.scanSpan -> runtime.spanSetScans -> internal/runtime/atomic.Or64

### internal/runtime/atomic.Or8

runtime.init -> runtime.init#8 -> runtime.forcegchelper -> runtime.gcStart -> runtime.gcMarkTinyAllocs -> runtime.greyobject -> internal/runtime/atomic.Or8

### internal/runtime/atomic.Store

runtime.init -> runtime.init#8 -> runtime.forcegchelper -> runtime.gcStart -> internal/runtime/atomic.Store

### internal/runtime/atomic.Store64

runtime.init -> runtime.init#8 -> runtime.forcegchelper -> runtime.gcStart -> (*runtime.gcControllerState).startCycle -> (*runtime.gcControllerState).revise -> (*internal/runtime/atomic.Float64).Store -> (*internal/runtime/atomic.Uint64).Store -> internal/runtime/atomic.Store64

### internal/runtime/atomic.Store8

runtime.init -> runtime.init#8 -> runtime.forcegchelper -> (*internal/runtime/atomic.Bool).Store -> (*internal/runtime/atomic.Uint8).Store -> internal/runtime/atomic.Store8

### internal/runtime/atomic.StoreRel

runtime.init -> runtime.init#8 -> runtime.forcegchelper -> runtime.gcStart -> runtime.gcBgMarkStartWorkers -> runtime.gcBgMarkWorker -> runtime.gcMarkDone -> runtime.gcMarkTermination -> runtime.injectglist -> runtime.runqputbatch -> internal/runtime/atomic.StoreRel

### internal/runtime/atomic.StoreReluintptr

github.com/djosh34/tigerstore/internal/sim.TestMembershipFaults -> (*github.com/djosh34/tigerstore/internal/sim.effects).check -> fmt.Sprintf -> fmt.newPrinter -> (*sync.Pool).Get -> (*sync.Pool).pin -> (*sync.Pool).pinSlow -> internal/runtime/atomic.StoreReluintptr

### internal/runtime/atomic.Storeint32

github.com/djosh34/tigerstore/internal/core.init -> hash/crc32.MakeTable -> runtime.gcMarkDone$4 -> runtime.startTheWorldWithSema -> runtime.procresize -> (*runtime.p).destroy -> (*runtime.timers).take -> (*internal/runtime/atomic.Int32).Store -> internal/runtime/atomic.Storeint32

### internal/runtime/atomic.Storeint64

runtime.init -> runtime.init#8 -> runtime.forcegchelper -> runtime.gcStart -> (*runtime.gcControllerState).startCycle -> (*internal/runtime/atomic.Int64).Store -> internal/runtime/atomic.Storeint64

### internal/runtime/atomic.StorepNoWB

runtime.init -> runtime.init#8 -> runtime.forcegchelper -> runtime.gcStart -> runtime.clearpools -> runtime.atomicstorep -> internal/runtime/atomic.StorepNoWB

### internal/runtime/atomic.Storeuintptr

runtime.init -> runtime.init#8 -> runtime.forcegchelper -> runtime.gcStart -> runtime.sweepone -> (*runtime.sweepLocked).sweep -> (*runtime.spanSet).push -> (*internal/runtime/atomic.Uintptr).Store -> internal/runtime/atomic.Storeuintptr

### internal/runtime/atomic.Xadd

runtime.init -> runtime.init#8 -> runtime.forcegchelper -> runtime.gcStart -> (*internal/runtime/atomic.Uint32).Add -> internal/runtime/atomic.Xadd

### internal/runtime/atomic.Xadd64

runtime.init -> runtime.init#8 -> runtime.forcegchelper -> runtime.gcStart -> runtime.sweepone -> (*runtime.sweepLocked).sweep -> internal/runtime/atomic.Xadd64

### internal/runtime/atomic.Xaddint32

runtime.init -> runtime.init#8 -> runtime.forcegchelper -> runtime.gcStart -> runtime.schedEnableUser -> runtime.startm -> runtime.pidleget -> (*internal/runtime/atomic.Int32).Add -> internal/runtime/atomic.Xaddint32

### internal/runtime/atomic.Xaddint64

runtime.init -> runtime.init#8 -> runtime.forcegchelper -> runtime.gcStart -> (*runtime.gcCPULimiterState).startGCTransition -> (*runtime.gcCPULimiterState).updateLocked -> (*internal/runtime/atomic.Int64).Add -> internal/runtime/atomic.Xaddint64

### internal/runtime/atomic.Xadduintptr

runtime.init -> runtime.init#8 -> runtime.forcegchelper -> runtime.gcStart -> runtime.sweepone -> (*internal/runtime/atomic.Uintptr).Add -> internal/runtime/atomic.Xadduintptr

### internal/runtime/atomic.Xchg

net/netip.init -> unique.Make[net/netip.addrDetail] -> (*unique.canonMap[net/netip.addrDetail]).LoadOrStore -> runtime.AddCleanup[net/netip.addrDetail, struct{}] -> (*runtime.cleanupQueue).createGs -> (*internal/runtime/atomic.Uint32).Swap -> internal/runtime/atomic.Xchg

### internal/runtime/atomic.Xchg8

runtime.init -> runtime.init#8 -> runtime.forcegchelper -> runtime.lock -> runtime.lockWithRank -> runtime.lock2 -> internal/runtime/atomic.Xchg8

### internal/runtime/atomic.Xchgint32

github.com/djosh34/tigerstore/internal/core.init -> hash/crc32.MakeTable -> runtime.gcResetMarkState -> runtime.forEachG -> runtime.exitsyscallNoP -> runtime.schedule -> runtime.findRunnable -> (*internal/runtime/atomic.Int32).Swap -> internal/runtime/atomic.Xchgint32

### internal/runtime/atomic.Xchgint64

github.com/djosh34/tigerstore/internal/core.init -> hash/crc32.MakeTable -> runtime.gcResetMarkState -> runtime.forEachG -> runtime.exitsyscallNoP -> runtime.schedule -> runtime.findRunnable -> (*internal/runtime/atomic.Int64).Swap -> internal/runtime/atomic.Xchgint64

### internal/runtime/atomic.casPointer

github.com/djosh34/tigerstore/internal/node.TestConcurrentCleanerSimulation -> (*testing.T).Run -> runtime.Goexit -> (*runtime._panic).nextDefer -> runtime.deferconvert -> (*internal/runtime/atomic.Pointer[runtime._defer]).CompareAndSwap -> (*internal/runtime/atomic.UnsafePointer).CompareAndSwap -> internal/runtime/atomic.casPointer

### internal/runtime/atomic.storePointer

runtime.init -> runtime.init#8 -> runtime.forcegchelper -> runtime.gcStart -> runtime.schedEnableUser -> runtime.startm -> runtime.newm -> runtime.allocm -> runtime.mcommoninit -> runtime.newMWeakPointer -> (*internal/runtime/atomic.Pointer[runtime.m]).Store -> (*internal/runtime/atomic.UnsafePointer).Store -> internal/runtime/atomic.storePointer

### internal/runtime/sys.DITEnabled

github.com/djosh34/tigerstore/internal/node.TestConcurrentCleanerSimulation -> (*testing.T).Run -> testing.tRunner -> testing.callerName -> testing.pcToName -> (*runtime.Frames).Next -> runtime.expandCgoFrames -> runtime.callCgoSymbolizer -> runtime.cgocall -> internal/runtime/sys.DITEnabled

### internal/runtime/sys.DisableDIT

github.com/djosh34/tigerstore/internal/node.TestConcurrentCleanerSimulation -> (*testing.T).Run -> testing.tRunner -> testing.callerName -> testing.pcToName -> (*runtime.Frames).Next -> runtime.expandCgoFrames -> runtime.callCgoSymbolizer -> runtime.cgocall -> internal/runtime/sys.DisableDIT

### internal/runtime/sys.EnableDIT

github.com/djosh34/tigerstore/internal/node.TestConcurrentCleanerSimulation -> (*testing.T).Run -> testing.tRunner -> testing.callerName -> testing.pcToName -> (*runtime.Frames).Next -> runtime.expandCgoFrames -> runtime.callCgoSymbolizer -> runtime.cgocall -> internal/runtime/sys.EnableDIT

### internal/runtime/sys.GetCallerPC

github.com/djosh34/tigerstore/internal/node.TestConcurrentCleanerSimulation -> (*testing.T).Run -> runtime.Goexit -> internal/runtime/sys.GetCallerPC

### internal/runtime/sys.GetCallerSP

github.com/djosh34/tigerstore/internal/node.TestConcurrentCleanerSimulation -> (*testing.T).Run -> runtime.Goexit -> internal/runtime/sys.GetCallerSP

### internal/runtime/syscall/linux.Syscall6

syscall.init -> syscall.init#1 -> syscall.Getrlimit -> syscall.prlimit1 -> syscall.RawSyscall6 -> internal/runtime/syscall/linux.Syscall6

### internal/sync.fatal

go.etcd.io/raft/v3.init -> log.New -> (*log.Logger).SetOutput -> (*sync.Mutex).Unlock -> (*internal/sync.Mutex).Unlock -> (*internal/sync.Mutex).unlockSlow -> internal/sync.fatal

### internal/sync.runtime_SemacquireMutex

go.etcd.io/raft/v3.init -> log.New -> (*log.Logger).SetOutput -> (*sync.Mutex).Lock -> (*internal/sync.Mutex).Lock -> (*internal/sync.Mutex).lockSlow -> internal/sync.runtime_SemacquireMutex

### internal/sync.runtime_Semrelease

go.etcd.io/raft/v3.init -> log.New -> (*log.Logger).SetOutput -> (*sync.Mutex).Unlock -> (*internal/sync.Mutex).Unlock -> (*internal/sync.Mutex).unlockSlow -> internal/sync.runtime_Semrelease

### internal/sync.runtime_canSpin

go.etcd.io/raft/v3.init -> log.New -> (*log.Logger).SetOutput -> (*sync.Mutex).Lock -> (*internal/sync.Mutex).Lock -> (*internal/sync.Mutex).lockSlow -> internal/sync.runtime_canSpin

### internal/sync.runtime_doSpin

go.etcd.io/raft/v3.init -> log.New -> (*log.Logger).SetOutput -> (*sync.Mutex).Lock -> (*internal/sync.Mutex).Lock -> (*internal/sync.Mutex).lockSlow -> internal/sync.runtime_doSpin

### internal/sync.runtime_nanotime

go.etcd.io/raft/v3.init -> log.New -> (*log.Logger).SetOutput -> (*sync.Mutex).Lock -> (*internal/sync.Mutex).Lock -> (*internal/sync.Mutex).lockSlow -> internal/sync.runtime_nanotime

### internal/sync.runtime_rand

expvar.init -> expvar.init#1 -> expvar.Publish -> (*sync.Map).LoadOrStore -> (*internal/sync.HashTrieMap[any, any]).LoadOrStore -> (*internal/sync.HashTrieMap[any, any]).init -> (*internal/sync.HashTrieMap[any, any]).initSlow -> internal/sync.runtime_rand

### internal/sync.throw

go.etcd.io/raft/v3.init -> log.New -> (*log.Logger).SetOutput -> (*sync.Mutex).Lock -> (*internal/sync.Mutex).Lock -> (*internal/sync.Mutex).lockSlow -> internal/sync.throw

### internal/synctest.IsInBubble

github.com/djosh34/tigerstore/internal/node.TestConcurrentCleanerSimulation -> github.com/djosh34/tigerstore/internal/node.TestConcurrentCleanerSimulation$1 -> github.com/djosh34/tigerstore/internal/node.TestConcurrentCleanerSimulation$1$1 -> github.com/djosh34/tigerstore/internal/node.concurrentCleaning -> github.com/djosh34/tigerstore/internal/node.concurrentCleaning$3 -> (*github.com/djosh34/tigerstore/internal/node.Node).Run -> (*sync.WaitGroup).Wait -> internal/synctest.IsInBubble

### internal/synctest.Run

github.com/djosh34/tigerstore/internal/node.TestConcurrentCleanerSimulation -> github.com/djosh34/tigerstore/internal/node.TestConcurrentCleanerSimulation$1 -> testing/synctest.Test -> internal/synctest.Run

### internal/synctest.associate

github.com/djosh34/tigerstore/internal/node.TestConcurrentCleanerSimulation -> github.com/djosh34/tigerstore/internal/node.TestConcurrentCleanerSimulation$1 -> github.com/djosh34/tigerstore/internal/node.TestConcurrentCleanerSimulation$1$1 -> github.com/djosh34/tigerstore/internal/node.concurrentCleaning -> github.com/djosh34/tigerstore/internal/node.concurrentCleaning$3 -> (*github.com/djosh34/tigerstore/internal/node.Node).Run -> (*sync.WaitGroup).Go -> (*sync.WaitGroup).Add -> internal/synctest.Associate[sync.WaitGroup] -> internal/synctest.associate

### internal/synctest.disassociate

github.com/djosh34/tigerstore/internal/node.TestConcurrentCleanerSimulation -> github.com/djosh34/tigerstore/internal/node.TestConcurrentCleanerSimulation$1 -> github.com/djosh34/tigerstore/internal/node.TestConcurrentCleanerSimulation$1$1 -> github.com/djosh34/tigerstore/internal/node.concurrentCleaning -> github.com/djosh34/tigerstore/internal/node.concurrentCleaning$3 -> (*github.com/djosh34/tigerstore/internal/node.Node).Run -> (*sync.WaitGroup).Wait -> internal/synctest.Disassociate[sync.WaitGroup] -> internal/synctest.disassociate

### internal/synctest.isAssociated

github.com/djosh34/tigerstore/internal/node.TestConcurrentCleanerSimulation -> github.com/djosh34/tigerstore/internal/node.TestConcurrentCleanerSimulation$1 -> github.com/djosh34/tigerstore/internal/node.TestConcurrentCleanerSimulation$1$1 -> github.com/djosh34/tigerstore/internal/node.concurrentCleaning -> github.com/djosh34/tigerstore/internal/node.concurrentCleaning$3 -> (*github.com/djosh34/tigerstore/internal/node.Node).Run -> (*sync.WaitGroup).Wait -> internal/synctest.IsAssociated[sync.WaitGroup] -> internal/synctest.isAssociated

### internal/syscall/unix.vgetrandom

crypto/fips140.init -> (*internal/godebug.Setting).Value -> (*sync.Once).Do -> (*sync.Once).doSlow -> net.readServices -> (*net.file).readLine -> io.ReadFull -> io.ReadAtLeast -> (crypto/internal/rand.reader).Read -> crypto/internal/fips140/drbg.Read -> crypto/internal/sysrand.Read -> crypto/internal/sysrand.read -> internal/syscall/unix.GetRandom -> internal/syscall/unix.vgetrandom

### sync.runtime_SemacquireWaitGroup

github.com/djosh34/tigerstore/internal/node.TestConcurrentCleanerSimulation -> github.com/djosh34/tigerstore/internal/node.TestConcurrentCleanerSimulation$1 -> github.com/djosh34/tigerstore/internal/node.TestConcurrentCleanerSimulation$1$1 -> github.com/djosh34/tigerstore/internal/node.concurrentCleaning -> github.com/djosh34/tigerstore/internal/node.concurrentCleaning$3 -> (*github.com/djosh34/tigerstore/internal/node.Node).Run -> (*sync.WaitGroup).Wait -> sync.runtime_SemacquireWaitGroup

### time.runtimeIsBubbled

github.com/djosh34/tigerstore/internal/node.TestConcurrentCleanerSimulation -> github.com/djosh34/tigerstore/internal/node.TestConcurrentCleanerSimulation$1 -> github.com/djosh34/tigerstore/internal/node.TestConcurrentCleanerSimulation$1$1 -> github.com/djosh34/tigerstore/internal/node.concurrentCleaning -> github.com/djosh34/tigerstore/internal/node.concurrentCleaning$6 -> (*github.com/djosh34/tigerstore/internal/node.Node).Serve -> time.Since -> time.runtimeIsBubbled

### time.runtimeNow

github.com/djosh34/tigerstore/internal/sim.TestMembershipFaults -> github.com/djosh34/tigerstore/internal/sim.TestMembershipFaults$1 -> github.com/djosh34/tigerstore/internal/sim.agree -> github.com/djosh34/tigerstore/check.Witnessed -> time.Now -> time.runtimeNow

### weak.runtime_makeStrongFromWeak

google.golang.org/protobuf/reflect/protodesc.init -> google.golang.org/protobuf/reflect/protodesc.init#1 -> fmt.Fprintf -> (*crypto/tls.Conn).Write -> (*crypto/tls.Conn).Handshake -> (*crypto/tls.Conn).HandshakeContext -> (*crypto/tls.Conn).handshakeContext -> (*crypto/tls.Conn).clientHandshake$bound -> (*crypto/tls.Conn).clientHandshake -> (*crypto/tls.clientHandshakeStateTLS13).handshake -> (*crypto/tls.clientHandshakeStateTLS13).readServerCertificate -> (*crypto/tls.Conn).verifyServerCertificate -> (*crypto/tls.weakCertCache).newCert -> (weak.Pointer[crypto/x509.Certificate]).Value -> weak.runtime_makeStrongFromWeak

### weak.runtime_registerWeakPointer

google.golang.org/protobuf/reflect/protodesc.init -> google.golang.org/protobuf/reflect/protodesc.init#1 -> fmt.Fprintf -> (*crypto/tls.Conn).Write -> (*crypto/tls.Conn).Handshake -> (*crypto/tls.Conn).HandshakeContext -> (*crypto/tls.Conn).handshakeContext -> (*crypto/tls.Conn).clientHandshake$bound -> (*crypto/tls.Conn).clientHandshake -> (*crypto/tls.clientHandshakeStateTLS13).handshake -> (*crypto/tls.clientHandshakeStateTLS13).readServerCertificate -> (*crypto/tls.Conn).verifyServerCertificate -> (*crypto/tls.weakCertCache).newCert -> weak.Make[crypto/x509.Certificate] -> weak.runtime_registerWeakPointer

## Additional bodyless symbols in the requested namespaces

This includes existing working hooks as well as newly moved crypto symbols; bodylessness alone does not mean missing implementation.

### crypto/internal/fips140.fatal

frontier; crypto/internal/fips140/aes.init -> crypto/internal/fips140/aes.init#2 -> crypto/internal/fips140.CAST -> crypto/internal/fips140.fatal

### crypto/internal/fips140.getIndicator

frontier; crypto/internal/fips140/check.init -> crypto/internal/fips140/check.init#1 -> crypto/internal/fips140/hmac.New[*crypto/internal/fips140/sha256.Digest] -> (*crypto/internal/fips140/sha256.Digest).Sum -> crypto/internal/fips140.RecordApproved -> crypto/internal/fips140.getIndicator

### crypto/internal/fips140.setIndicator

frontier; crypto/internal/fips140/check.init -> crypto/internal/fips140/check.init#1 -> (*crypto/internal/fips140/hmac.HMAC).Sum -> crypto/internal/fips140.RecordNonApproved -> crypto/internal/fips140.setIndicator

### crypto/internal/fips140/nistec.p256FromMont

frontier; crypto/internal/fips140/aes.init -> crypto/internal/fips140/aes.init#2 -> crypto/internal/fips140.CAST -> crypto/internal/fips140/ecdh.init$1$1 -> crypto/internal/fips140/ecdh.ecdh[*crypto/internal/fips140/nistec.P256Point] -> (*crypto/internal/fips140/nistec.P256Point).SetBytes -> crypto/internal/fips140/nistec.p256FromMont

### crypto/internal/fips140/nistec.p256MovCond

frontier; crypto/internal/fips140/aes.init -> crypto/internal/fips140/aes.init#2 -> crypto/internal/fips140.CAST -> crypto/internal/fips140/ecdsa.init$6$1 -> crypto/internal/fips140/ecdsa.verify[*crypto/internal/fips140/nistec.P256Point] -> crypto/internal/fips140/ecdsa.verifyGeneric[*crypto/internal/fips140/nistec.P256Point] -> (*crypto/internal/fips140/nistec.P256Point).Add -> crypto/internal/fips140/nistec.p256MovCond

### crypto/internal/fips140/nistec.p256Mul

frontier; crypto/internal/fips140/aes.init -> crypto/internal/fips140/aes.init#2 -> crypto/internal/fips140.CAST -> crypto/internal/fips140/ecdh.init$1$1 -> crypto/internal/fips140/ecdh.ecdh[*crypto/internal/fips140/nistec.P256Point] -> (*crypto/internal/fips140/nistec.P256Point).SetBytes -> crypto/internal/fips140/nistec.p256Mul

### crypto/internal/fips140/nistec.p256NegCond

frontier; crypto/internal/fips140/aes.init -> crypto/internal/fips140/aes.init#2 -> crypto/internal/fips140.CAST -> crypto/internal/fips140/ecdh.init$1$1 -> crypto/internal/fips140/ecdh.ecdh[*crypto/internal/fips140/nistec.P256Point] -> (*crypto/internal/fips140/nistec.P256Point).SetBytes -> crypto/internal/fips140/nistec.p256NegCond

### crypto/internal/fips140/nistec.p256PointAddAffineAsm

frontier; crypto/internal/fips140/aes.init -> crypto/internal/fips140/aes.init#2 -> crypto/internal/fips140.CAST -> crypto/internal/fips140/ecdsa.init$6$1 -> crypto/internal/fips140/ecdsa.verify[*crypto/internal/fips140/nistec.P256Point] -> crypto/internal/fips140/ecdsa.verifyGeneric[*crypto/internal/fips140/nistec.P256Point] -> (*crypto/internal/fips140/nistec.P256Point).ScalarBaseMult -> (*crypto/internal/fips140/nistec.P256Point).p256BaseMult -> crypto/internal/fips140/nistec.p256PointAddAffineAsm

### crypto/internal/fips140/nistec.p256PointAddAsm

frontier; crypto/internal/fips140/aes.init -> crypto/internal/fips140/aes.init#2 -> crypto/internal/fips140.CAST -> crypto/internal/fips140/ecdsa.init$6$1 -> crypto/internal/fips140/ecdsa.verify[*crypto/internal/fips140/nistec.P256Point] -> crypto/internal/fips140/ecdsa.verifyGeneric[*crypto/internal/fips140/nistec.P256Point] -> (*crypto/internal/fips140/nistec.P256Point).Add -> crypto/internal/fips140/nistec.p256PointAddAsm

### crypto/internal/fips140/nistec.p256PointDoubleAsm

frontier; crypto/internal/fips140/aes.init -> crypto/internal/fips140/aes.init#2 -> crypto/internal/fips140.CAST -> crypto/internal/fips140/ecdsa.init$6$1 -> crypto/internal/fips140/ecdsa.verify[*crypto/internal/fips140/nistec.P256Point] -> crypto/internal/fips140/ecdsa.verifyGeneric[*crypto/internal/fips140/nistec.P256Point] -> (*crypto/internal/fips140/nistec.P256Point).Add -> crypto/internal/fips140/nistec.p256PointDoubleAsm

### crypto/internal/fips140/nistec.p256Select

frontier; crypto/internal/fips140/aes.init -> crypto/internal/fips140/aes.init#2 -> crypto/internal/fips140.CAST -> crypto/internal/fips140/ecdh.init$1$1 -> crypto/internal/fips140/ecdh.ecdh[*crypto/internal/fips140/nistec.P256Point] -> (*crypto/internal/fips140/nistec.P256Point).ScalarMult -> (*crypto/internal/fips140/nistec.P256Point).p256ScalarMult -> crypto/internal/fips140/nistec.p256Select

### crypto/internal/fips140/nistec.p256SelectAffine

frontier; crypto/internal/fips140/aes.init -> crypto/internal/fips140/aes.init#2 -> crypto/internal/fips140.CAST -> crypto/internal/fips140/ecdsa.init$6$1 -> crypto/internal/fips140/ecdsa.verify[*crypto/internal/fips140/nistec.P256Point] -> crypto/internal/fips140/ecdsa.verifyGeneric[*crypto/internal/fips140/nistec.P256Point] -> (*crypto/internal/fips140/nistec.P256Point).ScalarBaseMult -> (*crypto/internal/fips140/nistec.P256Point).p256BaseMult -> crypto/internal/fips140/nistec.p256SelectAffine

### crypto/internal/fips140/nistec.p256Sqr

frontier; crypto/internal/fips140/aes.init -> crypto/internal/fips140/aes.init#2 -> crypto/internal/fips140.CAST -> crypto/internal/fips140/ecdh.init$1$1 -> crypto/internal/fips140/ecdh.ecdh[*crypto/internal/fips140/nistec.P256Point] -> (*crypto/internal/fips140/nistec.P256Point).SetBytes -> crypto/internal/fips140/nistec.p256CheckOnCurve -> crypto/internal/fips140/nistec.p256Sqr

### sync.fatal

frontier; expvar.init -> expvar.init#1 -> expvar.Publish -> (*sync.RWMutex).Unlock -> sync.fatal

### sync.runtime_SemacquireRWMutex

frontier; expvar.init -> expvar.init#1 -> expvar.Publish -> (*sync.RWMutex).Lock -> sync.runtime_SemacquireRWMutex

### sync.runtime_SemacquireRWMutexR

frontier; github.com/djosh34/tigerstore/internal/sim.init -> os.Getenv -> syscall.Getenv -> (*sync.RWMutex).RLock -> sync.runtime_SemacquireRWMutexR

### sync.runtime_Semrelease

frontier; expvar.init -> expvar.init#1 -> expvar.Publish -> (*sync.RWMutex).Unlock -> sync.runtime_Semrelease

### sync.runtime_notifyListAdd

frontier; crypto/fips140.init -> (*internal/godebug.Setting).Value -> (*sync.Once).Do -> (*sync.Once).doSlow -> net.readServices -> (*net.file).readLine -> io.ReadFull -> io.ReadAtLeast -> (net/http/internal/http2.transportResponseBody).Read -> (*net/http/internal/http2.pipe).Read -> (*sync.Cond).Wait -> sync.runtime_notifyListAdd

### sync.runtime_notifyListCheck

frontier; sync.init -> sync.init#2 -> sync.runtime_notifyListCheck

### sync.runtime_notifyListNotifyAll

frontier; crypto/fips140.init -> (*internal/godebug.Setting).Value -> (*sync.Once).Do -> (*sync.Once).doSlow -> net.readServices -> (*net.file).readLine -> io.ReadFull -> io.ReadAtLeast -> (net/http/internal/http2.transportResponseBody).Read -> (*net/http/internal/http2.clientStream).abortStream -> (*net/http/internal/http2.clientStream).abortStreamLocked -> (*sync.Cond).Broadcast -> sync.runtime_notifyListNotifyAll

### sync.runtime_notifyListNotifyOne

frontier; crypto/fips140.init -> (*internal/godebug.Setting).Value -> (*sync.Once).Do -> (*sync.Once).doSlow -> (*net/http/internal/http2.requestBody).Close$1 -> (*net/http/internal/http2.pipe).BreakWithError -> (*net/http/internal/http2.pipe).closeWithError -> (*sync.Cond).Signal -> sync.runtime_notifyListNotifyOne

### sync.runtime_notifyListWait

frontier; crypto/fips140.init -> (*internal/godebug.Setting).Value -> (*sync.Once).Do -> (*sync.Once).doSlow -> net.readServices -> (*net.file).readLine -> io.ReadFull -> io.ReadAtLeast -> (net/http/internal/http2.transportResponseBody).Read -> (*net/http/internal/http2.pipe).Read -> (*sync.Cond).Wait -> sync.runtime_notifyListWait

### sync.runtime_procPin

frontier; github.com/djosh34/tigerstore/internal/sim.TestMembershipFaults -> (*github.com/djosh34/tigerstore/internal/sim.effects).check -> fmt.Sprintf -> fmt.newPrinter -> (*sync.Pool).Get -> (*sync.Pool).pin -> sync.runtime_procPin

### sync.runtime_procUnpin

frontier; github.com/djosh34/tigerstore/internal/sim.TestMembershipFaults -> (*github.com/djosh34/tigerstore/internal/sim.effects).check -> fmt.Sprintf -> fmt.newPrinter -> (*sync.Pool).Get -> sync.runtime_procUnpin

### sync.runtime_randn

frontier; github.com/djosh34/tigerstore/internal/sim.TestMembershipFaults -> (*github.com/djosh34/tigerstore/internal/sim.effects).check -> fmt.Sprintf -> (*fmt.pp).free -> (*sync.Pool).Put -> sync.runtime_randn

### sync.runtime_registerPoolCleanup

frontier; sync.init -> sync.init#1 -> sync.runtime_registerPoolCleanup

### time.Sleep

frontier; github.com/djosh34/tigerstore/internal/node.TestConcurrentCleanerSimulation -> github.com/djosh34/tigerstore/internal/node.TestConcurrentCleanerSimulation$1 -> github.com/djosh34/tigerstore/internal/node.TestConcurrentCleanerSimulation$1$1 -> github.com/djosh34/tigerstore/internal/node.concurrentCleaning -> time.Sleep

### time.newTimer

frontier; github.com/djosh34/tigerstore/internal/sim.TestMembershipFaults -> github.com/djosh34/tigerstore/internal/sim.TestMembershipFaults$1 -> github.com/djosh34/tigerstore/internal/sim.agree -> github.com/djosh34/tigerstore/check.Linearizable -> time.AfterFunc -> time.newTimer

### time.resetTimer

frontier; github.com/djosh34/tigerstore/internal/node.TestConcurrentCleanerSimulation -> github.com/djosh34/tigerstore/internal/node.TestConcurrentCleanerSimulation$1 -> github.com/djosh34/tigerstore/internal/node.TestConcurrentCleanerSimulation$1$1 -> github.com/djosh34/tigerstore/internal/node.concurrentCleaning -> github.com/djosh34/tigerstore/internal/node.concurrentCleaning$3 -> (*github.com/djosh34/tigerstore/internal/node.Node).Run -> (*sync.WaitGroup).Go -> (*sync.WaitGroup).Go$1 -> (*github.com/djosh34/tigerstore/internal/peer.sender).run$bound -> (*github.com/djosh34/tigerstore/internal/peer.sender).run -> (*github.com/djosh34/tigerstore/internal/peer.sender).dial -> (*net.Dialer).DialContext -> (*net.sysDialer).dialParallel -> (*time.Timer).Reset -> time.resetTimer

### time.runtimeNano

frontier; time.init -> time.runtimeNano

### time.stopTimer

frontier; github.com/djosh34/tigerstore/internal/sim.TestMembershipFaults -> github.com/djosh34/tigerstore/internal/sim.TestMembershipFaults$1 -> github.com/djosh34/tigerstore/internal/sim.agree -> github.com/djosh34/tigerstore/check.Linearizable -> (*time.Timer).Stop -> time.stopTimer

### unique.runtime_rand

cut; net/netip.init -> unique.Make[net/netip.addrDetail] -> unique.newCanonMap[net/netip.addrDetail] -> unique.runtime_rand

## Other selected namespace members

All other selected functions, with a candidate path each. These are additional namespace reachability, not missing hooks.

* `crypto/rand.Int` [frontier]: github.com/djosh34/tigerstore/internal/node.TestConcurrentCleanerSimulation -> github.com/djosh34/tigerstore/internal/node.TestConcurrentCleanerSimulation$1 -> github.com/djosh34/tigerstore/internal/node.TestConcurrentCleanerSimulation$1$1 -> github.com/djosh34/tigerstore/internal/node.concurrentCleaning -> github.com/djosh34/tigerstore/internal/node.newOnDisk -> (*github.com/djosh34/tigerstore/internal/node.Node).open -> github.com/djosh34/tigerstore/internal/core.New -> (*go.etcd.io/raft/v3.RawNode).Bootstrap -> (*go.etcd.io/raft/v3.raft).becomeFollower -> (*go.etcd.io/raft/v3.raft).reset -> (*go.etcd.io/raft/v3.raft).resetRandomizedElectionTimeout -> (*go.etcd.io/raft/v3.lockedRand).Intn -> crypto/rand.Int
* `crypto/rand.init` [frontier]: crypto/rand.init
* `encoding/json/jsontext.AllowDuplicateNames` [frontier]: github.com/djosh34/tigerstore/internal/sim.TestPlanted -> github.com/djosh34/tigerstore/internal/sim.TestPlanted$2 -> github.com/djosh34/tigerstore/internal/sim.runSeed -> github.com/djosh34/tigerstore/internal/sim.runWith -> github.com/djosh34/tigerstore/internal/sim.runWith$2 -> github.com/djosh34/tigerstore/internal/sim.explain -> github.com/djosh34/tigerstore/internal/sim.saveRun -> encoding/json.MarshalIndent -> encoding/json.appendIndent -> encoding/json/jsontext.AllowDuplicateNames
* `encoding/json/jsontext.AllowInvalidUTF8` [frontier]: github.com/djosh34/tigerstore/internal/sim.TestPlanted -> github.com/djosh34/tigerstore/internal/sim.TestPlanted$2 -> github.com/djosh34/tigerstore/internal/sim.runSeed -> github.com/djosh34/tigerstore/internal/sim.runWith -> github.com/djosh34/tigerstore/internal/sim.runWith$2 -> github.com/djosh34/tigerstore/internal/sim.explain -> github.com/djosh34/tigerstore/internal/sim.saveRun -> encoding/json.MarshalIndent -> encoding/json.appendIndent -> encoding/json/jsontext.AllowInvalidUTF8
* `encoding/json/jsontext.AppendFormat[[]byte]` [frontier]: github.com/djosh34/tigerstore/internal/sim.TestPlanted -> github.com/djosh34/tigerstore/internal/sim.TestPlanted$2 -> github.com/djosh34/tigerstore/internal/sim.runSeed -> github.com/djosh34/tigerstore/internal/sim.runWith -> github.com/djosh34/tigerstore/internal/sim.runWith$2 -> github.com/djosh34/tigerstore/internal/sim.explain -> github.com/djosh34/tigerstore/internal/sim.saveRun -> encoding/json.MarshalIndent -> encoding/json.appendIndent -> encoding/json/jsontext.AppendFormat[[]byte]
* `encoding/json/jsontext.AppendQuote[[]byte]` [frontier]: google.golang.org/grpc.init -> google.golang.org/grpc.init#1 -> google.golang.org/grpc.parseServiceConfig -> encoding/json.Marshal -> encoding/json/v2.Marshal -> encoding/json/v2.marshalEncode -> encoding/json/v2.makeStringArshaler$1 -> encoding/json/jsontext.AppendQuote[[]byte]
* `encoding/json/jsontext.AppendUnquote[encoding/json/jsontext.Value]` [frontier]: google.golang.org/grpc.init -> google.golang.org/grpc.init#1 -> google.golang.org/grpc.parseServiceConfig -> encoding/json.Unmarshal -> encoding/json/v2.Unmarshal -> encoding/json.transformUnmarshalError -> encoding/json/jsontext.AppendUnquote[encoding/json/jsontext.Value]
* `encoding/json/jsontext.Bool` [frontier]: google.golang.org/grpc.init -> google.golang.org/grpc.init#1 -> google.golang.org/grpc.parseServiceConfig -> encoding/json.Marshal -> encoding/json/v2.Marshal -> encoding/json/v2.marshalEncode -> encoding/json/v2.makeBoolArshaler$1 -> encoding/json/jsontext.Bool
* `encoding/json/jsontext.Float` [frontier]: google.golang.org/grpc.init -> google.golang.org/grpc.init#1 -> google.golang.org/grpc.parseServiceConfig -> encoding/json.Marshal -> encoding/json/v2.Marshal -> encoding/json/v2.marshalEncode -> encoding/json/v2.makeFloatArshaler$1 -> encoding/json/jsontext.Float
* `encoding/json/jsontext.Multiline` [frontier]: github.com/djosh34/tigerstore/internal/sim.TestPlanted -> github.com/djosh34/tigerstore/internal/sim.TestPlanted$2 -> github.com/djosh34/tigerstore/internal/sim.runSeed -> github.com/djosh34/tigerstore/internal/sim.runWith -> github.com/djosh34/tigerstore/internal/sim.runWith$2 -> github.com/djosh34/tigerstore/internal/sim.explain -> github.com/djosh34/tigerstore/internal/sim.saveRun -> encoding/json.MarshalIndent -> encoding/json.appendIndent -> encoding/json/jsontext.Multiline
* `encoding/json/jsontext.PreserveRawStrings` [frontier]: github.com/djosh34/tigerstore/internal/sim.TestPlanted -> github.com/djosh34/tigerstore/internal/sim.TestPlanted$2 -> github.com/djosh34/tigerstore/internal/sim.runSeed -> github.com/djosh34/tigerstore/internal/sim.runWith -> github.com/djosh34/tigerstore/internal/sim.runWith$2 -> github.com/djosh34/tigerstore/internal/sim.explain -> github.com/djosh34/tigerstore/internal/sim.saveRun -> encoding/json.MarshalIndent -> encoding/json.appendIndent -> encoding/json/jsontext.PreserveRawStrings
* `encoding/json/jsontext.String` [frontier]: encoding/json/jsontext.init -> encoding/json/jsontext.String
* `encoding/json/jsontext.WithIndent` [frontier]: github.com/djosh34/tigerstore/internal/sim.TestPlanted -> github.com/djosh34/tigerstore/internal/sim.TestPlanted$2 -> github.com/djosh34/tigerstore/internal/sim.runSeed -> github.com/djosh34/tigerstore/internal/sim.runWith -> github.com/djosh34/tigerstore/internal/sim.runWith$2 -> github.com/djosh34/tigerstore/internal/sim.explain -> github.com/djosh34/tigerstore/internal/sim.saveRun -> encoding/json.MarshalIndent -> encoding/json.appendIndent -> encoding/json/jsontext.WithIndent
* `encoding/json/jsontext.WithIndentPrefix` [frontier]: github.com/djosh34/tigerstore/internal/sim.TestPlanted -> github.com/djosh34/tigerstore/internal/sim.TestPlanted$2 -> github.com/djosh34/tigerstore/internal/sim.runSeed -> github.com/djosh34/tigerstore/internal/sim.runWith -> github.com/djosh34/tigerstore/internal/sim.runWith$2 -> github.com/djosh34/tigerstore/internal/sim.explain -> github.com/djosh34/tigerstore/internal/sim.saveRun -> encoding/json.MarshalIndent -> encoding/json.appendIndent -> encoding/json/jsontext.WithIndentPrefix
* `encoding/json/jsontext.appendEscapePointerName` [frontier]: google.golang.org/grpc.init -> google.golang.org/grpc.init#1 -> google.golang.org/grpc.parseServiceConfig -> encoding/json.Unmarshal -> encoding/json/v2.Unmarshal -> encoding/json/v2.unmarshalDecode -> encoding/json/v2.makeMethodArshaler$5 -> (*encoding/json/jsontext.decoderState).ReadValue -> encoding/json/jsontext.wrapWithObjectName -> encoding/json/jsontext.appendEscapePointerName
* `encoding/json/jsontext.getBufferedDecoder` [frontier]: google.golang.org/grpc.init -> google.golang.org/grpc.init#1 -> google.golang.org/grpc.parseServiceConfig -> encoding/json.Unmarshal -> encoding/json/v2.Unmarshal -> (encoding/json/jsontext.export).GetBufferedDecoder -> encoding/json/jsontext.getBufferedDecoder
* `encoding/json/jsontext.getBufferedEncoder` [frontier]: google.golang.org/grpc.init -> google.golang.org/grpc.init#1 -> google.golang.org/grpc.parseServiceConfig -> encoding/json.Marshal -> encoding/json/v2.Marshal -> (encoding/json/jsontext.export).GetBufferedEncoder -> encoding/json/jsontext.getBufferedEncoder
* `encoding/json/jsontext.getObjectMembers` [frontier]: google.golang.org/grpc.init -> google.golang.org/grpc.init#1 -> google.golang.org/grpc.parseServiceConfig -> encoding/json.Marshal -> encoding/json/v2.Marshal -> encoding/json/v2.marshalEncode -> encoding/json/v2.makeMethodArshaler$3 -> (*encoding/json/jsontext.Encoder).WriteValue -> (*encoding/json/jsontext.encoderState).WriteValue -> encoding/json/jsontext.mustReorderObjects -> encoding/json/jsontext.mustReorderObjectsFromDecoder -> encoding/json/jsontext.getObjectMembers
* `encoding/json/jsontext.init` [frontier]: encoding/json/jsontext.init
* `encoding/json/jsontext.init$1` [frontier]: github.com/djosh34/tigerstore/internal/sim.TestMembershipFaults -> (*github.com/djosh34/tigerstore/internal/sim.effects).check -> fmt.Sprintf -> fmt.newPrinter -> (*sync.Pool).Get -> encoding/json/jsontext.init$1
* `encoding/json/jsontext.init$2` [frontier]: github.com/djosh34/tigerstore/internal/sim.TestMembershipFaults -> (*github.com/djosh34/tigerstore/internal/sim.effects).check -> fmt.Sprintf -> fmt.newPrinter -> (*sync.Pool).Get -> encoding/json/jsontext.init$2
* `encoding/json/jsontext.init$3` [frontier]: github.com/djosh34/tigerstore/internal/sim.TestMembershipFaults -> (*github.com/djosh34/tigerstore/internal/sim.effects).check -> fmt.Sprintf -> fmt.newPrinter -> (*sync.Pool).Get -> encoding/json/jsontext.init$3
* `encoding/json/jsontext.init$4` [frontier]: github.com/djosh34/tigerstore/internal/sim.TestMembershipFaults -> (*github.com/djosh34/tigerstore/internal/sim.effects).check -> fmt.Sprintf -> fmt.newPrinter -> (*sync.Pool).Get -> encoding/json/jsontext.init$4
* `encoding/json/jsontext.init$5` [frontier]: github.com/djosh34/tigerstore/internal/sim.TestMembershipFaults -> (*github.com/djosh34/tigerstore/internal/sim.effects).check -> fmt.Sprintf -> fmt.newPrinter -> (*sync.Pool).Get -> encoding/json/jsontext.init$5
* `encoding/json/jsontext.init$6` [frontier]: github.com/djosh34/tigerstore/internal/sim.TestMembershipFaults -> (*github.com/djosh34/tigerstore/internal/sim.effects).check -> fmt.Sprintf -> fmt.newPrinter -> (*sync.Pool).Get -> encoding/json/jsontext.init$6
* `encoding/json/jsontext.mustReorderObjects` [frontier]: google.golang.org/grpc.init -> google.golang.org/grpc.init#1 -> google.golang.org/grpc.parseServiceConfig -> encoding/json.Marshal -> encoding/json/v2.Marshal -> encoding/json/v2.marshalEncode -> encoding/json/v2.makeMethodArshaler$3 -> (*encoding/json/jsontext.Encoder).WriteValue -> (*encoding/json/jsontext.encoderState).WriteValue -> encoding/json/jsontext.mustReorderObjects
* `encoding/json/jsontext.mustReorderObjectsFromDecoder` [frontier]: google.golang.org/grpc.init -> google.golang.org/grpc.init#1 -> google.golang.org/grpc.parseServiceConfig -> encoding/json.Marshal -> encoding/json/v2.Marshal -> encoding/json/v2.marshalEncode -> encoding/json/v2.makeMethodArshaler$3 -> (*encoding/json/jsontext.Encoder).WriteValue -> (*encoding/json/jsontext.encoderState).WriteValue -> encoding/json/jsontext.mustReorderObjects -> encoding/json/jsontext.mustReorderObjectsFromDecoder
* `encoding/json/jsontext.mustReorderObjectsFromDecoder$1` [frontier]: google.golang.org/grpc.init -> google.golang.org/grpc.init#1 -> google.golang.org/grpc.parseServiceConfig -> encoding/json.Marshal -> encoding/json/v2.Marshal -> encoding/json/v2.marshalEncode -> encoding/json/v2.makeMethodArshaler$3 -> (*encoding/json/jsontext.Encoder).WriteValue -> (*encoding/json/jsontext.encoderState).WriteValue -> encoding/json/jsontext.mustReorderObjects -> encoding/json/jsontext.mustReorderObjectsFromDecoder -> encoding/json/jsontext.mustReorderObjectsFromDecoder$1
* `encoding/json/jsontext.putBufferedDecoder` [frontier]: google.golang.org/grpc.init -> google.golang.org/grpc.init#1 -> google.golang.org/grpc.parseServiceConfig -> encoding/json.Unmarshal -> encoding/json/v2.Unmarshal -> (encoding/json/jsontext.export).PutBufferedDecoder -> encoding/json/jsontext.putBufferedDecoder
* `encoding/json/jsontext.putBufferedEncoder` [frontier]: google.golang.org/grpc.init -> google.golang.org/grpc.init#1 -> google.golang.org/grpc.parseServiceConfig -> encoding/json.Marshal -> encoding/json/v2.Marshal -> (encoding/json/jsontext.export).PutBufferedEncoder -> encoding/json/jsontext.putBufferedEncoder
* `encoding/json/jsontext.putObjectMembers` [frontier]: google.golang.org/grpc.init -> google.golang.org/grpc.init#1 -> google.golang.org/grpc.parseServiceConfig -> encoding/json.Marshal -> encoding/json/v2.Marshal -> encoding/json/v2.marshalEncode -> encoding/json/v2.makeMethodArshaler$3 -> (*encoding/json/jsontext.Encoder).WriteValue -> (*encoding/json/jsontext.encoderState).WriteValue -> encoding/json/jsontext.mustReorderObjects -> encoding/json/jsontext.mustReorderObjectsFromDecoder -> encoding/json/jsontext.putObjectMembers
* `encoding/json/jsontext.rawToken` [frontier]: encoding/json/jsontext.init -> encoding/json/jsontext.rawToken
* `encoding/json/jsontext.unescapePointerToken` [frontier]: github.com/djosh34/tigerstore/internal/sim.TestMembershipFaults -> github.com/djosh34/tigerstore/internal/sim.TestMembershipFaults$1 -> github.com/djosh34/tigerstore/internal/sim.agree -> (*encoding/json/v2.SemanticError).Error -> (encoding/json/jsontext.Pointer).LastToken -> encoding/json/jsontext.unescapePointerToken
* `encoding/json/jsontext.wrapSyntacticError` [frontier]: google.golang.org/grpc.init -> google.golang.org/grpc.init#1 -> google.golang.org/grpc.parseServiceConfig -> encoding/json.Unmarshal -> encoding/json/v2.Unmarshal -> encoding/json/v2.unmarshalDecode -> (*encoding/json/jsontext.decoderState).CheckNextValue -> encoding/json/jsontext.wrapSyntacticError
* `encoding/json/jsontext.wrapWithArrayIndex` [frontier]: google.golang.org/grpc.init -> google.golang.org/grpc.init#1 -> google.golang.org/grpc.parseServiceConfig -> encoding/json.Unmarshal -> encoding/json/v2.Unmarshal -> encoding/json/v2.unmarshalDecode -> (*encoding/json/jsontext.decoderState).CheckNextValue -> (*encoding/json/jsontext.decoderState).consumeValue -> (*encoding/json/jsontext.decoderState).consumeArray -> encoding/json/jsontext.wrapWithArrayIndex
* `encoding/json/jsontext.wrapWithObjectName` [frontier]: google.golang.org/grpc.init -> google.golang.org/grpc.init#1 -> google.golang.org/grpc.parseServiceConfig -> encoding/json.Unmarshal -> encoding/json/v2.Unmarshal -> encoding/json/v2.unmarshalDecode -> encoding/json/v2.makeMethodArshaler$5 -> (*encoding/json/jsontext.decoderState).ReadValue -> encoding/json/jsontext.wrapWithObjectName
* `internal/cpu.Name` [frontier]: crypto/fips140.init -> (*internal/godebug.Setting).Value -> (*sync.Once).Do -> (*sync.Once).doSlow -> sync.OnceValue[string]$1$1 -> internal/sysinfo.init$1 -> internal/cpu.Name
* `internal/cpu.init` [frontier]: internal/cpu.init
* `runtime/debug.Stack` [cut]: github.com/djosh34/tigerstore/internal/node.TestConcurrentCleanerSimulation -> (*testing.T).Run -> testing.tRunner -> testing.tRunner$1 -> runtime/debug.Stack
* `runtime/debug.init` [frontier]: runtime/debug.init
* `unique.Make[net/netip.addrDetail]` [cut]: net/netip.init -> unique.Make[net/netip.addrDetail]
* `unique.buildArrayCloneSeq` [cut]: net/netip.init -> unique.Make[net/netip.addrDetail] -> unique.makeCloneSeq -> unique.buildArrayCloneSeq
* `unique.buildStructCloneSeq` [cut]: net/netip.init -> unique.Make[net/netip.addrDetail] -> unique.makeCloneSeq -> unique.buildStructCloneSeq
* `unique.clone[net/netip.addrDetail]` [cut]: net/netip.init -> unique.Make[net/netip.addrDetail] -> unique.clone[net/netip.addrDetail]
* `unique.init` [cut]: unique.init
* `unique.makeCloneSeq` [cut]: net/netip.init -> unique.Make[net/netip.addrDetail] -> unique.makeCloneSeq
* `unique.newCanonMap[net/netip.addrDetail]` [cut]: net/netip.init -> unique.Make[net/netip.addrDetail] -> unique.newCanonMap[net/netip.addrDetail]
* `unique.newEntryNode[net/netip.addrDetail]` [cut]: net/netip.init -> unique.Make[net/netip.addrDetail] -> (*unique.canonMap[net/netip.addrDetail]).LoadOrStore -> unique.newEntryNode[net/netip.addrDetail]
* `unique.newIndirectNode[net/netip.addrDetail]` [cut]: net/netip.init -> unique.Make[net/netip.addrDetail] -> unique.newCanonMap[net/netip.addrDetail] -> unique.newIndirectNode[net/netip.addrDetail]
