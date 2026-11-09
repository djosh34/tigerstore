# Follow-up: AWS-LC dispatch and kTLS sendfile opt-in

Run https://github.com/djosh34/tigerstore/actions/runs/42698811772,
commit `4fc7d736`. Three pinned repetitions; median [min–max]. aes.c verifies
an authenticated encrypt/decrypt round trip outside timing at every size.

## Best measured AES-GCM, not a claim of globally optimal assembly

AWS-LC `bab562f6ad169475bd46e08de57e6d5cde239fef` normally excludes N2 from its
8x path. Its environment override refuses forging a V1 capability (the earlier
run's empty forced-output files are **failures**, not data). The follow-up
archives `aws-8x-dispatch.patch`: change the GCM-only 8x predicate to permit
already-detected SHA3 on N2, rebuild, and measure. No ISA capabilities are
added. This is an experiment, not a production config recommendation.

The selected implementation is
[`crypto/fipsmodule/modes/asm/aesv8-gcm-armv8-unroll8.pl`](https://github.com/aws/aws-lc/blob/bab562f6ad169475bd46e08de57e6d5cde239fef/crypto/fipsmodule/modes/asm/aesv8-gcm-armv8-unroll8.pl),
with generated Linux arm64 assembly under `generated-src/linux-aarch64/crypto/fipsmodule/`.
The dispatch restriction is
[`cpucap/internal.h:242–247`](https://github.com/aws/aws-lc/blob/bab562f6ad169475bd46e08de57e6d5cde239fef/crypto/fipsmodule/cpucap/internal.h#L242-L247);
key-size and length dispatch is
[`modes/gcm.c:104–139`](https://github.com/aws/aws-lc/blob/bab562f6ad169475bd46e08de57e6d5cde239fef/crypto/fipsmodule/modes/gcm.c#L104-L139).

| Harness / library | AES128 1 KiB GB/s | AES128 16 KiB GB/s | AES256 1 KiB GB/s | AES256 16 KiB GB/s |
|---|---:|---:|---:|---:|
| Go 1.27.2 Seal, main run | 3.614 [3.610–3.614] | 4.181 [4.180–4.182] | 3.164 [3.164–3.164] | 3.569 [3.568–3.569] |
| OpenSSL 3.0.13 EVP, process CPU, main run | 2.002 [2.002–2.004] | 3.352 [3.352–3.352] | 1.875 [1.874–1.876] | 3.018 [3.018–3.018] |
| AWS-LC default EVP, process CPU, follow-up | 2.855 [2.852–2.862] | 3.522 [3.522–3.523] | 2.591 [2.589–2.592] | 3.121 [3.119–3.121] |
| AWS-LC patched 8x EVP, process CPU | 3.631 [3.626–3.638] | 4.801 [4.801–4.801] | 3.186 [3.184–3.186] | 4.017 [4.016–4.017] |
| AWS-LC patched bssl speed EVP encrypt | 3.606 [3.605–3.608] | 4.798 [4.798–4.799] | 3.151 [3.150–3.152] | 4.012 [4.006–4.013] |

Process CPU comparator (same C EVP loop, includes per-message IV, 13-byte AAD,
finalization and tag; no syscalls in the hot loop):

| Variant | AES128 1 KiB CPU ns | AES128 16 KiB CPU ns | AES256 1 KiB CPU ns | AES256 16 KiB CPU ns |
|---|---:|---:|---:|---:|
| default | 358.71 [357.79–359.06] | 4651.63 [4650.19–4652.14] | 395.19 [395.11–395.52] | 5250.40 [5250.20–5252.45] |
| patched 8x | 282.04 [281.49–282.39] | 3412.73 [3412.61–3412.94] | 321.38 [321.36–321.60] | 4078.96 [4078.89–4080.15] |

At 16 KiB the patched path is ~15% faster than Go AES128 and ~13% faster for
AES256, not 3x. At 1 KiB it is essentially tied with Go. The AES128 16 KiB
saving vs Go is only about **0.51 µs per encrypted record** (~0.031 ns/byte),
before Go/C integration overhead. This is a primitive comparison, not a measured
mixed-workload benefit. Go uses nil AAD, whereas the C loop uses 13-byte AAD;
speed-tool harnesses have their own timing conventions.

## kTLS TLS_TX_ZEROCOPY_RO is not software zero-copy

TX sender process CPU ns/message, hot memfd -> kTLS socket:

| Option | 256 B | 1 KiB | 4 KiB | 16 KiB | 64 KiB |
|---|---:|---:|---:|---:|---:|
| normal sendfile | 4463 [4457–4480] | 4844 [4841–4849] | 6343 [6340–6360] | 11989 [11972–11993] | 42736 [42722–43141] |
| TLS_TX_ZEROCOPY_RO=1 | 4493 [4463–4510] | 4859 [4856–4880] | 6355 [6316–6392] | 12012 [11953–12020] | 42786 [42761–42891] |

Setting the option succeeds, but TLS stats count **zero device sessions** and
these CPU numbers are indistinguishable at this scale. Kernel docs explicitly
limit its true zero-copy behavior to device offload:
[`Documentation/networking/tls.rst:254–268`](https://github.com/torvalds/linux/blob/v7.0/Documentation/networking/tls.rst#L254-L268).
No true NIC/TLS offload or remote-network zero-copy was measured.

## Isolated kernel AES attempt

AF_ALG AEAD bind initially returns ENOENT. The module exists and CONFIG_CRYPTO_USER_API_AEAD=m,
but this Ubuntu image's `modprobe algif_aead` runs an `install /bin/false`
directive. kTLS is unaffected because it uses kernel-internal AEAD, not AF_ALG.
A disposable-runner follow-up explicitly loaded the module with
`sudo modprobe --ignore-install algif_aead` and succeeded:
https://github.com/djosh34/tigerstore/actions/runs/42699545613, commit `8c53321f`.
This temporary module load bypasses that image's default test-API policy; it is
not needed for kTLS or proposed for production. Three pinned repetitions:

| AF_ALG API (2 syscalls/message) | 256 B CPU ns | 1 KiB CPU ns | 4 KiB CPU ns | 16 KiB CPU ns | 64 KiB CPU ns |
|---|---:|---:|---:|---:|---:|
| AES128 gcm(aes) | 1620 [1617–1634] | 1844 [1829–1848] | 3216 [3200–3222] | 8239 [8229–8260] | 28516 [28509–28553] |
| AES256 gcm(aes) | 1622 [1616–1641] | 1877 [1871–1889] | 3387 [3384–3388] | 8962 [8952–8985] | 31439 [31434–31473] |

AES128 throughput is 1.989 [1.984–1.991] GB/s at 16 KiB and 2.298
[2.295–2.299] GB/s at 64 KiB; AES256 is 1.828 [1.823–1.830] and 2.085
[2.082–2.085] GB/s respectively. `/proc/crypto` lists the synchronous
`gcm-aes-ce` driver. **This includes two syscalls, kernel API allocation,
copying and buffer/scatterlist management; it is not isolated raw assembly
throughput.** The measured kernel interfaces are not faster than user-space
AES-GCM here, but these tests do not prove the kernel's AES inner loop alone
is slower. Likewise kTLS-minus-plain TCP also includes TLS framing, record
allocations/copying and changed wire/receive behavior.
