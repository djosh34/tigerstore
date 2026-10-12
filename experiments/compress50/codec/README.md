# Compression probe

This is a throwaway Linux Go module for an `etcdctl get --prefix '' -w json` dump. It reads base64 keys and values from `kvs`. It never writes keys, values, compressed payloads, or trained dictionaries. Only aggregate CSV files leave the process. Unknown resource names become `other`; known Kubernetes resource names come from a fixed whitelist. Input parse errors do not include dump content. Keep the input and output directories private. Dictionary and codec buffers can contain credentials in process memory, so disable core dumps before running.

## Run

Use Go 1.27.2 or a compatible newer version. Dependencies are pinned in `go.mod` and `go.sum`.

```sh
go test ./...
go build -o codec-helper .
ulimit -c 0
taskset -c 0 env GOMAXPROCS=1 timeout --kill-after=2s 235s \
  ./codec-helper -input /private/dump.json -out /results
```

Choose a CPU in the process's allowed affinity set. `./run.sh /private/dump.json /results [cpu]` builds the executable and chooses the first allowed CPU by default. The caller must stop cluster activity before starting the measurements. The executable requires external `GOMAXPROCS=1` and does not change affinity itself.

The CLI accepts `-input`, `-out`, and optional `-duration 40ms`. Duration must be between 40ms and 100ms. It always runs five repetitions per configuration. The default targets 40ms for each encode and decode loop. The executable starts a 230-second budget when it begins reading the input. `run.sh` adds a 235-second hard timeout. Compilation is outside that timer. Slow calls can exceed the internal budget until they return. A hard timeout or error leaves partial aggregate files; use results only when the last `status` row in `metadata.csv` is `complete`. The probe was sized for roughly 4,000 values and 8 MiB, not unbounded dumps.

## Split and dictionary training

The first eight SHA-256 bytes of each decoded key form a big-endian u64. Keys whose number modulo five is zero enter `train20`; all others enter `test80`. The proportion is approximate, not an exact 20% quota. Duplicate keys are rejected. Sorting the input does not change membership.

Only `train20` values enter dictionary training. Training samples follow SHA-256 key order, with limits of 4,096 samples, 8 MiB total, and the first 256 KiB of any one value. Values shorter than eight bytes are excluded. Metadata reports eligible train values and actual training sample counts and bytes. Whole keys never enter the trainer.

`github.com/klauspost/compress/dict.BuildZstdDict` learns history from repeated sequences and trains Zstandard entropy tables. This is a trained dictionary, not arbitrary bytes passed as a raw dictionary. The maximum history size is 32 KiB; entropy tables can make the serialized dictionary larger. Levels 1 and 3 each get a separately trained dictionary and a fixed dictionary ID. Training gets no test values or synthetic controls. Insufficient or untrainable corpora produce an explicit skipped status. There is no raw-dictionary fallback. Dictionaries stay in memory.

The no-dictionary codecs test every value in `all`, plus the separate `train20` and `test80` cohorts. Dictionary codecs test only `test80`, its size subsets, and independent synthetic controls. No dictionary result includes training keys. `all` overlaps the two split cohorts and must not be summed with them.

## Inputs and codecs

Each single-value input contains the original value with no added framing. Batches target 16, 64, or 256 values. Each member contributes a little-endian u32 byte length followed by its value. Keys are not packed. The final batch can be smaller. Each cohort batches independently, so train and test values cannot enter the same batch.

Real batches use both a fixed shuffle with seed 42 and resource order followed by key order. Unknown resources sort as `other`. Batches can cross resource boundaries. Synthetic controls use only shuffled batches because their resource-sorted version would duplicate the same resource composition.

Actual value size subsets use inclusive ranges 896 to 1,152 bytes and 9,216 to 11,264 bytes. Each subset has single-value and 16/64/256 batch measurements, for both `all` and `test80`. Empty subsets appear in metadata and have no benchmark rows. Inventory includes the actual minimum and maximum sizes in every populated bin. No real value is padded or resized.

Independent synthetic controls contain 256 values at each exact size, 1,024 and 10,240 bytes. One control uses deterministic pseudo-random bytes. The other repeats a fixed 13-byte pattern. Controls get single-value and shuffled batch measurements. They are not part of the real inventory or training corpus.

Codecs are Zstandard levels 1 and 3 from `klauspost/compress`, with checksums and single-worker encoder and decoder state; `pierrec/lz4/v4` fast blocks; and `klauspost/compress/snappy` block format. `snappy.Encode` uses that package's default better-compression path, not Google's implementation or a stream encoder. Zstandard uses `EncodeAll` and `DecodeAll`, including normal frame overhead. Empty values produce real Zstandard frames.

LZ4 needs a wrapper because its block encoder signals incompressible input with a zero-length result. The wrapper stores a u32 original length and one byte for raw versus compressed data. It stores the original bytes when compression does not reduce their size. Reported LZ4 sizes and timing include this five-byte wrapper. Snappy reports block framing. These are not equal wire protocols.

## Measurements

Every configuration first encodes and decodes every input, compares all bytes, and checks packed length framing. This pass also computes exact compressed sizes for the entire corpus. It runs outside timed loops. Decode timing reads cached compressed inputs; it never includes encode work. Both directions reuse codec state and output buffers. Initial allocation, dictionary training, packing, validation, and compressed-input caching are outside timing.

Each timed loop completes whole corpus passes, with at least one full pass per repetition. It only checks the clock at pass boundaries. Every input therefore gets equal pass coverage instead of a prefix-weighted sample. A pass can make the loop longer than its target; very large inputs can exceed 100ms. Batch ns/value divides by actual member counts, including a short final batch. Ns/batch means one codec call. For single values, ns/value and ns/batch are equal. Packing and unpacking costs are not included in these codec-call timings.

Wall time uses Go's monotonic clock. CPU time uses Linux `getrusage(RUSAGE_SELF)` user plus system time, with microsecond resolution, around each timed loop. CPU time covers the process, including runtime work. GC stays enabled. `runtime.MemStats` differences give warmed-loop allocations and allocated bytes per codec call. Timer boundaries and CPU-accounting syscalls can add small overhead. No extra goroutines run benchmarks. Single-core affinity and `GOMAXPROCS=1` constrain runtime and codec execution.

`benchmark.csv` has one row per repetition. Corpus byte totals are repeated across the five rows, not five independent byte samples. Ratios are compressed bytes divided by original value bytes or framed input bytes; lower is better. A ratio above one means expansion. The dictionary-inclusive ratio adds one dictionary per entire reported corpus, not one per value or batch. It excludes key storage. Ratios with a zero denominator are blank. Wall and CPU ns/value and ns/batch are arithmetic per-loop averages, not latency percentiles. Aggregate repetitions by median and keep the individual rows to show variation. Dataset labels identify synthetic and held-out rows.

`inventory.csv` has split/resource/format/size-bin groups, count, decoded key bytes, original value bytes, their sum, and minimum/maximum value size. `*` means aggregation across that dimension. These overlapping rows must not all be summed. Resource labels are whitelisted types only, never namespaces or object names. Format counts distinguish the `k8s\x00` Kubernetes protobuf prefix, valid JSON, and `raw_other`. This is an envelope classification, not a protobuf parser. Encrypted values and unrelated binary values belong to `raw_other`.

`metadata.csv` records the split rule, sample counts, training wall/CPU time, dictionary byte counts and status, versions, configuration count, empty subsets, and completion status. There are no dictionary contents or input paths in metadata. Read the last row for repeated metadata names.

## Tests

`go test ./...` covers all codecs with empty, repeated, and incompressible inputs; real dictionary construction; split membership; resource redaction; size boundaries; deterministic packing; malformed framing; full-pass timing; CSV width; and aggregate inventory privacy. Tests use generated data, not a cluster dump.
