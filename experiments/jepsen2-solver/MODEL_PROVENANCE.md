# Model source provenance

The supplied `sources/register.clj` has the existing serial WGL workload checker wrapper. It is not a byte-for-byte copy of the whole original upstream file. Its client, `VersionedRegister` record, and generator are unchanged.

The original source at `jepsen-io/etcd` commit `b47cc401f27bfb7c28afd9ac088b5f0dca4501b8` is retained as `sources/pinned_register.clj`. Its Git blob is `bffd0d999afdd77ed6a7bef76c729710199c01dc` and its full-file SHA-256 is `02d9feff2d36ab137887d760184db6375cf83d5520158b0a322418af3aa58c92`.

The supplied checker-configured file has SHA-256 `72a57a361462b3ef2095e79b112c98b5aa75695ef6f1d152fcd000a7e60b4668`. The extracted `VersionedRegister` record has SHA-256 `9d4bbb5a3652132be1553cc3f7acb66b5c845b9a82c0922b6d77c9bfe6dd2919` in both files and in `offline/src/offline/versioned_register.clj`. The record comparison removes surrounding whitespace only. This proves the replay uses the original upstream model without a semantic change.

Archived runs through the full fresh matrix used the legacy field name `upstream_source_sha256` for the copied checker-configured file's hash. Their original output is preserved. This field was a provenance-label error, not a model error. Later runs record the original source hash, copied checker-source hash, and model-record hash separately.

The retained Knossos history source is byte-identical to commit `9242b067e78fc414832ac6acab8af44ddb5e0d60`. Its SHA-256 is `2ff845e401e1bc42d3455482ed9e7367b716a0eb33aa7bbf0b1c6eecfaea7780`. Its `complete` and `without-failures` functions justify the documented upstream failure handling.

`upstream-source-verification.json` records these independent GitHub blob comparisons. The parent separately confirmed the original, supplied, and replay record hashes. No solver encoding, state transition, certificate, or verdict changed as a result of this source audit.
