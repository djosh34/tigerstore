# Plan 5 review probes

Base: `bd1ade319a4d0bfc92082a8bf8d335aa01199428` (`origin/main`).
Branch: `exp/plan5-review-astra2`. These tests record current behavior;
they are not fixes and are not for merging.

On this arm64 host, Go 1.27.2:

```
GOMAXPROCS=2 GOFLAGS=-p=2 go test -count=1 -v ./internal/testcert ./cmd/tigerstore -run '^TestPlan5Review'
```

All three probes passed:

- The certificate helper used by `internal/sim/net.go` creates a CA and
  leaf that expire after 24 hours. Verification after a 25-hour clock
  jump returned `x509.CertificateInvalidError` with reason `x509.Expired`.
- A native TLS 1.3 handshake negotiated `TLS_AES_128_GCM_SHA256`.
- Three bootstrap lists with the same token and names but per-link proxy
  URLs produced three different cluster IDs. Saving and reading their
  identity files preserved them: `aae89461e544e976`, `65e2ee256ebe4828`,
  `9a25f2d7208e9ef2`. This models the differing initial-cluster strings
  in `cmd/chaos/cluster.go`, not a full rolling-upgrade run.

```
GODEBUG=cpu.aes=off GOMAXPROCS=2 GOFLAGS=-p=2 go test -count=1 -v ./internal/testcert -run '^TestPlan5ReviewTLSCipher$'
```

Passed, negotiating `TLS_CHACHA20_POLY1305_SHA256`. CPU capability changes
TLS negotiation, not just the implementation speed. This is a native
probe; no modified gosim fork or cross-architecture hash comparison ran.

No long test, campaign, or full release ran. The remaining review findings
come from the issue specifications and source inspection.
