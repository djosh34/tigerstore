Part of #50. Stacked on #77; the base becomes `main` as #75, #76 and #77 merge.

The [benchmark on #75](https://github.com/djosh34/tigerstore/pull/75) measured 34.6 leader allocations per Put on #76 at 256 clients. This PR removes the per-request allocations that the batch path still has in the frontend and the core:

- **Header blocks.** x/net's `ReadMetaHeaders` built a `MetaHeadersFrame` with a fresh field slice for every request: 10 allocations per gRPC header block, measured with `testing.AllocsPerRun` on a grpc-go request. The reader now owns one `hpack.Decoder` whose emit function fills the request's fields directly. Once the fields are in the HPACK dynamic table, which happens from the second request on a connection, a block decodes with 0 allocations; `TestHeaderDecodeAllocs` holds that. The checks x/net made are kept: lowercase token field names, valid values, pseudo-fields first and only the known ones, and a 1 MiB limit on the decoded block. A block that breaks them resets its stream, and the HPACK state stays in step. CONTINUATION frames feed the same decoder; the Framer still checks their order.
- **Replies.** The Put replies of one completion batch are encoded into one shared buffer, each a clipped slice of it, instead of one buffer each.
- **Output items.** A stream's output slice starts with room for headers, message and trailers, so a unary reply no longer grows it twice.
- **Core bookkeeping.** `core.Node.SubmitAll` takes a batch's `pending` records in one allocation; the node driver uses it for `SubmitBatch`.

Tests: `go test -race -short` of server, node and core pass locally, 16 simulator seeds pass, and golangci-lint is clean. A Sol helper is benchmarking #77; this PR will get the same comparison.
