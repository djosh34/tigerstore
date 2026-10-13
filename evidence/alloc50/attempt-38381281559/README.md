# Second runner attempt

Run: https://github.com/djosh34/tigerstore/actions/runs/38381281559
Source: 191e12ca104c78f1646ee0481cec29ca209118b8.

Both jobs passed the ordinary correctness gates and isolated census processes. The shared lifetime race gate also passed. Both jobs completed all five pristine-base/ring/arena timing repetitions, with 60000 successful Puts per cohort and valid placement, revision, replica catch-up and term checks. Raw primary windows remain here; a later complete run is used for the headline comparison.

The jobs then stopped during stalled-client diagnostics: ring at 256 clients and arena at 64 clients hit the raw client's 10s write deadline while submitting its 4096-Put prefix. The fixture submitted all raw writes synchronously before starting healthy traffic, so these failures establish a sender stall, not a healthy-connection failure. Ring's 64-client diagnostic completed, pinning 256 slots, refusing 3840 additional streams and committing 256 slow Puts. Near-request-limit cases passed in the diagnostic processes that reached them.

The follow-up runs one bounded asynchronous raw sender and starts healthy traffic independently. It records planned versus actually transmitted requests, requires quota saturation and denial growth, keeps the client unread, verifies sustained bounded retention and healthy progress, and joins the sender after intentional close. It captures server goroutine stacks if the sender remains unfinished. No primary workload, managed allocator or Raft/WAL lifetime change is needed for that fixture change. Any pre-close sender error remains a failure.

Raw outputs, profiles, matching production and census symbols, source manifests and logs are retained in `c64`, `c256` and `alloc50-lifetime-race`. The initial artifacts are also available from the linked Actions run.
