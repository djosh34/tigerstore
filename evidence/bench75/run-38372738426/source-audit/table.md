# PR77 final source-audit table

## Concise table — per successful ordinary 1 KiB Put

M = main, B = PR76 batch, W = PR77 wire, **exact rerun commits below**. Send and receive are separate operations; closes are separate. N is the number of successful Puts in a fully accounted cohort. A is actual admission batches; Q is persist-queue arrays; F is sync completion groups; O/I are queued-and-consumed outbound / delivered-and-consumed inbound peer messages on the leader; T is selected ticker receives; Sw/Rw are successful writer-wake sends / selected wake receives. These populations differ. No batch sizes or message counts were measured or inferred.

| Source-defined cost per Put | M | B | W |
|---|---:|---:|---:|
| Fixed non-amortized request/reply channel operations | **2 sends + 2 receives** | **0** | **0** |
| Admission channel send / receive | Included above | **A/N sends + A/N receives** | Same as B |
| Completion reply-channel send / receive | Included above | **0**, direct grouped call + atomic list | Same as B |
| Custom frontend writer-wake send / receive | Library transport excluded | **Sw/N sends + Rw/N receives**, conditional/coalesced | Same as B |
| `room` channel operations | — | **0: absent at final target** | **0: absent at final target** |
| Disk queue + sync completion send / receive | **(Q+F)/N sends + (Q+F)/N receives** | Same | Same |
| Leader peer queue + inbox send / receive | **(O+I)/N sends + (O+I)/N receives** | Same | Same |
| Background tick receive | **T/N receives** | Same | Same |
| Explicit repository channel closes on normal Put | **0** | **0** | **0** |
| Fresh explicit per-Put reply channels | **1** | **0** | **0** |
| Named direct leader value materializations, excluding peers/growth/transport | **4** | **5** | **3** |
| Additional verified Raft Entry.Data deep clone on leader | **1** | **1** | **1** |
| **Known leader payload-copy stage subtotal** | **5** | **6** | **4** |
| Known follower stages for one received, newly persisted, applied occurrence | **3** | **3** | **2** |
| Sender + that one follower (peer encode + follower stages) | **4** | **4** | **3** |
| Unenumerated transport/runtime copies and library lifecycle channels | **Excluded, not zero** | **Excluded, not zero** | **Excluded, not zero** |

**Not total physical memcpy counts or total channels/op.** The main and custom frontends have different excluded transport internals, so 5 versus 4 is not an end-to-end physical-copy comparison. The same-frontend B→W comparison removes **two known full-value materializations on the leader and one at apply on each follower**. Payload bytes do not amortize as 1/batch-size: an entry copy contains every Put's full 1,024 bytes.

