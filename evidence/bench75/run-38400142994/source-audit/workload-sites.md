# FINAL workload / accounting source sites
Commit `6148beab08e91a02c243ccd2d2b788ecdacd45fd`; engine bytes identical to `e3bc29f5de3b9184b8aae5f64ed290681d6e1d6f`. No runtime results inferred.
| File:line | Site/classification | Exact text |
|---|---|---|
| `experiments/bench75/load/main.go:96` | tls: TLS1.3 mTLS certificate/roots | <code>cfg := &amp;tls.Config{RootCAs: pool, Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13}</code> |
| `experiments/bench75/load/main.go:100` | control: one untimed grpc control connection | <code>controlConns = 1</code> |
| `experiments/bench75/load/main.go:121` | keyspace: 4096 existing keyspace | <code>const keyspace = 4096</code> |
| `experiments/bench75/load/main.go:125` | keys: original Kube-shaped keys | <code>keys[i] = []byte(fmt.Sprintf(&quot;/registry/pods/ns-%03d/pod-%08d&quot;, i%64, i))</code> |
| `experiments/bench75/load/main.go:128` | values: independent SHA256-derived constant 1KiB value per key | <code>s := sha256.Sum256([]byte(fmt.Sprintf(&quot;pre50-independent-%d-%d&quot;, i, j)))</code> |
| `experiments/bench75/load/main.go:146` | pool: untimed raw warm RPCs before accounting | <code>lean, e = newLeanPool(*endpoint, cfg, *op, *workers, *conns, keys, values, revisions)</code> |
| `experiments/bench75/load/main.go:153` | before: revision probe after warm writes | <code>before, e := probe()</code> |
| `experiments/bench75/load/main.go:262` | pre-cpu: CPU/IO sampling before phase start | <code>pre[i], e = sample(p)</code> |
| `experiments/bench75/load/main.go:279` | start: release measured phase | <code>close(start)</code> |
| `experiments/bench75/load/main.go:288` | post-cpu: CPU/IO sampling after measured phase | <code>post[i], e = sample(p)</code> |
| `experiments/bench75/load/main.go:36` | proc-stat: read cumulative process CPU counters; not inferred server syscalls | <code>b, e := os.ReadFile(fmt.Sprintf(&quot;/proc/%d/stat&quot;, pid))</code> |
| `experiments/bench75/load/main.go:57` | proc-io: read cumulative syscr/syscw counters | <code>io, e := os.ReadFile(fmt.Sprintf(&quot;/proc/%d/io&quot;, pid))</code> |
| `experiments/bench75/load/main.go:350` | cpu-per-op: CPU microseconds per completed op arithmetic | <code>cpus[i] = secs * 1e6 / float64(len(all))</code> |
| `experiments/bench75/load/main.go:383` | syscr-per-op: actual leader syscr delta/completed calls | <code>result[&quot;leader_syscr_per_op&quot;] = float64(post[0].Syscr-pre[0].Syscr) / float64(len(all))</code> |
| `experiments/bench75/load/main.go:384` | syscw-per-op: actual leader syscw delta/completed calls | <code>result[&quot;leader_syscw_per_op&quot;] = float64(post[0].Syscw-pre[0].Syscw) / float64(len(all))</code> |
| `experiments/bench75/load/newlean.go:182` | slot-count: 64/256 total slots split across 16 active raw connections | <code>counts[i] = workers / len(counts)</code> |
| `experiments/bench75/load/newlean.go:198` | active: one raw connection per slot-count entry | <code>for _, slots := range leanSlotCounts(workers, conns) {</code> |
| `experiments/bench75/load/newlean.go:224` | warm: one untimed warm RPC per raw connection, outside N | <code>l.commands &lt;- leanCommand{phase: &amp;leanPhase{start: start, total: 1, warm: true}, warmKey: i}</code> |
| `experiments/bench75/load/newlean.go:128` | global-claim: global bounded operation claim, no retries | <code>if p.next.CompareAndSwap(i, i+1) {</code> |
| `experiments/bench75/load/newlean.go:551` | permutation: original global permutation | <code>k := int((i * 2654435761) % uint64(len(l.keys)))</code> |
| `experiments/bench75/load/newlean.go:50` | put-template: prebuilt ordinary Put message | <code>r[k].put = grpcWire(p)</code> |
| `experiments/bench75/load/newlean.go:253` | fields: full timed metadata list omits grpc-timeout | <code>l.fields = []hpack.HeaderField{{Name: &quot;:method&quot;, Value: &quot;POST&quot;}, {Name: &quot;:scheme&quot;, Value: &quot;https&quot;}, {Name: &quot;:path&quot;, Value: path}, {Name: &quot;:authority&quot;, Value: authority}, {Name: &quot;content-type&quot;, Value: &quot;application/grpc&quot;}, {Name: &quot;te&quot;, Value: &quot;trailers&quot;}, {Name: &quot;user-agent&quot;, Value: &quot;bench75-lean/1&quot;}}</code> |
| `experiments/bench75/load/newlean.go:296` | hpack-first: first HPACK template | <code>l.firstHeaders = bytes.Clone(l.headerBuf.Bytes())</code> |
| `experiments/bench75/load/newlean.go:303` | hpack-warm: warm HPACK template | <code>l.headers = bytes.Clone(l.headerBuf.Bytes())</code> |
| `experiments/bench75/load/newlean.go:589` | data: DATA may fragment for actual flow credit/maxFrame | <code>if err := l.f.WriteData(id, end == len(p.request), p.request[p.sent:end]); err != nil {</code> |
| `experiments/bench75/load/newlean.go:30` | credit-size: 64KiB coalesced receive credit | <code>const leanReceiveCredit = 64 &lt;&lt; 10</code> |
| `experiments/bench75/load/newlean.go:709` | conn-credit: coalesced connection WINDOW_UPDATE | <code>if l.received &gt;= leanReceiveCredit {</code> |
| `experiments/bench75/load/newlean.go:717` | stream-credit: coalesced open-stream receive credit | <code>if p.received &gt;= leanReceiveCredit &amp;&amp; !ev.end {</code> |
| `experiments/bench75/load/newlean.go:278` | write-deadline: cached 10s deadline renew only <=5s remaining, no grpc-timeout header | <code>if l.writeDeadline.Sub(now) &gt; leanTimeout/2 {</code> |
| `experiments/bench75/run.py:277` | five-reps: five reps per selected workload and store | <code>for rep in range(1,6):</code> |
| `.github/workflows/bench75.yml:88` | matrix: put-64-256 suite selects two Put jobs | <code>include: ${{ fromJSON(inputs.suite == &#x27;put-cas-256&#x27; &amp;&amp; &#x27;[{&quot;workload&quot;:&quot;put&quot;,&quot;clients&quot;:256},{&quot;workload&quot;:&quot;cas&quot;,&quot;clients&quot;:256}]&#x27; &#124;&#124; inputs.suite == &#x27;put-64-256&#x27; &amp;&amp; &#x27;[{&quot;workload&quot;:&quot;put&quot;,&quot;clients&quot;:64},{&quot;workload&quot;:&quot;put&quot;,&quot;clients&quot;:256}]&#x27; &#124;&#124; &#x27;[{&quot;workload&quot;:&quot;put&quot;,&quot;clients&quot;:64},{&quot;workload&quot;:&quot;put&quot;,&quot;clients&quot;:256},{&quot;workload&quot;:&quot;cas&quot;,&quot;clients&quot;:256}]&#x27;) }}</code> |
| `.github/workflows/bench75.yml:98` | lean-env: raw lean generator selected | <code>GENERATOR: lean</code> |
