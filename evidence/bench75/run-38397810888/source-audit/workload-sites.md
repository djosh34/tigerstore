# Workload observed sites — FINAL immutable generator source, parent-confirmed

| File:line | Observation | Exact text |
|---|---|---|
| `experiments/bench75/load/main.go:96` | tls-config: client certificate + roots; TLS1.3 min/max | <code>cfg := &amp;tls.Config{RootCAs: pool, Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13}</code> |
| `experiments/bench75/load/main.go:72` | workers: 256 total closed-loop slots by default | <code>workers := flag.Int(&quot;workers&quot;, 256, &quot;closed-loop concurrency&quot;)</code> |
| `experiments/bench75/load/main.go:73` | connections: 16 active raw connections by default | <code>conns := flag.Int(&quot;conns&quot;, 16, &quot;connections&quot;)</code> |
| `experiments/bench75/load/main.go:100` | control-count: one grpc control connection when lean enabled | <code>controlConns = 1</code> |
| `experiments/bench75/load/main.go:121` | keyspace: 4096 keys | <code>const keyspace = 4096</code> |
| `experiments/bench75/load/main.go:125` | keys: Kubernetes-shaped key naming | <code>keys[i] = []byte(fmt.Sprintf(&quot;/registry/pods/ns-%03d/pod-%08d&quot;, i%64, i))</code> |
| `experiments/bench75/load/main.go:128` | values: 32 independent SHA256 chunks per 1KiB constant per-key value | <code>s := sha256.Sum256([]byte(fmt.Sprintf(&quot;pre50-independent-%d-%d&quot;, i, j)))</code> |
| `experiments/bench75/load/main.go:139` | cache-seed: untimed CAS cache seed before pool construction | <code>revisions, e = seedRevisions(clients[0], keys)</code> |
| `experiments/bench75/load/main.go:146` | raw-pool: pool construction warms raw connections before before probe | <code>lean, e = newLeanPool(*endpoint, cfg, *op, *workers, *conns, keys, values, revisions)</code> |
| `experiments/bench75/load/main.go:153` | before-revision: revision fence after startup writes | <code>before, e := probe()</code> |
| `experiments/bench75/load/main.go:262` | before-cpu: CPU/io sampling after warm and before start | <code>pre[i], e = sample(p)</code> |
| `experiments/bench75/load/main.go:279` | start: timed phase start after snapshots | <code>close(start)</code> |
| `experiments/bench75/load/cas.go:16` | compare: positive cached MOD EQUAL comparison | <code>Compare: []*pb.Compare{{Key: key, Target: pb.Compare_MOD, Result: pb.Compare_EQUAL, TargetUnion: &amp;pb.Compare_ModRevision{ModRevision: revision}}},</code> |
| `experiments/bench75/load/cas.go:17` | success-put: one same-key Put on success | <code>Success: []*pb.RequestOp{{Request: &amp;pb.RequestOp_RequestPut{RequestPut: &amp;pb.PutRequest{Key: key, Value: value}}}},</code> |
| `experiments/bench75/load/cas.go:18` | failure-range: one same-key value-returning point Range on failure | <code>Failure: []*pb.RequestOp{{Request: &amp;pb.RequestOp_RequestRange{RequestRange: &amp;pb.RangeRequest{Key: key}}}},</code> |
| `experiments/bench75/load/cas.go:25` | seed-range: untimed keys-only prefix cache seeding | <code>r, err := client.Range(ctx, &amp;pb.RangeRequest{Key: []byte(&quot;/registry/pods/&quot;), RangeEnd: []byte(&quot;/registry/pods0&quot;), KeysOnly: true})</code> |
| `experiments/bench75/load/cas.go:67` | validate-range: fallback validates shape, keys, exact values and positive revision | <code>if q == nil &#124;&#124; q.Header == nil &#124;&#124; q.Header.Revision != r.Header.Revision &#124;&#124; (q.Header.RaftTerm != 0 &amp;&amp; q.Header.RaftTerm != r.Header.RaftTerm) &#124;&#124; q.More &#124;&#124; q.Count != 1 &#124;&#124; len(q.Kvs) != 1 &#124;&#124; q.Kvs[0] == nil &#124;&#124; !bytes.Equal(q.Kvs[0].Key, key) &#124;&#124; !bytes.Equal(q.Kvs[0].Value, value) &#124;&#124; q.Kvs[0].ModRevision &lt;= 0 &#124;&#124; q.Kvs[0].ModRevision &gt; r.Header.Revision {</code> |
| `experiments/bench75/load/cas.go:76` | revision-monotonic: shared cache only advances monotonically | <code>if slot.CompareAndSwap(old, revision) {</code> |
| `experiments/bench75/load/newlean.go:42` | templates: prebuilt Put or CAS compare/branches templates | <code>func leanTemplates(keys, values [][]byte, op string) ([]leanTemplate, error) {</code> |
| `experiments/bench75/load/newlean.go:75` | cas-revision: minimal dynamic positive ModRevision varint | <code>n := binary.PutUvarint(v[:], uint64(revision))</code> |
| `experiments/bench75/load/newlean.go:77` | cas-length: dynamic Compare byte length | <code>dst = binary.AppendUvarint(dst, uint64(len(t.compare)+n))</code> |
| `experiments/bench75/load/newlean.go:128` | global-claim: bounded global operation claim, no retries | <code>if p.next.CompareAndSwap(i, i+1) {</code> |
| `experiments/bench75/load/newlean.go:198` | active-connections: active raw connection population from min(workers,conns) slot counts | <code>for _, slots := range leanSlotCounts(workers, conns) {</code> |
| `experiments/bench75/load/newlean.go:182` | connection-slots: total concurrency evenly spread across raw connections | <code>counts[i] = workers / len(counts)</code> |
| `experiments/bench75/load/newlean.go:224` | warm-rpc: one untimed raw RPC per connection | <code>l.commands &lt;- leanCommand{phase: &amp;leanPhase{start: start, total: 1, warm: true}, warmKey: i}</code> |
| `experiments/bench75/load/newlean.go:253` | fields: complete timed field list; no grpc-timeout | <code>l.fields = []hpack.HeaderField{{Name: &quot;:method&quot;, Value: &quot;POST&quot;}, {Name: &quot;:scheme&quot;, Value: &quot;https&quot;}, {Name: &quot;:path&quot;, Value: path}, {Name: &quot;:authority&quot;, Value: authority}, {Name: &quot;content-type&quot;, Value: &quot;application/grpc&quot;}, {Name: &quot;te&quot;, Value: &quot;trailers&quot;}, {Name: &quot;user-agent&quot;, Value: &quot;bench75-lean/1&quot;}}</code> |
| `experiments/bench75/load/newlean.go:296` | first-headers: prebuilt first HPACK block | <code>l.firstHeaders = bytes.Clone(l.headerBuf.Bytes())</code> |
| `experiments/bench75/load/newlean.go:303` | warm-headers: prebuilt warmed HPACK block | <code>l.headers = bytes.Clone(l.headerBuf.Bytes())</code> |
| `experiments/bench75/load/newlean.go:551` | permutation: global multiplicative permutation | <code>k := int((i * 2654435761) % uint64(len(l.keys)))</code> |
| `experiments/bench75/load/newlean.go:561` | cached-revision: uses shared positive cached revision without timed pre-read | <code>p.scratch, err = l.templates[k].casWire(p.scratch, l.revisions[k].Load())</code> |
| `experiments/bench75/load/newlean.go:589` | data-framing: actual DATA can fragment according to flow credit/maxFrame; no unconditional single-frame assertion | <code>if err := l.f.WriteData(id, end == len(p.request), p.request[p.sent:end]); err != nil {</code> |
| `experiments/bench75/load/newlean.go:30` | receive-credit-size: 64KiB response credit threshold | <code>const leanReceiveCredit = 64 &lt;&lt; 10</code> |
| `experiments/bench75/load/newlean.go:709` | connection-credit: coalesced connection receive credit, includes padding | <code>if l.received &gt;= leanReceiveCredit {</code> |
| `experiments/bench75/load/newlean.go:717` | stream-credit: coalesced stream receive credit for sufficiently large open replies | <code>if p.received &gt;= leanReceiveCredit &amp;&amp; !ev.end {</code> |
| `experiments/bench75/load/newlean.go:278` | deadline-renewal: renew 10s write deadline only when remaining time <=5s | <code>if l.writeDeadline.Sub(now) &gt; leanTimeout/2 {</code> |
| `experiments/bench75/load/newlean.go:733` | response-cache: valid response monotonically refreshes shared cache | <code>storeRevision(&amp;l.revisions[p.key], revision)</code> |
| `experiments/bench75/run.py:277` | five-reps: five paired repetitions | <code>for rep in range(1,6):</code> |
| `.github/workflows/bench75.yml:98` | lean-env: Actions timed generator choice | <code>GENERATOR: lean</code> |
| `.github/workflows/bench75.yml:88` | matrix: Put256 and CAS256 requested default suite | <code>include: ${{ fromJSON(inputs.suite == &#x27;put-cas-256&#x27; &amp;&amp; &#x27;[{&quot;workload&quot;:&quot;put&quot;,&quot;clients&quot;:256},{&quot;workload&quot;:&quot;cas&quot;,&quot;clients&quot;:256}]&#x27; &#124;&#124; inputs.suite == &#x27;put-64-256&#x27; &amp;&amp; &#x27;[{&quot;workload&quot;:&quot;put&quot;,&quot;clients&quot;:64},{&quot;workload&quot;:&quot;put&quot;,&quot;clients&quot;:256}]&#x27; &#124;&#124; &#x27;[{&quot;workload&quot;:&quot;put&quot;,&quot;clients&quot;:64},{&quot;workload&quot;:&quot;put&quot;,&quot;clients&quot;:256},{&quot;workload&quot;:&quot;cas&quot;,&quot;clients&quot;:256}]&#x27;) }}</code> |
