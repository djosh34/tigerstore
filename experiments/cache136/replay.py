#!/usr/bin/env python3
"""Replay #136's etcd read trace (keys and sizes only) against value caches."""
import bisect, collections, gzip, json, re, sys
D = sys.argv[1]
marks = {}
for l in open(f"{D}/marks.jsonl"):
    try: marks.update(json.loads(l))
    except Exception: pass

# --- writes and initial state
state = {}          # key -> (mod_rev, size)
snap_rev = 0
for l in gzip.open(f"{D}/snapshot.jsonl.gz", "rt"):
    o = json.loads(l)
    if "snap_rev" in o: snap_rev = o["snap_rev"]; continue
    k, mr, cr, ver, sz = o["s"]; state[k] = (mr, sz)
writes = [json.loads(l) for l in gzip.open(f"{D}/writes.jsonl.gz", "rt")]

# --- requests
ESC = re.compile(r'\\([0-7]{3}|x[0-9a-fA-F]{2}|.)')
def unq(s):
    def r(m):
        g = m.group(1)
        if len(g) == 3 and g.isdigit(): return chr(int(g, 8))
        if g[0] == 'x' and len(g) == 3: return chr(int(g[1:], 16))
        return {'n': '\n', 't': '\t', '"': '"', '\\': '\\'}.get(g, g)
    return ESC.sub(r, s)
FIELD = re.compile(r'(\w+):\s*(?:"((?:[^"\\]|\\.)*)"|([\w.-]+))')
import datetime
def ts(s):
    s = s.replace('Z', '+00:00')
    m = re.match(r'(.*\.\d{6})\d*(.*)', s)
    if m: s = m.group(1) + m.group(2)
    return datetime.datetime.fromisoformat(s).timestamp()
reqs = []
for l in gzip.open(f"{D}/requests.log.gz", "rt"):
    i = l.find('{')
    if i < 0: continue
    try: o = json.loads(l[i:])
    except Exception: continue
    if o.get("msg") != "request stats": continue
    reqs.append(o)
print("requests", len(reqs), "writes", len(writes), "snapshot keys", len(state), "snap_rev", snap_rev, file=sys.stderr)
methods = collections.Counter(o["response type"] for o in reqs)
print(json.dumps(methods.most_common()), file=sys.stderr)
if len(sys.argv) > 2:
    for o in reqs[:int(sys.argv[2])]: print(json.dumps(o))

def res(k):
    p = k.split('/')
    if len(p) < 3: return k
    if '.' in p[2] and len(p) > 3: return p[2] + '/' + p[3]
    return p[2]

def parse_content(s):
    f = collections.defaultdict(list)
    for m in FIELD.finditer(s):
        f[m.group(1)].append(unq(m.group(2)) if m.group(2) is not None else m.group(3))
    return f

# --- merged timeline
OVER = 40  # rough per-entry bytes on top of key and value
events = [(w["t"], 1, i) for i, w in enumerate(writes)] + [(ts(o["start time"]), 0, i) for i, o in enumerate(reqs)]
events.sort()
CUT = marks.get('apiserver_restart', 1e18) + 60
events = [e for e in events if e[0] <= CUT]
streams = collections.Counter()
try:
    for l in open(f"{D}/apiserver-metrics.txt"):
        m = re.match(r'etcd_requests_total\{(.*)\} (\S+)', l)
        if not m: continue
        lab = dict(re.findall(r'(\w+)="([^"]*)"', m.group(1)))
        if lab.get("operation") == "listStream": streams[lab.get("resource", "").split('.')[0]] += int(float(m.group(2)))
except FileNotFoundError: pass
print("listStream counts", dict(streams), file=sys.stderr)
rt = marks.get("apiserver_restart")
if rt:
    for name, n in streams.items():
        for j in range(n): events.append((rt + 5 + j, 2, name))
    events.sort(key=lambda e: (e[0], e[1]))
pos = {}   # vid -> (log pos, time)
L = 0
for k, (mr, sz) in sorted(state.items(), key=lambda x: x[1][0]):
    L += len(k) + sz + OVER
    pos[(k, mr)] = (L, marks.get("trace_start", 0) - 1)
pre_L = L
ops = []   # (kind, vid, size, key, t, L) ; kinds: w write, r read (with reason), x dead
skip = collections.Counter()
reqkinds = collections.Counter()
sorted_keys = sorted(state)
dirty = False
def phase(t):
    p = "setup"
    for name in ("workload_start", "churn_start", "apiserver_restart", "idle_start"):
        if name in marks and t >= marks[name]: p = name
    return p
for t, typ, i in events:
    if typ == 1:
        w = writes[i]
        k = w["kv"][0]; mr = w["kv"][1]
        p = w.get("p")
        if p:
            ops.append(("r", (k, p[1]), p[4], k, t, "prev_kv", L))
            ops.append(("x", (k, p[1]), p[4], k, t, None, L))
        if w["d"] == 1:
            state.pop(k, None); dirty = True
            L += len(k) + OVER
        else:
            if k not in state: dirty = True
            state[k] = (mr, w["kv"][4])
            L += len(k) + w["kv"][4] + OVER
            pos[(k, mr)] = (L, t)
            ops.append(("w", (k, mr), w["kv"][4], k, t, None, L))
        continue
    if typ == 2:
        # RangeStream LISTs are streams and leave no "request stats" line; kube-apiserver's
        # etcd_requests_total{operation="listStream"} counts them per resource. Place them
        # at the kube-apiserver restart as full-prefix reads (estimate).
        name = i
        for k in sorted(state):
            r = res(k)
            if r == name or r.endswith('/' + name) or (name == "services" and r == "services"):
                mr, sz = state[k]
                ops.append(("r", (k, mr), sz, k, t, "list_stream", L))
        reqkinds["listStream (from apiserver metrics)"] += 1
        continue
    o = reqs[i]
    m = o["response type"].rsplit('/', 1)[-1]
    c = parse_content(o.get("request content", ""))
    if m == "Range":
        key = c["key"][0] if c["key"] else ""
        if key == "\x00" or key == "": skip["full keyspace range (etcdctl)"] += 1; continue
        if c.get("keys_only") == ["true"] or c.get("count_only") == ["true"]:
            reqkinds["range keys/count only"] += 1; continue
        if c["range_end"]:
            end = c["range_end"][0]
            lim = int(c["limit"][0]) if c["limit"] else 0
            if dirty: sorted_keys = sorted(state); dirty = False
            a = bisect.bisect_left(sorted_keys, key)
            b = len(sorted_keys) if end == "\x00" else bisect.bisect_left(sorted_keys, end)
            ks = sorted_keys[a:b]
            if lim: ks = ks[:lim]
            reqkinds["list"] += 1
            why = "list"
            for k in ks:
                mr, sz = state[k]
                ops.append(("r", (k, mr), sz, k, t, why, L))
        else:
            if key in state:
                mr, sz = state[key]
                reqkinds["get (found)"] += 1
                ops.append(("r", (key, mr), sz, key, t, "get", L))
            else:
                reqkinds["get (absent)"] += 1
    elif m == "Txn":
        reqkinds["txn"] += 1
        s = o.get("request content", "")
        fi = s.find("failure")
        if fi >= 0 and "request_range" in s[fi:] and int(o.get("response size", 0)) > 150:
            k = parse_content(s[fi:])["key"][0]
            if k in state:
                mr, sz = state[k]
                reqkinds["txn failed (value returned)"] += 1
                ops.append(("r", (k, mr), sz, k, t, "txn_failure", L))
    else:
        reqkinds[m] += 1
print("pre-trace bytes", pre_L, "trace log bytes", L - pre_L, file=sys.stderr)
print(json.dumps(reqkinds.most_common()), json.dumps(skip), file=sys.stderr)

reads = [o for o in ops if o[0] == "r"]
MiB = 1 << 20
live_bytes = sum(sz for mr, sz in state.values())
print(f"\n## Trace\nlog bytes before trace {pre_L/MiB:.1f} MiB, during trace {(L-pre_L)/MiB:.1f} MiB, live values at end {live_bytes/MiB:.1f} MiB in {len(state)} keys")
dur = max(w['t'] for w in writes) - min(w['t'] for w in writes)
print(f"writes {len(writes)} over {dur:.0f} s; marks {json.dumps({k: round(v - marks.get('trace_start', 0), 1) for k, v in marks.items() if isinstance(v, float) and v > 1e9})}")
print("requests by kind:", json.dumps(reqkinds.most_common()))
print("value reads by reason and phase (count, MiB):")
agg = collections.defaultdict(lambda: [0, 0])
for o in reads:
    a = agg[(o[5], phase(o[4]))]; a[0] += 1; a[1] += o[2]
for k in sorted(agg): print(f"  {k[0]:12} {k[1]:18} {agg[k][0]:8} {agg[k][1]/MiB:8.2f}")
print("value reads by resource (count, MiB, live MiB at end, reads per live byte):")
byres = collections.defaultdict(lambda: [0, 0, 0, 0])
for o in reads:
    a = byres[res(o[3])]; a[0] += 1; a[1] += o[2]
    if o[5] == "prev_kv": a[3] += 1
liveres = collections.Counter()
for k, (mr, sz) in state.items(): liveres[res(k)] += sz
for r, a in sorted(byres.items(), key=lambda x: -x[1][1])[:25]:
    print(f"  {r:40} {a[0]:7} (prev_kv {a[3]:6}) {a[1]/MiB:8.2f} {liveres[r]/MiB:7.2f} {a[1]/max(liveres[r],1):8.1f}")
cur = [o for o in reads if o[5] != "prev_kv"]
for name, rs in (("all value reads", reads), ("current-version reads", cur), ("prev_kv reads", [o for o in reads if o[5] == "prev_kv"])):
    perkey = collections.Counter(o[3] for o in rs)
    c = sorted(perkey.values(), reverse=True); n = sum(c)
    if not n: continue
    top = lambda f: sum(c[:max(1, int(len(c) * f))]) / n
    print(f"skew, {name}: {n} reads of {len(c)} keys; top 1% keys {top(.01):.1%}, top 10% {top(.1):.1%}, top 50% {top(.5):.1%}")
    ages = collections.Counter(); dist = collections.Counter()
    for o in rs:
        p, wt = pos.get(o[1], (None, None))
        if p is None or p <= pre_L: ages["written before trace"] += 1; dist["before trace"] += 1; continue
        a = o[4] - wt
        ages["<1 s" if a < 1 else "<10 s" if a < 10 else "<60 s" if a < 60 else "<300 s" if a < 300 else ">=300 s"] += 1
        d = (o[6] - p) / MiB
        dist["<1 MiB" if d < 1 else "<4 MiB" if d < 4 else "<16 MiB" if d < 16 else "<64 MiB" if d < 64 else ">=64 MiB"] += 1
    print("   age since write:", {k: f"{v/n:.1%}" for k, v in sorted(ages.items())})
    print("   log bytes since write:", {k: f"{v/n:.1%}" for k, v in sorted(dist.items())})

# ---- policies
def run_window(B, pinned=None):
    out = []
    for o in ops:
        if o[0] != "r": continue
        if pinned and res(o[3]) in pinned: out.append(True); continue
        p = pos.get(o[1])
        out.append(p is not None and o[6] - p[0] <= B)
    return out

def run_lru(B, write_alloc=True):
    c = collections.OrderedDict(); used = 0; out = []
    def ins(v, sz):
        nonlocal used
        c[v] = sz; used += sz
        while used > B and c:
            _, s = c.popitem(last=False); used -= s
    for o in ops:
        k, v, sz = o[0], o[1], o[2]
        if k == "r":
            if v in c: c.move_to_end(v); out.append(True)
            else:
                out.append(False)
                if o[5] != "prev_kv": ins(v, sz)
        elif k == "w":
            if write_alloc: ins(v, sz)
        elif k == "x":
            if v in c: used -= c.pop(v)
    return out

def run_s3fifo(B, write_alloc=True):
    S = collections.deque(); M = collections.deque(); G = collections.OrderedDict()
    where = {}; freq = {}; size = {}; used = {"S": 0, "M": 0}; out = []
    sB = B * 0.1
    def evict():
        while used["S"] + used["M"] > B:
            if used["S"] > sB and S:
                v = S.popleft()
                if where.get(v) != "S": continue
                if freq[v] > 0:
                    where[v] = "M"; freq[v] = 0; M.append(v); used["S"] -= size[v]; used["M"] += size[v]
                else:
                    used["S"] -= size[v]; del where[v]; G[v] = 1
                    while len(G) > max(1, len(M)): G.popitem(last=False)
            elif M:
                v = M.popleft()
                if where.get(v) != "M": continue
                if freq[v] > 0: freq[v] -= 1; M.append(v)
                else: used["M"] -= size[v]; del where[v]
            elif S:
                v = S.popleft()
                if where.get(v) == "S": used["S"] -= size[v]; del where[v]
            else: break
    def ins(v, sz):
        size[v] = sz; freq[v] = 0
        if v in G: del G[v]; where[v] = "M"; M.append(v); used["M"] += sz
        else: where[v] = "S"; S.append(v); used["S"] += sz
        evict()
    for o in ops:
        k, v, sz = o[0], o[1], o[2]
        if k == "r":
            if v in where: freq[v] = min(freq[v] + 1, 3); out.append(True)
            else:
                out.append(False)
                if o[5] != "prev_kv": ins(v, sz)
        elif k == "w":
            if write_alloc: ins(v, sz)
        elif k == "x":
            if v in where: used[where.pop(v)] -= size[v]
            G.pop(v, None)
    return out

maxlive = collections.Counter()
cur_live = collections.Counter()
readbytes = collections.Counter(res(o[3]) for o in reads)
rb = collections.Counter()
for o in reads: rb[res(o[3])] += o[2]
def pick_pins(B):
    # Oracle: prefixes with the most read bytes per live byte at the end of the trace, until B is used.
    chosen = set(); used = 0
    for r in sorted(liveres, key=lambda r: -rb[r] / max(liveres[r], 1)):
        if rb[r] == 0: continue
        if used + liveres[r] <= B: chosen.add(r); used += liveres[r]
    return chosen, used

kinds = [o[5] for o in reads]
sizes = [o[2] for o in reads]
def summarize(h):
    n = len(h); tot = sum(sizes)
    def rate(sel):
        idx = [i for i in range(n) if sel(kinds[i])]
        if not idx: return "-"
        return f"{sum(h[i] for i in idx)/len(idx):.0%}"
    hb = sum(s for s, x in zip(sizes, h) if x) / tot
    return f"{sum(h)/n:5.0%} | {hb:5.0%} | {rate(lambda k: k == 'prev_kv'):>5} | {rate(lambda k: k in ('get', 'txn_failure')):>5} | {rate(lambda k: k in ('list', 'list_stream')):>5}"
budgets = [x * MiB for x in (0.5, 1, 2, 4, 8, 16, 32)]
print("\n## Hit rates (reads by count | by bytes | prev_kv | point get and txn failure | list)")
for B in budgets:
    pins, pused = pick_pins(B)
    rows = [("window of recent writes (E)", run_window(B)),
            ("LRU, writes and read misses insert", run_lru(B)),
            ("LRU, read misses insert only", run_lru(B, False)),
            ("S3-FIFO, writes and read misses insert", run_s3fifo(B)),
            ("S3-FIFO, read misses insert only", run_s3fifo(B, False)),
            (f"pin prefixes (oracle, {pused/MiB:.1f} MiB: {','.join(sorted(pins))[:60]})", run_window(0, pins)),
            ("pin prefixes in B/2 + window B/2", run_window(B / 2, pick_pins(B / 2)[0]))]
    print(f"\n### {B/MiB:g} MiB")
    for name, h in rows: print(f"  {name:70} {summarize(h)}")
