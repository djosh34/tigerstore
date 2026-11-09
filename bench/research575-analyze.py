#!/usr/bin/env python3
"""No dependencies: read pprof's protobuf wire format and emit every sampled allocation site.
Counts/bytes are inverse-probability estimates already scaled by Go's profile writer.
"""
import csv
import gzip
import json
from pathlib import Path
import sys


def varint(b, i):
    n = shift = 0
    while True:
        v = b[i]
        i += 1
        n |= (v & 127) << shift
        if v < 128:
            return n, i
        shift += 7


def fields(b):
    i = 0
    out = {}
    while i < len(b):
        tag, i = varint(b, i)
        num, typ = tag >> 3, tag & 7
        if typ == 0:
            v, i = varint(b, i)
        elif typ == 2:
            n, i = varint(b, i)
            v, i = b[i:i+n], i+n
        elif typ in (1, 5):
            n = 8 if typ == 1 else 4
            v, i = b[i:i+n], i+n
        else:
            raise ValueError(typ)
        out.setdefault(num, []).append(v)
    return out


def packed(f, n):
    out = []
    for b in f.get(n, []):
        if isinstance(b, int):
            out.append(b)
            continue
        i = 0
        while i < len(b):
            v, i = varint(b, i)
            out.append(v - (1 << 64) if v >= 1 << 63 else v)
    return out


def profile(path):
    p = fields(gzip.decompress(Path(path).read_bytes()))
    strings = [s.decode() for s in p[6]]
    funcs = {}
    for b in p[5]:
        f = fields(b)
        funcs[f[1][0]] = (strings[f[2][0]], strings[f[4][0]])
    locs = {}
    for b in p[4]:
        f = fields(b)
        lines = []
        for line in f.get(4, []):
            x = fields(line)
            name, file = funcs[x[1][0]]
            lines.append((name, file, x.get(2, [0])[0]))
        locs[f[1][0]] = tuple(lines)
    types = [strings[fields(b)[1][0]] for b in p[1]]
    rows = {}
    for b in p[2]:
        s = fields(b)
        stack = tuple(line for loc in packed(s, 1) for line in locs[loc])
        vals = dict(zip(types, packed(s, 2)))
        row = rows.setdefault(stack, {t: 0 for t in types})
        for t, v in vals.items():
            row[t] += v
    return rows


def recipe(stack):
    names = ' '.join(x[0] for x in stack)
    own = 'github.com/djosh34/tigerstore/'
    for name, file, line in stack:
        if 'runtime/pprof' in file or 'net/http/pprof' in file or 'prometheus' in name or 'server.(*Server).metrics' in name:
            return 'measurement/control', 'exclude from request-only target; bound observability separately'
    name, file, line = stack[0]
    if name.startswith(own) or name.startswith(('bytes.', 'slices.', 'encoding/binary.', 'strings.')) and any(x[0].startswith(own) for x in stack):
        owner = 'tigerstore (including std helper called by it)'
        if any(s in names for s in ('appendRangeStart', 'fillHole', 'txnEnc).message', 'server.marshal', 'watchMessage')):
            return owner, 'bounded reply slabs; release after final writer/reset, not encode; medium ownership rewrite'
        if 'rangeAt' in names or 'fillValues' in names or 'Cursor).' in names:
            return owner, 'fixed per-admitted-read KV/value/piece scratch with cursor; medium rewrite'
        if 'fillCache' in names or 'cache).' in names:
            return owner, 'fixed byte-cache arena plus offsets/free lists; medium rewrite'
        if 'mvcc' in names and any(s in names for s in ('writer).add', 'allocKey', 'addKey', 'history', 'pack', 'writer).put')):
            return owner, 'fixed key/version/history arenas; spill cold metadata or capacity admission; large storage rewrite'
        if any(s in names for s in ('decode', 'Decode', 'placer', 'newOp')):
            return owner, 'request-slot embedded protobuf/oneof/slice storage or own bounded parsed IR; medium-large rewrite'
        if 'Watch' in names or 'watch' in names or 'Change.Event' in names:
            return owner, 'bounded watch/event slots, shared immutable events, history cursors; medium-large rewrite'
        if any(s in names for s in ('encodeBatch', 'copyChunk', 'State).', 'raftLog', 'persist', 'diskLog', 'core.(*Node).message')):
            return owner, 'ref-counted fixed log/message slabs through Raft/WAL/peer completion; large lifetime rewrite'
        return owner, 'explicit startup slots/rings, recycle only after last asynchronous user; medium rewrite'
    if 'go.etcd.io/raft' in name or 'google.golang.org/protobuf' in name:
        return 'dependency', 'caller-supplied typed storage where supported; otherwise bounded Raft/protobuf codec fork; large + ongoing maintenance'
    if 'http2' in name or 'hpack' in name:
        return 'dependency', 'reuse all frame structs; fixed HPACK byte/table arena or narrow fork; small frames / medium-large HPACK'
    if 'crypto/tls' in file:
        return 'dependency (Go TLS)', 'warm records reduce it; hard guarantee needs explicit connection-owned buffers/handshake allocator fork; high security maintenance'
    if name.startswith('runtime.'):
        return 'Go runtime', 'bound and warm goroutines/stacks/timers; process-wide no-allocation guarantee not provided'
    return 'dependency/stdlib', 'bounded typed reuse at caller if exposed, else fork or control-plane allocation exception'


def cpu(path):
    # Exclusive first-match stack attribution: GC assists are not also allocation.
    totals = {}
    for stack, values in profile(path).items():
        names = ' '.join(x[0] for x in stack)
        if any(s in names for s in ('runtime.gcBgMarkWorker', 'runtime.gcAssistAlloc', 'runtime.gcStart', 'runtime.gcMarkDone', 'runtime.bgsweep', 'runtime.bgscavenge')):
            cat = 'GC'
        elif any(s in names for s in ('runtime.mallocgc', 'runtime.newobject', 'runtime.makeslice', 'runtime.growslice')):
            cat = 'allocation'
        elif 'github.com/google/btree' in names:
            cat = 'Btree'
        elif 'crypto/tls' in names:
            cat = 'TLS (includes IO below it)'
        else:
            cat = 'other'
        totals[cat] = totals.get(cat, 0) + values.get('cpu', 0)
    total = sum(totals.values())
    print(path, 'sampled CPU seconds', total/1e9, 'exclusive shares', {k: round(v/total*100, 2) for k, v in totals.items()})


def census(out):
    out = Path(out)
    result = json.loads((out/'result.json').read_text())
    ops = result['ops']
    print(out, 'valid', result['valid'], 'ops', ops, 'throughput', result['throughput'])
    for end in sorted(out.glob('1-*.allocs')):
        port = end.stem[2:]
        a, b = profile(out/f'0-{port}.allocs'), profile(end)
        sites = {}
        for stack in a.keys() | b.keys():
            if not stack:
                continue
            count = b.get(stack, {}).get('alloc_objects', 0) - a.get(stack, {}).get('alloc_objects', 0)
            size = b.get(stack, {}).get('alloc_space', 0) - a.get(stack, {}).get('alloc_space', 0)
            if not count and not size:
                continue
            ownframe = next((x for x in stack if x[0].startswith('github.com/djosh34/tigerstore/')), None)
            key = (stack[0], ownframe, recipe(stack))
            row = sites.setdefault(key, [0, 0])
            row[0] += count
            row[1] += size
        with (out/f'{port}-sites.csv').open('w') as f:
            w = csv.writer(f)
            w.writerow(['allocation_site', 'tigerstore_caller', 'owner', 'zero_route_and_cost', 'estimated_count_per_mix_op', 'estimated_bytes_per_mix_op'])
            for (site, caller, (owner, route)), (count, size) in sorted(sites.items(), key=lambda x: -x[1][1]):
                fmt = lambda x: '' if x is None else f'{x[0]} {x[1]}:{x[2]}'
                w.writerow([fmt(site), fmt(caller), owner, route, count/ops, size/ops])
        print(port, 'sites',len(sites), 'allocs/op', sum(v[0] for v in sites.values())/ops,'B/op', sum(v[1] for v in sites.values())/ops)


if __name__ == '__main__':
    for arg in sys.argv[1:]:
        census(arg)
        for path in Path(arg).glob('*.cpu'):
            cpu(path)
