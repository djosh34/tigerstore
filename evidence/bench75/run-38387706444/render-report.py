#!/usr/bin/env python3
"""Render two comments from the validated median CSV and an immutable evidence commit."""
import csv
import sys
from pathlib import Path

root = Path(__file__).resolve().parent
commit = sys.argv[1]
assert len(commit) == 40 and all(c in '0123456789abcdef' for c in commit)
repo = 'https://github.com/djosh34/tigerstore'
path = 'evidence/bench75/run-38387706444'
rows = list(csv.DictReader((root / 'summary.csv').open()))

def table(op, clients):
    data = {r['store']: r for r in rows if r['op'] == op and int(r['clients']) == clients}
    names = {'baseline': 'main', 'candidate': 'PR 77' if op == 'put' else 'PR 80',
             'second': 'PR 79', 'third': 'PR 80', 'etcd': 'etcd 3.7.2'}
    order = ['baseline', 'candidate', 'second', 'third', 'etcd'] if op == 'put' else ['baseline', 'candidate', 'etcd']
    unit = 'Put' if op == 'put' else 'Txn'
    out = [f'### {unit}, {clients} clients', '',
           f'| Store | {unit}/s | Mean / p50 / p99 / p99.9 ms | Leader / total CPU µs/{unit} | Generator µs/{unit} / cores | Objects / bytes per {unit} | GC CPU share | syscr / syscw per {unit} |',
           '|---|---:|---:|---:|---:|---:|---:|---:|']
    for store in order:
        r = data[store]
        latency = ' / '.join(f'{float(r[k]):.3f}' for k in ['mean_ms', 'p50_ms', 'p99_ms', 'p999_ms'])
        cpu = ' / '.join(f'{float(r[k]):.2f}' for k in ['leader_cpu_us_op', 'server_cpu_us_op'])
        alloc = f"{float(r['objects_per_op']):.2f} / {float(r['bytes_per_op']):,.0f}"
        gc = f"{100 * float(r['gc_cpu_share']):.2f}%" if r['gc_cpu_share'] else 'n/a'
        io = ' / '.join(f'{float(r[k]):.4f}' for k in ['leader_syscr_per_op', 'leader_syscw_per_op'])
        out.append(f"| {names[store]} | {float(r['ops_s']):,.0f} | {latency} | {cpu} | {float(r['generator_cpu_us_op']):.2f} / {float(r['generator_cores']):.2f} | {alloc} | {gc} | {io} |")
    return '\n'.join(out)

cas = '''## CAS Txn, 256 clients

The CAS job alternates main, PR 80, etcd and the reverse order. Before each warmup and measured subprocess, an untimed keys-only prefix Range seeds positive existing-key mod revisions. Each worker compares its cached ModRevision with EQUAL, then puts 1 KiB on success, else ranges that same key and validates its value/revision. The shared cache only advances. There are no separate timed pre-reads or client retries. This compares the PR 80 stack with main, not the alias change alone.

Each store completed 300,000 measured transactions, all with compare-pass and none with compare-fail. The failure arm was present but not exercised. These are successful-CAS results, not conflict or failed-CAS latency measurements. Revision growth equalled compare passes. The following costs divide by completed Txns, not by separate Puts inside them.

''' + table('cas', 256)

profile = '''## Separate PR 80 profile, Put at 256 clients

After the paired Put repetitions, the same runner started a fresh PR 80 cluster with the same seed/warmup. It captured a 20-second leader CPU profile during a separate 25-second closed-loop load. These samples do not enter the medians. The profile had 17.22 seconds of CPU samples. [Raw `go tool pprof -peek=Syscall6` output](@BLOB@/put-c256/profile-third/cpu-peek-Syscall6.txt) and [expanded caller peek](@BLOB@/put-c256/profile-third/cpu-peek-callers.txt) use the matching archived symbol binary.

| Function | Flat CPU | Cumulative CPU | Share of total samples |
|---|---:|---:|---:|
| `internal/runtime/syscall/linux.Syscall6` | 7.46 s | 7.46 s | 43.32% |
| Direct caller `syscall.RawSyscall6` | 0 s | 6.82 s | 39.61% |
| Direct caller `internal/runtime/syscall/linux.EpollWait` | 0.01 s | 0.65 s | 3.77% |
| `syscall.Syscall`, caller of RawSyscall6 | 0.01 s | 6.92 s | 40.19% |

The peek attributes 6.82 s of Syscall6 to RawSyscall6 and 0.64 s to EpollWait. The expanded peek attributes incoming edges to `syscall.Syscall` of 3.29 s from `syscall.write`, 2.23 s from `syscall.read`, and 1.40 s from `syscall.Fsync`. Edge attribution is not the caller's whole cumulative time. These nested times overlap and must not be added as independent costs. The earlier PR 77 capture's 41% flat share came from a different run, not a paired profile comparison. CPU samples are not syscall counts or I/O-wait time; the `/proc/io` columns above do not include fsync or epoll.
'''

base = (root / 'report-base.md').read_text()
base = base.replace('@PUT_TABLES@', table('put', 64) + '\n\n' + table('put', 256))
base = base.replace('@CAS_SECTION@', cas)
base = base.replace('@ETCD_SOURCE_COUNTS@', (root / 'etcd-source-counts-comment.md').read_text())
heads = {
    79: 'PR 79 reduced leader object allocations versus PR 77 by 33.1% at 64 clients and 36.2% at 256, and allocated bytes by 14.1% and 14.6%. Put throughput medians changed by -0.1% and +1.2%. The nearly full generator CPU at 256 clients limits any capacity claim.\n\n',
    80: 'PR 80 reduced allocated bytes per CAS Txn versus main by 22.4%, but used 6.5% more objects. Its median CAS throughput was 5.0% lower and total server CPU per Txn was 5.8% higher. This run demonstrates a byte-allocation reduction, not a CAS throughput win. Put results stayed near PR 79.\n\n',
}
for pr in [79, 80]:
    text = heads[pr] + base.replace('@PROFILE_SECTION@', profile if pr == 80 else '')
    text = text.replace('@EVIDENCE@', f'{repo}/tree/{commit}/{path}')
    text = text.replace('@BLOB@', f'{repo}/blob/{commit}/{path}')
    text = text.replace('@FIRST_EVIDENCE@', f'{repo}/tree/{commit}/evidence/bench75/run-38387063293')
    assert '@' not in text and '\u2014' not in text
    (root / f'report-pr{pr}.md').write_text(text)
