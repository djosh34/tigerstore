#!/usr/bin/env python3
"""Read saved Jepsen evidence only. Python 3.10+, standard library, no checker run.

Writes only below this script's directory. Refuses to start after the task deadline.
EDN reader supports the forms present in these histories/results, not arbitrary EDN.
It never evaluates forms. Original extracted bytes retain all outcome labels.
"""
import argparse
import collections
import csv
import datetime as dt
import hashlib
import json
import math
from pathlib import Path
import re
import resource
import shutil
import tarfile
import time

HERE = Path(__file__).resolve().parent
OUT = HERE
CORE_ROLES = ('unknown', 'matched-control', 'candidate', 'control')
DEADLINE = dt.datetime(2026, 10, 13, 22, tzinfo=dt.timezone.utc)
OUTCOMES = (':ok', ':fail', ':info')
FS = (':read', ':write', ':cas')


class EDN:
    """Small recursive reader. Keywords keep their colon; records become maps.

    Strings use JSON-compatible escapes in these saved files. Sets become lists
    because node sets are needed only for observation, not as map keys.
    Unknown symbols stay strings. No source or history field is rewritten.
    """
    token = re.compile(r'[^\s,\[\]{}()";]+')
    string = re.compile(r'"(?:[^"\\]|\\.)*"')

    def __init__(self, text):
        self.text = text
        self.i = 0

    def skip(self):
        while self.i < len(self.text):
            c = self.text[self.i]
            if c.isspace() or c == ',':
                self.i += 1
            elif c == ';':
                end = self.text.find('\n', self.i)
                self.i = len(self.text) if end < 0 else end + 1
            else:
                return

    def value(self):
        self.skip()
        c = self.text[self.i]
        if c == '"':
            m = self.string.match(self.text, self.i)
            if not m:
                raise ValueError('Unterminated EDN string')
            self.i = m.end()
            return json.loads(m[0])
        if c in '[({':
            end = {'[': ']', '(': ')', '{': '}'}[c]
            self.i += 1
            vals = []
            while True:
                self.skip()
                if self.text[self.i] == end:
                    self.i += 1
                    break
                vals.append(self.value())
            if c == '{':
                if len(vals) % 2:
                    raise ValueError('Odd EDN map')
                result = dict(zip(vals[::2], vals[1::2]))
                if len(result) != len(vals) // 2:
                    raise ValueError('Duplicate EDN map key')
                return result
            return vals
        if self.text.startswith('#{', self.i):
            self.i += 2
            vals = []
            while True:
                self.skip()
                if self.text[self.i] == '}':
                    self.i += 1
                    return vals
                vals.append(self.value())
        m = self.token.match(self.text, self.i)
        if not m:
            raise ValueError(f'Unsupported EDN at {self.i}: {self.text[self.i:self.i+80]!r}')
        self.i = m.end()
        s = m[0]
        if s.startswith('#'):
            # Saved checker models are record tags followed by ordinary maps.
            if s.startswith('#jepsen.') or s.startswith('#knossos.'):
                v = self.value()
                if not isinstance(v, dict):
                    raise ValueError('Record tag without map')
                return v
            raise ValueError(f'Unsupported EDN tag {s}')
        if s == 'nil':
            return None
        if s in ('true', 'false'):
            return s == 'true'
        if re.fullmatch(r'[+-]?\d+', s):
            return int(s)
        if re.fullmatch(r'[+-]?(?:\d+\.\d*|\d*\.\d+|\d+)(?:[Ee][+-]?\d+)?', s):
            return float(s)
        return s

    def load(self):
        value = self.value()
        self.skip()
        if self.i != len(self.text):
            raise ValueError(f'Trailing EDN at {self.i}')
        return value


def read_edn(path):
    return EDN(path.read_text()).load()


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


def csv_write(path, rows):
    if not rows:
        path.write_text('')
        return
    with path.open('w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)


def compact(value):
    return json.dumps(value, separators=(',', ':'), sort_keys=True)


def latency_summary(values):
    values = sorted(values)
    if not values:
        return dict(n=0, min_ms=None, mean_ms=None, p50_ms=None,
                    p95_ms=None, p99_ms=None, max_ms=None)
    def q(p):
        return round(values[max(0, math.ceil(p * len(values))-1)], 6)
    return dict(n=len(values), min_ms=round(values[0], 6),
                mean_ms=round(sum(values)/len(values), 6), p50_ms=q(.5),
                p95_ms=q(.95), p99_ms=q(.99), max_ms=round(values[-1], 6))


def options(command):
    d = dict(re.findall(r'--([\w-]+)\s+(\S+)', command))
    d.setdefault('ops-per-key', '200')
    return d


def plan():
    rows = []
    def add(run, store, fault, rep, budget, role, pair):
        suffix = f'-rep{rep}-1' if rep is not None else '-1'
        rows.append(dict(run=run, case=f'jepsen-{store}-register-{fault}{suffix}',
                         store=store, fault=fault, budget=budget, role=role, pair=pair))
    for fault in ('kill', 'partition'):
        for rep in (1, 2, 3):
            pair = f'{fault}-200-rep{rep}'
            add('38428716698' if rep == 1 else '38430056885', 'tigerstore',
                fault, None if rep == 1 else rep, 200, 'unknown', pair)
            add('38427801610' if rep == 1 else '38430056885', 'etcd',
                fault, None if rep == 1 else rep, 200, 'matched-control', pair)
        pair = f'{fault}-50-rep1'
        add('38432262482', 'tigerstore', fault, 1, 50, 'unknown', pair)
        add('38430904489', 'etcd', fault, 1, 50, 'matched-control', pair)
        for rep in (1, 2):
            add('38430224306', 'etcd', fault, rep, 50, 'earlier-50-control', '')
            pair = f'{fault}-10-rep{rep}'
            add('38433448661', 'etcd', fault, rep, 10, 'bounded-control', pair)
            add('38434478844', 'tigerstore', fault, rep, 10, 'bounded-tigerstore', pair)
    for r in rows:
        r['id'] = r['run'] + '/' + r['case']
        r['assert_old_verdict'] = True
    return rows


def metadata(run, case, command, store_hint=''):
    opts = options(command)
    store_match = re.search(r'jepsen2?-(etcd|tigerstore)-', case)
    store = store_match[1] if store_match else store_hint
    if store not in ('etcd', 'tigerstore'):
        raise ValueError(f'Cannot identify store for {case}; preserve artifact name or runner.log')
    rep = re.search(r'-rep(\d+)', case)
    budget, fault = int(opts['ops-per-key']), opts['nemesis']
    return dict(run=run, case=case, id=run+'/'+case, store=store, fault=fault, budget=budget,
                role='control' if store == 'etcd' else 'candidate',
                pair=f"{fault}-{budget}-rep{rep[1] if rep else 'unspecified'}",
                assert_old_verdict=False)


def discover(evidence):
    rows, skipped = [], []
    for path in sorted(evidence.glob('*.tar.gz')):
        with tarfile.open(path) as archive:
            names = {m.name: m for m in archive if m.isfile()}
            for name in sorted(names):
                if not name.endswith('/command.txt'):
                    continue
                case = name.removesuffix('/command.txt')
                command = archive.extractfile(names[name]).read().decode()
                if options(command).get('workload') != 'register':
                    continue
                if case+'/store/latest/results.edn' not in names:
                    skipped.append(dict(run=path.name.removesuffix('.tar.gz'), case=case,
                                        reason='No saved checker result; not certified'))
                    continue
                hint = ''
                runner = names.get(case+'/runner.log')
                if runner:
                    m = re.search(r'^start=.*? store=(\S+)', archive.extractfile(runner).read().decode(), re.M)
                    hint = m[1] if m else ''
                rows.append(metadata(path.name.removesuffix('.tar.gz'), case, command, hint))
    return rows, skipped


def copy_loose(source, run):
    rows, skipped = [], []
    for command_path in sorted(source.rglob('command.txt')):
        root = command_path.parent
        command = command_path.read_text()
        if options(command).get('workload') != 'register':
            continue
        case = next((p.name for p in (root, *root.parents)
                     if re.search(r'jepsen2?-(etcd|tigerstore)-', p.name)), root.name)
        latest = root/'store/latest'
        if not (latest/'results.edn').is_file():
            skipped.append(dict(run=run, case=case, reason='No saved checker result; not certified'))
            continue
        hint = ''
        if (root/'runner.log').exists():
            m = re.search(r'^start=.*? store=(\S+)', (root/'runner.log').read_text(), re.M)
            hint = m[1] if m else ''
        meta = metadata(run, case, command, hint)
        if any(r['id'] == meta['id'] for r in rows):
            raise ValueError(f'Duplicate artifact identifier {meta["id"]}')
        rows.append(meta)
        dest = OUT/'histories'/run/case
        dest.mkdir(parents=True, exist_ok=True)
        for p in root.iterdir():
            if p.is_file() and (p.name in ('command.txt', 'upstream.patch', 'runner.log', 'server.sha256', 'summary.json')
                                or p.name.startswith('fault-observer-')):
                shutil.copyfile(p, dest/p.name)
        for p in latest.rglob('*'):
            if p.is_file() and p.name in ('history.edn', 'results.edn', 'jepsen.log'):
                d = dest/'store/latest'/p.relative_to(latest)
                d.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(p, d)
    return rows, skipped


def extract(evidence, rows):
    sums = evidence/'SHA256SUMS'
    checksums = dict((line.split()[1], line.split()[0])
                     for line in sums.read_text().splitlines()) if sums.exists() else {}
    archives = []
    for run in sorted(set(r['run'] for r in rows)):
        path = evidence/f'{run}.tar.gz'
        with path.open('rb') as f:
            digest = hashlib.sha256(f.read()).hexdigest()
        if path.name in checksums:
            assert digest == checksums[path.name], f'Checksum mismatch {path}'
        archives.append(dict(file=path.name, sha256=digest,
                             verified_against_saved_sum=path.name in checksums))
        cases = {r['case'] for r in rows if r['run'] == run}
        with tarfile.open(path) as archive:
            for member in archive:
                parts = Path(member.name).parts
                if not parts or parts[0] not in cases or not member.isfile():
                    continue
                # Histories, results, exact commands/patches and log evidence.
                keep = member.name.endswith(('/history.edn', '/results.edn', '/jepsen.log'))
                keep |= len(parts) == 2 and (parts[1] in ('command.txt', 'upstream.patch',
                                                         'runner.log', 'server.sha256')
                                            or parts[1].startswith('fault-observer-'))
                if not keep:
                    continue
                assert '..' not in parts and not Path(member.name).is_absolute()
                out = OUT/'histories'/run/member.name
                out.parent.mkdir(parents=True, exist_ok=True)
                with archive.extractfile(member) as f:
                    out.write_bytes(f.read())
    return archives


def checker_log(path):
    text = path.read_text()
    lines = text.splitlines()
    ts = lambda s: dt.datetime.strptime(s[:23], '%Y-%m-%d %H:%M:%S,%f')
    starts = [ts(l) for l in lines if 'jepsen.core: Analyzing...' in l]
    ends = [ts(l) for l in lines if 'jepsen.core: Analysis complete' in l]
    elapsed = (ends[-1]-starts[-1]).total_seconds() if starts and ends else None
    reporter = [l for l in lines if 'knossos.search: {' in l]
    costs = [float(n) for l in reporter for n in re.findall(r':cost (\d+(?:\.\d+)?(?:E[+-]?\d+)?)', l)]
    interesting = [l for l in lines if 'Out of memory; aborting search' in l
                   or 'More than 1024 reachable models' in l or l in reporter]
    return dict(analysis_wall_seconds=elapsed,
                memory_abort_log_lines=sum('Out of memory; aborting search' in l for l in lines),
                memo_limit_log_lines=sum('More than 1024 reachable models' in l for l in lines),
                reporter_log_lines=len(reporter), reporter_max_numeric_cost=max(costs, default=None),
                reporter_non_numeric_costs=[m[1] for l in reporter
                    if (m := re.search(r':cost ([^, }]+)', l))
                    and not re.fullmatch(r'\d+(?:\.\d+)?(?:E[+-]?\d+)?', m[1])],
                lines=interesting)


def analyze_case(meta, saved_summary):
    root = OUT/'histories'/meta['run']/meta['case']
    latest = root/'store/latest'
    result = read_edn(latest/'results.edn')
    opts = options((root/'command.txt').read_text())
    assert int(opts['ops-per-key']) == meta['budget']
    assert opts['nemesis'] == meta['fault'] and opts['workload'] == 'register'
    history_path = latest/'history.edn'
    events = [EDN(line).load() for line in history_path.read_text().splitlines() if line.strip()]
    assert all(e[':index'] == i for i, e in enumerate(events))
    assert all(a[':time'] <= b[':time'] for a, b in zip(events, events[1:]))
    pending = {}
    ops = []
    by_invoke = {}
    client_info_other = []
    max_open = 0
    for e in events:
        if e[':process'] == ':nemesis':
            continue
        p, typ = e[':process'], e[':type']
        if typ == ':invoke':
            assert p not in pending
            pending[p] = e
            max_open = max(max_open, len(pending))
        elif typ in OUTCOMES:
            if p not in pending:
                client_info_other.append(e)
                continue
            inv = pending.pop(p)
            assert inv[':f'] == e[':f'] and inv[':value'][0] == e[':value'][0]
            op = dict(process=p, f=e[':f'], key=inv[':value'][0], outcome=typ,
                      invoke_index=inv[':index'], completion_index=e[':index'],
                      invoke_time_ns=inv[':time'], completion_time_ns=e[':time'],
                      latency_ms=(e[':time']-inv[':time'])/1e6,
                      invoke_value=inv[':value'], completion_value=e[':value'],
                      error=e.get(':error'), exception=e.get(':exception'))
            ops.append(op)
            by_invoke[inv[':index']] = op
        else:
            raise ValueError(f'Unexpected client type {typ}')
    # Residual invocations and unpaired infos are recorded, never relabelled.
    counts = collections.Counter(o['outcome'] for o in ops)
    stats = result[':stats']
    assert sum(counts.values()) == stats[':count']
    assert all(counts[t] == stats[t+'-count'] for t in OUTCOMES)
    if saved_summary and 'stats' in saved_summary:
        assert saved_summary['stats']['count'] == len(ops)
    for f in FS:
        for typ in OUTCOMES:
            assert sum(o['f'] == f and o['outcome'] == typ for o in ops) == stats[':by-f'][f][typ+'-count']
    core = {**meta, 'verdict': result[':valid?'], 'command': (root/'command.txt').read_text().strip(),
            'count': len(ops), 'ok': counts[':ok'], 'fail': counts[':fail'], 'info': counts[':info'],
            'max_open_client_calls': max_open, 'unmatched_invokes': len(pending),
            'client_info_without_invoke': len(client_info_other),
            'history_sha256': hashlib.sha256(history_path.read_bytes()).hexdigest(),
            'coverage_errors_from_saved_summary': saved_summary.get('coverage_errors') if saved_summary else None,
            'residual_invocations': list(pending.values()), 'unpaired_client_infos': client_info_other}
    patch = (root/'upstream.patch').read_text() if (root/'upstream.patch').exists() else ''
    core['checker_resources_from_patch'] = dict(
        added_heap_options=re.findall(r'^\+.*?(-Xmx\w+)', patch, re.M),
        wgl_explicitly_added=bool(re.search(r'^\+.*?:algorithm\s+:wgl', patch, re.M)),
        added_concurrency_limits=re.findall(r'^\+.*?concurrency-limit\s+(\d+)', patch, re.M))
    if meta['assert_old_verdict']:
        if meta['role'] == 'unknown':
            assert result[':valid?'] == ':unknown'
        else:
            assert result[':valid?'] is True
    latency_rows, count_rows, key_rows = [], [], []
    for f in (':all', *FS):
        for typ in OUTCOMES:
            selected = [o for o in ops if o['outcome'] == typ and (f == ':all' or o['f'] == f)]
            count_rows.append(dict(history=meta['id'], store=meta['store'], role=meta['role'],
                                   f=f, outcome=typ, count=len(selected)))
            latency_rows.append(dict(history=meta['id'], store=meta['store'], role=meta['role'],
                                     f=f, outcome=typ,
                                     **latency_summary([o['latency_ms'] for o in selected])))
    key_results = result[':workload'][':results']
    for key in sorted(key_results):
        key_ops = [o for o in ops if o['key'] == key]
        kr = key_results[key]
        saved_key = latest/f'independent/{key}/results.edn'
        if saved_key.exists():
            assert kr == read_edn(saved_key)
        kc = collections.Counter(o['outcome'] for o in key_ops)
        mutation_info = [o for o in key_ops if o['outcome'] == ':info' and o['f'] in (':write', ':cas')]
        assert len(key_ops) <= meta['budget']
        # Lower-level histories add all unkeyed nemesis records. Cross-check clients.
        subpath = latest/f'independent/{key}/history.edn'
        if subpath.exists():
            subevents = [EDN(l).load() for l in subpath.read_text().splitlines() if l.strip()]
            subclients = [e for e in subevents if e[':process'] != ':nemesis']
            expected = {i for o in key_ops for i in (o['invoke_index'], o['completion_index'])}
            expected.update(e[':index'] for e in pending.values() if e[':value'][0] == key)
            expected.update(e[':index'] for e in client_info_other if isinstance(e.get(':value'), list) and e[':value'][0] == key)
            assert {e[':index'] for e in subclients} == expected
        key_rows.append(dict(history=meta['id'], store=meta['store'], role=meta['role'], key=key,
                             budget=meta['budget'], count=len(key_ops), ok=kc[':ok'], fail=kc[':fail'],
                             info=kc[':info'], info_mutations=len(mutation_info),
                             info_write=sum(o['f'] == ':write' for o in mutation_info),
                             info_cas=sum(o['f'] == ':cas' for o in mutation_info),
                             no_leader_infos=sum(o['error'] == [':unavailable', 'etcdserver: no leader']
                                                 and o['outcome'] == ':info' for o in key_ops),
                             first_invoke_time_ns=min(o['invoke_time_ns'] for o in key_ops),
                             last_completion_time_ns=max(o['completion_time_ns'] for o in key_ops),
                             observed_duration_ms=(max(o['completion_time_ns'] for o in key_ops)
                                                   -min(o['invoke_time_ns'] for o in key_ops))/1e6,
                             verdict=kr[':valid?'], cause=kr[':linear'].get(':cause'),
                             saved_configs=len(kr[':linear'].get(':configs', [])),
                             saved_max_config_pending=max((len(c.get(':pending', []))
                                 for c in kr[':linear'].get(':configs', [])), default=0)))
    core['keys'] = len(key_rows)
    core['unknown_keys'] = sum(r['verdict'] == ':unknown' for r in key_rows)
    core['oom_keys'] = sum(r['cause'] == ':out-of-memory' for r in key_rows)
    core['max_info_mutations_per_key'] = max(r['info_mutations'] for r in key_rows)
    core['all_info_keys'] = [r['key'] for r in key_rows if r['info'] == r['count']]
    faults = []
    active = {}
    nemesis_inv = None
    unknown_seen = collections.Counter()
    for e in events:
        if e[':process'] != ':nemesis':
            if e[':type'] == ':invoke':
                active[e[':process']] = e
            else:
                active.pop(e[':process'], None)
                if e[':type'] == ':info' and e[':f'] in (':write', ':cas'):
                    unknown_seen[e[':value'][0]] += 1
            continue
        assert e[':type'] == ':info'
        if nemesis_inv is None:
            nemesis_inv = e
            phase = 'invoke'
            duration, applied = None, None
        else:
            assert e[':f'] == nemesis_inv[':f']
            phase = 'completion'
            duration = (e[':time']-nemesis_inv[':time'])/1e6
            v = e[':value']
            if e[':f'] == ':start-partition':
                assert isinstance(v, list) and v[0] == ':isolated'
                applied = any(bool(nodes) for nodes in v[1].values())
            elif e[':f'] == ':stop-partition':
                applied = v == ':network-healed'
            else:
                assert isinstance(v, dict)
                applied = bool(v) and not e.get(':error')
            nemesis_inv = None
        opens = []
        for inv in sorted(active.values(), key=lambda x: x[':index']):
            o = by_invoke.get(inv[':index'])
            opens.append(dict(process=inv[':process'], f=inv[':f'], key=inv[':value'][0],
                              invoke_index=inv[':index'], invoke_time_ns=inv[':time'],
                              age_ms=round((e[':time']-inv[':time'])/1e6, 6),
                              eventual_completion_index=o['completion_index'] if o else None,
                              eventual_outcome=o['outcome'] if o else None,
                              eventual_latency_ms=round(o['latency_ms'], 6) if o else None))
        faults.append(dict(history=meta['id'], store=meta['store'], role=meta['role'],
                           fault=meta['fault'], action='start' if e[':f'] in (':kill', ':pause', ':start-partition') else 'heal',
                           f=e[':f'], phase=phase, index=e[':index'], time_ns=e[':time'],
                           time_s=round(e[':time']/1e9, 9), duration_ms=duration,
                           applied_nonempty=applied, value_json=compact(e[':value']),
                           open_count=len(opens),
                           open_read=sum(o['f'] == ':read' for o in opens),
                           open_write=sum(o['f'] == ':write' for o in opens),
                           open_cas=sum(o['f'] == ':cas' for o in opens),
                           info_mutations_completed_before=sum(unknown_seen.values()),
                           info_mutations_before_by_key_json=compact(unknown_seen),
                           open_operations_json=compact(opens)))
    core['uncompleted_nemesis_invocation'] = nemesis_inv
    edge_lines = [f"history={meta['id']} fault={meta['fault']} budget={meta['budget']} "
                  'Open counts refer to client calls without a recorded completion, not checker ambiguity.']
    for edge in faults:
        edge_lines.append(f"time_s={edge['time_s']:.9f} index={edge['index']} recorded_type=:info "
                          f"phase={edge['phase']} action={edge['action']} f={edge['f']} "
                          f"applied_nonempty={edge['applied_nonempty']} "
                          f"open={edge['open_count']} read={edge['open_read']} "
                          f"write={edge['open_write']} cas={edge['open_cas']} "
                          f"value={edge['value_json']} calls={edge['open_operations_json']}")
    (root/'fault-edges.log').write_text('\n'.join(edge_lines)+'\n')
    log = checker_log(latest/'jepsen.log')
    (root/'checker-excerpts.log').write_text('\n'.join(log.pop('lines'))+'\n')
    core['checker'] = log
    for o in ops:
        o.update(history=meta['id'], store=meta['store'], role=meta['role'])
    write_json(root/'operations.json', ops)
    return core, ops, count_rows, latency_rows, key_rows, faults


def tables(cases, all_ops, faults):
    rows = ['# Generated tables', '',
            'Core means the selected candidate histories and their direct controls. Auxiliary histories are separate.', '',
            '| History | Role | Budget | Verdict | Operations | :ok | :fail | :info | Unknown keys | Max :info mutations/key | Analysis seconds |',
            '| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |']
    for c in cases:
        rows.append(f"| {c['id']} | {c['role']} | {c['budget']} | {c['verdict']} | {c['count']} | {c['ok']} | {c['fail']} | {c['info']} | {c['unknown_keys']}/{c['keys']} | {c['max_info_mutations_per_key']} | {c['checker']['analysis_wall_seconds']} |")
    rows += ['', '## Core per-store and per-operation counts', '',
             '| Store | Operation | :ok | :fail | :info | Total |', '| --- | --- | --- | --- | --- | --- |']
    for store in ('etcd', 'tigerstore'):
        for f in (':all', *FS):
            ops = [o for o in all_ops if o['store'] == store and o['role'] in CORE_ROLES
                   and (f == ':all' or o['f'] == f)]
            counts = collections.Counter(o['outcome'] for o in ops)
            rows.append(f"| {store} | {f} | {counts[':ok']} | {counts[':fail']} | {counts[':info']} | {len(ops)} |")
    rows += ['', '## Core latency summaries', '',
             'All values are milliseconds. Quantiles use nearest rank. Includes completed client calls only.', '',
             '| Store | Operation | Outcome | n | Median | p95 | p99 | Max |', '| --- | --- | --- | --- | --- | --- | --- | --- | --- |']
    for store in ('etcd', 'tigerstore'):
        for f in (':all', *FS):
            for typ in OUTCOMES:
                v = [o['latency_ms'] for o in all_ops if o['store'] == store
                     and o['role'] in CORE_ROLES and o['outcome'] == typ
                     and (f == ':all' or o['f'] == f)]
                s = latency_summary(v)
                rows.append(f"| {store} | {f} | {typ} | {s['n']} | {s['p50_ms']} | {s['p95_ms']} | {s['p99_ms']} | {s['max_ms']} |")
    rows += ['', '## Core counts for each history and operation', '',
             '| History | Operation | :ok | :fail | :info |', '| --- | --- | --- | --- | --- |']
    for c in cases:
        if c['role'] not in CORE_ROLES:
            continue
        for f in FS:
            counts = collections.Counter(o['outcome'] for o in all_ops if o['history'] == c['id'] and o['f'] == f)
            rows.append(f"| {c['id']} | {f} | {counts[':ok']} | {counts[':fail']} | {counts[':info']} |")
    rows += ['', '## Core fault-edge summaries', '',
             'Each edge log lists every invocation and completion in order, with exact open client calls.', '',
             '| History | Action | Phase | Edges | Mean open | Maximum open | Edge log |',
             '| --- | --- | --- | --- | --- | --- | --- |']
    for c in cases:
        if c['role'] not in CORE_ROLES:
            continue
        for action in ('start', 'heal'):
            for phase in ('invoke', 'completion'):
                v = [int(f['open_count']) for f in faults if f['history'] == c['id'] and f['action'] == action and f['phase'] == phase]
                if v:
                    rows.append(f"| {c['id']} | {action} | {phase} | {len(v)} | {sum(v)/len(v):.3f} | {max(v)} | [log](histories/{c['id']}/fault-edges.log) |")
    (OUT/'tables.md').write_text('\n'.join(rows)+'\n')


def main():
    global OUT
    start = time.monotonic()
    if dt.datetime.now(dt.timezone.utc) >= DEADLINE:
        raise SystemExit('Deadline reached; starting no analysis')
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--evidence', type=Path, default=(HERE/'../../jepsen/evidence').resolve(),
                    help='Original archive directory; default fits experiments/jepsen2/analysis')
    ap.add_argument('--discover', action='store_true', help='Analyze all register cases found in tar.gz archives')
    ap.add_argument('--artifacts', type=Path, help='Read downloaded, unzipped artifact directories instead of tar.gz archives')
    ap.add_argument('--run-id', default='loose-artifacts', help='Run identifier for --artifacts')
    ap.add_argument('--output', type=Path, default=Path('.'), help='Output directory, restricted to below this script directory')
    args = ap.parse_args()
    OUT = (HERE/args.output).resolve()
    if not OUT.is_relative_to(HERE):
        raise SystemExit('Output must be below the analysis directory')
    OUT.mkdir(parents=True, exist_ok=True)
    skipped = []
    if args.artifacts:
        if args.artifacts.resolve().is_relative_to(OUT):
            raise SystemExit('Input artifacts cannot be under the output directory')
        rows, skipped = copy_loose(args.artifacts, args.run_id)
        archives = []
    else:
        rows, skipped = discover(args.evidence) if args.discover else (plan(), [])
        archives = extract(args.evidence, rows)
    if not rows:
        write_json(OUT/'skipped-cases.json', skipped)
        raise SystemExit('No completed register histories found')
    write_json(OUT/'skipped-cases.json', skipped)
    cases, all_ops, count_rows, latency_rows, key_rows, faults = [], [], [], [], [], []
    for r in rows:
        saved_path = args.evidence/f"{r['run']}-summary.json"
        saved = None
        if saved_path.exists():
            saved = next((s for s in json.loads(saved_path.read_text()) if s['directory'] == r['case']), None)
        elif args.artifacts:
            local_summary = OUT/'histories'/r['run']/r['case']/'summary.json'
            if local_summary.exists():
                candidates = json.loads(local_summary.read_text())
                saved = candidates[0] if isinstance(candidates, list) and len(candidates) == 1 else None
        c, ops, counts, lats, keys, fs = analyze_case(r, saved)
        cases.append(c)
        all_ops.extend(ops)
        count_rows.extend(counts)
        latency_rows.extend(lats)
        key_rows.extend(keys)
        faults.extend(fs)
    # Aggregate by explicit scope, so auxiliary controls never inflate core totals.
    store_counts, store_lats = [], []
    for scope in ('core', 'bounded-10', 'earlier-50-control', 'all-extracted'):
        for store in ('etcd', 'tigerstore'):
            selected = [o for o in all_ops if o['store'] == store and (
                scope == 'all-extracted' or scope == 'core' and o['role'] in CORE_ROLES
                or scope == 'bounded-10' and o['role'].startswith('bounded-')
                or scope == 'earlier-50-control' and o['role'] == scope)]
            for f in (':all', *FS):
                for typ in OUTCOMES:
                    v = [o for o in selected if o['outcome'] == typ and (f == ':all' or o['f'] == f)]
                    store_counts.append(dict(scope=scope, store=store, f=f, outcome=typ, count=len(v)))
                    store_lats.append(dict(scope=scope, store=store, f=f, outcome=typ,
                                           **latency_summary([o['latency_ms'] for o in v])))
    write_json(OUT/'summary.json', dict(cases=cases, archives=archives))
    csv_write(OUT/'counts-by-history.csv', count_rows)
    csv_write(OUT/'counts-by-store.csv', store_counts)
    csv_write(OUT/'latencies-by-history.csv', latency_rows)
    csv_write(OUT/'latencies-by-store.csv', store_lats)
    csv_write(OUT/'keys.csv', key_rows)
    csv_write(OUT/'fault-events.csv', faults)
    error_groups = collections.defaultdict(list)
    for o in all_ops:
        error_groups[(o['history'], o['store'], o['role'], o['f'], o['outcome'], compact(o['error']))].append(o['latency_ms'])
    csv_write(OUT/'errors.csv', [dict(history=k[0], store=k[1], role=k[2], f=k[3], outcome=k[4], error_json=k[5],
                                    **latency_summary(v)) for k, v in sorted(error_groups.items())])
    no_leader_rows = []
    for c in cases:
        ops = [o for o in all_ops if o['history'] == c['id']]
        for f in (':all', *FS):
            selected = [o for o in ops if o['error'] == [':unavailable', 'etcdserver: no leader'] and (f == ':all' or o['f'] == f)]
            no_leader_rows.append(dict(history=c['id'], store=c['store'], role=c['role'], fault=c['fault'],
                                       budget=c['budget'], f=f, info=sum(o['outcome'] == ':info' for o in selected),
                                       fail=sum(o['outcome'] == ':fail' for o in selected),
                                       at_most_10ms=sum(o['latency_ms'] <= 10 for o in selected),
                                       at_most_100ms=sum(o['latency_ms'] <= 100 for o in selected),
                                       **latency_summary([o['latency_ms'] for o in selected])))
    csv_write(OUT/'no-leader.csv', no_leader_rows)
    config_counts, config_latencies, config_summary = [], [], []
    case_groups = collections.defaultdict(list)
    for c in cases:
        scope = 'core' if c['role'] in CORE_ROLES else 'bounded-10' if c['role'].startswith('bounded-') else c['role']
        case_groups[(scope, c['store'], c['fault'], c['budget'])].append(c)
    for k, cs in sorted(case_groups.items()):
        ids = {c['id'] for c in cs}
        ops = [o for o in all_ops if o['history'] in ids]
        info_mutations = [o for o in ops if o['outcome'] == ':info' and o['f'] in (':write', ':cas')]
        no_leader = [o for o in info_mutations if o['error'] == [':unavailable', 'etcdserver: no leader']]
        config_summary.append(dict(scope=k[0], store=k[1], fault=k[2], budget=k[3], histories=len(cs),
                                   count=len(ops), ok=sum(c['ok'] for c in cs), fail=sum(c['fail'] for c in cs),
                                   info=sum(c['info'] for c in cs), info_mutations=len(info_mutations),
                                   no_leader_infos=len(no_leader),
                                   no_leader_at_most_10ms=sum(o['latency_ms'] <= 10 for o in no_leader),
                                   no_leader_p50_ms=latency_summary([o['latency_ms'] for o in no_leader])['p50_ms'],
                                   max_info_mutations_per_key=max(c['max_info_mutations_per_key'] for c in cs)))
        for f in (':all', *FS):
            for typ in OUTCOMES:
                v = [o['latency_ms'] for o in ops if o['outcome'] == typ and (f == ':all' or o['f'] == f)]
                fields = dict(scope=k[0], store=k[1], fault=k[2], budget=k[3], histories=len(cs), f=f, outcome=typ)
                config_counts.append(dict(**fields, count=len(v)))
                config_latencies.append(dict(**fields, **latency_summary(v)))
    csv_write(OUT/'counts-by-configuration.csv', config_counts)
    csv_write(OUT/'latencies-by-configuration.csv', config_latencies)
    csv_write(OUT/'configuration-summary.csv', config_summary)
    tables(cases, all_ops, faults)
    write_json(OUT/'analysis-runtime.json', dict(elapsed_seconds=round(time.monotonic()-start, 3),
               max_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
               histories=len(cases), completed_client_calls=len(all_ops),
               fault_records=len(faults), checker_executed=False))
    print(f"Analyzed {len(cases)} saved histories, {len(all_ops)} completed calls, {len(faults)} fault records")
    print(f"Outputs in {OUT}; no checker executed")


if __name__ == '__main__':
    main()
