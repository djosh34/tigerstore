#!/usr/bin/env python3
"""Exact existential completion search. Actual checks require GitHub Actions."""
import argparse
import datetime as dt
from fractions import Fraction
import json
import os
from pathlib import Path
import time

import z3
from history_io import prepare, sha256, write_edn

DEADLINE = dt.datetime.fromisoformat('2026-10-13T22:00:00+00:00').timestamp()


def allowed():
    assert os.environ.get('GITHUB_ACTIONS') == 'true', 'Solves require GitHub Actions'
    assert time.time() < DEADLINE, 'Deadline reached'


def build(calls, event_count):
    active = [o for o in calls if o['status'] != 'fail']
    mutations = [o for o in active if o['f'] != 'read']
    n = len(mutations)
    values = {None}
    for o in active:
        v = o['model_value'][1]
        values.update(v if o['f'] == 'cas' else [v])
    values = [None] + sorted(v for v in values if v is not None)
    code = {v: i for i, v in enumerate(values)}
    s = z3.Solver()
    s.set(random_seed=0)
    state = z3.Array('state', z3.IntSort(), z3.IntSort())
    clocks = z3.Array('clocks', z3.IntSort(), z3.RealSort())
    count = z3.Int('mutation_count')
    s.add(count >= 0, count <= n, state[0] == code[None], clocks[0] == -1)
    # Unused slots can always be placed after every selected read/mutation.
    s.add(clocks[n + 1] == event_count + n + 2)
    for k in range(n + 1):
        s.add(clocks[k] < clocks[k + 1])
    chosen, versions, times = {}, {}, {}
    for o in active:
        oid = o['id']
        chosen[oid] = z3.Bool('included_' + oid)
        versions[oid] = z3.Int('version_' + oid)
        times[oid] = z3.Real('time_' + oid)
        p, v, t = chosen[oid], versions[oid], times[oid]
        if o['status'] == 'ok':
            s.add(p)
        s.add(z3.Implies(p, t > o['invoke_pos']))
        if o['status'] == 'ok':
            s.add(z3.Implies(p, t < o['complete_pos']))
        ov, val = o['model_value']
        if ov is not None:
            s.add(z3.Implies(p, v == ov))
        if o['f'] == 'read':
            s.add(z3.Implies(p, z3.And(v >= 0, v <= count,
                                     clocks[v] < t, t < clocks[v + 1])))
            if val is not None:
                s.add(z3.Implies(p, state[v] == code[val]))
        else:
            s.add(z3.Implies(p, z3.And(v >= 1, v <= count, t == clocks[v])))
            if o['f'] == 'cas':
                old, new = val
                s.add(z3.Implies(p, state[v - 1] == code[old]))
            else:
                new = val
            s.add(z3.Implies(p, state[v] == code[new]))
    s.add(count == z3.Sum([z3.If(chosen[o['id']], 1, 0) for o in mutations]))
    if mutations:
        s.add(z3.Distinct([z3.If(chosen[o['id']], versions[o['id']], -i - 1)
                           for i, o in enumerate(mutations)]))
    # Redundant version bounds from known, completed observations. They do
    # not give an info response any completion bound.
    anchors = [o for o in active if o['status'] == 'ok' and o['model_value'][0] is not None]
    for o in active:
        p, v = chosen[o['id']], versions[o['id']]
        for a in anchors:
            av = a['model_value'][0]
            if a['complete_pos'] < o['invoke_pos']:
                s.add(z3.Implies(p, v >= av + (o['f'] != 'read')))
            if o['status'] == 'ok' and o['complete_pos'] < a['invoke_pos']:
                s.add(z3.Implies(p, v <= av - (a['f'] != 'read')))
    return s, chosen, versions, times, state, values


def rational(x):
    assert z3.is_rational_value(x), str(x)
    return str(Fraction(x.numerator_as_long(), x.denominator_as_long()))


def search(full, saved, key, out, seconds=240, restricted_seconds=20):
    allowed()
    started = time.time()
    out.mkdir(parents=True, exist_ok=True)
    events, projection, calls, keys = prepare(full, saved, key)
    s, chosen, versions, times, state, values = build(calls, len(events))
    no_leader = [o['id'] for o in calls if o['status'] == 'info'
                 and 'no leader' in json.dumps(o['response'].get('error'))]
    attempts = []
    result = None
    phase = None
    for phase, limit in [('omit-no-leader', restricted_seconds), ('unrestricted', seconds)]:
        if phase == 'omit-no-leader' and not no_leader:
            continue
        if time.time() >= DEADLINE:
            attempts.append({'phase': phase, 'result': 'deadline-not-started'})
            break
        s.push()
        if phase == 'omit-no-leader':
            s.add([z3.Not(chosen[oid]) for oid in no_leader])
        budget = min(limit, max(0, DEADLINE - time.time() - 5))
        if budget <= 0:
            s.pop()
            break
        s.set(timeout=max(1, int(budget * 1000)))
        print(f'key={key} phase={phase} calls={len(calls)} no_leader={len(no_leader)} budget={budget}', flush=True)
        allowed()
        t0 = time.time()
        result = s.check()
        entry = {'phase': phase, 'result': str(result), 'seconds': time.time() - t0,
                 'statistics': str(s.statistics())}
        if result == z3.unknown:
            entry['reason'] = s.reason_unknown()
        attempts.append(entry)
        (out / 'attempts.json').write_text(json.dumps(attempts, indent=2) + '\n')
        print(json.dumps({k: v for k, v in entry.items() if k != 'statistics'}), flush=True)
        if result == z3.sat:
            break
        s.pop()
    summary = {'key': key, 'valid': 'unknown', 'solver_result': str(result),
               'phase': phase, 'attempts': attempts, 'seconds': time.time() - started,
               'z3_version': z3.get_version_string(), 'source_keys': keys,
               'full_sha256': sha256(full), 'independent_sha256': sha256(saved),
               'operation_count': len(calls), 'key_event_count': len(projection)}
    if result != z3.sat:
        summary['note'] = 'UNSAT is not a product bug verdict; unknowns do not pass.'
        (out / 'solve.json').write_text(json.dumps(summary, indent=2) + '\n')
        return summary
    model = s.model()
    decisions = []
    for o in calls:
        d = dict(o)
        if o['status'] == 'fail':
            d['decision'] = 'upstream-fail'
        elif not z3.is_true(model.eval(chosen[o['id']], model_completion=True)):
            d['decision'] = 'omitted'
        else:
            d['decision'] = 'included'
            v = model.eval(versions[o['id']], model_completion=True).as_long()
            d['model_version'] = v
            d['time'] = rational(model.eval(times[o['id']], model_completion=True))
            value_code = model.eval(state[v], model_completion=True).as_long()
            d['state_after'] = {'version': v, 'value': values[value_code]}
        decisions.append(d)
    order = [o['id'] for o in sorted((o for o in decisions if o['decision'] == 'included'),
                                     key=lambda o: (Fraction(o['time']), o['invoke_pos']))]
    witness = {'schema': 1, 'key': key, 'full_sha256': sha256(full),
               'independent_sha256': sha256(saved), 'phase': phase,
               'full_event_count': len(events), 'events': projection,
               'operations': decisions, 'order': order}
    (out / 'witness.json').write_text(json.dumps(witness, indent=2) + '\n')
    (out / 'witness.edn').write_text(write_edn(witness) + '\n')
    summary['note'] = 'SAT candidate only. Both independent validators must pass.'
    summary['included_infos'] = sum(o['decision'] == 'included' and o['status'] in ('info', 'unfinished') for o in decisions)
    summary['omitted_infos'] = sum(o['decision'] == 'omitted' for o in decisions)
    (out / 'solve.json').write_text(json.dumps(summary, indent=2) + '\n')
    return summary


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('full', type=Path)
    p.add_argument('saved', type=Path)
    p.add_argument('key', type=int)
    p.add_argument('out', type=Path)
    p.add_argument('--seconds', type=int, default=240)
    p.add_argument('--restricted-seconds', type=int, default=20)
    args = p.parse_args()
    print(json.dumps(search(args.full, args.saved, args.key, args.out,
                            args.seconds, args.restricted_seconds), indent=2))
