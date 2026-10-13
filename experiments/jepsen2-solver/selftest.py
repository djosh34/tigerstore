#!/usr/bin/env python3
"""Runner-only regression and exhaustive small-history differential tests."""
import copy
import itertools
import json
from pathlib import Path
import random
import sys

from history_io import prepare, write_edn
from solve import allowed, search
from verify import verify


def event(p, kind, f, value, error=None):
    e = {'process': p, 'type': kind, 'f': f, 'value': [0, value]}
    if error is not None:
        e['error'] = error
    return e


def pair(p, f, arg, result, kind='ok', error=None):
    return [event(p, 'invoke', f, arg), event(p, kind, f, result, error)]


def raw_edn(events):
    text = '\n'.join(write_edn(e) for e in events) + '\n'
    for x in ('invoke', 'ok', 'info', 'fail'):
        text = text.replace(':type "' + x + '"', ':type :' + x)
    for x in ('read', 'write', 'cas'):
        text = text.replace(':f "' + x + '"', ':f :' + x)
    return text


def brute(calls):
    required = [o for o in calls if o['status'] == 'ok']
    optional = [o for o in calls if o['status'] in ('info', 'unfinished')]
    for mask in range(1 << len(optional)):
        chosen = required + [o for i, o in enumerate(optional) if mask & (1 << i)]
        for order in itertools.permutations(chosen):
            ranks = {o['id']: i for i, o in enumerate(order)}
            if any(a['complete_pos'] < b['invoke_pos'] and ranks[a['id']] >= ranks[b['id']]
                   for a in required for b in chosen):
                continue
            version, value, legal = 0, None, True
            for o in order:
                ov, val = o['model_value']
                if o['f'] == 'read':
                    legal = (ov is None or ov == version) and (val is None or val == value)
                else:
                    legal = ov is None or ov == version + 1
                    if o['f'] == 'cas':
                        old, new = val
                        legal = legal and old == value
                    else:
                        new = val
                    version += 1
                    value = new
                if not legal:
                    break
            if legal:
                return True
    return False


def fixed():
    w1 = pair(0, 'write', [None, 1], [1, 1])
    return [
        ('metadata-set-symbol', w1 + pair(1, 'write', [None, 2], [None, 2], 'info', 'SYMBOL_SENTINEL')
             + pair(2, 'read', [None, None], [1, 1]), True),
        ('empty-read', pair(0, 'read', [None, None], [0, None]), True),
        ('write-read', w1 + pair(1, 'read', [None, None], [1, 1]), True),
        ('read-wildcards', w1 + pair(1, 'read', [None, None], [None, None]), True),
        ('stale-read', w1 + pair(1, 'read', [None, None], [0, 1]), False),
        ('version-gap', pair(0, 'write', [None, 1], [2, 1]), False),
        ('cas-compare', w1 + pair(1, 'cas', [None, [2, 3]], [2, [2, 3]]), False),
        ('cas-nil-compare', pair(0, 'cas', [None, [None, 2]], [1, [None, 2]]), True),
        ('required-info-write', pair(0, 'write', [None, 2], [None, 2], 'info')
             + pair(1, 'read', [None, None], [1, 2]), True),
        ('required-no-leader', w1 + pair(1, 'write', [None, 2], [None, 2], 'info', ['unavailable', 'etcdserver: no leader'])
             + pair(2, 'read', [None, None], [2, 2]), True),
        ('required-info-cas', w1 + pair(1, 'cas', [None, [1, 2]], [None, [1, 2]], 'info')
             + pair(2, 'read', [None, None], [2, 2]), True),
        ('bad-info-cas', w1 + pair(1, 'cas', [None, [7, 2]], [None, [7, 2]], 'info')
             + pair(2, 'read', [None, None], [2, 2]), False),
        ('late-info-cas', w1 + pair(1, 'cas', [None, [2, 3]], [None, [2, 3]], 'info')
             + pair(2, 'write', [None, 2], [2, 2]) + pair(3, 'read', [None, None], [3, 3]), True),
        ('failed-cas-removed', w1 + pair(1, 'cas', [None, [1, 9]], [None, [1, 9]], 'fail')
             + pair(2, 'read', [None, None], [1, 1]), True),
        ('failed-write-no-effect', pair(0, 'write', [None, 2], [None, 2], 'fail')
             + pair(1, 'read', [None, None], [1, 2]), False),
        ('unfinished-needed', [event(0, 'invoke', 'write', [None, 2])]
             + pair(1, 'read', [None, None], [1, 2]), True),
        ('future-invoke-forbidden', pair(1, 'read', [None, None], [1, 2])
             + [event(0, 'invoke', 'write', [None, 2])], False),
        ('same-value-increments', w1 + pair(1, 'write', [None, 1], [2, 1])
             + pair(2, 'read', [None, None], [2, 1]), True),
        ('nil-write', w1 + pair(1, 'write', [None, None], [2, None])
             + pair(2, 'read', [None, None], [2, None]), True),
    ]


def randomized(rng):
    queues = []
    for p in range(rng.randint(1, 5)):
        f = rng.choice(['read', 'write', 'cas'])
        kind = rng.choice(['ok', 'ok', 'info', 'fail'])
        arg = [None, None if f == 'read' else
               [rng.choice([None, 0, 1, 2]), rng.randrange(3)] if f == 'cas' else rng.randrange(3)]
        result = list(arg)
        if kind == 'ok':
            result[0] = rng.choice([None, 0, 1, 2, 3])
            if f == 'read':
                result[1] = rng.choice([None, 0, 1, 2])
        queues.append(pair(p, f, arg, result, kind))
    events = []
    while queues:
        q = rng.randrange(len(queues))
        events.append(queues[q].pop(0))
        if not queues[q]:
            queues.pop(q)
    return events


def main(root):
    allowed()
    root.mkdir(parents=True, exist_ok=True)
    rng = random.Random(617241)
    tests = fixed() + [(f'differential-{i:03}', randomized(rng), None) for i in range(80)]
    reports, clojure_args = [], []
    for name, events, expected in tests:
        allowed()
        directory = root / name
        directory.mkdir()
        full = directory / 'full.edn'
        saved = directory / 'independent.edn'
        saved_events = [dict(e, value=e['value'][1]) for e in events]
        full_text, saved_text = raw_edn(events), raw_edn(saved_events)
        if name == 'metadata-set-symbol':
            nemesis = '{:process :nemesis :type :info :f :partition :value #{#{"n1" "n2"} #{"n3"}}}\n'
            full_text = nemesis + full_text.replace('"SYMBOL_SENTINEL"', 'io.netty.channel.StacklessClosedChannelException')
            saved_text = nemesis + saved_text.replace('"SYMBOL_SENTINEL"', 'io.netty.channel.StacklessClosedChannelException')
        full.write_text(full_text)
        saved.write_text(saved_text)
        _, _, calls, _ = prepare(full, saved, 0)
        exact = brute(calls)
        if expected is not None:
            assert exact == expected, name
        result = search(full, saved, 0, directory, seconds=10, restricted_seconds=2)
        assert result['solver_result'] in ('sat', 'unsat'), (name, result)
        assert (result['solver_result'] == 'sat') == exact, name
        if exact:
            validation = verify(full, saved, directory / 'witness.json')
            (directory / 'python-validation.json').write_text(json.dumps(validation, indent=2) + '\n')
            clojure_args += [str(full), str(saved), str(directory / 'witness.edn'), str(directory / 'clojure-validation.edn')]
            if name == 'required-no-leader':
                assert result['phase'] == 'unrestricted'
                assert result['included_infos'] == 1
            if name == 'late-info-cas':
                w = json.loads((directory / 'witness.json').read_text())
                late = next(o for o in w['operations'] if o['status'] == 'info')
                from fractions import Fraction
                assert Fraction(late['time']) > late['complete_pos']
        reports.append({'test': name, 'expected_sat': exact, 'result': result['solver_result']})
    # Reject coverage, state, time, and optional-completion tampering in both
    # validators. Each candidate starts from a separately accepted witness.
    negatives = []
    for name in ('omit-success', 'bad-state', 'bad-time', 'reversed-order', 'missing-operation', 'omit-required-info'):
        original = root / ('required-no-leader' if name == 'omit-required-info' else 'write-read')
        w = copy.deepcopy(json.loads((original / 'witness.json').read_text()))
        included = [o for o in w['operations'] if o['decision'] == 'included']
        target = next(o for o in included if o['status'] == 'info') if name == 'omit-required-info' else included[0]
        if name in ('omit-success', 'omit-required-info'):
            target['decision'] = 'omitted'
            for k in ('time', 'state_after', 'model_version'):
                target.pop(k)
            w['order'].remove(target['id'])
        elif name == 'bad-state':
            target['state_after']['value'] = 999
        elif name == 'bad-time':
            target['time'] = str(target['invoke_pos'])
        elif name == 'reversed-order':
            w['order'].reverse()
        elif name == 'missing-operation':
            w['operations'].remove(target)
            w['order'].remove(target['id'])
        dest = root / ('reject-' + name)
        dest.mkdir()
        (dest / 'witness.json').write_text(json.dumps(w, indent=2) + '\n')
        (dest / 'witness.edn').write_text(write_edn(w) + '\n')
        rejected = False
        try:
            verify(original / 'full.edn', original / 'independent.edn', dest / 'witness.json')
        except AssertionError:
            rejected = True
        assert rejected, name
        negatives += [str(original / 'full.edn'), str(original / 'independent.edn'),
                      str(dest / 'witness.edn'), str(dest / 'should-not-exist.edn')]
    # Run all exact-model replays in one small JVM, without any search.
    (root / 'clojure-args.json').write_text(json.dumps(clojure_args))
    (root / 'clojure-reject-args.json').write_text(json.dumps(negatives))
    (root / 'summary.json').write_text(json.dumps({'valid': True, 'tests': reports,
        'differential_cases': 80, 'negative_verifier_cases': len(negatives) // 4,
        'exact_model_sat_witnesses': len(clojure_args) // 4}, indent=2) + '\n')
    print(f'{len(tests)} solver/brute tests passed; {len(clojure_args)//4} witnesses await exact Clojure replay')


if __name__ == '__main__':
    main(Path(sys.argv[1]))
