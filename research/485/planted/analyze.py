#!/usr/bin/env python3
"""Aggregate exp485-planted artifacts, including disjoint recovery prefixes.

No third-party modules. Rates are survey execution only; control-inclusive
rates charge each eligible seed's control anew, even when the driver cached it.
Nominal 95% Wilson intervals describe per-seed catch proportions, not rates.
"""
import argparse
from collections import defaultdict
import csv
import json
import math
from pathlib import Path

DS = [5, 10, 20, 40, 80, 160, 320]
BUGS = 'B01a B01b B02 B03 B04a B04b B04c B05 B06a B06b B06c B06d B07 B08 B09a B09b B10 B11 B12 B15 B16 B18 B19 B20 B21 B23 B24 B25 B27'.split()
SPEEDUP = 3.4
BUDGET_S = 3600 / len(BUGS)


def wilson(k, n):
    if not 0 <= k <= n or n <= 0:
        raise ValueError('need 0 <= catches <= seeds and seeds > 0')
    z = 1.96
    den = 1 + z*z/n
    mid = (k/n + z*z/(2*n))/den
    delta = z*math.sqrt((k/n)*(1-k/n)/n + z*z/(4*n*n))/den
    return max(0, mid-delta), min(1, mid+delta)


def distinct(p, trials):
    """Plug-in probability of at least one catch in a whole number of seeds."""
    if not 0 <= p <= 1 or trials < 0:
        raise ValueError('invalid catch probability or trial count')
    trials = math.floor(trials)
    if trials == 0 or p == 0:
        return 0.0
    if p == 1:
        return 1.0
    return -math.expm1(trials * math.log1p(-p))


def observed_within(rows, budget, controls=False):
    consumed = 0.0
    for row in rows:
        consumed += (row['wall_ms'] + (row.get('control_wall_ms', 0) if controls else 0))/1000
        if consumed > budget:
            break
        if row['caught']:
            return 1
    return 0


def analyze(rows):
    cells = defaultdict(list)
    all_by_d = defaultdict(list)
    for r in rows:
        if r['D'] not in DS or r['bug'] not in ['plain'] + BUGS:
            raise ValueError(f'unknown cell: {r["bug"]}/{r["D"]}')
        if not math.isfinite(r['wall_ms']) or r['wall_ms'] <= 0:
            raise ValueError('wall time must be positive and finite')
        if r['caught'] and (r['bug'] == 'plain' or r['other_fail'] or r['baseline_fail'] or not r['finding'] or r.get('control_wall_ms', 0) <= 0):
            raise ValueError('invalid validated catch')
        all_by_d[r['D']].append(r)
        if r['phase'] == 'survey':
            cells[r['bug'], r['D']].append(r)
        elif r['phase'] != 'control':
            raise ValueError('unknown phase')
    records, missing = [], []
    for bug in ['plain'] + BUGS:
        for d in DS:
            rr = sorted(cells[bug, d], key=lambda r: r['seed'])
            if not rr:
                missing.append(f'{bug}/{d}')
                continue
            if [r['seed'] for r in rr] != list(range(1, len(rr)+1)):
                raise ValueError(f'{bug}/{d}: nonconsecutive or duplicate survey seeds')
            n = len(rr)
            k = sum(r['caught'] for r in rr)
            wall = sum(r['wall_ms'] for r in rr)/1000
            standalone = wall + sum(r.get('control_wall_ms', 0) for r in rr)/1000
            lo, hi = wilson(k, n)
            rss = [r['peak_rss_kb']/1024 for r in rr if r.get('peak_rss_kb') is not None]
            records.append(dict(
                bug=bug, D=d, seeds=n, catches=k, p=k/n, p95_lo=lo, p95_hi=hi,
                wall_s=wall, s_per_seed=wall/n, catches_per_h=3600*k/wall,
                catches_per_h_4_est=SPEEDUP*3600*k/wall,
                standalone_validated_catches_per_h=3600*k/standalone,
                standalone_s_per_seed=standalone/n,
                other_fails=sum(r['other_fail'] for r in rr),
                missing_result_failures=sum(r['finding'].startswith('infrastructure:') for r in rr),
                baseline_rejects=sum(r['baseline_fail'] for r in rr),
                max_rss_mib=max(rss) if rss else None,
                median_rss_mib=sorted(rss)[len(rss)//2] if rss else None,
                observed_bug_at_1h_equal_share=observed_within(rr, BUDGET_S),
                observed_validated_bug_at_1h_equal_share=observed_within(rr, BUDGET_S, controls=True),
                expected_bug_at_1h_equal_share=distinct(k/n, BUDGET_S/(wall/n)),
                expected_bug_at_1h_4_est=distinct(k/n, SPEEDUP*BUDGET_S/(wall/n)),
                expected_validated_bug_at_1h_equal_share=distinct(k/n, BUDGET_S/(standalone/n)),
                run_ids=';'.join(sorted({str(r['run_id']) for r in rr})),
                commits=';'.join(sorted({r['commit'] for r in rr}))))
    rec = {(r['bug'], r['D']): r for r in records}
    totals = []
    for d in DS:
        cc = [rec[b, d] for b in BUGS if (b, d) in rec]
        if not cc:
            continue
        plain = rec.get(('plain', d), {})
        # Always divide by all 29 bugs: missing cells must never inflate rates.
        totals.append(dict(
            D=d, bugs_sampled=len(cc), seeds=sum(c['seeds'] for c in cc),
            catches=sum(c['catches'] for c in cc),
            bugs_ever_caught=sum(c['catches'] > 0 for c in cc),
            equal_share_catches_per_h=sum(c['catches_per_h'] for c in cc)/len(BUGS),
            equal_share_catches_per_h_4_est=SPEEDUP*sum(c['catches_per_h'] for c in cc)/len(BUGS),
            equal_share_validated_catches_per_h=sum(c['standalone_validated_catches_per_h'] for c in cc)/len(BUGS),
            observed_bugs_1h=sum(c['observed_bug_at_1h_equal_share'] for c in cc),
            observed_validated_bugs_1h=sum(c['observed_validated_bug_at_1h_equal_share'] for c in cc),
            expected_bugs_1h=sum(c['expected_bug_at_1h_equal_share'] for c in cc),
            expected_bugs_1h_4_est=sum(c['expected_bug_at_1h_4_est'] for c in cc),
            expected_validated_bugs_1h=sum(c['expected_validated_bug_at_1h_equal_share'] for c in cc),
            survey_runner_h=(sum(c['wall_s'] for c in cc) + plain.get('wall_s', 0))/3600,
            measured_runner_h_excluding_builds=sum(r['wall_ms'] for r in all_by_d[d])/3600000,
            other_fails=sum(c['other_fails'] for c in cc),
            missing_result_failures=sum(c['missing_result_failures'] for c in cc),
            baseline_rejects=sum(c['baseline_rejects'] for c in cc),
            plain_seeds=plain.get('seeds'), plain_false_fails=plain.get('other_fails'),
            plain_s_per_seed=plain.get('s_per_seed'), plain_max_rss_mib=plain.get('max_rss_mib')))
    return dict(missing=missing, totals=totals, records=records)


def write_csv(path, records):
    with path.open('w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=list(records[0]))
        writer.writeheader()
        writer.writerows(records)


def tables(summary):
    rec = {(r['bug'], r['D']): r for r in summary['records']}
    lines = ['# Planted-bug duration results', '',
             'Each cell: **catches/seeds (catch % [nominal 95% Wilson interval]); wall s/seed; catches/runner-h, 1 process / estimated 4 processes**.',
             'Rates exclude controls and builds. Four-process rate = 3.4x, not measured. Every counted catch passed its identical unplanted seed.', '',
             '| Bug | ' + ' | '.join(str(d)+' s' for d in DS) + ' |',
             '|---|' + '---|'*len(DS)]
    for bug in BUGS:
        vals = []
        for d in DS:
            c = rec.get((bug, d))
            vals.append((f"{c['catches']}/{c['seeds']} ({100*c['p']:.1f}% [{100*c['p95_lo']:.1f}–{100*c['p95_hi']:.1f}]); "
                         f"{c['s_per_seed']:.3f}; {c['catches_per_h']:.0f}/{c['catches_per_h_4_est']:.0f}") if c else 'MISSING')
        lines.append('| '+bug+' | '+' | '.join(vals)+' |')
    lines += ['', '## One runner-hour shared equally across all 29 mutants', '',
              'Observed distinct counts use only completed survey seeds in the first 3600/29 = 124.14 seconds per mutant.',
              'Expected distinct counts use p̂ and floor(budget/mean seed cost) independent trials. Zero observed catches is not proof of zero probability.',
              'Cells use consecutive, duration-dependent seed prefixes and outcome-dependent costs; intervals and extrapolations are descriptive, not a randomized duration-effect test.', '',
              '| D | Ever caught / 29 | Catches/h | 4-process estimate | Observed distinct in 1 h | Expected distinct in 1 h | 4-process distinct estimate | Plain failures/seeds | Plain s/seed |',
              '|---|---|---|---|---|---|---|---|---|']
    for t in summary['totals']:
        lines.append(f"| {t['D']} | {t['bugs_ever_caught']} | {t['equal_share_catches_per_h']:.1f} | {t['equal_share_catches_per_h_4_est']:.1f} | {t['observed_bugs_1h']} | {t['expected_bugs_1h']:.3f} | {t['expected_bugs_1h_4_est']:.3f} | {t['plain_false_fails']}/{t['plain_seeds']} | {t['plain_s_per_seed']:.3f} |")
    lines += ['', '## Standalone control validation charged anew per eligible seed', '',
              '| D | Validated catches/h | Observed validated distinct / 1 h | Estimated validated distinct / 1 h | Other failures | Missing-result failures (included in other) | Baseline rejects | Actual seed+control runner-h |',
              '|---|---|---|---|---|---|---|---|']
    for t in summary['totals']:
        lines.append(f"| {t['D']} | {t['equal_share_validated_catches_per_h']:.1f} | {t['observed_validated_bugs_1h']} | {t['expected_validated_bugs_1h']:.3f} | {t['other_fails']} | {t['missing_result_failures']} | {t['baseline_rejects']} | {t['measured_runner_h_excluding_builds']:.3f} |")
    lines += ['', 'Run IDs: '+', '.join(sorted({rid for r in summary['records'] for rid in r['run_ids'].split(';')})),
              'Commit SHAs: '+', '.join(sorted({sha for r in summary['records'] for sha in r['commits'].split(';')})),
              'Missing cells: '+(', '.join(summary['missing']) or 'none'), '']
    return '\n'.join(lines)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('artifacts', type=Path)
    ap.add_argument('--out', type=Path, default=Path('/tmp/exp485-planted-analysis'))
    ap.add_argument('--allow-incomplete', action='store_true')
    args = ap.parse_args()
    rows = []
    for path in sorted(args.artifacts.rglob('seeds.jsonl')):
        rows.extend(json.loads(line) for line in path.read_text().splitlines() if line)
    if not rows:
        raise SystemExit('no seed rows')
    summary = analyze(rows)
    if summary['missing'] and not args.allow_incomplete:
        raise SystemExit('missing cells (use --allow-incomplete for provisional analysis): '+', '.join(summary['missing']))
    args.out.mkdir(parents=True, exist_ok=True)
    write_csv(args.out/'cells.csv', summary['records'])
    write_csv(args.out/'totals.csv', summary['totals'])
    (args.out/'summary.json').write_text(json.dumps(summary, indent=2)+'\n')
    (args.out/'tables.md').write_text(tables(summary))
    print(json.dumps(dict(missing=summary['missing'], totals=summary['totals']), indent=2))


if __name__ == '__main__':
    main()
