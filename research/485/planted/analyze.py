#!/usr/bin/env python3
"""Aggregate downloaded exp485-planted artifacts; no third-party modules."""
import argparse
from collections import defaultdict
import csv
import json
import math
from pathlib import Path

DS = [5, 10, 20, 40, 80, 160, 320]
BUGS = 'B01a B01b B02 B03 B04a B04b B04c B05 B06a B06b B06c B06d B07 B08 B09a B09b B10 B11 B12 B15 B16 B18 B19 B20 B21 B23 B24 B25 B27'.split()

def wilson(k, n):
    z = 1.96
    den = 1 + z*z/n
    mid = (k/n + z*z/(2*n))/den
    delta = z*math.sqrt((k/n)*(1-k/n)/n + z*z/(4*n*n))/den
    return max(0, mid-delta), min(1, mid+delta)

def distinct(p, trials):
    if p == 1:
        return 1.0
    return -math.expm1(trials * math.log1p(-p))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('artifacts', type=Path)
    ap.add_argument('--out', type=Path, default=Path('/tmp/exp485-planted-analysis'))
    args = ap.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    rows = []
    for path in sorted(args.artifacts.rglob('seeds.jsonl')):
        rows.extend(json.loads(line) for line in path.read_text().splitlines() if line)
    cells = defaultdict(list)
    all_by_d = defaultdict(list)
    for r in rows:
        all_by_d[r['D']].append(r)
        if r['phase'] == 'survey':
            cells[r['bug'], r['D']].append(r)
    records = []
    missing = []
    for bug in ['plain'] + BUGS:
        for d in DS:
            rr = sorted(cells[bug, d], key=lambda r: r['seed'])
            if not rr:
                missing.append(f'{bug}/{d}')
                continue
            assert [r['seed'] for r in rr] == list(range(1, len(rr)+1)), (bug, d, 'nonconsecutive or duplicate seeds')
            n = len(rr)
            k = sum(r['caught'] for r in rr)
            wall = sum(r['wall_ms'] for r in rr)/1000
            standalone = wall + sum(r.get('control_wall_ms', 0) for r in rr)/1000
            lo, hi = wilson(k, n)
            budget = 3600/len(BUGS)
            consumed = standalone_consumed = 0.0
            caught_budget = standalone_caught_budget = False
            for r in rr:
                consumed += r['wall_ms']/1000
                standalone_consumed += (r['wall_ms']+r.get('control_wall_ms', 0))/1000
                if consumed <= budget and r['caught']:
                    caught_budget = True
                if standalone_consumed <= budget and r['caught']:
                    standalone_caught_budget = True
            rss = [r['peak_rss_kb']/1024 for r in rr if r.get('peak_rss_kb') is not None]
            record = dict(bug=bug, D=d, seeds=n, catches=k, p=k/n, p95_lo=lo, p95_hi=hi,
                          wall_s=wall, s_per_seed=wall/n, catches_per_h=3600*k/wall,
                          catches_per_h_4_est=3.4*3600*k/wall,
                          standalone_validated_catches_per_h=3600*k/standalone,
                          standalone_s_per_seed=standalone/n,
                          other_fails=sum(r['other_fail'] for r in rr),
                          baseline_rejects=sum(r['baseline_fail'] for r in rr),
                          max_rss_mib=max(rss) if rss else None,
                          median_rss_mib=sorted(rss)[len(rss)//2] if rss else None,
                          observed_bug_at_1h_equal_share=int(caught_budget),
                          observed_validated_bug_at_1h_equal_share=int(standalone_caught_budget),
                          expected_bug_at_1h_equal_share=distinct(k/n, budget/(wall/n)),
                          expected_bug_at_1h_4_est=distinct(k/n, 3.4*budget/(wall/n)),
                          expected_validated_bug_at_1h_equal_share=distinct(k/n, budget/(standalone/n)),
                          run_ids=';'.join(sorted({str(r['run_id']) for r in rr})),
                          commits=';'.join(sorted({r['commit'] for r in rr})))
            records.append(record)
    if not records:
        raise SystemExit('no survey rows')
    with (args.out/'cells.csv').open('w') as f:
        writer = csv.DictWriter(f, fieldnames=list(records[0])); writer.writeheader(); writer.writerows(records)
    rec = {(r['bug'], r['D']): r for r in records}
    totals = []
    for d in DS:
        cc = [rec[b, d] for b in BUGS if (b, d) in rec]
        if not cc:
            continue
        plain = rec.get(('plain', d), {})
        totals.append(dict(D=d, bugs_sampled=len(cc), seeds=sum(c['seeds'] for c in cc),
                           catches=sum(c['catches'] for c in cc),
                           bugs_ever_caught=sum(c['catches']>0 for c in cc),
                           equal_share_catches_per_h=sum(c['catches_per_h'] for c in cc)/len(cc),
                           equal_share_catches_per_h_4_est=3.4*sum(c['catches_per_h'] for c in cc)/len(cc),
                           equal_share_validated_catches_per_h=sum(c['standalone_validated_catches_per_h'] for c in cc)/len(cc),
                           observed_bugs_1h=sum(c['observed_bug_at_1h_equal_share'] for c in cc),
                           observed_validated_bugs_1h=sum(c['observed_validated_bug_at_1h_equal_share'] for c in cc),
                           expected_bugs_1h=sum(c['expected_bug_at_1h_equal_share'] for c in cc),
                           expected_bugs_1h_4_est=sum(c['expected_bug_at_1h_4_est'] for c in cc),
                           expected_validated_bugs_1h=sum(c['expected_validated_bug_at_1h_equal_share'] for c in cc),
                           survey_runner_h=sum(c['wall_s'] for c in cc)/3600 + plain.get('wall_s', 0)/3600,
                           measured_runner_h_excluding_builds=sum(r['wall_ms'] for r in all_by_d[d])/3600000,
                           other_fails=sum(c['other_fails'] for c in cc), baseline_rejects=sum(c['baseline_rejects'] for c in cc),
                           plain_seeds=plain.get('seeds'), plain_false_fails=plain.get('other_fails'),
                           plain_s_per_seed=plain.get('s_per_seed'), plain_max_rss_mib=plain.get('max_rss_mib')))
    with (args.out/'totals.csv').open('w') as f:
        writer = csv.DictWriter(f, fieldnames=list(totals[0])); writer.writeheader(); writer.writerows(totals)
    (args.out/'summary.json').write_text(json.dumps(dict(missing=missing, totals=totals, records=records), indent=2)+'\n')
    lines = ['# Measured planted-bug duration results', '',
             'Each cell is **catches/seeds; wall seconds/seed; catches per one-process runner-hour**.',
             'Every catch passed a paired unplanted control. Control time is excluded here; the CSV also charges standalone controls.', '',
             '| Bug | ' + ' | '.join(str(d)+' s' for d in DS) + ' |',
             '|---|' + '---|'*len(DS)]
    for bug in BUGS:
        vals = []
        for d in DS:
            c = rec.get((bug, d))
            vals.append(f"{c['catches']}/{c['seeds']}; {c['s_per_seed']:.2f}; {c['catches_per_h']:.1f}" if c else 'MISSING')
        lines.append('| '+bug+' | '+' | '.join(vals)+' |')
    lines += ['', 'Four-process catches/hour for **each cell** = 3.4 times its last number (estimate from #448).', '',
              '| D | Ever caught / 29 | Catches/h, equal shares | 4-proc estimate | Observed distinct in 1 h | Expected distinct in 1 h | 4-proc distinct estimate | Plain failures/seeds | Plain s/seed | Plain peak MiB |',
              '|---|---|---|---|---|---|---|---|---|---|']
    for t in totals:
        lines.append(f"| {t['D']} | {t['bugs_ever_caught']} | {t['equal_share_catches_per_h']:.1f} | {t['equal_share_catches_per_h_4_est']:.1f} | {t['observed_bugs_1h']} | {t['expected_bugs_1h']:.2f} | {t['expected_bugs_1h_4_est']:.2f} | {t['plain_false_fails']}/{t['plain_seeds']} | {t['plain_s_per_seed']:.2f} | {t['plain_max_rss_mib']:.0f} |")
    lines += ['', '## Full cost of standalone control validation', '',
              '| D | Validated catches/h | Observed validated distinct / 1h | Estimated validated distinct / 1h | Other failures | Baseline rejections | Actual seed+control runner-h |',
              '|---|---|---|---|---|---|---|']
    for t in totals:
        lines.append(f"| {t['D']} | {t['equal_share_validated_catches_per_h']:.1f} | {t['observed_validated_bugs_1h']} | {t['expected_validated_bugs_1h']:.2f} | {t['other_fails']} | {t['baseline_rejects']} | {t['measured_runner_h_excluding_builds']:.3f} |")
    lines += ['', 'Run IDs: '+', '.join(sorted({str(r['run_id']) for r in rows})),
              'Commit SHAs: '+', '.join(sorted({r['commit'] for r in rows})),
              'Missing cells: '+(', '.join(missing) or 'none'), '']
    (args.out/'tables.md').write_text('\n'.join(lines))
    print(json.dumps(dict(missing=missing, totals=totals), indent=2))

if __name__ == '__main__':
    main()
