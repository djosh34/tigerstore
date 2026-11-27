#!/usr/bin/env python3
"""#897 option-A gate, separate from the unchanged absolute #833 diagnostic.

Five alternating AB/BA pairs. EVERY candidate/main throughput ratio is >=0.98.
P95 noise is one robust baseline-only pair-to-pair sigma, in milliseconds:
    1.4826 * median(abs(main_p95[i] - median(main_p95)))
Every candidate p95 must be <= its paired main p95 plus that allowance.
No candidate variance, absolute-target substitution, outlier removal, or
post-hoc tolerance floor is used. P99/max remain visible diagnostics.
"""
import math
import statistics


NOISE_METHOD = '1.4826 * MAD of all five main p95 values, milliseconds; baseline-only; no floor or outlier removal'


def number(value, name):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
        raise ValueError(f'{name} must be finite and positive')
    return value


def distribution(values):
    result = dict(median=statistics.median(values), min=min(values), max=max(values))
    if len(values) > 1:
        q = statistics.quantiles(values, n=4, method='inclusive')
        result['iqr'] = q[2]-q[0]
    else:
        result['iqr'] = None
    return result


def assess(pairs):
    if len(pairs) > 5:
        raise ValueError('option A needs exactly five chronological pairs')
    rows, reasons = [], []
    for i, pair in enumerate(pairs):
        if pair.get('pair') != i+1 or pair.get('order') != ('AB' if i % 2 == 0 else 'BA'):
            raise ValueError('pair numbers/order must be 1..5, alternating AB/BA')
        b, c = pair['baseline'], pair['candidate']
        ratio = number(c['throughput'], 'candidate throughput') / number(b['throughput'], 'main throughput')
        delta = number(c['latency_ms']['p95'], 'candidate p95') - number(b['latency_ms']['p95'], 'main p95')
        rows.append(dict(pair=i+1, order=pair['order'], throughput_ratio=ratio,
                         throughput_pass=ratio >= .98, p95_delta_ms=delta))
        if ratio < .98:
            reasons.append(f'pair {i+1} throughput ratio {ratio:.12f} < 0.98')
    report = dict(schema=1, policy='PR897-option-A', required_pairs=5,
                  completed_pairs=len(pairs), pairs=rows, noise_method=NOISE_METHOD,
                  noise_ms=None, status='INCOMPLETE', reasons=reasons)
    if pairs:
        report['sides'] = {}
        for side in ('baseline', 'candidate'):
            report['sides'][side] = dict(
                throughput=distribution([p[side]['throughput'] for p in pairs]),
                wall_ms=distribution([p[side]['wall_ms'] for p in pairs]),
                latency_ms={tail: distribution([p[side]['latency_ms'][tail] for p in pairs]) for tail in ('p95', 'p99', 'max')},
                refusals={field: [p[side]['refusals'][field] for p in pairs] for field in ('total', 'max_per_range')})
    if len(pairs) == 5:
        baseline = [p['baseline']['latency_ms']['p95'] for p in pairs]
        center = statistics.median(baseline)
        mad = statistics.median(abs(v-center) for v in baseline)
        noise = 1.4826 * mad
        report.update(noise_ms=noise, baseline_p95_ms=baseline,
                      baseline_p95_median_ms=center, baseline_p95_mad_ms=mad)
        for row in rows:
            row['p95_pass'] = row['p95_delta_ms'] <= noise
            if not row['p95_pass']:
                reasons.append(f"pair {row['pair']} p95 delta {row['p95_delta_ms']:.6f} ms exceeds baseline noise {noise:.6f} ms")
        # Preserve the independent refusal non-regression condition. This
        # does not replace either of option A's throughput/p95 conditions.
        for field in ('total', 'max_per_range'):
            b = [p['baseline']['refusals'][field] for p in pairs]
            c = [p['candidate']['refusals'][field] for p in pairs]
            if sum(c) > sum(b) or max(c) > max(b):
                reasons.append(f'independent refusal regression: {field}')
    report['status'] = 'RULING' if reasons else 'PASS' if len(pairs) == 5 else 'INCOMPLETE'
    return report
