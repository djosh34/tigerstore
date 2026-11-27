#!/usr/bin/env python3
"""Actions-only named resident control; NOT cold/reuse/cache-removal evidence.

Uses range-1k's fixed initial max-load step (6 connections x 256 streams),
3-GiB nodes, ten thousand 1-KiB values, same-member leader routing and RTT 0.
All targets must prove no fetch since boot, no fetched cache and no value
reload during the measured window. No invented reuse counts: zero cache
activity follows from the measured never-populated cache. Only scrape-time
observers are added; no probe on any RPC/value/cache lookup.
"""
import argparse
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import subprocess
import sys

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('policy', HERE / 'fetch-pr897-gate.py')
policy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(policy)
PREFIX = 'tigerstore_fetch_measure_'
NAMES = ('jobs_total', 'values_total', 'loads_total', 'slab_used_bytes', 'memory_budget_bytes', 'cache_entries', 'cursor_routing')


def replace(text, old, new):
    if text.count(old) != 1:
        raise ValueError(f'overlay anchor not unique: {old!r}')
    return text.replace(old, new)


def prepare(checkout, evidence, side):
    source = checkout / 'internal/wl/prom.go'
    text = source.read_text()
    text = replace(text, 'type promSnap struct {\n', 'type promSnap struct {\n\tfetchMeasure [7]float64\n')
    text = replace(text, 'p := &promSnap{stats: n.gs, format: n.format}', 'p := &promSnap{stats: n.gs, format: n.format}\n\tp.fetchMeasure = fetchMeasureNode(n)')
    text = replace(text, 'func (c *promCollector) Describe(ch chan<- *prometheus.Desc) {\n', 'func (c *promCollector) Describe(ch chan<- *prometheus.Desc) {\n\tfor _, d := range fetchMeasureDescs { ch <- d }\n')
    text = replace(text, '\tif p == nil {\n\t\treturn\n\t}\n\tg := &p.stats', '\tif p == nil {\n\t\treturn\n\t}\n\tfor i, d := range fetchMeasureDescs { constGauge(ch, d, p.fetchMeasure[i]) }\n\tg := &p.stats')
    source.write_text(text)
    route = 'route := 0.0' + ('; if n.cursorFetch { route = 1 }' if side == 'candidate' else '')
    descs = ',\n'.join(f'prometheus.NewDesc("{PREFIX+name}", "Measurement-only scrape-time observer.", nil, nil)' for name in NAMES)
    helper = checkout / 'internal/wl/fetch_measure_resident.go'
    helper.write_text('package wl\nimport "github.com/prometheus/client_golang/prometheus"\nvar fetchMeasureDescs = []*prometheus.Desc{'+descs+',\n}\nfunc fetchMeasureNode(n *node) [7]float64 {\n'+route+'\nreturn [7]float64{float64(n.stats.fetches),float64(n.stats.fetched),float64(n.st.loads),float64(n.st.slab.used),float64(n.st.budget),float64(len(n.st.fetched)),route}\n}\n')
    subprocess.run(['gofmt', '-w', str(source), str(helper)], check=True)
    target = evidence / f'{side}-overlay'
    target.mkdir()
    for path in (source, helper):
        (target / path.name).write_bytes(path.read_bytes())
    (target / 'tracked.diff').write_bytes(subprocess.check_output(['git', 'diff', '--binary'], cwd=checkout))


def adapt(data, side):
    if data.get('valid') is not True or data.get('errors'):
        raise ValueError(f'{side}: invalid result/errors: {data.get("errors")}')
    expected = dict(workload='range', value_size=1024, keys=10000, conns=6, streams_per_conn=256, offered_rate=0, duration_s=10)
    for name, value in expected.items():
        if data.get(name) != value:
            raise ValueError(f'{side}: unmatched {name}: {data.get(name)!r}')
    if data.get('ramp') or data.get('ramp_climbing') or data.get('unsent') or data.get('cohort_failed'):
        raise ValueError(f'{side}: fixed cohort/load not preserved')
    if not 0 <= data.get('client_cores', float('inf')) <= .8:
        raise ValueError(f'{side}: generator-limited; not qualifying evidence')
    # stats.complete counts throughput by completion time, but latencies by
    # intended-send cohort; the warmup/window/drain boundary need not give
    # identical Ops and Cohort. Preserve the driver's exact cohort definition.
    if data.get('ops', 0) <= 0 or data.get('cohort', 0) <= 0 or data['latency_us']['count'] != data['cohort'] or data['per_op'].get('kvs') != 1:
        raise ValueError(f'{side}: incomplete successful RPC/value cohort')
    cores = data.get('server_cores_by_node')
    if not isinstance(cores, list) or len(cores) != 3 or any(not isinstance(c, (int,float)) or not math.isfinite(c) or c < 0 for c in cores) or cores[0] < .9:
        raise ValueError(f'{side}: fixed leader is not CPU-saturated; max-load control not qualified')
    q = data['latency_us']
    if any(not isinstance(q.get(k), (int, float)) or not math.isfinite(q[k]) or q[k] <= 0 for k in ('p95', 'p99', 'max')) or not q['p95'] <= q['p99'] <= q['max']:
        raise ValueError(f'{side}: invalid latency quantiles')
    samples = data['fetch_measure']
    if len(samples) != 2 or any(len(s) != 3 for s in samples):
        raise ValueError('need synchronized before/after probes for all three members')
    for before, after in zip(*samples):
        for sample in (before, after):
            if sample is None or set(sample) != {PREFIX+n for n in NAMES} or any(not isinstance(v, (int, float)) or not math.isfinite(v) or v < 0 for v in sample.values()):
                raise ValueError(f'{side}: missing/invalid scrape observer')
            for key in ('jobs_total', 'values_total', 'cache_entries'):
                if sample[PREFIX+key] != 0:
                    raise ValueError(f'{side}: resident control has fetched activity, cannot assert zero reuse')
            if sample[PREFIX+'memory_budget_bytes'] != 3 << 30 or sample[PREFIX+'cursor_routing'] != (side == 'candidate'):
                raise ValueError(f'{side}: wrong actual memory budget/routing')
            if not 0 < sample[PREFIX+'slab_used_bytes'] < sample[PREFIX+'memory_budget_bytes']:
                raise ValueError(f'{side}: missing resident slab')
        if before[PREFIX+'loads_total'] != after[PREFIX+'loads_total']:
            raise ValueError(f'{side}: value reloads during the resident cohort')
    if not math.isclose(data['throughput'], data['ops']/data['duration_s'], rel_tol=1e-12):
        raise ValueError(f'{side}: successful throughput does not match the cohort')
    return dict(throughput=data['throughput'], wall_ms=data['duration_s']*1000,
                latency_ms={k:q[k]/1000 for k in ('p95', 'p99', 'max')},
                refusals=dict(total=0, max_per_range=0),
                qualification='resident control: no fetch since boot, cache empty, no measured value loads',
                client_cores=data['client_cores'], source_samples=samples)


def run(args):
    if os.environ.get('GITHUB_ACTIONS') != 'true':
        raise ValueError('Actions only')
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=False)
    refs = dict(baseline=args.baseline, candidate=args.candidate)
    for ref in refs.values():
        if len(ref) != 40 or any(c not in '0123456789abcdef' for c in ref):
            raise ValueError('full immutable refs required')
    if args.baseline == args.candidate:
        raise ValueError('distinct refs required')
    manifest = dict(schema=1, status='INCOMPLETE', scope='resident-leader-profile-DIAGNOSTIC-NOT-GATE' if args.profile else 'resident-3g-range-1k-RTT0-only',
                    refs=refs, tooling_ref=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
                    noise_method=policy.NOISE_METHOD, pairs=[], run_url=os.environ['FETCH_MEASURE_RUN_URL'],
                    settings=dict(memory_bytes=3<<30, cache='resident-3g', RTT_ms=0,
                                  offered_load='fixed 6 connections x 256 streams', seed=94125874,
                                  log_ring_bytes=536870912, warmup_s=3, measured_s=10))
    def save():
        (out / 'manifest.json').write_text(json.dumps(manifest, indent=2, allow_nan=False)+'\n')
    save()
    for name in ('fetch-resident-measure.py', 'fetch-pr897-gate.py'):
        (out / name).write_bytes((HERE / name).read_bytes())
    bins = out / 'bins'
    bins.mkdir()
    for name, package in [('loadgen','./cmd/loadgen'), ('nullstore','./cmd/nullstore')]:
        subprocess.run(['go','build','-o',str(bins/name),package],check=True)
    for side, ref in refs.items():
        checkout = Path(os.environ['RUNNER_TEMP']) / f'probe-{side}'
        subprocess.run(['git','worktree','add','--detach',str(checkout),ref],check=True)
        prepare(checkout,out,side)
        subprocess.run(['go','build','-o',str(bins/side),'./cmd/tigerstore'],cwd=checkout,check=True)
    (bins / 'etcdctl').write_bytes(Path(os.environ['FETCH_ETCDCTL']).read_bytes())
    (bins / 'etcdctl').chmod(0o755)
    certs = out / 'certs'
    subprocess.run(['bash','scripts/dev-certs.sh',str(certs)],check=True)
    env = dict(os.environ, CERTS=str(certs), BINS=str(bins), LOADGEN=str(bins/'loadgen'),
               NULLSTORE=str(bins/'nullstore'), BENCH_GOMEMLIMIT='3GiB',
               BENCH_FLAGS='--memory-budget-bytes 3221225472'+(' --enable-pprof' if args.profile else ''), ETCD_UNSAFE_LOG_RING_BYTES='536870912',
               FETCH_MEASURE_FIXED_LOAD='1', MODES='max', SEEDS='94125874', REPS='1', LAYOUT='3 0 1 2', LAYOUTS='', LEAD='1')
    calibration = out / 'generator'
    with (out / 'generator.log').open('w') as log:
        subprocess.run(['bash','bench/run.sh','range-1k',str(calibration)],env=dict(env,MODES='generator'),
                       stdout=log,stderr=subprocess.STDOUT,check=True)
    ceiling_file = calibration / 'generator-range-1k.json'
    ceiling = json.loads(ceiling_file.read_text())
    if ceiling.get('valid') is not True or ceiling.get('errors') or ceiling.get('throughput',0) <= 0:
        raise ValueError('invalid generator calibration')
    manifest['nullstore_diagnostic'] = dict(raw_file=str(ceiling_file.relative_to(out)),
        raw_sha256=hashlib.sha256(ceiling_file.read_bytes()).hexdigest(),throughput=ceiling['throughput'],
        client_cores=ceiling['client_cores'],server_cores_by_node=ceiling['server_cores_by_node'],
        qualification='diagnostic only: nullstore can be server-limited; actual targets require <=0.8 client core and >=0.9 fixed-leader core')
    save()
    for number in range(1, 2 if args.profile else 6):
        order = 'AB' if number % 2 else 'BA'
        pair = dict(pair=number, order=order, case='range-1k')
        for side in (('baseline','candidate') if order=='AB' else ('candidate','baseline')):
            destination = out / f'{number:02}-{side}'
            logfile = out / f'{number:02}-{side}.log'
            with logfile.open('w') as log:
                result = subprocess.run(['bash','bench/run.sh','range-1k',str(destination)],env=dict(env,TARGETS=side, FETCH_PROFILE_LEADER=str(out/f'{side}.cpu.pprof') if args.profile else ''),
                                        stdout=log,stderr=subprocess.STDOUT,check=False)
            failures = [str(p.relative_to(out)) for p in destination.rglob('*.failed')]
            if result.returncode or failures:
                manifest.update(status='RULING', reasons=[f'{side} benchmark or leader check failed'],failed_log=logfile.name,failed_steps=failures)
                save()
                return 1
            rawpath = destination / f'range-1k/{side}-max-94125874.1.json'
            try:
                raw = rawpath.read_bytes()
                pair[side] = adapt(json.loads(raw),side)
                pair[side].update(raw_file=str(rawpath.relative_to(out)),raw_sha256=hashlib.sha256(raw).hexdigest())
            except (OSError, KeyError, TypeError, ValueError) as error:
                manifest.update(status='INCOMPLETE',reasons=[str(error)],failed_log=logfile.name)
                save()
                return 2
            manifest['partial_pair'] = pair
            save()
        manifest['pairs'].append(pair)
        del manifest['partial_pair']
        if args.profile:
            manifest.update(status='DIAGNOSTIC_COMPLETE_NOT_GATE')
            for side in refs:
                (out/f'{side}.profile-bin').write_bytes((bins/side).read_bytes())
                for suffix, flags in [('top',['-top','-nodefraction=0']),('range',['-list=tryFillRange|serveRange|rangeKVs|stateAt'])]:
                    with (out/f'{side}-{suffix}.txt').open('w') as log:
                        subprocess.run(['go','tool','pprof',*flags,str(bins/side),str(out/f'{side}.cpu.pprof')],stdout=log,stderr=subprocess.STDOUT,check=True)
            save()
            return 0
        report = policy.assess(manifest['pairs'])
        manifest.update(status=report['status'],report=report)
        save()
        print('PR897_RESIDENT '+json.dumps(report),flush=True)
        if report['status']=='RULING':
            return 1
    return 0 if manifest['status']=='PASS' else 2


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--profile',action='store_true',help='one profiled pair, diagnostic only, never a gate verdict')
    p.add_argument('--baseline',required=True)
    p.add_argument('--candidate',required=True)
    p.add_argument('--out',type=Path,required=True)
    a=p.parse_args()
    try:
        sys.exit(run(a))
    except (OSError,ValueError,subprocess.CalledProcessError) as error:
        print(f'INCOMPLETE resident campaign: {error}',file=sys.stderr)
        sys.exit(2)
