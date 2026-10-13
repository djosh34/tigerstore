#!/usr/bin/env python3
"""Generate tables from complete cohorts; never summarize an invalid cell as capacity."""
import csv
import json
import math
from pathlib import Path
import statistics
import sys

here=Path(__file__).parent
root=Path(sys.argv[1]).resolve()
out=here/'report-data'
out.mkdir(exist_ok=True)
manifest=json.loads((here/'campaign.json').read_text())
rows=[]

def parse(v):
    if v=='':return None
    if v in ['True','False']:return v=='True'
    try:return int(v)
    except ValueError:pass
    try:return float(v)
    except ValueError:return v

for run in manifest['runs']:
    if run['role']=='superseded-arrival-bug':continue
    for raw in csv.DictReader((root/run['folder']/'windows.csv').open()):
        r={k:parse(v) for k,v in raw.items()}
        r.update(run_id=run['id'],role=run['role'],source_commit=run['commit'])
        # These CPU clocks differ slightly from the dispatch clock. Do not
        # multiply two cell medians to estimate utilization.
        p=Path(r['path'])/'load.json'
        if p.exists():
            d=json.loads(p.read_text()); s=d.get('shape',{}); cpu=s.get('cpu_through_drain') or {}
            elapsed=cpu.get('elapsed_s')
            if elapsed and s.get('success'):
                r['cohort_cpu_elapsed_s']=elapsed
                for i,v in enumerate(cpu['server_us_per_op_by_node']):
                    r[f'n{i}_cohort_cores']=v*s['success']/1e6/elapsed
                r['client_cohort_cores']=cpu['client_us_per_op']*s['success']/1e6/elapsed
        if r.get('fenced_wall_s') and r.get('n0_fsync_wall_s') is not None:
            r['n0_measured_wal_duty']=(r['n0_fsync_wall_s']+(r.get('n0_write_wall_s') or 0))/r['fenced_wall_s']
        # Publish portable paths relative to the extracted results root.
        r['path']=str(Path(r['path']).resolve().relative_to(root))
        rows.append(r)
fields=list(dict.fromkeys(k for r in rows for k in r))
with (out/'all-current-windows.csv').open('w',newline='') as f:
    w=csv.DictWriter(f,fields,lineterminator='\n');w.writeheader();w.writerows(rows)

def aggregate(selected):
    groups={}
    for r in selected:groups.setdefault((r['case'],r['kind'],r['offered_rate']),[]).append(r)
    result=[]
    for (case,kind,rate),items in sorted(groups.items()):
        valid=sum(bool(r.get('window_valid',False)) for r in items)
        cell={'case':case,'kind':kind,'rate':rate,'planned':len(items),'valid_windows':valid,
              'all_windows_valid':valid==len(items),'run_ids':sorted({r['run_id'] for r in items}),
              'paired_job_failed':any(bool(r.get('job_failed')) for r in items)}
        # Failed cells retain counts and raw rows, not passing-only aggregates.
        if cell['all_windows_valid']:
            for k in fields:
                values=[r[k] for r in items if isinstance(r.get(k),(int,float)) and not isinstance(r[k],bool)]
                if len(values)==len(items):
                    cell[k]=statistics.median(values)
                    cell[k+'_min']=min(values);cell[k+'_max']=max(values)
        result.append(cell)
    return result

closed=[r for r in rows if r['role'] in ['primary-closed','primary-list'] and r['case']!='range-1k-leader-c4096']
closed += [r for r in rows if r['role']=='setup-retry']
sets={'closed':closed,'open':[r for r in rows if r['role']=='primary-open'],
      'matched':[r for r in rows if r['role']=='matched-open']}

def fmt(v,d=2):
    if v is None:return 'n/a'
    if not math.isfinite(v):return 'overflow'
    return f'{v:,.{d}f}'

def triple(c,prefix,suffix='',div=1,d=2):
    return '/'.join(fmt(c.get(f'{prefix}{i}{suffix}',None)/div if c.get(f'{prefix}{i}{suffix}') is not None else None,d) for i in range(3))

for name,selected in sets.items():
    cells=aggregate(selected)
    (out/f'{name}.json').write_text(json.dumps(cells,indent=2)+'\n')
    lines=[f'# {name.capitalize()} results','',
      'Each value is the median of five windows. Brackets show the full five-window range. Throughput and CPU include the drained cohort. Latency is scheduled-arrival to completion for fixed-rate cases. Failed cells have no numerical aggregate. Raw rows remain in `all-current-windows.csv`.','',
      '| Case | Store | Offered/s | Valid | Cohort ops/s [range] | p50 / p99 / p99.9 ms | p99 range ms | CPU us/op L/F1/F2 | Cores L/F1/F2 | Generator cores | Queue at stop |',
      '| --- | --- | ---: | --- | --- | --- | --- | --- | --- | ---: | ---: |']
    for c in cells:
        tag=f'{c["valid_windows"]}/{c["planned"]}'+('*' if c['paired_job_failed'] else '')
        base=f'| {c["case"]} | {c["kind"]} | {fmt(c["rate"],0)} | {tag} |'
        if not c['all_windows_valid']:
            lines.append(base+' invalid | | | | | | |');continue
        tp=f'{fmt(c["cohort_throughput"],0)} [{fmt(c["cohort_throughput_min"],0)}, {fmt(c["cohort_throughput_max"],0)}]'
        latency='/'.join(fmt(c[f'latency_{q}']/1000,3) for q in ['p50','p99','p999'])
        lr=f'[{fmt(c["latency_p99_min"]/1000,3)}, {fmt(c["latency_p99_max"]/1000,3)}]'
        lines.append(base+f' {tp} | {latency} | {lr} | {triple(c,"n","_cohort_cpu_us_op")} | {triple(c,"n","_cohort_cores")} | {fmt(c.get("client_cohort_cores"))} | {fmt(c.get("queue_at_arrival_stop"),0)} |')
    lines += ['', '*The paired job failed, although this store can have five individually valid windows. Do not treat it as a passing two-store comparison. The original ten Range leader-4096 startup failures remain in raw data; the closed table uses the explicit setup retry.','']
    if name=='closed':
        lines += ['## Fenced WAL and sampled queues','',
          'Fsync quantiles below are histogram bucket upper bounds, not exact percentiles. WAL duty uses the externally fenced interval, not the arrival or cohort clock. Physical calls include WAL headers but exclude directory syncs. Tigerstore duty includes Write and Sync; etcd duty includes only its measured WAL fsync calls. Queue peaks are samples at a nominal 100 ms cadence.','',
          '| Case | Store | Puts/fsync L/F1/F2 | Mean fsync ms L/F1/F2 | Leader fsync p50/p99 upper ms | Leader measured WAL duty | Leader queued-write peak | Leader persist-queue peak | Leader peer data-byte peak |',
          '| --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: |']
        for c in cells:
            if not c['all_windows_valid'] or not c['case'].startswith('put-'):continue
            duty=c['n0_measured_wal_duty']
            pfs='/'.join(fmt(c.get(f'n{i}_ops_per_fsync'),1) for i in range(3))
            fs=triple(c,'n','_fsync_mean_us',1000,3)
            bounds='/'.join(fmt(c.get(f'n0_fsync_{q}_upper_us',0)/1000,3) for q in ['p50','p99'])
            lines.append(f'| {c["case"]} | {c["kind"]} | {pfs} | {fs} | {bounds} | {fmt(duty,3)} | {fmt(c.get("n0_q_queued_writes_max"),0)} | {fmt(c.get("n0_q_persist_queue_batches_max"),0)} | {fmt(c.get("n0_q_peer_data_bytes_max"),0)} |')
        lines += ['', 'All primary point-Range and LIST cohorts have zero measured WAL fsyncs in both stores. Tigerstore also has zero measured WAL file writes; an equivalent etcd WAL Write-call counter is not available. Etcd backend-commit counters remain separate in the CSV.','']
    (out/f'{name}.md').write_text('\n'.join(lines))
print('Wrote',len(rows),'current planned windows and closed/open/matched tables.')
