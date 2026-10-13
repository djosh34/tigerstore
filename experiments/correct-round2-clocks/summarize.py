#!/usr/bin/env python3
"""Read archived verdicts and stream only operation metadata for the final report.
Full values remain unchanged in the archive and were used by all four checkers.
"""
import gzip
import json
from pathlib import Path

BASE=Path(__file__).resolve().parent
CAP=1536*1024

def load(path):
    with gzip.open(path,'rt') as f:return json.load(f)

def operations(path):
    # Go MarshalIndent writes one field per line. Stream the ops array only.
    inside=False; op=None; mode=None
    with gzip.open(path,'rt') as f:
        for line in f:
            text=line.strip(); indent=len(line)-len(line.lstrip())
            if not inside:
                if indent==2 and text=='"ops": [':inside=True
                continue
            if indent==2 and text.startswith(']'):break
            if indent==4 and text=='{':
                op={'values':[],'response':False,'txn_success':False};mode=None
                continue
            if indent==4 and text.startswith('}'):
                yield op;op=None;continue
            if op is None:continue
            if indent==6:
                if text.startswith('"req":'):mode='req'
                elif text.startswith('"resp":'):mode='resp';op['response']=True
                elif text.startswith('"err":'):mode='err'
                elif text.startswith('}'):
                    mode=None
                else:
                    for key in ('client','call','return'):
                        if text.startswith(f'"{key}":'):
                            op[key]=json.loads(text.split(':',1)[1].rstrip(','))
            if mode=='req':
                if indent==8:
                    for kind in ('put','txn','delete','range'):
                        if text.startswith(f'"{kind}":'):op['kind']=kind
                if text.startswith('"value":'):
                    value=json.loads(text.split(':',1)[1].rstrip(','))
                    op['values'].append({'bytes':len(value.encode()),'prefix':value[:48]})
            elif mode=='resp' and text.startswith('"succeeded":'):
                op['txn_success']=json.loads(text.split(':',1)[1].rstrip(','))
            elif mode=='err':
                for key in ('code','message','ambiguous'):
                    if text.startswith(f'"{key}":'):
                        op[key]=json.loads(text.split(':',1)[1].rstrip(','))

runs=[];cpu=[];clock=[];admissions=[];payloads=[];checkers=[];by_product={}
for root in sorted((BASE/'results').glob('run-*')):
    summary=json.loads((root/'summary.json').read_text())
    runs.append({'run_id':root.name[4:],'attempts':len(summary),
        'controllers':sum(x['reached_controller'] for x in summary),
        'passed':sum(x['controller_pass'] and x['offline_pass'] for x in summary),
        'operations':sum(x['operations'] for x in summary),
        'histories':sum(x['oracle_sets'] for x in summary),
        'verdicts':sum(x['checker_verdicts'] for x in summary),
        'checker_passes':sum(x['checker_passes'] for x in summary)})
    for s in summary:
        artifact=root/s['artifact'];case=s['attempt']['case'];target=case['target']
        totals=by_product.setdefault(target,{'attempts':0,'passed':0,'operations':0,'histories':0,'verdicts':0,'checker_passes':0})
        for field,amount in [('attempts',1),('passed',int(s['controller_pass'] and s['offline_pass'])),('operations',s['operations']),('histories',s['oracle_sets']),('verdicts',s['checker_verdicts']),('checker_passes',s['checker_passes'])]:totals[field]+=amount
        for path in (artifact/'run').glob('check-*.json.gz'):
            if path.name.endswith('.coverage.json.gz'):continue
            checkers+=load(path)
        path=artifact/'run/faults.json.gz'
        if path.exists():
            f=load(path)
            for name,phases in f['faults'].items():
                if not all(x in phases for x in ('requested','installed','exercised','healed')):continue
                if name=='cpu-quota':
                    first=phases['installed']['cpu_stat'];last=phases['exercised']['cpu_stat']
                    cpu.append({'artifact':s['artifact'],**case,'cpu_max':phases['installed']['cpu_max'],
                        'nr_throttled_delta':last['nr_throttled']-first['nr_throttled'],
                        'throttled_usec_delta':last['throttled_usec']-first['throttled_usec'],
                        'cpu_seconds':phases['exercised']['cpu_seconds'],'wall_seconds':phases['exercised']['wall_seconds']})
                if name.startswith('clock-'):
                    node=str(phases['requested']['node']);proof=phases['exercised']['nodes']
                    clock.append({'artifact':s['artifact'],**case,'fault':name,
                        'node':int(node),'offset_seconds':phases['requested']['offset_seconds'],
                        'nodes':{n:{k:v for k,v in p.items() if k in ('wall_seconds','monotonic_seconds','server_clock_hits')} for n,p in proof.items()}})
        path=artifact/'run/workload/limit.json.gz'
        if path.exists():
            records=load(path)
            for row in records:
                if row['label']!='largest-admitted-allowance':continue
                fit=next(x for x in records if x.get('client')==33+row['client'] and x.get('response') and x.get('strict_framed_bytes')==CAP+row['extra_bytes'])
                admissions.append({'artifact':s['artifact'],**case,'client':row['client'],
                    'extra_bytes':row['extra_bytes'],'largest_put_value_bytes':fit['value_bytes'],
                    'strict_framed_bytes':fit['strict_framed_bytes'],'one_more_byte_rejected':row['rejected_extra_bytes']==row['extra_bytes']+1})
        path=artifact/'run/workload/history.json.gz'
        if path.exists():
            exact=0;accepted={'put':set(),'txn':set()};mutations={'put':0,'txn':0};count=0
            for op in operations(path):
                count+=1
                for v in op['values']:
                    if v['bytes']==CAP and op.get('code')=='InvalidArgument' and 'request is too large' in op.get('message','') and not op.get('ambiguous',False):exact+=1
                    if not op['response'] or not v['prefix'].startswith('s') or v['bytes']<CAP-512:continue
                    kind=op.get('kind')
                    if kind=='put' or kind=='txn' and op['txn_success']:
                        accepted[kind].add(v['bytes']);mutations[kind]+=1
            assert count==s['operations'],(s['artifact'],count,s['operations'])
            payloads.append({'artifact':s['artifact'],**case,'operations':count,
                'exact_1_5_MiB_definite_rejections':exact,
                'successful_workload_put_value_bytes':sorted(accepted['put']),
                'successful_workload_txn_value_bytes':sorted(accepted['txn']),
                'successful_near_limit_mutations':mutations})
result={'runs':runs,'by_product':by_product,'cpu':cpu,'clock':clock,'admissions':admissions,'payloads':payloads,
        'checker_timeouts':sum('timeout' in x.get('error','').lower() or 'timed out' in x.get('error','').lower() for x in checkers),
        'synthetic_missing_verdicts':sum(x.get('synthetic_missing_verdict',False) for x in checkers),
        'max_linearizable_seconds':max(x['seconds'] for x in checkers if x['name']=='Linearizable')}
(BASE/'facts.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({'runs':runs,'by_product':by_product,'cpu_cases':len(cpu),
    'clock_jumps':sum(x['offset_seconds']!=0 for x in clock),
    'zero_controls':sum(x['offset_seconds']==0 for x in clock),
    'admission_boundaries':len(admissions),
    'exact_rejections':sum(x['exact_1_5_MiB_definite_rejections'] for x in payloads),
    'timeouts':result['checker_timeouts'],'synthetic_missing_verdicts':result['synthetic_missing_verdicts']},indent=2))
