import copy
import importlib.util
import json
import math
from pathlib import Path
import unittest

HERE=Path(__file__).parent
s=importlib.util.spec_from_file_location('final',HERE/'fetch-final-profiles.py');m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
s=importlib.util.spec_from_file_location('fixture',HERE/'test_fetch_resident_measure.py');f=importlib.util.module_from_spec(s);s.loader.exec_module(f)

class FinalProfiles(unittest.TestCase):
 def case(self):return dict(bench='range-1k',case='range-1k',shape='',seed=94125874,rtt=0)
 def row(self):
  d=f.row();d.update(warmup_s=3,invalid_causes=[],unsent=0,cohort_failed=0,resource_exhausted_cohort=0);return d
 def test_native_cohort_and_actual_routing(self):
  self.assertEqual(m.adapt(self.row(),'candidate',self.case())['latency_ms']['p95'],.1)
 def test_bad_load_or_generator_or_cohort_is_not_a_win(self):
  for key,value in [('client_cores',.81),('conns',7),('streams_per_conn',128),('offered_rate',100),('warmup_s',2),('duration_s',11),('unsent',1),('cohort_failed',1),('errors',{'busy':1}),('ramp',[{}])]:
   d=self.row();d[key]=value
   with self.assertRaises(ValueError,msg=key):m.adapt(d,'candidate',self.case())
 def test_nonfinite_or_missing_source_is_rejected(self):
  for value in (None,{}, {m.resident.PREFIX+n:float('nan') for n in m.resident.NAMES}):
   d=self.row();d['fetch_measure'][1][2]=value
   with self.assertRaises(ValueError):m.adapt(d,'candidate',self.case())
 def test_watch_uses_delivery_tail_not_only_put_latency(self):
  d=self.row();d['workload']='watch';d['watch']={'latency_us':dict(p95=250,p99=300,max=350)}
  c=self.case();c.update(bench='watch-1k',case='watch-1k')
  self.assertEqual(m.adapt(d,'candidate',c)['latency_ms']['p95'],.25)
 def test_k8s_shape_and_fixed_load_are_matched(self):
  d=self.row();d.update(workload='k8s',conns=3,streams_per_conn=64,k8s={'shape':'small'})
  c=self.case();c.update(bench='k8s',case='k8s-small',shape='small')
  m.adapt(d,'candidate',c);d['k8s']['shape']='large'
  with self.assertRaises(ValueError):m.adapt(d,'candidate',c)
 def test_plan_uses_all_predeclared_k8s_seeds_and_no_duplicate_resident_gate(self):
  doc=json.loads((HERE.parent/'bench/fetch-measure-plan.json').read_text());jobs=m.plan(doc)
  self.assertEqual(len(jobs),179)
  self.assertFalse(any(x['bench']=='range-1k' and x['rtt']==0 for x in jobs))
  for shape,cell in doc['shapes'].items():self.assertEqual({x['seed'] for x in jobs if x['shape']==shape},set(cell['seeds']))
 def overloaded(self):
  d=self.row();d.update(valid=False,invalid_causes=['request_errors'],cohort_failed=200,resource_exhausted_cohort=200,errors={'txn: grpc ResourceExhausted: etcdserver: too many requests':900})
  d['latency_us']['count']=800
  return d
 def test_busy_only_exception_keeps_success_goodput_and_latency(self):
  d=self.overloaded();r=m.adapt(d,'candidate',self.case())
  self.assertEqual(r['refusal_share'],.2)
  self.assertEqual(r['throughput'],100)
  self.assertFalse(r['native_valid'])
  self.assertEqual(r['refusals']['total'],200) # NOT whole-phase diagnostic900
 def test_busy_errors_do_not_hide_other_native_invalidity(self):
  for causes in (None,[],['server_cpu','request_errors'],['phase','request_errors'],['watch','request_errors'],['k8s','request_errors']):
   d=self.overloaded();d['invalid_causes']=causes
   with self.assertRaises(ValueError):m.adapt(d,'candidate',self.case())
 def test_nonbusy_resource_exhausted_is_never_a_busy_exception(self):
  for err in ('grpc ResourceExhausted: message too large','grpc Unavailable: etcdserver: too many requests','unexpected: grpc ResourceExhausted: etcdserver: too many requests'):
   d=self.overloaded();d['errors']={err:1}
   with self.assertRaises(ValueError):m.adapt(d,'candidate',self.case())
 def test_counter_or_success_sample_inconsistency_is_rejected(self):
  for k,v in [('resource_exhausted_cohort',None),('resource_exhausted_cohort',199),('resource_exhausted_cohort',True),('cohort_failed',1001),('cohort',0),('unsent',1),('ops',True)]:
   d=self.overloaded();d[k]=v
   with self.assertRaises(ValueError):m.adapt(d,'candidate',self.case())
  d=self.overloaded();d['latency_us']['count']=1000
  with self.assertRaises(ValueError):m.adapt(d,'candidate',self.case())
  d=self.overloaded();d['errors']={'grpc ResourceExhausted: etcdserver: too many requests':1}
  with self.assertRaises(ValueError):m.adapt(d,'candidate',self.case())
 def test_zero_cohort_refusals_with_only_warmup_busy_is_valid(self):
  d=self.overloaded();d.update(cohort_failed=0,resource_exhausted_cohort=0);d['latency_us']['count']=1000
  self.assertEqual(m.adapt(d,'candidate',self.case())['refusal_share'],0)
 def pairs(self,baseline,candidate):
  pairs=[]
  for i,(b,c) in enumerate(zip(baseline,candidate)):
   row=m.adapt(self.row(),'candidate',self.case())
   main=copy.deepcopy(row);main['refusal_share']=b;row['refusal_share']=c
   pairs.append(dict(pair=i+1,order='AB' if i%2==0 else 'BA',baseline=main,candidate=row))
  return pairs
 def test_refusal_noise_is_all_five_main_only(self):
  p=self.pairs([.1,.2,.3,.4,.5],[.2,.3,.4,.5,.6]);r=m.assess(p)
  self.assertEqual(r['status'],'PASS');self.assertAlmostEqual(r['refusal_noise'],.14826)
  p[0]['candidate']['refusal_share']=.249
  self.assertEqual(m.assess(p)['status'],'RULING')
 def test_no_floor_no_candidate_variance_no_rounding_away_loss(self):
  p=self.pairs([0]*5,[0,0,1e-15,0,0]);r=m.assess(p)
  self.assertEqual(r['refusal_noise'],0);self.assertEqual(r['status'],'RULING')
 def test_exact_refusal_boundary_and_next_float_loss(self):
  p=self.pairs([0,.1,.2,.3,.4],[0,.1,.2,.3,.4]);noise=m.assess(p)['refusal_noise']
  p[0]['candidate']['refusal_share']=noise
  self.assertEqual(m.assess(p)['status'],'PASS')
  p[0]['candidate']['refusal_share']=math.nextafter(noise,math.inf)
  self.assertEqual(m.assess(p)['status'],'RULING')
 def test_outlier_stays_and_p95_is_still_a_gate(self):
  p=self.pairs([0,0,0,0,1],[0,0,0,0,1]);r=m.assess(p)
  self.assertEqual(r['baseline_refusal_shares'],[0,0,0,0,1]);self.assertEqual(r['refusal_noise'],0)
  p[0]['candidate']['latency_ms']['p95']=.2
  self.assertEqual(m.assess(p)['status'],'RULING')
 def test_five_shares_required_and_throughput_is_still_each_pair(self):
  p=self.pairs([.2]*5,[.1]*5)
  self.assertEqual(m.assess(p[:4])['status'],'INCOMPLETE')
  p[2]['candidate']['throughput']=97.999
  self.assertEqual(m.assess(p)['status'],'RULING')
 def test_old_median_policy_is_not_loaded(self):
  self.assertEqual(m.policy.NOISE_METHOD,'1.4826 * MAD of all five main p95 values, milliseconds; baseline-only; no floor or outlier removal')

if __name__=='__main__':unittest.main()
