import copy
import importlib.util
import json
from pathlib import Path
import unittest

HERE=Path(__file__).parent
s=importlib.util.spec_from_file_location('final',HERE/'fetch-final-profiles.py');m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
s=importlib.util.spec_from_file_location('fixture',HERE/'test_fetch_resident_measure.py');f=importlib.util.module_from_spec(s);s.loader.exec_module(f)

class FinalProfiles(unittest.TestCase):
 def case(self):return dict(bench='range-1k',case='range-1k',shape='',seed=94125874,rtt=0)
 def row(self):
  d=f.row();d['warmup_s']=3;return d
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
 def test_old_median_policy_is_not_loaded(self):
  self.assertEqual(m.policy.NOISE_METHOD,'1.4826 * MAD of all five main p95 values, milliseconds; baseline-only; no floor or outlier removal')

if __name__=='__main__':unittest.main()
