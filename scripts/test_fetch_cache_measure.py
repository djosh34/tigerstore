import copy
import importlib.util
from pathlib import Path
import unittest
s=importlib.util.spec_from_file_location('cache',Path(__file__).with_name('fetch-cache-measure.py'));m=importlib.util.module_from_spec(s);s.loader.exec_module(m)

def probe(side='baseline'):
 b={n:0 for n in m.NAMES};b.update(member=1,budget=560<<20,routing=int(side=='candidate'),live_bytes=1<<30,slab_used=100<<20)
 for k in ('a','b'):b.update({f'{k}_off_lo':4096,f'{k}_mod':12,f'{k}_size':65536,f'{k}_sum':777})
 a=copy.deepcopy(b);a.update(jobs=1,values=1)
 if side=='baseline':a.update(a_cached=1,a_at_sec=123,a_at_nsec=456,cache_entries=1)
 return dict(case='get-get',Size=65536,Clients=10,Delay=100,Results=[{'request_id':i+1,'latency_ms':2,'busy_retries':0} for i in range(10)],ClientCores=.4,OriginBefore=b,OriginAfter=a,ReuseBefore=copy.deepcopy(a),ReuseAfter=copy.deepcopy(a),OriginRetries=0,ElapsedCacheMS=102,FinishedUnixNano=123102000456,DeviceReadBefore={'1':0},DeviceReadAfter={'1':4096},Throughput=5000,WallMS=2,Latency={'p95':2,'p99':2,'max':2},Refusals={'total':0,'max_per_range':0})

class CacheMeasure(unittest.TestCase):
 def test_native_lowercase_case_is_required_not_the_old_synthetic_schema(self):
  p=probe();p['Case']=p.pop('case')
  with self.assertRaisesRegex(ValueError,'native lowercase case'):m.adapt(p,'baseline','past-ram-560m',560<<20)
  p=probe();p['case']='unknown'
  with self.assertRaisesRegex(ValueError,'native lowercase case'):m.adapt(p,'baseline','past-ram-560m',560<<20)
 def test_distinct_request_reuse_requires_fetch_and_same_cache_generation(self):
  r=m.adapt(probe(),'baseline','past-ram-560m',560<<20);self.assertEqual(r['distinct_request_reuses'],10);self.assertEqual(r['origin_fulfillment'],1)
 def test_origin_or_same_consumer_is_never_relabelled_as_reuse(self):
  for snap,key,value in [('OriginBefore','a_cached',1),('OriginAfter','jobs',0),('ReuseBefore','a_cached',0),('ReuseAfter','a_at_nsec',457)]:
   p=probe();p[snap][key]=value
   with self.assertRaises(ValueError):m.adapt(p,'baseline','past-ram-560m',560<<20)
 def test_immutable_source_identity_is_required(self):
  for key in ('a_off_lo','a_mod','a_size','a_sum'):
   p=probe();p['ReuseAfter'][key]+=1
   with self.assertRaises(ValueError):m.adapt(p,'baseline','past-ram-560m',560<<20)
 def test_no_generator_or_expired_cache_waiver(self):
  for key,value in [('ClientCores',.81),('ElapsedCacheMS',1001)]:
   p=probe();p[key]=value
   with self.assertRaises(ValueError):m.adapt(p,'baseline','past-ram-560m',560<<20)
 def test_past_ram_must_exceed_actual_member_limit(self):
  p=probe();p['OriginBefore']['live_bytes']=640<<20
  with self.assertRaises(ValueError):m.adapt(p,'baseline','past-ram-560m',560<<20)
 def test_candidate_must_not_have_unexpected_retained_cache(self):
  p=probe('candidate');m.adapt(p,'candidate','past-ram-560m',560<<20);p['ReuseAfter']['cache_entries']=1
  with self.assertRaises(ValueError):m.adapt(p,'candidate','past-ram-560m',560<<20)
 def test_prev_kv_catchup_is_checked(self):
  p=probe();p['case']='overlapping-prev-kv';p['WatchValidated']=False
  with self.assertRaises(ValueError):m.adapt(p,'baseline','past-ram-560m',560<<20)
 def test_failed_rpc_not_qualifying(self):
  p=probe();p['Results'][2]['error']='wrong value'
  with self.assertRaises(ValueError):m.adapt(p,'baseline','past-ram-560m',560<<20)

if __name__=='__main__':unittest.main()
