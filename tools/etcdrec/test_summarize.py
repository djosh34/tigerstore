import importlib.util
from pathlib import Path
import unittest

spec=importlib.util.spec_from_file_location('summary',Path(__file__).with_name('summarize.py'))
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)

class SummaryTest(unittest.TestCase):
 def test_windows_successful_writes_streams_and_weighted_concurrency(self):
  def e(t,phase,call=None,method='',data=None,**kw):
   return dict(t=t,phase=phase,call=call,method='/etcdserverpb.KV/'+method,data=data or {},conn=1,**kw)
  comp={'key':'/registry/leases/ns/key','target':'MOD','result':'EQUAL'}
  txn={'compare':[comp],'success':[{'request_put':{'key':comp['key'],'value_bytes':99,'lease':0}}],'failure':[{'request_range':{'key':comp['key']}}]}
  es=[e(0,'conn_open'),e(1,'start',1,'Watch'),e(2,'request',1,'Watch',{'create_request':{'key':'/registry/leases/','range_end':'/registry/leases0'}}),e(3,'response',1,'Watch',{'created':True,'watch_id':4}),e(10,'start',2,'Txn'),e(11,'request',2,'Txn',txn),e(12,'response',2,'Txn',{'succeeded':True,'responses':[{'response_put':{}}]}),e(13,'end',2,'Txn'),e(14,'start',3,'Txn'),e(15,'request',3,'Txn',txn),e(16,'response',3,'Txn',{'succeeded':False,'responses':[{'response_range':{'_bytes':123}}]}),e(17,'end',3,'Txn'),e(18,'response',1,'Watch',{'watch_id':4,'events':[{'kv':{'key':comp['key']},'_bytes':150}]}),e(21,'end',1,'Watch')]
  s=m.summarize(es,10,20)
  self.assertEqual(s['request_messages'],2)
  self.assertEqual(s['txn_failure_pct'],50)
  self.assertEqual(s['resources']['/registry/leases/']['put_count'],1)
  self.assertEqual(s['resources']['/registry/leases/']['read_response_bytes']['mean'],123)
  self.assertEqual(s['key_popularity']['distinct_keys_put'],1)
  self.assertEqual(s['watch_streams_at_start'],1)
  self.assertEqual(s['active_watches_at_start'],{'/registry/leases/':1})
  self.assertEqual(s['connections_seen'],1)
  self.assertEqual(s['inflight_all_rpc']['max'],2)
  self.assertAlmostEqual(s['inflight_all_rpc']['mean'],1.6)
  self.assertAlmostEqual(s['inflight_unary']['mean'],.6)
  self.assertAlmostEqual(s['resources']['/registry/leases/']['watch_events_per_s'],.1)

if __name__=='__main__':unittest.main()
