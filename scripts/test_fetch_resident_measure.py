import copy
import importlib.util
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('resident', Path(__file__).with_name('fetch-resident-measure.py'))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


def row(side='candidate'):
    sample = dict(zip((m.PREFIX+n for n in m.NAMES), (0,0,7,12000000,3<<30,0,int(side=='candidate'))))
    return dict(valid=True, errors={}, workload='range',value_size=1024,keys=10000,conns=6,streams_per_conn=256,
                offered_rate=0,duration_s=10,client_cores=.4,ops=1000,throughput=100,
                latency_us=dict(count=1000,p95=100,p99=120,max=130),per_op=dict(kvs=1),
                fetch_measure=[[copy.deepcopy(sample) for _ in range(3)] for _ in range(2)])


class ResidentMeasure(unittest.TestCase):
    def test_both_matched_sides_qualify(self):
        for side in ('baseline','candidate'):
            result=m.adapt(row(side),side)
            self.assertEqual(result['latency_ms']['p95'],.1)
            self.assertEqual(result['refusals'],dict(total=0,max_per_range=0))

    def test_no_fetch_is_required_since_boot_not_just_in_window(self):
        data=row()
        for snap in data['fetch_measure']:
            for sample in snap:
                sample[m.PREFIX+'jobs_total']=1
        with self.assertRaisesRegex(ValueError,'cannot assert zero reuse'):
            m.adapt(data,'candidate')

    def test_every_members_routing_budget_cache_and_loads_are_checked(self):
        for key,value in [('memory_budget_bytes',560<<20),('cursor_routing',0),('cache_entries',1),('values_total',1),('loads_total',8),('slab_used_bytes',0)]:
            data=row()
            data['fetch_measure'][1][2][m.PREFIX+key]=value
            with self.assertRaises(ValueError):
                m.adapt(data,'candidate')

    def test_missing_or_nonfinite_metrics_are_rejected(self):
        for bad in (None,{}, {m.PREFIX+n:float('nan') for n in m.NAMES}):
            data=row()
            data['fetch_measure'][1][1]=bad
            with self.assertRaises(ValueError):
                m.adapt(data,'candidate')

    def test_no_mismatched_load_or_ramped_or_failed_cohort(self):
        for key,value in [('conns',3),('keys',20000),('duration_s',9),('unsent',1),('cohort_failed',1),('ramp',[{}]),('valid',False),('errors',{'busy':1})]:
            data=row();data[key]=value
            with self.assertRaises(ValueError):
                m.adapt(data,'candidate')

    def test_generator_limited_is_not_a_win_or_regression(self):
        data=row();data['client_cores']=.81
        with self.assertRaisesRegex(ValueError,'generator-limited'):
            m.adapt(data,'candidate')

    def test_partial_or_bad_value_rpc_samples_are_rejected(self):
        for key,value in [('count',999),('p95',float('nan')),('p99',90),('max',0)]:
            data=row();data['latency_us'][key]=value
            with self.assertRaises(ValueError):
                m.adapt(data,'candidate')
        data=row();data['per_op']['kvs']=.99
        with self.assertRaises(ValueError):
            m.adapt(data,'candidate')

    def test_unique_overlay_anchors_fail_closed(self):
        for text in ('absent','anchor anchor'):
            with self.assertRaises(ValueError):
                m.replace(text,'anchor','replacement')

    def test_scrape_only_overlay(self):
        with tempfile.TemporaryDirectory() as td:
            checkout=Path(td)/'tree';evidence=Path(td)/'evidence';evidence.mkdir()
            dest=checkout/'internal/wl/prom.go';dest.parent.mkdir(parents=True)
            dest.write_bytes((Path(__file__).parents[1]/'internal/wl/prom.go').read_bytes())
            with patch.object(m.subprocess,'run'),patch.object(m.subprocess,'check_output',return_value=b'diff'):
                m.prepare(checkout,evidence,'candidate')
            helper=(checkout/'internal/wl/fetch_measure_resident.go').read_text()
            self.assertIn('n.cursorFetch',helper)
            self.assertIn('n.stats.fetches',helper)
            self.assertIn('n.st.loads',helper)
            self.assertIn('fetchMeasureNode(n)',dest.read_text())
            self.assertEqual(len(list((evidence/'candidate-overlay').iterdir())),3)

    def test_campaign_does_not_run_on_host(self):
        with patch.dict(m.os.environ,GITHUB_ACTIONS='false'):
            with self.assertRaisesRegex(ValueError,'Actions only'):
                m.run(SimpleNamespace())


if __name__=='__main__':
    unittest.main()
