import unittest
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from summarize import disk_node, hist_upper, one, prometheus, queues


class SummaryTests(unittest.TestCase):
    def test_histogram_bounds(self):
        self.assertEqual(hist_upper([1,4,16],[5,4,1],.5),1)
        self.assertEqual(hist_upper([1,4,16],[5,4,1],.99),16)
        self.assertIsNone(hist_upper([1],[0],.99))

    def test_etcd_deltas_not_lifetime(self):
        def metrics(n):
            return '\n'.join([
                f'etcd_disk_wal_fsync_duration_seconds_count {n}',
                f'etcd_disk_wal_fsync_duration_seconds_sum {n*.001}',
                f'etcd_disk_wal_fsync_duration_seconds_bucket{{le="0.001"}} {n}',
                f'etcd_disk_wal_fsync_duration_seconds_bucket{{le="+Inf"}} {n}',
            ])
        d=disk_node(metrics(100),metrics(110),'etcd',1000)
        self.assertEqual(d['fsyncs'],10)
        self.assertEqual(d['ops_per_fsync'],100)
        self.assertAlmostEqual(d['fsync_mean_us'],1000)
        self.assertEqual(d['fsync_p99_upper_us'],1000)

    def test_zero_fsync_for_reads(self):
        counts={'calls':5,'errors':0,'bytes':1024,'wall_ns_sum':1000,'wall_ns_buckets':[5,0]}
        snap={'files':{'fsync':counts,'write':counts},'wall_ns_bounds':[1000]}
        d=disk_node(snap,snap,'tigerstore',100)
        self.assertEqual(d['fsyncs'],0)
        self.assertIsNone(d['ops_per_fsync'])

    def test_etcd_commit_apply_gap(self):
        text='etcd_server_proposals_pending 20\netcd_server_proposals_committed_total 300\netcd_server_proposals_applied_total 250\n'
        q=queues([{'nodes':[text]}],'etcd',0)
        self.assertEqual(q['post_save_minus_applied']['max'],50)

    def test_observation_control_has_cpu_but_no_disk_claims(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d)/'case'/'pristine-1'
            root.mkdir(parents=True)
            (root/'load.json').write_text(json.dumps({'label':'pristine','valid':True,'cohort':100,
                'latency_us':{},'service_latency_us':{}}))
            (root/'audit.json').write_text(json.dumps({'valid':True,'observation_control':True,
                'physical_counter_fence':False}))
            for name,mono,utime in [('before',1,2),('after',2,5)]:
                (root/f'{name}.json').write_text(json.dumps({'monotonic':mono,'nodes':[None],
                    'proc':[{'starttime':10,'utime':utime,'stime':0}], 'diskstats':'not parsed'}))
            (root/'samples.jsonl').write_text('')
            row=one(root/'load.json')
            self.assertEqual(row['n0_fenced_cpu_us_op'],300)
            self.assertTrue(row['observation_control'])
            self.assertFalse(any(k.startswith('disk_') or 'fsync' in k for k in row))

    def test_missing_windows_never_form_passing_group(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d)
            plan={'windows':[{'case':'put-1k-leader-c256','kind':'etcd','rate':0,'rep':i,
                              'path':f'put-1k-leader-c256/etcd-{i}/load.json'} for i in [1,2]]}
            (root/'plan.json').write_text(json.dumps(plan))
            run=subprocess.run([sys.executable,str(Path(__file__).with_name('summarize.py')),d],capture_output=True,text=True)
            self.assertEqual(run.returncode,1)
            summary=json.loads((root/'summary.json').read_text())
            self.assertFalse(summary[0]['all_valid'])
            self.assertEqual(summary[0]['expected_repetitions'],2)
            self.assertEqual(summary[0]['repetitions'],2)


if __name__=='__main__':
    unittest.main()
