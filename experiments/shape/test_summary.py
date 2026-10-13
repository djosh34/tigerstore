import unittest
from summarize import disk_node, hist_upper, prometheus, queues


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
        self.assertEqual(q['commit_apply_gap']['max'],50)


if __name__=='__main__':
    unittest.main()
