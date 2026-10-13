"""Pure controller tests. No process signals, mounts, packets, or fault runs."""
import argparse
import json
from pathlib import Path
import signal
import tempfile
import unittest
from unittest.mock import Mock, patch

import runner


class ControllerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir=Path(__file__).parent)
        args = argparse.Namespace(out=str(Path(self.tmp.name) / 'artifacts'), seed=49,
            repetition=1, target='tigerstore', scenario='mixed', server='/fake/server',
            client='/fake/client', duration=90, disk_mib=128)
        self.c = runner.Controller(args)
        self.c.cert = self.c.out / 'certs'

    def tearDown(self):
        self.c.commands.close()
        self.tmp.cleanup()

    def test_fault_inventory_and_phase_persistence(self):
        self.assertEqual(len(self.c.required), 8)
        self.c.phase('kill-leader', 'requested', node=1)
        self.c.phase('kill-leader', 'installed', signal=9)
        saved = json.loads((self.c.out / 'faults.json').read_text())
        phases = saved['faults']['kill-leader']
        self.assertLessEqual(phases['requested']['monotonic_ns'], phases['installed']['monotonic_ns'])
        origin = saved['utc_origin']
        self.c.event('test')
        self.assertEqual(json.loads((self.c.out / 'faults.json').read_text())['utc_origin'], origin)
        with self.assertRaisesRegex(RuntimeError, 'duplicate phase'):
            self.c.phase('kill-leader', 'requested')

    def test_etcd_only_flags_and_addresses(self):
        tiger = self.c.server_args(2)
        self.assertNotIn('--initial-advertise-peer-urls', tiger)
        self.assertIn('https://127.77.0.2:2380', tiger)
        self.assertIn('https://127.78.0.2:2379', tiger)
        self.c.a.target = 'etcd'
        etcd = self.c.server_args(2)
        self.assertIn('--initial-advertise-peer-urls', etcd)
        self.assertIn('--peer-client-cert-auth', etcd)

    def test_waitpid_signal_evidence(self):
        p = Mock(pid=123, returncode=None)
        with patch.object(runner.os, 'waitpid', return_value=(123, 9)):
            proof = self.c.wait_status(p, 0, lambda s: runner.os.WIFSIGNALED(s))
        self.assertEqual(proof['termsig'], signal.SIGKILL)
        self.assertTrue(proof['wifsignaled'])
        self.assertEqual(p.returncode, -9)
        p = Mock(pid=123, returncode=None)
        with patch.object(runner.os, 'waitpid', return_value=(123, 256)):
            with self.assertRaisesRegex(RuntimeError, 'unexpected waitpid'):
                self.c.wait_status(p, 0, lambda s: runner.os.WIFSIGNALED(s))

    def test_cleanup_can_handle_already_reaped_exit(self):
        p = Mock(pid=123, returncode=1)
        self.c.nodes[1] = {'p': p, 'generation': 1}
        with patch.object(runner.os, 'waitpid', side_effect=AssertionError('must not wait twice')):
            with patch.object(runner.os, 'kill', side_effect=AssertionError('must not signal dead PID')):
                result = self.c.kill_node(1, cleanup=True)
        self.assertTrue(result['already_exited'])
        self.assertEqual(self.c.nodes, {})

    def test_scoped_filters_do_not_assume_source_binding(self):
        calls = []
        expected = []
        for proto in ('tcp', 'udp'):
            for ip, port in [('dst_ip', 'dst_port'), ('src_ip', 'src_port')]:
                expected.append({'eth_type': 'ipv4', 'ip_proto': proto, ip: '127.77.0.2', port: 2380})
        state = {'qdisc': [{'kind': 'netem', 'options': {'limit': 10000,
                  'loss-random': {'loss': .25}}}], 'filters': [
                  {'options': {'keys': k, 'classid': '1:3'}} for k in expected]}
        self.c.tc = lambda *args, **kwargs: calls.append(args)
        self.c.tc_state = lambda: state
        self.c.install_netem(2, ['loss', '25%'])
        rules = [x for x in calls if x[0] == 'filter']
        self.assertEqual(len(rules), 4)
        for rule in rules:
            self.assertNotEqual('src_ip' in rule, 'dst_ip' in rule)
            self.assertEqual('src_ip' in rule, 'src_port' in rule)
            self.assertEqual('dst_ip' in rule, 'dst_port' in rule)
        self.assertTrue(self.c.netem)

    def test_wrong_effective_netem_fails(self):
        self.c.tc = Mock()
        self.c.tc_state = lambda: {'qdisc': [{'kind': 'netem', 'options': {'limit': 10}}], 'filters': []}
        with self.assertRaisesRegex(RuntimeError, 'wrong rule count'):
            self.c.install_netem(1, ['delay', '5ms'])

    def test_cold_copies_are_independent_and_pair_only(self):
        for i, path in self.c.data.items():
            path.mkdir(parents=True)
            (path / 'wal').write_text(f'original-{i}')
        self.c.preserve_original([{'node': i, 'termsig': 9} for i in range(1, 4)])
        started = []
        verified = []
        def start(i):
            started.append(i)
            self.c.nodes[i] = {'fake': True}
            # Mimic recovery changing a log. The next case must use original bytes.
            self.assertEqual((self.c.data[i] / 'wal').read_text(), f'original-{i}')
            (self.c.data[i] / 'wal').write_text('modified-by-recovery')
        def reap(**_kwargs):
            evidence = [{'node': i, 'termsig': 9} for i in self.c.nodes]
            self.c.nodes.clear()
            return evidence
        self.c.start_node = start
        self.c.ready = lambda members, **kwargs: {'members': members}
        self.c.inspect = lambda members, path: verified.append(members)
        self.c.reap_all = reap
        self.c.cold_recovery()
        self.assertEqual(started, [2, 3, 1, 3, 1, 2])
        self.assertEqual(verified, [[2, 3], [1, 3], [1, 2]])
        manifest = json.loads((self.c.out / 'oracles.json').read_text())
        self.assertEqual(len(manifest), 4)
        for entry in manifest:
            self.assertEqual(set(entry), {'name', 'data_dirs', 'stopped_file'})
            self.assertTrue(Path(entry['stopped_file']).is_file())
            self.assertTrue(all(Path(p).is_absolute() for p in entry['data_dirs']))
        for i in range(1, 4):
            self.assertEqual((self.c.out / 'original' / f'n{i}' / 'wal').read_text(), f'original-{i}')

    def test_failed_cold_inspection_still_records_reaped_pair(self):
        for i, path in self.c.data.items():
            path.mkdir(parents=True)
            (path / 'wal').write_text(str(i))
        self.c.preserve_original([])
        self.c.start_node = lambda i: self.c.nodes.update({i: {}})
        self.c.ready = Mock()
        self.c.inspect = Mock(side_effect=RuntimeError('receipt mismatch'))
        def reap(**_kwargs):
            self.c.nodes.clear()
            return []
        self.c.reap_all = reap
        with self.assertRaisesRegex(RuntimeError, 'receipt mismatch'):
            self.c.cold_recovery()
        self.assertEqual(len(self.c.oracles), 2)
        self.assertEqual(self.c.oracles[1]['name'], 'cold-missing-n1')
        self.assertEqual(self.c.nodes, {})

    def test_deadline_blocks_new_case(self):
        with patch.object(runner, 'deadline_seconds', return_value=-1):
            with self.assertRaisesRegex(RuntimeError, 'UTC deadline'):
                self.c.cold_recovery()
        self.assertEqual(self.c.nodes, {})


if __name__ == '__main__':
    unittest.main()
