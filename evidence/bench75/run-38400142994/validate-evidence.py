#!/usr/bin/env python3
"""Check every raw comparison window; never include the separate profile run."""
import json
import math
from pathlib import Path

root = Path(__file__).resolve().parent
for cell, expected in [('put-c64', 15), ('put-c256', 15)]:
    op = cell.split('-')[0]
    files = list((root / cell).glob(f'[1-5]-*/{op}.json'))
    assert len(files) == expected, cell
    for file in files:
        j = json.loads(file.read_text())
        n = j['successes']
        assert j['generator'] == j['effective_generator'] == 'lean', file
        assert j['active_connections'] == 16 and j['control_connections'] == 1, file
        assert j['connection_outstanding_limits'] == [j['workers'] // 16] * 16, file
        assert j['untimed_warm_rpcs'] == j['untimed_warm_writes'] == 16, file
        assert n == 60000 and j['valid'] and not j['errors'], file
        assert len(j['terms']) == 1 and sum(j['terms'].values()) == n, file
        changes = j['cas_compare_passed'] if op == 'cas' else n
        assert j['revision_after'] - j['revision_before'] == changes, file
        if op == 'cas':
            assert changes + j['cas_compare_failed'] == n, file
        assert len(j['cpu_before']) == len(j['cpu_after']) == 4, file
        for i, (before, after) in enumerate(zip(j['cpu_before'], j['cpu_after'])):
            assert before['PID'] == after['PID'] and before['Start'] == after['Start'], file
            assert after['Ticks'] >= before['Ticks'], file
            cpu = (after['Ticks'] - before['Ticks']) * 1e6 / j['clock_hz'] / n
            assert math.isclose(j['cpu_us_op'][i], cpu), file
            if i == 0:
                for counter in ['Syscr', 'Syscw']:
                    assert after[counter] >= before[counter], file
                    value = (after[counter] - before[counter]) / n
                    assert math.isclose(j[f'leader_{counter.lower()}_per_op'], value), file
        heap = j['leader_heap']
        before, after = heap['before'], heap['after']
        for out, counter in [('objects_per_op', 'allocated_objects'), ('bytes_per_op', 'allocated_bytes')]:
            assert after[counter] >= before[counter], file
            assert math.isclose(heap[out], (after[counter] - before[counter]) / n), file
        fence = json.loads((file.parent / 'after-fence.json').read_text())
        assert fence['expected'] == j['revision_after'], file
        assert len(fence['headers']) == 3, file
        assert all(h['revision'] >= j['revision_after'] for h in fence['headers']), file
        placement = json.loads((file.parent / 'placement.json').read_text())
        assert [x['PID'] for x in j['cpu_before'][:3]] == [int(p) for p in placement['pids'].split(',')], file
        affinities = json.loads((file.parent / 'affinities.json').read_text())
        for cpu, node in enumerate(placement['node_order']):
            assert affinities[str(node)], file
            assert all(cpus == [cpu] for cpus in affinities[str(node)].values()), file
        layout = json.loads((file.parent / 'layout.json').read_text())
        assert layout['server_cpus_in_role_order'] == [0, 1, 2], file
        assert layout['generator_cpu'] == 3 and layout['generator_gomaxprocs'] == 1, file
    print(f'{cell}: {expected} windows validated')
print('30 comparison windows, 1,800,000 completed operations. Lean generator for every store.')
