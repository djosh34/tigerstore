import base64
import copy
import unittest
import audit


def fixture():
    values = []
    events = []
    for i in range(32):
        key = f'/correct-round1/list/k{i:02d}'
        value = f'seed-{i}'
        rev = i + 2
        values.append({'key': base64.b64encode(key.encode()).decode(),
                       'value': base64.b64encode(value.encode()).decode(),
                       'create_revision': rev, 'mod_revision': rev, 'version': 1})
        events.append({'type': 1, 'kv': {'key': key, 'value': value,
                       'createRevision': rev, 'modRevision': rev, 'version': 1}})
    return {'passed': True, 'call_utc_ns': 10, 'return_utc_ns': 20, 'target': 33,
            'list': {'passed': True, 'chunks': [{'kvs': values[:10]},
                     {'kvs': values[10:30]}, {'kvs': values[30:], 'header': {'revision': 33}, 'count': 32}],
                     'historical_unary': {'kvs': values, 'count': 32}},
            'watch_responses': [{'revision': 33, 'events': events}, {'revision': 33, 'progress': True}]}


class AuditTests(unittest.TestCase):
    def test_valid(self):
        self.assertEqual(audit.probe_check(fixture(), 1, 30)['watch_events'], 32)

    def test_reject_corruption(self):
        changes = [
            lambda p: p.update(call_utc_ns=0),
            lambda p: p['list']['chunks'][0].update(count=32),
            lambda p: p['list']['chunks'][-1].update(count=31),
            lambda p: p['list']['chunks'][1]['kvs'][0].update(version=2),
            lambda p: p['watch_responses'][0]['events'].pop(),
            lambda p: p['watch_responses'][-1].update(revision=32),
            lambda p: p['watch_responses'][0]['events'].append(copy.deepcopy(p['watch_responses'][0]['events'][0])),
        ]
        for change in changes:
            with self.subTest(change=change):
                p = copy.deepcopy(fixture())
                change(p)
                with self.assertRaises(AssertionError):
                    audit.probe_check(p, 1, 30)


if __name__ == '__main__':
    unittest.main()
