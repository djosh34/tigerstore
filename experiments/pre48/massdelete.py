#!/usr/bin/env python3
"""Question: how long to create/delete 50k ConfigMaps through Kubernetes backed by three TLS etcd 3.7.2 members?"""
import base64
import concurrent.futures
import http.client
import json
import os
from pathlib import Path
import ssl
import subprocess
import threading
import time
import urllib.parse

root = Path.cwd() / '.pre48'
bin_dir = root / 'bin'
cfg = json.loads(subprocess.check_output([str(bin_dir / 'kubectl'), 'config', 'view', '--raw', '--minify', '-o', 'json']))
cluster = cfg['clusters'][0]['cluster']
user = cfg['users'][0]['user']
for key, source in [('ca', cluster['certificate-authority-data']), ('cert', user['client-certificate-data']), ('key', user['client-key-data'])]:
    path = root / f'api-{key}.pem'
    path.write_bytes(base64.b64decode(source))
    path.chmod(0o600)
ctx = ssl.create_default_context(cafile=str(root / 'api-ca.pem'))
ctx.load_cert_chain(str(root / 'api-cert.pem'), str(root / 'api-key.pem'))
url = urllib.parse.urlsplit(cluster['server'])
local = threading.local()


def request(method, path, value=None):
    if not hasattr(local, 'conn'):
        local.conn = http.client.HTTPSConnection(url.hostname, url.port, context=ctx, timeout=120)
    data = None if value is None else json.dumps(value).encode()
    local.conn.request(method, path, data, {'Content-Type': 'application/json'})
    response = local.conn.getresponse()
    data = response.read()
    if response.status not in (200, 201, 202, 404):
        raise RuntimeError((method, path, response.status, data[:2000]))
    return response.status, json.loads(data) if data else {}


def raw_count(namespace):
    result = []
    for n in (1, 2, 3):
        output = subprocess.check_output(['docker', 'exec', f'external-etcd-{n}', '/bin-etcd/etcdctl',
            '--endpoints', f'https://external-etcd-{n}:2379', '--cacert', '/certs/ca.crt',
            '--cert', '/certs/client.crt', '--key', '/certs/client.key', 'get',
            f'/registry/configmaps/{namespace}/', '--prefix', '--count-only', '-w', 'fields'])
        result.append(int(next(line.split(b':', 1)[1] for line in output.splitlines() if line.startswith(b'"Count"'))))
    return result


namespace = 'pre48-mass-50000'
count = 50000
workers = 64
status, _ = request('POST', '/api/v1/namespaces', {'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': namespace}})
assert status == 201
base = f'/api/v1/namespaces/{namespace}'


def create(i):
    status, data = request('POST', base + '/configmaps', {'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': {'name': f'cm-{i:05d}', 'namespace': namespace, 'labels': {'pre48': 'mass'}}, 'data': {'value': 'x' * 256}})
    assert status == 201, (i, status, data)


start = time.monotonic()
with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
    # Consume results, so any request failure fails the job.
    for _ in pool.map(create, range(count)):
        pass
fill = time.monotonic() - start
# Check exact labelled count via paginated API reads and direct storage count.
total = 0
continuation = ''
while True:
    query = urllib.parse.urlencode({'limit': 1000, 'labelSelector': 'pre48=mass', 'continue': continuation})
    status, data = request('GET', base + '/configmaps?' + query)
    assert status == 200
    total += len(data['items'])
    continuation = data.get('metadata', {}).get('continue', '')
    if not continuation:
        break
assert total == count, total
before = raw_count(namespace)
assert all(n in (count, count + 1) for n in before), before
# No extra settle delay. Deletion includes the controller's own five-second delay.
start = time.monotonic()
status, _ = request('DELETE', base, {})
assert status in (200, 202)
request_seconds = time.monotonic() - start
while True:
    status, _ = request('GET', base)
    if status == 404:
        break
    if time.monotonic() - start > 480:
        raise RuntimeError('namespace still present after 480 seconds')
    time.sleep(0.5)
delete = time.monotonic() - start
status, items = request('GET', base + '/configmaps')
assert status in (200, 404)
assert not items.get('items', []), items
remaining = raw_count(namespace)
assert remaining == [0, 0, 0], remaining
result = {'objects': count, 'payload_bytes': 256, 'client_threads': workers, 'fill_seconds': fill,
          'fill_objects_per_second': count/fill, 'verified_labelled_api_count': total,
          'raw_etcd_counts_before': before, 'namespace_delete_request_seconds': request_seconds,
          'namespace_gone_seconds': delete, 'deletion_objects_per_second': count/delete,
          'post_delete_list_http_status': status, 'raw_etcd_counts_after': remaining}
print(json.dumps(result, indent=2), flush=True)
(root / 'results/massdelete.json').write_text(json.dumps(result, indent=2) + '\n')
