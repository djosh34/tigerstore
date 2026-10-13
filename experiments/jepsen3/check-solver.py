#!/usr/bin/env python3
"""Runner-only provenance, differential tests and exact Clojure replay."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import urllib.request

assert os.environ.get('GITHUB_ACTIONS') == 'true'
base = Path('experiments/jepsen2-solver')
out = Path('solver-results')
out.mkdir(exist_ok=True)
upstream = (base / 'sources/pinned_register.clj').read_text()
url = 'https://raw.githubusercontent.com/jepsen-io/etcd/b47cc401f27bfb7c28afd9ac088b5f0dca4501b8/src/jepsen/etcd/register.clj'
with urllib.request.urlopen(url, timeout=30) as response:
    live = response.read()
assert live == (base / 'sources/pinned_register.clj').read_bytes()
(out / 'upstream-register.clj').write_bytes(live)
record = upstream[upstream.index('(defrecord VersionedRegister'):upstream.index('(defn r   ')].strip()
replay = (base / 'offline/src/offline/versioned_register.clj').read_text()
assert replay[replay.index('(defrecord VersionedRegister'):].strip() == record
(out / 'model-source.json').write_text(json.dumps({
    'upstream_commit': 'b47cc401f27bfb7c28afd9ac088b5f0dca4501b8',
    'upstream_source_sha256': hashlib.sha256(upstream.encode()).hexdigest(),
    'model_record_sha256': hashlib.sha256(record.encode()).hexdigest(),
    'record_identical': True}, indent=2) + '\n')
with (out / 'selftest.log').open('w') as log:
    subprocess.run([sys.executable, str(base / 'selftest.py'), str(out / 'selftest')],
                   stdout=log, stderr=subprocess.STDOUT, timeout=180, check=True)
cp = (out / 'classpath.txt').read_text().strip()
for suffix, mode in [('', []), ('-reject', ['reject'])]:
    args = json.loads((out / f'selftest/clojure{suffix}-args.json').read_text())
    with (out / f'selftest-clojure{suffix}.log').open('w') as log:
        subprocess.run(['java', '-Xmx512m', '-cp', cp, 'clojure.main', '-m',
                        'offline.verify', *mode, *args], stdout=log,
                       stderr=subprocess.STDOUT, timeout=120, check=True)
    assert all(Path(args[i]).exists() is (not mode) for i in range(3, len(args), 4))
