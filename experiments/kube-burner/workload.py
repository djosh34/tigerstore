#!/usr/bin/env python3
"""Run four bounded kube-burner v2.8.6 phases and write local evidence."""
import argparse
from collections import Counter
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import uuid


def utcnow():
    return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')


class Jsonl:
    def __init__(self, path):
        self.file = open(path, 'a', encoding='utf-8')
        self.lock = threading.Lock()

    def put(self, value):
        with self.lock:
            self.file.write(json.dumps(value, sort_keys=True) + '\n')
            self.file.flush()

    def close(self):
        self.file.close()


ROOT = Path(__file__).resolve().parent
DEADLINE = datetime(2026, 10, 13, 22, tzinfo=timezone.utc).timestamp()
RESOURCES = 'namespaces,pods,deployments,replicasets,services,configmaps,secrets'
PHASES = [('mixed', 'workload-create.yml', 150),
          ('node', 'workload-node.yml', 150),
          ('update', 'workload-update.yml', 150),
          ('delete', 'workload-delete.yml', 90)]


def dump(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


def documents(text):
    """Read arrays, JSONL, or concatenated pretty-printed JSON documents."""
    decoder = json.JSONDecoder()
    pos = 0
    while pos < len(text):
        while pos < len(text) and text[pos].isspace():
            pos += 1
        if pos == len(text):
            break
        value, pos = decoder.raw_decode(text, pos)
        yield from value if isinstance(value, list) else [value]


def successful_operations(api, verbs):
    requests = api.get('requests', {})
    rows = [r for r in requests.get('series', [])
            if r.get('labels', {}).get('verb', '').upper() in verbs
            and r.get('labels', {}).get('code', '').startswith('2')]
    complete = bool(rows) and not requests.get('issues') and all(r.get('delta') is not None for r in rows)
    values = [r['delta'] for r in rows if r.get('delta') is not None]
    return dict(count=sum(values) if complete else None, complete=complete,
                comparablePartialCount=sum(values) if values else None,
                attribution='API-server counters include background traffic', series=rows)


def terminate(process):
    if process and process.poll() is None:
        try:
            os.killpg(process.pid, signal.SIGTERM)
            process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=2)
        except ProcessLookupError:
            process.wait()


class Runner:
    def __init__(self, args):
        self.args = args
        self.out = Path(args.out).resolve()
        self.out.mkdir(parents=True, exist_ok=False)
        self.start = time.monotonic()
        self.end = min(self.start + args.timeout_seconds, self.start + DEADLINE - time.time())
        self.errors = Jsonl(self.out / 'errors.jsonl')
        self.commands = Jsonl(self.out / 'commands.jsonl')
        self.events = Jsonl(self.out / 'events.jsonl')
        self.run_uuid = str(uuid.uuid4())
        self.prefix = 'burner-' + self.run_uuid[:8]
        self.phases = []
        self.processes = []
        self.allowed_namespaces = {f'{self.prefix}-mixed-{i}' for i in range(8)} | {f'{self.prefix}-node'}
        self.env = dict(os.environ, GOFLAGS='-p=2', GOMEMLIMIT='2GiB')
        # kube-burner environment variables override user-data. Do not inherit stale values.
        for key in ('METRICS_DIRECTORY', 'WORKLOAD_PREFIX', 'WORKLOAD_IMAGE', 'UUID'):
            self.env.pop(key, None)
        kubeconfig = self.env.get('KUBECONFIG')
        if not kubeconfig:
            raise ValueError('Set KUBECONFIG to the private kind-burner configuration')
        private = args.private or self.env.get('PRIVATE')
        if private is None:
            private = str(Path(kubeconfig.split(os.pathsep)[0]).resolve().parent / 'workload-private')
        self.private = Path(private).resolve() / self.run_uuid
        if self.private == self.out or self.out in self.private.parents:
            raise ValueError('PRIVATE must be outside the evidence directory')
        self.private.mkdir(parents=True, mode=0o700)
        self.metrics = Path(args.metrics).resolve()
        self.summary = dict(uuid=self.run_uuid, prefix=self.prefix, startedAt=utcnow(),
                            image=args.image, phases=self.phases, steadyWorkloadPods=112,
                            plannedPeakWorkloadPods=128, proxyHop=False,
                            genericNativeObjectLatency=None,
                            objectLatencySource='Per-phase diff.json apiserver.request_duration histogram estimates',
                            timeoutSeconds=args.timeout_seconds)

    def remaining(self):
        return self.end - time.monotonic()

    def can_start(self):
        return self.remaining() > 0 and time.time() < DEADLINE

    def failure(self, message, **fields):
        self.errors.put(dict(timestamp=utcnow(), source='runner', workloadFailure=True,
                             message=message, **fields))

    def run(self, name, command, seconds=20, env=None):
        if not self.can_start():
            self.failure('No new command started after the time limit', command=name)
            return None, ''
        started = time.monotonic()
        path = self.out / (name + '.log')
        timed_out = False
        print(f'{utcnow()} {name}', flush=True)
        with path.open('w') as log:
            process = subprocess.Popen(command, cwd=ROOT, env=env or self.env,
                                       stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            self.processes.append(process)
            try:
                process.wait(timeout=max(0.1, min(seconds, self.remaining())))
            except subprocess.TimeoutExpired:
                timed_out = True
                terminate(process)
        elapsed = time.monotonic() - started
        rc = 124 if timed_out else process.returncode
        # No command includes credentials, only the private kubeconfig's path.
        self.commands.put(dict(timestamp=utcnow(), name=name, argv=command,
                               returncode=rc, timedOut=timed_out, elapsedSeconds=elapsed,
                               log=path.name))
        text = path.read_text(errors='replace')
        if rc:
            self.failure('Command failed', command=name, returncode=rc, log=path.name)
        if name.endswith('-burner'):
            for number, line in enumerate(text.splitlines(), 1):
                clean = re.sub(r'\x1b\[[0-9;]*m', '', line)
                if re.search(r'(?i)level=(error|fatal|panic)|\b(ERROR|ERRO|FATAL|FATA|PANIC|PANI)\b', clean):
                    self.errors.put(dict(timestamp=utcnow(), source='kube-burner', workloadFailure=True,
                                         command=name, line=number, message=line, log=path.name))
        return rc, text

    def background(self, name, command, stdout_path, stderr_path=None):
        if not self.can_start():
            raise TimeoutError('No new background process after the time limit')
        with open(stdout_path, 'w') as out:
            if stderr_path:
                with open(stderr_path, 'w') as err:
                    process = subprocess.Popen(command, cwd=ROOT, env=self.env, stdout=out,
                                               stderr=err, start_new_session=True)
            else:
                process = subprocess.Popen(command, cwd=ROOT, env=self.env, stdout=out,
                                           stderr=subprocess.STDOUT, start_new_session=True)
        self.processes.append(process)
        self.commands.put(dict(timestamp=utcnow(), name=name, argv=command, background=True))
        return process

    def counts(self, name, selector=None):
        command = ['kubectl', 'get', RESOURCES, '-A', '--request-timeout=15s',
                   '-o', 'jsonpath={range .items[*]}{.kind}{"\\t"}{.metadata.namespace}'
                         '{"\\t"}{.metadata.name}{"\\n"}{end}']
        if selector:
            command += ['-l', selector]
        rc, text = self.run(name, command)
        if rc != 0:
            return None
        counts = Counter(line.split('\t')[0] for line in text.splitlines() if line)
        return dict(sorted(counts.items()))

    def snapshot(self, name, path):
        return self.run(name, ['python3', str(self.metrics), 'snapshot', str(path)], 20)[0]

    def execute(self):
        if not self.metrics.is_file():
            raise FileNotFoundError(f'Missing snapshot helper: {self.metrics}')
        for executable in ('kube-burner', 'kubectl'):
            if not shutil.which(executable):
                raise FileNotFoundError(f'{executable} is not in PATH')
        rc, version = self.run('version', ['kube-burner', 'version'])
        if rc or not re.search(r'(?<![\d.])v?2\.8\.6(?![\d.])', version):
            raise ValueError('This workload requires kube-burner v2.8.6')
        self.summary['kubeBurnerVersion'] = version.strip()
        rc, nodes = self.run('nodes', ['kubectl', 'get', 'nodes', '--request-timeout=15s',
                                    '-o', 'jsonpath={range .items[*]}{.metadata.name}{"\\n"}{end}'])
        node_names = nodes.splitlines()
        if rc or len(node_names) != 3 or not all(n.startswith('burner-') for n in node_names):
            raise ValueError('Expected three kind nodes with the burner- name prefix')
        baseline = self.counts('baseline-counts')
        self.summary['baselineCounts'] = baseline
        if baseline is None or baseline.get('Pod', 0) > 22:
            raise ValueError('Baseline must be readable and have at most 22 pods')
        # Run stock kube-burner directly against the inherited private kubeconfig.
        events_raw = self.private / 'events-stream.json'
        event_process = self.background('events-watch', ['kubectl', 'get', 'events', '-A',
            '--watch', '--output-watch-events', '-o', 'json'], events_raw,
            self.out / 'events-watch.log')
        for phase, config, budget in PHASES:
            # Reserve time for deletion and its evidence before starting another create/update.
            if not self.can_start():
                self.failure('Time limit prevented phase start', phase=phase)
                continue
            if phase != 'delete' and self.remaining() < 115:
                self.failure('Skipped phase to reserve time for deletion', phase=phase)
                continue
            phase_dir = self.out / phase
            phase_dir.mkdir()
            data = dict(WORKLOAD_PREFIX=self.prefix, WORKLOAD_IMAGE=self.args.image,
                        METRICS_DIRECTORY=str(phase_dir / 'indexed'))
            data_path = phase_dir / 'user-data.json'
            dump(data_path, data)
            phase_start = time.monotonic()
            before, after = phase_dir / 'before', phase_dir / 'after'
            before_rc = self.snapshot(phase + '-before', before)
            reserved = 90 if phase != 'delete' else 30
            phase_timeout = max(1, int(min(budget, self.remaining() - reserved)))
            start = time.monotonic()
            rc, _ = self.run(phase + '-burner', ['kube-burner', 'init', '-c', str(ROOT / config),
                '--uuid', self.run_uuid, '--user-data', str(data_path),
                '--timeout', f'{phase_timeout}s', '--skip-log-file', '--log-level', 'debug'],
                phase_timeout + 2)
            workload_seconds = time.monotonic() - start
            after_rc = self.snapshot(phase + '-after', after)
            owned_counts = self.counts(phase + '-counts', f'experiment.kube-burner/run={self.run_uuid}')
            diff_rc, _ = self.run(phase + '-diff', ['python3', str(self.metrics), 'diff',
                                  str(before), str(after), str(phase_dir / 'diff.json')], 20)
            result = dict(name=phase, returncode=rc, workloadSeconds=workload_seconds,
                          totalSeconds=time.monotonic() - phase_start,
                          snapshotBeforeReturncode=before_rc, snapshotAfterReturncode=after_rc,
                          diffReturncode=diff_rc, runOwnedCounts=owned_counts)
            diff_path = phase_dir / 'diff.json'
            if diff_rc == 0 and diff_path.exists():
                diff = json.loads(diff_path.read_text())
                api = diff.get('apiserver', {})
                result['apiRequestDuration'] = api.get('request_duration')
                result['apiRequests'] = api.get('requests')
                result['apiStatusDetails'] = api.get('status_details')
                result['successfulCreates'] = successful_operations(api, {'POST', 'CREATE'})
                result['successfulDeletes'] = successful_operations(api, {'DELETE', 'DELETECOLLECTION'})
                for row in api.get('requests', {}).get('series', []):
                    code = row.get('labels', {}).get('code', '')
                    if code.startswith(('4', '5')) and (row.get('delta') or 0) > 0:
                        self.errors.put(dict(timestamp=utcnow(), source='apiserver-error-counter',
                                             workloadFailure=False, phase=phase, result=row,
                                             attribution='Includes background requests, not only this workload'))
            self.phases.append(result)
            dump(phase_dir / 'result.json', result)
            if event_process.poll() is not None:
                self.failure('Event watch exited unexpectedly', returncode=event_process.returncode)
        # The delete job waits for namespace removal. Count all objects, including ReplicaSets.
        selector = f'experiment.kube-burner/run={self.run_uuid}'
        owned = self.counts('remaining-run-counts', selector)
        final = self.counts('final-counts')
        self.summary.update(remainingRunCounts=owned, finalCounts=final,
                            baselineRestored=baseline == final and owned == {})
        if owned is None or owned:
            self.failure('Run-owned objects remain after deletion', counts=owned)
        if baseline != final:
            self.summary['backgroundCountDrift'] = dict(before=baseline, after=final,
                note='Whole-cluster drift is not itself a workload failure or a store bug')
        terminate(event_process)
        try:
            for event in documents(events_raw.read_text(errors='replace')):
                obj = event.get('object', event)
                namespace = obj.get('metadata', {}).get('namespace', '')
                if namespace not in self.allowed_namespaces:
                    continue
                entry = dict(timestamp=utcnow(), namespace=namespace, watchType=event.get('type'),
                             event=obj)
                self.events.put(entry)
                if obj.get('type') == 'Warning':
                    self.errors.put(dict(source='kubernetes-event', workloadFailure=False, **entry))
        except (ValueError, OSError) as error:
            self.failure('Failed to parse the event watch', detail=str(error))

    def finish(self):
        for process in reversed(self.processes):
            terminate(process)
        native = []
        for phase in self.phases:
            directory = self.out / phase['name'] / 'indexed'
            docs = []
            for path in directory.rglob('*.json'):
                try:
                    docs.extend(documents(path.read_text()))
                except (ValueError, OSError) as error:
                    self.failure('Failed to parse a native index', file=str(path), detail=str(error))
            native.extend(dict(phase=phase['name'], **doc) for doc in docs if isinstance(doc, dict)
                          and doc.get('metricName') in {'podLatencyQuantilesMeasurement', 'jobSummary'})
            if not any(isinstance(doc, dict) and doc.get('metricName') == 'jobSummary' for doc in docs):
                self.failure('Missing native job summary', phase=phase['name'])
            has_pods = any(isinstance(doc, dict) and doc.get('metricName') ==
                           'podLatencyQuantilesMeasurement' for doc in docs)
            phase['podQuantilesAvailable'] = has_pods
            if phase['name'] in {'mixed', 'node'} and not has_pods:
                self.failure('Missing native pod quantiles', phase=phase['name'])
            if phase['name'] == 'update' and not has_pods:
                phase['measurementIssue'] = 'No new ready pods observed by the native update measurement'
            for doc in docs:
                if isinstance(doc, dict) and doc.get('metricName') == 'jobSummary' and not doc.get('passed'):
                    self.errors.put(dict(source='native-job-summary', workloadFailure=True,
                                         phase=phase['name'], result=doc))
        dump(self.out / 'native-summary.json', native)
        errors = list(documents((self.out / 'errors.jsonl').read_text()))
        failures = [error for error in errors if error.get('workloadFailure', True)]
        workload_passed = not failures
        self.summary.update(finishedAt=utcnow(), totalSeconds=time.monotonic() - self.start,
                            errorCount=len(errors), observedErrorCount=len(errors) - len(failures),
                            workloadFailureCount=len(failures), workloadPassed=workload_passed,
                            passed=workload_passed, nativeSummary='native-summary.json')
        dump(self.out / 'summary.json', self.summary)
        for sink in (self.errors, self.commands, self.events):
            sink.close()
        return 0 if self.summary['workloadPassed'] else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', required=True, help='Fresh evidence directory')
    parser.add_argument('--private', help='Private directory outside evidence, defaults to PRIVATE')
    parser.add_argument('--metrics', default=str(ROOT / 'metrics.py'))
    parser.add_argument('--image', default='registry.k8s.io/pause:3.10')
    parser.add_argument('--timeout-seconds', type=int, default=570)
    args = parser.parse_args()
    if not 1 <= args.timeout_seconds <= 590:
        parser.error('--timeout-seconds must be between 1 and 590')
    if time.time() >= DEADLINE:
        parser.error('Deadline 2026-10-13T22:00:00Z has passed. Nothing will start.')
    if not Path(args.out).is_absolute():
        parser.error('--out must be absolute')
    runner = None
    try:
        runner = Runner(args)
        runner.execute()
    except (Exception, KeyboardInterrupt) as error:
        if runner is None:
            print(f'workload setup failed: {error}', file=sys.stderr)
            return 1
        runner.failure(f'{type(error).__name__}: {error}')
    return runner.finish()


if __name__ == '__main__':
    sys.exit(main())
