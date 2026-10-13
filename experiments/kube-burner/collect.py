#!/usr/bin/env python3
"""Collect diagnostic logs, never certificates, kubeconfigs or Secret objects."""
import json
import os
from pathlib import Path
import subprocess
import sys
import time

out = Path(sys.argv[1])
out.mkdir(parents=True, exist_ok=True)
records = []


def run(name, args, timeout=30):
    start = time.time()
    try:
        p = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
        stdout, stderr, code = p.stdout, p.stderr, p.returncode
    except subprocess.TimeoutExpired as e:
        stdout = e.stdout or b""
        stderr = e.stderr or b""
        stdout = stdout.decode(errors="replace") if isinstance(stdout, bytes) else stdout
        stderr = stderr.decode(errors="replace") if isinstance(stderr, bytes) else stderr
        stderr += "\nDiagnostic command timed out\n"
        code = 124
    (out / (name + ".stdout")).write_text(stdout)
    (out / (name + ".stderr")).write_text(stderr)
    records.append(dict(name=name, argv=args, exit=code, elapsed=time.time() - start))
    return stdout if code == 0 else ""


for n in range(1, 4):
    name = f"external-etcd-{n}"
    run(name, ["docker", "logs", "--timestamps", name])
    run(name + "-inspect", ["docker", "inspect", name])
for node in ["burner-control-plane", "burner-worker", "burner-worker2"]:
    run(node + "-kubelet", ["docker", "exec", node, "journalctl", "-u", "kubelet", "--no-pager"])
    containers = run(node + "-containers", ["docker", "exec", node, "crictl", "ps", "-a", "-o", "json"])
    if not containers:
        continue
    for c in json.loads(containers).get("containers", []):
        name = c.get("metadata", {}).get("name", "")
        if name in ["kube-apiserver", "kube-controller-manager", "kube-scheduler", "coredns"]:
            run(node + "-" + name + "-" + c["id"][:12], ["docker", "exec", node, "crictl", "logs", c["id"]])
for resource in ["pods", "events", "namespaces"]:
    run(resource, ["kubectl", "--request-timeout=20s", "get", resource, "-A", "-o", "json"])
run("readyz", ["kubectl", "--request-timeout=20s", "get", "--raw", "/readyz?verbose"])
run("disk", ["df", "-B1", os.environ["RUNNER_TEMP"]])
run("memory", ["free", "-b"])
(out / "commands.json").write_text(json.dumps(records, indent=2) + "\n")
