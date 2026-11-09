"""Drive a kind cluster with kwok nodes into one cluster shape, then churn it.

    python3 k8stest/load.py setup <shape>
    python3 k8stest/load.py churn <shape> <seconds> <seed> <summary.json>

Shapes are in k8stest/shapes.json. setup creates the shape's namespaces,
deployments, services, config maps, secrets and, in some shapes, CRDs with
custom objects, through kubectl, and waits until every deployment is
available. churn runs the shape's number of workers, each doing one
kubectl action after another until the time is up: rollouts, scaling,
updates of config maps, secrets and custom objects, jobs, deleted pods, and
namespaces made and removed. The cluster's controllers, scheduler and kwok
turn these into the etcd calls a real cluster makes.
"""

import json
import random
import string
import subprocess
import sys
import threading
import time
from pathlib import Path

SHAPES = json.loads((Path(__file__).parent / "shapes.json").read_text())
GROUP = "bench.tigerstore.io"


def kubectl(*args, stdin=None):
    subprocess.run(["kubectl", *args], input=stdin, text=True, check=True, stdout=subprocess.DEVNULL)


def text(rng, n):
    return "".join(rng.choices(string.ascii_letters + string.digits, k=n))


def ns_name(i):
    return f"team-{i:03d}"


def crd(i):
    kind = f"Widget{i}"
    return {
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": f"widget{i}s.{GROUP}"},
        "spec": {
            "group": GROUP,
            "scope": "Namespaced",
            "names": {"plural": f"widget{i}s", "singular": f"widget{i}", "kind": kind},
            "versions": [{
                "name": "v1", "served": True, "storage": True,
                "subresources": {"status": {}},
                "schema": {"openAPIV3Schema": {"type": "object", "x-kubernetes-preserve-unknown-fields": True}},
            }],
        },
    }


def deployment(ns, i, replicas):
    labels = {"app": f"app-{i}"}
    return {
        "apiVersion": "apps/v1", "kind": "Deployment",
        "metadata": {"name": f"app-{i}", "namespace": ns, "labels": labels},
        "spec": {
            "replicas": replicas,
            "selector": {"matchLabels": labels},
            "template": {
                "metadata": {"labels": labels},
                "spec": {"containers": [{
                    "name": "app", "image": "registry.k8s.io/pause:3.10",
                    "env": [{"name": f"SETTING_{k}", "value": f"value-{k}"} for k in range(8)],
                    "resources": {"requests": {"cpu": "10m", "memory": "16Mi"}},
                }]},
            },
        },
    }


def objects(shape, ns, rng):
    s = SHAPES[shape]
    out = []
    for i in range(s["configmaps"]):
        out.append({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": f"config-{i}", "namespace": ns},
                    "data": {"settings": text(rng, rng.randint(200, 4000))}})
    for i in range(s["secrets"]):
        out.append({"apiVersion": "v1", "kind": "Secret", "metadata": {"name": f"secret-{i}", "namespace": ns},
                    "stringData": {"token": text(rng, rng.randint(100, 2000))}})
    for i in range(s["deployments"]):
        out.append(deployment(ns, i, s["replicas"]))
    for i in range(s["services"]):
        app = f"app-{i % max(s['deployments'], 1)}"
        out.append({"apiVersion": "v1", "kind": "Service", "metadata": {"name": f"svc-{i}", "namespace": ns},
                    "spec": {"selector": {"app": app}, "ports": [{"port": 80 + i}]}})
    for c in range(s["crds"]):
        for i in range(s["crs"]):
            out.append({"apiVersion": f"{GROUP}/v1", "kind": f"Widget{c}",
                        "metadata": {"name": f"w-{i}", "namespace": ns},
                        "spec": {"size": rng.randint(1, 9), "config": text(rng, rng.randint(100, 1500))}})
    return out


def apply(objs):
    for i in range(0, len(objs), 200):
        doc = json.dumps({"apiVersion": "v1", "kind": "List", "items": objs[i:i + 200]})
        kubectl("apply", "--server-side", "-f", "-", stdin=doc)


def setup(shape):
    s = SHAPES[shape]
    rng = random.Random(1)
    if s["crds"]:
        apply([crd(i) for i in range(s["crds"])])
        kubectl("wait", "--for=condition=Established", "crd", "--all", "--timeout=120s")
    apply([{"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": ns_name(n)}} for n in range(s["namespaces"])])
    objs = []
    for n in range(s["namespaces"]):
        objs += objects(shape, ns_name(n), rng)
    apply(objs)
    kubectl("wait", "--for=condition=Available", "deployment", "--all", "--all-namespaces", "--timeout=900s")


class Churn:
    def __init__(self, shape, seconds, seed):
        self.s = SHAPES[shape]
        self.shape = shape
        self.deadline = time.monotonic() + seconds
        self.seed = seed
        self.lock = threading.Lock()
        self.counts = {}
        self.failures = {}
        self.temp = []  # temporary namespaces, oldest first
        self.next_temp = 0
        self.next_job = 0

    def actions(self):
        s = self.s
        acts = [(self.rollout, 2), (self.scale, 2), (self.configmap, 4), (self.secret, 1), (self.job, 2),
                (self.delete_pod, 2), (self.namespace, 1)]
        if s["crds"] and s["crs"]:
            acts += [(self.cr_spec, 4), (self.cr_status, 2)]
        return acts

    def ns(self, rng):
        return ns_name(rng.randrange(self.s["namespaces"]))

    def rollout(self, rng):
        if self.s["deployments"]:
            kubectl("rollout", "restart", f"deployment/app-{rng.randrange(self.s['deployments'])}", "-n", self.ns(rng))

    def scale(self, rng):
        if self.s["deployments"]:
            replicas = rng.randint(1, 2 * self.s["replicas"])
            kubectl("scale", f"deployment/app-{rng.randrange(self.s['deployments'])}", f"--replicas={replicas}", "-n", self.ns(rng))

    def configmap(self, rng):
        if self.s["configmaps"]:
            patch = json.dumps({"data": {"settings": text(rng, rng.randint(200, 4000))}})
            kubectl("patch", "configmap", f"config-{rng.randrange(self.s['configmaps'])}", "-n", self.ns(rng), "--type=merge", "-p", patch)

    def secret(self, rng):
        if self.s["secrets"]:
            patch = json.dumps({"stringData": {"token": text(rng, rng.randint(100, 2000))}})
            kubectl("patch", "secret", f"secret-{rng.randrange(self.s['secrets'])}", "-n", self.ns(rng), "--type=merge", "-p", patch)

    def cr(self, rng):
        return f"widget{rng.randrange(self.s['crds'])}s.{GROUP}", f"w-{rng.randrange(self.s['crs'])}"

    def cr_spec(self, rng):
        kind, name = self.cr(rng)
        patch = json.dumps({"spec": {"size": rng.randint(1, 9), "config": text(rng, rng.randint(100, 1500))}})
        kubectl("patch", kind, name, "-n", self.ns(rng), "--type=merge", "-p", patch)

    def cr_status(self, rng):
        kind, name = self.cr(rng)
        patch = json.dumps({"status": {"phase": rng.choice(["Pending", "Ready", "Degraded"]), "observed": rng.randint(1, 1 << 30)}})
        kubectl("patch", kind, name, "-n", self.ns(rng), "--subresource=status", "--type=merge", "-p", patch)

    def job(self, rng):
        with self.lock:
            self.next_job += 1
            name = f"job-{self.seed}-{self.next_job}"
        n = rng.randint(1, 5)
        job = {"apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": name, "namespace": self.ns(rng)},
               "spec": {"completions": n, "parallelism": n, "ttlSecondsAfterFinished": 30,
                        "template": {"spec": {"restartPolicy": "Never", "containers": [
                            {"name": "work", "image": "registry.k8s.io/pause:3.10"}]}}}}
        kubectl("create", "-f", "-", stdin=json.dumps(job))

    def delete_pod(self, rng):
        ns = self.ns(rng)
        out = subprocess.run(["kubectl", "get", "pods", "-n", ns, "-o", "name"], text=True, check=True, capture_output=True).stdout.split()
        if out:
            kubectl("delete", rng.choice(out), "-n", ns, "--wait=false")

    def namespace(self, rng):
        with self.lock:
            self.next_temp += 1
            ns = f"temp-{self.seed}-{self.next_temp}"
            old = self.temp.pop(0) if len(self.temp) >= 3 else None
            self.temp.append(ns)
        kubectl("create", "namespace", ns)
        apply(objects(self.shape, ns, rng))
        if old:
            kubectl("delete", "namespace", old, "--wait=false")

    def worker(self, i):
        rng = random.Random(self.seed * 1000 + i)
        acts = self.actions()
        funcs, weights = [a for a, _ in acts], [w for _, w in acts]
        while time.monotonic() < self.deadline:
            act = rng.choices(funcs, weights)[0]
            name = act.__name__
            try:
                act(rng)
                with self.lock:
                    self.counts[name] = self.counts.get(name, 0) + 1
            except subprocess.CalledProcessError:
                # A clash with a controller or a missing object: the
                # cluster goes on, the summary counts it.
                with self.lock:
                    self.failures[name] = self.failures.get(name, 0) + 1

    def run(self):
        threads = [threading.Thread(target=self.worker, args=(i,)) for i in range(self.s["workers"])]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        return {"shape": self.shape, "seed": self.seed, "actions": self.counts, "failed": self.failures}


def main(argv):
    if len(argv) >= 3 and argv[1] == "setup":
        setup(argv[2])
    elif len(argv) == 6 and argv[1] == "churn":
        summary = Churn(argv[2], float(argv[3]), int(argv[4])).run()
        Path(argv[5]).write_text(json.dumps(summary, indent=2) + "\n")
        print(json.dumps(summary))
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main(sys.argv)
