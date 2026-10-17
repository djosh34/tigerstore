#!/usr/bin/env python3
"""Synthetic application fleet sent through kube-apiserver, never directly to etcd."""
import base64
import datetime
import hashlib
import json
import random
import subprocess
import time

RNG = random.Random(502026)
NAMESPACES = 12
APPS = 10


def kubectl(*args, objects=None):
    data = None if objects is None else json.dumps({"apiVersion": "v1", "kind": "List", "items": objects})
    result = subprocess.run(["kubectl", *args], input=data, text=True, capture_output=True)
    if result.returncode:
        raise RuntimeError(result.stderr)
    return result.stdout


def stamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def obj(version, kind, namespace, name, app):
    return {"apiVersion": version, "kind": kind, "metadata": {"namespace": namespace, "name": name,
            "labels": {"app.kubernetes.io/name": app, "app.kubernetes.io/part-of": "shop", "environment": "staging", "compression-experiment": "true"}}}


def objects(ns, i, round_id):
    app = f"service-{i:02}"
    labels = {"app": app, "tier": ["frontend", "backend", "worker"][i % 3]}
    result = []
    # Unequal configuration sizes, structured text and per-application identifiers.
    for c, target in enumerate(([512, 2048, 8192, 32768][i % 4], 1024, 10240)):
        cm = obj("v1", "ConfigMap", ns, f"{app}-config-{c}", app)
        rows = []
        while len("\n".join(rows)) < target:
            k = len(rows)
            rows.append(f"route.{k}.url=https://api-{k % 17}.{ns}.svc.cluster.local:8443/v1/{app}/{hashlib.sha256(f'{ns}/{i}/{c}/{k}'.encode()).hexdigest()[:12]}\nroute.{k}.timeout=2500\nroute.{k}.retries=3")
        cm["data"] = {"application.properties": "\n".join(rows)[:target], "log-level": "info", "generation": str(round_id)}
        result.append(cm)
    secret = obj("v1", "Secret", ns, f"{app}-credentials", app)
    secret["type"] = "Opaque"
    # Deliberately fake seeded bytes, not credentials usable outside this disposable cluster.
    rng = random.Random(f"fake/{ns}/{i}")
    secret["data"] = {"username": base64.b64encode(app.encode()).decode(),
                      "synthetic-token": base64.b64encode(rng.randbytes([64, 256, 1024, 4096][i % 4])).decode()}
    result.append(secret)
    deployment = obj("apps/v1", "Deployment", ns, app, app)
    deployment["spec"] = {"replicas": 1, "revisionHistoryLimit": 2, "selector": {"matchLabels": {"app": app}},
        "strategy": {"type": "RollingUpdate", "rollingUpdate": {"maxSurge": 0, "maxUnavailable": 1}},
        "template": {"metadata": {"labels": labels, "annotations": {"checksum/config": hashlib.sha256(f"{ns}/{i}/{round_id}".encode()).hexdigest(), "prometheus.io/scrape": "true", "prometheus.io/port": "8080"}},
        "spec": {"automountServiceAccountToken": False, "terminationGracePeriodSeconds": 1,
                 "securityContext": {"runAsNonRoot": True, "runAsUser": 65532},
                 "containers": [{"name": "application", "image": "registry.k8s.io/pause:3.10.1", "imagePullPolicy": "IfNotPresent",
                    "ports": [{"name": "http", "containerPort": 8080}],
                    "resources": {"requests": {"cpu": "1m", "memory": "4Mi"}, "limits": {"memory": "32Mi"}},
                    "env": [{"name": "ENVIRONMENT", "value": "staging"}, {"name": "POD_NAME", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}},
                            {"name": "TOKEN", "valueFrom": {"secretKeyRef": {"name": f"{app}-credentials", "key": "synthetic-token"}}}],
                    "volumeMounts": [{"name": "config", "mountPath": "/etc/application", "readOnly": True}],
                    "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True, "capabilities": {"drop": ["ALL"]}}}],
                 "volumes": [{"name": "config", "configMap": {"name": f"{app}-config-0"}}]}}}
    result.append(deployment)
    service = obj("v1", "Service", ns, app, app)
    service["spec"] = {"selector": {"app": app}, "ports": [{"name": "http", "port": 80, "targetPort": "http"}]}
    result.append(service)
    for e in range(8):
        event = obj("v1", "Event", ns, f"{app}.{e:016x}", app)
        event.update({"involvedObject": {"apiVersion": "apps/v1", "kind": "Deployment", "namespace": ns, "name": app},
                      "reason": ["ScalingReplicaSet", "SuccessfulCreate", "BackOff", "FailedScheduling"][e % 4],
                      "message": f"Controller observed generation {round_id} for {ns}/{app}; replica {e} is awaiting reconciliation on compression-control-plane",
                      "source": {"component": "synthetic-controller", "host": "compression-control-plane"},
                      "type": "Warning" if e % 4 > 1 else "Normal", "firstTimestamp": stamp(), "lastTimestamp": stamp(), "count": round_id + 1})
        result.append(event)
    for l in range(2):
        lease = obj("coordination.k8s.io/v1", "Lease", ns, f"{app}-leader-{l}", app)
        lease["spec"] = {"holderIdentity": f"{app}-{hashlib.md5(ns.encode()).hexdigest()[:8]}", "leaseDurationSeconds": 30,
                         "acquireTime": stamp(), "renewTime": stamp(), "leaseTransitions": round_id}
        result.append(lease)
        ep = obj("discovery.k8s.io/v1", "EndpointSlice", ns, f"{app}-external-{l}", app)
        ep["metadata"]["labels"].update({"kubernetes.io/service-name": f"{app}-external", "endpointslice.kubernetes.io/managed-by": "compression-experiment"})
        ep.update({"addressType": "IPv4", "ports": [{"name": "https", "protocol": "TCP", "port": 8443}],
                   "endpoints": [{"addresses": [f"10.90.{i}.{10 + l * 16 + k}"], "conditions": {"ready": True, "serving": True, "terminating": False},
                                  "hostname": f"{app}-{k}", "zone": f"zone-{k % 3}"} for k in range([3, 10, 25][i % 3])]})
        result.append(ep)
    # Two custom resources stored as JSON: a release object whose status a
    # second field manager owns, and a monitor object as Prometheus users have.
    release = obj("shop.example.com/v1", "AppRelease", ns, f"{app}-release", app)
    release["spec"] = {"image": f"registry.example.com/shop/{app}:1.{round_id}.{i}", "replicas": 3,
        "strategy": {"canary": {"maxSurge": "25%", "steps": [s for w in (10, 25, 50, 100) for s in ({"setWeight": w}, {"pause": {"duration": "60s"}})]}},
        "trafficRouting": {"routes": [{"name": f"route-{k}", "match": {"pathPrefix": f"/api/v{k}/{app}", "headers": {"x-tenant": f"tenant-{k}"}},
                                       "backend": {"service": app, "port": 8080, "weight": 100 - 10 * k}} for k in range(1 + i % 4)]},
        "analysis": {"templates": [{"templateName": "success-rate", "args": [{"name": "service-name", "value": f"{app}.{ns}.svc.cluster.local"}]}]}}
    status = obj("shop.example.com/v1", "AppRelease", ns, f"{app}-release", app)
    del status["metadata"]["labels"]
    status["status"] = {"phase": ["Progressing", "Healthy"][round_id % 2], "observedGeneration": round_id + 1, "currentStepIndex": round_id * 2,
        "stableRS": hashlib.sha256(f"{ns}/{i}/{round_id}".encode()).hexdigest()[:10],
        "conditions": [{"type": t, "status": "True", "lastTransitionTime": stamp(), "lastUpdateTime": stamp(), "reason": r,
                        "message": f"AppRelease {ns}/{app} {r.lower()} at generation {round_id}"}
                       for t, r in (("Available", "MinimumReplicasAvailable"), ("Progressing", "NewReplicaSetAvailable"), ("Paused", "CanaryPauseStep"))]}
    monitor = obj("shop.example.com/v1", "ServiceMonitor", ns, app, app)
    monitor["spec"] = {"selector": {"matchLabels": {"app": app}}, "namespaceSelector": {"matchNames": [ns]},
        "endpoints": [{"port": "http", "path": "/metrics", "interval": "30s", "scrapeTimeout": "10s",
                       "relabelings": [{"sourceLabels": ["__meta_kubernetes_pod_node_name"], "targetLabel": "node", "action": "replace"},
                                       {"sourceLabels": ["__meta_kubernetes_namespace"], "targetLabel": "namespace", "action": "replace"}]}]}
    result += [release, monitor]
    return result, status


CRDS = [{"apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition",
         "metadata": {"name": f"{plural}.shop.example.com"},
         "spec": {"group": "shop.example.com", "scope": "Namespaced",
                  "names": {"plural": plural, "singular": plural[:-1], "kind": kind},
                  "versions": [{"name": "v1", "served": True, "storage": True,
                                "schema": {"openAPIV3Schema": {"type": "object", "x-kubernetes-preserve-unknown-fields": True}}}]}}
        for plural, kind in (("appreleases", "AppRelease"), ("servicemonitors", "ServiceMonitor"))]


def main():
    started = time.monotonic()
    namespaces = [f"shop-{n:02}" for n in range(NAMESPACES)]
    kubectl("create", "-f", "-", objects=[{"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": ns}} for ns in namespaces] + CRDS)
    kubectl("wait", "--for=condition=Established", "crd/appreleases.shop.example.com", "crd/servicemonitors.shop.example.com", "--timeout=60s")
    kubectl("api-resources", "--api-group=shop.example.com", "--cached=false")
    for generation in range(4):
        for ns in namespaces:
            made = [objects(ns, i, generation) for i in range(APPS)]
            kubectl("apply", "--server-side", "--field-manager=compression-workload", "-f", "-", objects=[o for m, _ in made for o in m])
            kubectl("apply", "--server-side", "--field-manager=release-controller", "-f", "-", objects=[st for _, st in made])
        print(json.dumps({"generation": generation, "elapsed_seconds": time.monotonic() - started}), flush=True)
        if generation == 1:
            # Delete and recreate one complete application per namespace.
            for ns in namespaces:
                kubectl("delete", "deploy,svc,cm,secret,event,lease,endpointslice,appreleases,servicemonitors", "-n", ns,
                        "-l", "app.kubernetes.io/name=service-00", "--wait=false")
        time.sleep(10)
    # A bounded readiness wait establishes that these are live controller-managed Pods.
    for ns in namespaces:
        kubectl("wait", "--for=condition=Available", "deployment", "--all", "-n", ns, "--timeout=120s")
    summary = {}
    for kind in ["appreleases", "servicemonitors", "deployments", "pods", "replicasets", "configmaps", "secrets", "events", "leases", "endpointslices"]:
        data = json.loads(kubectl("get", kind, "-A", "-o", "json"))
        summary[kind] = len(data["items"])
        if kind == "pods":
            summary["pod_phases"] = {phase: sum(o.get("status", {}).get("phase") == phase for o in data["items"]) for phase in ["Running", "Pending", "Succeeded", "Failed"]}
    print(json.dumps({"objects": summary, "elapsed_seconds": time.monotonic() - started}), flush=True)


if __name__ == "__main__":
    main()
