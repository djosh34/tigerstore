"""Create 50,000 ConfigMaps, delete their namespace, check API and storage."""
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


COUNT = 50_000
NAMESPACE = "mass-delete-50000"
LABEL = "k8stest=mass-delete"


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def main():
    root = Path(os.environ["K8S_ROOT"])
    result = {"objects": COUNT, "namespace": NAMESPACE, "passed": False}
    try:
        run(root, result)
        result["passed"] = True
    finally:
        text = json.dumps(result, indent=2) + "\n"
        (root / "results/configmaps.json").write_text(text)
        print(text, flush=True)


def run(root, result):
    config = json.loads(subprocess.check_output([
        "kubectl", "config", "view", "--raw", "--minify", "-o", "json",
    ]))
    cluster = config["clusters"][0]["cluster"]
    user = config["users"][0]["user"]
    for name, data in [
        ("ca", cluster["certificate-authority-data"]),
        ("cert", user["client-certificate-data"]),
        ("key", user["client-key-data"]),
    ]:
        path = root / f"api-{name}.pem"
        path.touch(mode=0o600)
        path.write_bytes(base64.b64decode(data))
    ctx = ssl.create_default_context(cafile=str(root / "api-ca.pem"))
    ctx.load_cert_chain(str(root / "api-cert.pem"), str(root / "api-key.pem"))
    url = urllib.parse.urlsplit(cluster["server"])
    local = threading.local()

    def request(method, path, value=None, statuses=(200,)):
        if not hasattr(local, "conn"):
            local.conn = http.client.HTTPSConnection(
                url.hostname, url.port, context=ctx, timeout=120,
            )
        body = None if value is None else json.dumps(value).encode()
        local.conn.request(method, path, body, {"Content-Type": "application/json"})
        response = local.conn.getresponse()
        data = response.read()
        require(response.status in statuses,
                f"{method} {path}: HTTP {response.status}: {data[:2000]!r}")
        return response.status, json.loads(data) if data else {}

    def keys(endpoint, prefix):
        output = subprocess.check_output([
            "etcdctl", "get", prefix,
            "--prefix", "--keys-only", "--consistency=l", "-w", "json",
        ], timeout=60, env={**os.environ, "ETCDCTL_ENDPOINTS": endpoint})
        data = json.loads(output)
        return {base64.b64decode(kv["key"]).decode() for kv in data.get("kvs", [])}

    base = f"/api/v1/namespaces/{NAMESPACE}"
    expected = {f"cm-{i:05d}" for i in range(COUNT)}
    request("POST", "/api/v1/namespaces", {
        "apiVersion": "v1", "kind": "Namespace", "metadata": {"name": NAMESPACE},
    }, statuses=(201,))

    def create(i):
        request("POST", base + "/configmaps", {
            "apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {"name": f"cm-{i:05d}", "namespace": NAMESPACE,
                         "labels": {"k8stest": "mass-delete"}},
            "data": {"value": "x" * 256},
        }, statuses=(201,))

    start = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as pool:
        for created, _ in enumerate(pool.map(create, range(COUNT)), 1):
            if created % 5000 == 0:
                print(f"created {created}/{COUNT}", flush=True)
    result["create_seconds"] = time.monotonic() - start
    names = set()
    continuation = ""
    while True:
        query = urllib.parse.urlencode({
            "limit": 1000, "labelSelector": LABEL, "continue": continuation,
        })
        _, data = request("GET", base + "/configmaps?" + query)
        page = [item["metadata"]["name"] for item in data["items"]]
        require(len(set(page)) == len(page) and not names.intersection(page),
                "duplicate names in paginated API list")
        names.update(page)
        continuation = data["metadata"].get("continue", "")
        if not continuation:
            break
    require(names == expected, f"API inventory differs: {len(names)} names")
    result["api_count_before"] = len(names)
    prefix = f"/registry/configmaps/{NAMESPACE}/"
    expected_keys = {prefix + name for name in expected}
    endpoints = os.environ["ETCDCTL_ENDPOINTS"].split(",")
    before = {}
    for endpoint in endpoints:
        actual = keys(endpoint, prefix)
        require(expected_keys.issubset(actual), f"{endpoint}: missing ConfigMap keys")
        # Kubernetes may also create kube-root-ca.crt in the namespace.
        require(actual - expected_keys <= {prefix + "kube-root-ca.crt"},
                f"{endpoint}: unexpected ConfigMap keys")
        before[endpoint] = len(actual)
    result["storage_counts_before"] = before
    start = time.monotonic()
    request("DELETE", base, {}, statuses=(200, 202))
    while True:
        status, data = request("GET", base, statuses=(200, 404))
        if status == 404:
            break
        require(time.monotonic() - start < 900,
                f"namespace still present after 900s: {data}")
        time.sleep(1)
    result["namespace_delete_seconds"] = time.monotonic() - start
    # A successful empty LIST is required. A 404 alone does not prove the
    # storage is empty, and no per-object DELETE or forced finalization runs.
    _, data = request("GET", base + "/configmaps")
    require(data.get("items") == [], "API returned ConfigMaps after namespace deletion")
    after = {}
    for endpoint in endpoints:
        # Check every current registry key belonging to this namespace,
        # including defaults, Events and the namespace object itself.
        registry = keys(endpoint, "/registry/")
        remaining = sorted(key for key in registry
                           if f"/{NAMESPACE}/" in key
                           or key == f"/registry/namespaces/{NAMESPACE}")
        after[endpoint] = remaining
        require(not remaining, f"{endpoint}: remaining namespace keys: {remaining[:20]}")
    result["remaining_registry_keys"] = after
    result["api_count_after"] = 0


if __name__ == "__main__":
    main()
