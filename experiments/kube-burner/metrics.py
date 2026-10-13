#!/usr/bin/env python3
"""Capture comparison evidence without treating missing telemetry as zero."""
import argparse
import datetime
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
import time

NODES = [f"external-etcd-{n}" for n in (1, 2, 3)]
CONTAINERS = ["burner-control-plane", *NODES]
EXCLUDED = {"WATCH", "CONNECT"}
API_HIST = "apiserver_request_duration_seconds"
STORAGE_HISTS = ("etcd_request_duration_seconds", "apiserver_etcd_request_duration_seconds")
SERVER_COUNTERS = (
    "grpc_server_handled_total", "etcd_server_proposals_committed_total",
    "etcd_server_proposals_applied_total", "etcd_mvcc_put_total",
    "etcd_mvcc_delete_total", "etcd_mvcc_range_total",
    "etcd_network_client_grpc_received_bytes_total",
    "etcd_network_client_grpc_sent_bytes_total",
)
SAMPLE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{.*\})?\s+(\S+)(?:\s+.*)?$')
LABEL = re.compile(r'\s*([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\[\\"n])*)"\s*(?:,|$)')


def stamp():
    return {"monotonic_ns": time.monotonic_ns(), "utc": datetime.datetime.now(datetime.timezone.utc).isoformat()}


def issue(code, detail):
    return {"code": code, "detail": str(detail)}


def command(argv, timeout=30):
    result = {"command": argv, "started": stamp()}
    try:
        run = subprocess.run(argv, capture_output=True, text=True, timeout=timeout, check=False)
        result.update(returncode=run.returncode, stdout=run.stdout, stderr=run.stderr)
        result["issues"] = [] if run.returncode == 0 else [issue("command_failed", run.stderr or f"exit {run.returncode}")]
    except (OSError, subprocess.TimeoutExpired) as exc:
        result.update(returncode=None, stdout="", stderr=str(exc), issues=[issue("command_failed", exc)])
    result["finished"] = stamp()
    return result


def write_json(path, value):
    # Never emit NaN/Infinity, which are not JSON numbers.
    path = Path(path)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n")
    tmp.replace(path)


def parse_metrics(text, wanted):
    samples, problems, seen = [], [], set()
    for number, line in enumerate(text.splitlines(), 1):
        if not line or line.startswith("#"):
            continue
        name = re.split(r'[\s{]', line, maxsplit=1)[0]
        if name not in wanted:
            continue
        try:
            match = SAMPLE.fullmatch(line)
            if not match:
                raise ValueError("invalid sample syntax")
            name, label_text, value_text = match.groups()
            labels = {}
            if label_text:
                body, pos = label_text[1:-1], 0
                while pos < len(body):
                    label = LABEL.match(body, pos)
                    if not label:
                        raise ValueError("invalid labels")
                    key, value = label.groups()
                    if key in labels:
                        raise ValueError("duplicate label")
                    labels[key] = re.sub(r'\\([\\"n])', lambda m: '\n' if m[1] == 'n' else m[1], value)
                    pos = label.end()
            value = float(value_text)
            if not math.isfinite(value) or value < 0:
                raise ValueError("non-finite or negative counter")
            key = (name, tuple(sorted(labels.items())))
            if key in seen:
                raise ValueError("duplicate sample")
            seen.add(key)
            samples.append({"name": name, "labels": labels, "value": value})
        except ValueError as exc:
            problems.append(issue("invalid_sample", f"line {number}: {exc}"))
    return samples, problems


def metric_names():
    return {"process_start_time_seconds", "apiserver_request_total", *SERVER_COUNTERS, *(h + suffix for h in (API_HIST, *STORAGE_HISTS) for suffix in ("_bucket", "_count", "_sum"))}


def metrics_result(result, raw_path, http=False):
    text = result.pop("stdout")
    if http:
        body, sep, status = text.rpartition("\n")
        result["http_status"] = int(status) if sep and status.isdigit() else None
        text = body if sep and status.isdigit() else text
        if result["http_status"] == 404:
            result["availability"] = "unsupported"
            result["issues"].append(issue("unsupported_metrics", "HTTP 404; not a workload failure"))
        elif result["http_status"] != 200:
            result["availability"] = "unavailable"
            result["issues"].append(issue("http_error", result["http_status"]))
        else:
            result["availability"] = "available" if result["returncode"] == 0 else "unavailable"
    else:
        result["availability"] = "available" if result["returncode"] == 0 else "unavailable"
    Path(raw_path).write_text(text)
    result["raw_file"] = Path(raw_path).name
    result["samples"], errors = parse_metrics(text, metric_names())
    result["issues"].extend(errors)
    starts = [s["value"] for s in result["samples"] if s["name"] == "process_start_time_seconds" and not s["labels"]]
    result["process_start_time_seconds"] = starts[0] if len(starts) == 1 else None
    return result


def cpu_samples():
    inspection = command(["docker", "inspect", *CONTAINERS])
    try:
        objects = json.loads(inspection["stdout"])
        objects = {obj["Name"].lstrip("/"): obj for obj in objects}
    except (ValueError, KeyError, TypeError):
        objects = {}
    samples = {}
    for name in CONTAINERS:
        sample = {"started": stamp(), "issues": []}
        try:
            obj = objects.get(name)
            if obj is None:
                raise ValueError("container absent from docker inspect")
            state = obj["State"]
            if not state["Running"] or state["Pid"] <= 0:
                raise ValueError("container is not running")
            pid = state["Pid"]
            sample["identity"] = {"container_id": obj["Id"], "pid": pid, "container_started_at": state["StartedAt"]}
            # comm can contain spaces and ')' characters, so split at the last ')'.
            stat = Path(f"/proc/{pid}/stat").read_text().strip()
            fields = stat[stat.rindex(")") + 2:].split()
            sample["identity"]["starttime_ticks"] = int(fields[19])
            sample.update(
                raw_stat=stat, clock_ticks_per_second=os.sysconf("SC_CLK_TCK"),
                utime_ticks=int(fields[11]), stime_ticks=int(fields[12]),
                cutime_ticks=int(fields[13]), cstime_ticks=int(fields[14]),
                threads=int(fields[17]), rss_pages=int(fields[21]),
            )
        except (OSError, ValueError, KeyError, IndexError, TypeError) as exc:
            sample["issues"].append(issue("cpu_unavailable", exc))
        sample["finished"] = stamp()
        samples[name] = sample
    # Detect a restart during the sampling operation, not only between snapshots.
    verification = command(["docker", "inspect", *CONTAINERS])
    try:
        objects = {obj["Name"].lstrip("/"): obj for obj in json.loads(verification["stdout"])}
    except (ValueError, KeyError, TypeError):
        objects = {}
    for name, sample in samples.items():
        if "identity" not in sample:
            continue
        identity, obj = sample["identity"], objects.get(name)
        if not obj or not obj["State"]["Running"] or (obj["Id"], obj["State"]["Pid"], obj["State"]["StartedAt"]) != (identity["container_id"], identity["pid"], identity["container_started_at"]):
            sample["issues"].append(issue("cpu_identity_unverified", "container changed or second inspect failed"))
    inspection.pop("stdout")
    verification.pop("stdout")
    return {"samples": samples, "inspect": inspection, "verification": verification}


def snapshot(outdir, store):
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    if (outdir / "snapshot.json").exists():
        raise ValueError(f"snapshot already exists: {outdir}")
    result = {"schema_version": 1, "store": store, "started": stamp(), "issues": []}
    try:
        result["boot_id"] = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    except OSError as exc:
        result["boot_id"] = None
        result["issues"].append(issue("boot_id_unavailable", exc))
    result["apiserver"] = metrics_result(command(["kubectl", "--request-timeout=20s", "get", "--raw=/metrics"]), outdir / "apiserver.prom")
    result["cpu"] = cpu_samples()
    status = command([
        "docker", "exec", "external-etcd-1", "/bin-etcd/etcdctl",
        "--endpoints=" + ",".join(f"https://{name}:2379" for name in NODES),
        "--cacert=/certs/ca.crt", "--cert=/certs/client.crt", "--key=/certs/client.key",
        "--command-timeout=10s", "endpoint", "status", "-w", "json",
    ])
    (outdir / "etcd-status.json").write_text(status.pop("stdout"))
    try:
        status["endpoints"] = json.loads((outdir / "etcd-status.json").read_text())
        if not isinstance(status["endpoints"], list):
            raise ValueError("endpoint status must be an array")
    except ValueError as exc:
        status["endpoints"] = []
        status["issues"].append(issue("invalid_endpoint_status", exc))
    result["etcd_status"] = status
    result["server_metrics"] = {}
    for name in NODES:
        scrape = command([
            "docker", "exec", "burner-control-plane", "curl", "--silent", "--show-error", "--max-time", "15",
            "--cacert", "/etc/kubernetes/pki/external/ca.crt",
            "--cert", "/etc/kubernetes/pki/external/client.crt",
            "--key", "/etc/kubernetes/pki/external/client.key",
            "--write-out", "\n%{http_code}", f"https://{name}:2379/metrics",
        ])
        result["server_metrics"][name] = metrics_result(scrape, outdir / f"{name}.prom", http=True)
    result["finished"] = stamp()
    write_json(outdir / "snapshot.json", result)
    print(outdir / "snapshot.json")


def midpoint(record):
    return (record["started"]["monotonic_ns"] + record["finished"]["monotonic_ns"]) / 2


def elapsed(before, after, same_boot):
    if not same_boot:
        return None, [issue("clock_identity_mismatch", "snapshots need the same known host boot ID")]
    seconds = (midpoint(after) - midpoint(before)) / 1e9
    if seconds <= 0:
        return None, [issue("invalid_interval", "after must be later than before")]
    return seconds, []


def sample_index(record, name, exclude=False):
    return {tuple(sorted(s["labels"].items())): s["value"] for s in record.get("samples", []) if s["name"] == name and not (exclude and s["labels"].get("verb") in EXCLUDED)}


def delta(before, after):
    if before is None or after is None:
        return None, [issue("missing_counter", "missing " + ("before and after" if before is None and after is None else "before" if before is None else "after"))]
    value = after - before
    if value < 0:
        return None, [issue("counter_reset", f"{before} -> {after}")]
    return value, []


def counter_diff(before, after, name, seconds, exclude=False):
    b, a = sample_index(before, name, exclude), sample_index(after, name, exclude)
    problems, rows = [], []
    for side, record in (("before", before), ("after", after)):
        if record.get("availability") != "available" or record.get("issues"):
            problems.append(issue("source_incomplete", side + " scrape has issues or is unavailable"))
    bs, ast = before.get("process_start_time_seconds"), after.get("process_start_time_seconds")
    identity = "unknown" if bs is None or ast is None else "same" if bs == ast else "changed"
    if identity == "changed":
        problems.append(issue("source_process_changed", f"process start time {bs} -> {ast}"))
    infer_zero = identity == "same" and not problems
    if not a or (not b and not infer_zero):
        problems.append(issue("missing_metric", f"{name} missing from " + ("before and after" if not b and not a else "before" if not b else "after")))
    for labels in sorted(b.keys() | a.keys()):
        observed_before, observed_after = b.get(labels), a.get(labels)
        inferred = observed_before is None and observed_after is not None and infer_zero
        baseline = 0.0 if inferred else observed_before
        value, errors = delta(baseline, observed_after)
        if observed_before is None and observed_after is not None and not infer_zero:
            errors.append(issue("new_series_baseline_unknown", "zero inference requires valid scrapes and matching process start times"))
        if problems and any(p["code"] in {"source_incomplete", "source_process_changed"} for p in problems):
            value = None
        rows.append({"labels": dict(labels), "observed_before": observed_before, "before": baseline, "after": observed_after, "inferred_zero": inferred, "delta": value, "per_second": value / seconds if value is not None and seconds else None, "issues": errors})
    complete = bool(rows) and not problems and all(r["delta"] is not None for r in rows)
    comparable = [r["delta"] for r in rows if r["delta"] is not None]
    total = sum(comparable) if complete else None
    return {"metric": name, "exposure": "present" if b or a else "not_exposed", "process_identity": {"status": identity, "before": bs, "after": ast}, "complete": complete, "delta": total, "per_second": total / seconds if total is not None and seconds else None, "comparable_series_delta": sum(comparable) if comparable else None, "series": rows, "issues": problems}


def quantile(buckets, q):
    total = buckets[-1][1]
    if total == 0:
        return None
    rank, previous_bound, previous_count = q * total, 0.0, 0.0
    for bound, count in buckets:
        if count >= rank:
            if math.isinf(bound):
                return previous_bound if previous_bound > 0 else None
            if count == previous_count:
                return bound
            return previous_bound + (bound - previous_bound) * (rank - previous_count) / (count - previous_count)
        previous_bound, previous_count = bound, count
    return None


def histogram_diff(before, after, name, seconds, exclude=False):
    counts = counter_diff(before, after, name + "_count", seconds, exclude)
    buckets = counter_diff(before, after, name + "_bucket", seconds, exclude)
    verbs = sorted({r["labels"].get("verb") for r in counts["series"] + buckets["series"] if r["labels"].get("verb") is not None})

    resources = sorted({r["labels"]["resource"] for r in counts["series"] + buckets["series"] if "resource" in r["labels"]})

    def group(verb=None, resource=None):
        def matches(row):
            return (verb is None or row["labels"].get("verb") == verb) and (resource is None or row["labels"].get("resource") == resource)
        c = [r for r in counts["series"] if matches(r)]
        b = [r for r in buckets["series"] if matches(r)]
        errors = list(counts["issues"] + buckets["issues"])
        if not c or not b:
            errors.append(issue("missing_histogram", "count or buckets absent"))
        # Validate every original label series before merging. Aggregation must not hide a reset or malformed histogram.
        cb = {tuple(sorted(r["labels"].items())): r for r in c}
        grouped = {}
        for row in b:
            labels = dict(row["labels"])
            le = labels.pop("le", None)
            key = tuple(sorted(labels.items()))
            try:
                bound = float(le)
                if math.isnan(bound) or bound <= 0:
                    raise ValueError("non-positive or NaN bucket boundary")
                grouped.setdefault(key, []).append((bound, row))
            except (ValueError, TypeError) as exc:
                errors.append(issue("invalid_bucket", exc))
        merged = {}
        bounds = None
        for key in cb.keys() | grouped.keys():
            rows = sorted(grouped.get(key, []), key=lambda r: r[0])
            count = cb.get(key)
            current_bounds = [r[0] for r in rows]
            if bounds is None:
                bounds = current_bounds
            if not rows or current_bounds != bounds or not math.isinf(rows[-1][0]) or not count:
                errors.append(issue("incompatible_histogram", str(dict(key))))
                continue
            for _, row in rows:
                errors.extend(row["issues"])
            errors.extend(count["issues"])
            if count["delta"] is None or any(r["delta"] is None for _, r in rows):
                continue
            for field in ("before", "after", "delta"):
                values = [r[field] for _, r in rows]
                if any(y < x for x, y in zip(values, values[1:])) or values[-1] != count[field]:
                    errors.append(issue("invalid_histogram", f"{dict(key)}: non-cumulative buckets or +Inf != count at {field}"))
            for bound, row in rows:
                merged[bound] = merged.get(bound, 0) + row["delta"]
        n = sum(r["delta"] for r in c) if c and all(r["delta"] is not None for r in c) and not counts["issues"] else None
        pairs = sorted(merged.items())
        valid = bool(pairs) and not errors
        if valid and n == 0:
            errors.append(issue("no_observations", "zero observed requests; quantiles undefined"))
        return {"count_delta": n, "per_second": n / seconds if n is not None and seconds else None, "p50_seconds": quantile(pairs, .50) if valid else None, "p99_seconds": quantile(pairs, .99) if valid else None, "buckets": [{"le": "+Inf" if math.isinf(bound) else bound, "delta": value} for bound, value in pairs] if valid else None, "issues": errors}

    resource_verbs = {resource: sorted({r["labels"]["verb"] for r in counts["series"] + buckets["series"] if r["labels"].get("resource") == resource and "verb" in r["labels"]}) for resource in resources}
    return {"metric": name, "overall": group(), "by_verb": {verb: group(verb) for verb in verbs}, "by_resource": {resource: group(resource=resource) for resource in resources}, "by_resource_verb": {resource: {verb: group(verb, resource) for verb in resource_verbs[resource]} for resource in resources}, "counts": counts, "bucket_series": buckets["series"]}


def request_details(counters):
    by_code = {}
    for row in counters["series"]:
        code = row["labels"].get("code", "<missing code label>")
        by_code.setdefault(code, []).append(row)
    details = {}
    for code, rows in by_code.items():
        complete = not counters["issues"] and all(r["delta"] is not None for r in rows)
        details[code] = {"delta": sum(r["delta"] for r in rows) if complete else None, "series": rows}
    # Kubernetes error responses are HTTP 4xx and 5xx. Keep all status codes visible.
    errors = [row for code, rows in by_code.items() if code.isdigit() and 400 <= int(code) < 600 for row in rows]
    known_codes = all(code.isdigit() for code in by_code)
    total = sum(r["delta"] for r in errors) if errors and counters["complete"] and known_codes else None
    issues = [] if known_codes else [issue("missing_status_code", "cannot classify all requests")]
    if not errors:
        issues.append(issue("no_error_series", "no HTTP 4xx/5xx counter series present; do not infer zero errors"))
    return {"by_status_code": details, "http_4xx_5xx_delta": total, "issues": issues}


def revision_diff(before, after):
    def index(record):
        return {row.get("Endpoint"): row for row in record.get("endpoints", []) if isinstance(row, dict)}
    b, a = index(before), index(after)
    results = {}
    for name in NODES:
        endpoint = f"https://{name}:2379"
        row = {"before": None, "after": None, "delta": None, "issues": []}
        try:
            bh, ah = b[endpoint]["Status"]["header"], a[endpoint]["Status"]["header"]
            row["before"], row["after"] = bh["revision"], ah["revision"]
            row["identity_before"] = {k: bh[k] for k in ("cluster_id", "member_id")}
            row["identity_after"] = {k: ah[k] for k in ("cluster_id", "member_id")}
            if row["identity_before"] != row["identity_after"]:
                raise ValueError("cluster/member identity changed")
            if not all(type(row[k]) is int and row[k] >= 0 for k in ("before", "after")):
                raise ValueError("revision must be a nonnegative integer")
            row["delta"], errors = delta(row["before"], row["after"])
            row["issues"].extend(errors)
            for side, record in (("before", b[endpoint]), ("after", a[endpoint])):
                if record.get("Error") or record["Status"].get("errors"):
                    row["issues"].append(issue("endpoint_error", f"{side}: {record}"))
                    row["delta"] = None
        except (KeyError, ValueError, TypeError) as exc:
            row["issues"].append(issue("revision_unavailable", exc))
        results[name] = row
    return {"meaning": "MVCC revision change per replica, not requests or operations; do not sum replicas", "endpoints": results, "timing_before": {k: before[k] for k in ("started", "finished")}, "timing_after": {k: after[k] for k in ("started", "finished")}, "source_issues": {"before": before.get("issues", []), "after": after.get("issues", [])}}


def cpu_diff(before, after, same_boot):
    result = {}
    for name in CONTAINERS:
        b, a = before["samples"][name], after["samples"][name]
        seconds, errors = elapsed(b, a, same_boot)
        row = {"elapsed_seconds": seconds, "identity_before": b.get("identity"), "identity_after": a.get("identity"), "user_seconds": None, "system_seconds": None, "cpu_seconds": None, "average_cores": None, "percent_one_core": None, "issues": errors + b["issues"] + a["issues"]}
        if b.get("identity") != a.get("identity") or not b.get("identity"):
            row["issues"].append(issue("process_identity_changed", "container ID, PID, start time, or container start changed"))
        if b.get("clock_ticks_per_second") != a.get("clock_ticks_per_second"):
            row["issues"].append(issue("clock_tick_mismatch", "SC_CLK_TCK differs"))
        if not row["issues"]:
            user, ue = delta(b["utime_ticks"], a["utime_ticks"])
            system, se = delta(b["stime_ticks"], a["stime_ticks"])
            row["issues"].extend(ue + se)
            if not row["issues"]:
                hz = b["clock_ticks_per_second"]
                row.update(user_seconds=user / hz, system_seconds=system / hz, cpu_seconds=(user + system) / hz)
                row["average_cores"] = row["cpu_seconds"] / seconds
                row["percent_one_core"] = row["average_cores"] * 100
        result[name] = row
    return {"scope": "exact container main PID utime+stime only; excludes children and other container processes", "containers": result}


def diff(before_path, after_path, output):
    before = json.loads((Path(before_path) / "snapshot.json").read_text())
    after = json.loads((Path(after_path) / "snapshot.json").read_text())
    if before.get("schema_version") != 1 or after.get("schema_version") != 1:
        raise ValueError("unsupported snapshot schema")
    same_boot = bool(before.get("boot_id")) and before["boot_id"] == after.get("boot_id")
    seconds, errors = elapsed(before["apiserver"], after["apiserver"], same_boot)
    if before["store"] != after["store"]:
        raise ValueError("cannot compare different STORE values")
    b, a = before["apiserver"], after["apiserver"]
    requests = counter_diff(b, a, "apiserver_request_total", seconds, exclude=True)
    if not same_boot:
        # Matching process timestamps on different boots cannot establish identity.
        b, a = dict(b), dict(a)
        b["process_start_time_seconds"] = a["process_start_time_seconds"] = None
    result = {
        "schema_version": 1, "store": before["store"], "before": str(before_path), "after": str(after_path),
        "elapsed_seconds": seconds, "issues": errors,
        "timing": {"before": {k: before[k] for k in ("started", "finished", "boot_id")}, "after": {k: after[k] for k in ("started", "finished", "boot_id")}},
        "apiserver": {"excluded_verbs": sorted(EXCLUDED), "request_duration": histogram_diff(b, a, API_HIST, seconds, exclude=True), "requests": requests, "status_details": request_details(requests), "requests_including_watch_connect": counter_diff(b, a, "apiserver_request_total", seconds)},
        "storage_rpc": {"meaning": "client-observed apiserver storage RPC histogram count deltas, not etcd/Tigerstore server counters or workload request counts", "histograms": {name: histogram_diff(b, a, name, seconds) for name in STORAGE_HISTS}},
        "etcd_revision": revision_diff(before["etcd_status"], after["etcd_status"]),
        "cpu": cpu_diff(before["cpu"], after["cpu"], same_boot),
        "server_metrics": {},
        "source_issues": {"before": before["issues"] + b["issues"], "after": after["issues"] + a["issues"]},
    }
    for name in NODES:
        b, a = before["server_metrics"][name], after["server_metrics"][name]
        interval, timing_errors = elapsed(b, a, same_boot)
        if not same_boot:
            b, a = dict(b), dict(a)
            b["process_start_time_seconds"] = a["process_start_time_seconds"] = None
        result["server_metrics"][name] = {"elapsed_seconds": interval, "availability_before": b["availability"], "availability_after": a["availability"], "issues": timing_errors + b["issues"] + a["issues"], "meaning": "server counters per replica; do not sum replicated proposal counters", "counters": {metric: counter_diff(b, a, metric, interval) for metric in SERVER_COUNTERS}}
    write_json(output, result)
    print(output)


def selftest():
    """File-free synthetic tests; no Docker, Kubernetes, or network calls."""
    import unittest

    def source(text, start=100, available=True, problems=None):
        samples, errors = parse_metrics(text, metric_names())
        return {"samples": samples, "issues": errors + (problems or []), "availability": "available" if available else "unsupported", "process_start_time_seconds": start}

    def hist(verb, resource, values):
        prefix = f'verb="{verb}",resource="{resource}"'
        lines = [f'{API_HIST}_bucket{{{prefix},le="{bound}"}} {value}' for bound, value in zip(("0.1", "1", "+Inf"), values)]
        lines.append(f'{API_HIST}_count{{{prefix}}} {values[-1]}')
        return "\n".join(lines)

    class Tests(unittest.TestCase):
        def test_parser(self):
            samples, errors = parse_metrics('process_start_time_seconds 123\napiserver_request_total{verb="GET",resource="a\\\\b\\n\\\"c",code="200"} 4\n', metric_names())
            self.assertFalse(errors)
            self.assertEqual(samples[0]["value"], 123)
            self.assertEqual(samples[1]["labels"]["resource"], 'a\\b\n"c')
            self.assertTrue(parse_metrics('apiserver_request_total NaN', metric_names())[1])

        def test_new_whole_metric(self):
            result = counter_diff(source(''), source('apiserver_request_total{verb="GET",code="200"} 7'), 'apiserver_request_total', 2)
            self.assertTrue(result["complete"])
            self.assertEqual(result["delta"], 7)
            self.assertEqual(result["per_second"], 3.5)
            self.assertTrue(result["series"][0]["inferred_zero"])
            self.assertIsNone(result["series"][0]["observed_before"])

        def test_new_label(self):
            before = source('apiserver_request_total{code="200"} 5')
            after = source('apiserver_request_total{code="200"} 8\napiserver_request_total{code="500"} 2')
            result = counter_diff(before, after, 'apiserver_request_total', 1)
            self.assertEqual(result["delta"], 5)
            self.assertEqual(request_details(result)["http_4xx_5xx_delta"], 2)

        def test_identity_and_scrape_guards(self):
            after = source('apiserver_request_total 7')
            for before in (source('', start=None), source('', start=99), source('', available=False), source('', problems=[issue('parse_error', 'bad')])):
                result = counter_diff(before, after, 'apiserver_request_total', 1)
                self.assertIsNone(result["delta"])
                self.assertFalse(result["series"][0]["inferred_zero"])
            restarted = counter_diff(source('apiserver_request_total 2', start=99), after, 'apiserver_request_total', 1)
            self.assertIsNone(restarted["series"][0]["delta"])

        def test_absence_disappearance_and_reset(self):
            for before, after in (('', ''), ('apiserver_request_total 5', ''), ('apiserver_request_total 5', 'apiserver_request_total 4')):
                result = counter_diff(source(before), source(after), 'apiserver_request_total', 1)
                self.assertFalse(result["complete"])
                self.assertIsNone(result["delta"])
            self.assertEqual(counter_diff(source(''), source(''), SERVER_COUNTERS[0], 1)["exposure"], 'not_exposed')

        def test_histogram_groups_and_exclusion(self):
            before = source(hist('GET', 'pods', (0, 0, 0)))
            after = source('\n'.join((hist('GET', 'pods', (5, 10, 10)), hist('POST', 'secrets', (2, 4, 4)), hist('WATCH', 'pods', (99, 99, 99)), hist('CONNECT', 'pods', (99, 99, 99)))))
            result = histogram_diff(before, after, API_HIST, 2, exclude=True)
            self.assertEqual(result["overall"]["count_delta"], 14)
            self.assertAlmostEqual(result["overall"]["p50_seconds"], .1)
            self.assertAlmostEqual(result["by_resource_verb"]["pods"]["GET"]["p99_seconds"], .982)
            self.assertEqual(result["by_resource"]["secrets"]["count_delta"], 4)
            self.assertNotIn('WATCH', result["by_verb"])
            self.assertTrue(result["counts"]["series"][1]["inferred_zero"])

        def test_histogram_invalid_and_empty(self):
            zero = source(hist('GET', 'pods', (0, 0, 0)))
            empty = histogram_diff(zero, zero, API_HIST, 1)["overall"]
            self.assertEqual(empty["count_delta"], 0)
            self.assertIsNone(empty["p50_seconds"])
            invalid = histogram_diff(zero, source(hist('GET', 'pods', (3, 2, 4))), API_HIST, 1)["overall"]
            self.assertIsNone(invalid["p99_seconds"])
            self.assertTrue(invalid["issues"])

        def test_revision_identity_and_partial_status(self):
            def status(revision, member=2):
                return {"started": {"monotonic_ns": 1}, "finished": {"monotonic_ns": 2}, "issues": [], "endpoints": [{"Endpoint": f'https://{NODES[0]}:2379', "Status": {"header": {"revision": revision, "cluster_id": 1, "member_id": member}}}]}
            result = revision_diff(status(9007199254740993), status(9007199254740997))
            self.assertEqual(result["endpoints"][NODES[0]]["delta"], 4)
            self.assertIsNone(result["endpoints"][NODES[1]]["delta"])
            self.assertIsNone(revision_diff(status(1), status(2, member=3))["endpoints"][NODES[0]]["delta"])

        def test_new_histogram_without_identity_and_disappeared_bucket(self):
            after = source(hist('GET', 'pods', (1, 2, 2)))
            result = histogram_diff(source('', start=None), after, API_HIST, 1)
            self.assertIsNone(result["overall"]["p99_seconds"])
            inferred = histogram_diff(source(''), after, API_HIST, 1)
            self.assertTrue(all(row["inferred_zero"] for row in inferred["bucket_series"]))
            missing_bucket = source(hist('GET', 'pods', (2, 3, 3)).split('\n', 1)[1])
            self.assertIsNone(histogram_diff(after, missing_bucket, API_HIST, 1)["overall"]["p50_seconds"])

        def test_no_error_counter_is_not_zero(self):
            result = counter_diff(source('apiserver_request_total{code="200"} 1'), source('apiserver_request_total{code="200"} 2'), 'apiserver_request_total', 1)
            self.assertIsNone(request_details(result)["http_4xx_5xx_delta"])

        def test_cpu_identity_and_ticks(self):
            def cpu(ticks, when, identity):
                return {"samples": {name: {"started": {"monotonic_ns": when}, "finished": {"monotonic_ns": when}, "identity": identity, "clock_ticks_per_second": 100, "utime_ticks": ticks, "stime_ticks": ticks, "issues": []} for name in CONTAINERS}}
            before, after = cpu(10, 1_000_000_000, {'pid': 10, 'starttime_ticks': 1}), cpu(60, 3_000_000_000, {'pid': 10, 'starttime_ticks': 1})
            result = cpu_diff(before, after, True)["containers"][NODES[0]]
            self.assertEqual(result["cpu_seconds"], 1)
            self.assertEqual(result["average_cores"], .5)
            changed = cpu(60, 3_000_000_000, {'pid': 10, 'starttime_ticks': 2})
            self.assertIsNone(cpu_diff(before, changed, True)["containers"][NODES[0]]["cpu_seconds"])
            self.assertIsNone(cpu_diff(before, after, False)["containers"][NODES[0]]["cpu_seconds"])

    return 0 if unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Tests)).wasSuccessful() else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    snap = commands.add_parser("snapshot", help="capture fixed comparison topology")
    snap.add_argument("outdir")
    snap.add_argument("--store", default=os.environ.get("STORE", "unknown"), choices=("etcd", "tigerstore", "unknown"))
    compare = commands.add_parser("diff", help="summarize two snapshot directories")
    compare.add_argument("before")
    compare.add_argument("after")
    compare.add_argument("output")
    commands.add_parser("selftest", help="run synthetic tests without external commands")
    args = parser.parse_args()
    try:
        if args.command == "selftest":
            return selftest()
        if args.command == "snapshot":
            snapshot(args.outdir, args.store)
        else:
            diff(args.before, args.after, args.output)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        print(f"metrics.py: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
