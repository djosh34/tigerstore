#!/usr/bin/env python3
"""Run only on Actions. Each member gets its own cgroup, including page cache."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.error
import urllib.request


def fields(path):
    return {key: int(value) for key, value in
            (line.split() for line in path.read_text().splitlines())}


def metrics(port):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as response:
        text = response.read().decode()
    result = {}
    for line in text.splitlines():
        if line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) == 2 and "{" not in parts[0]:
            result[parts[0]] = float(parts[1])
    return result


def classify(result, progress):
    """Keep test failures distinct from measured, cgroup-confirmed limits."""
    # A kill after the last poll shows only in the final counters.
    if not result["failure"] and any(g["events"]["oom_kill"] for g in result.get("final_cgroups", [])):
        result["failure"] = "a member was OOM-killed"
    result["outcome"] = "failed" if result["failure"] else "passed"
    if not result["failure"] or result.get("censored"):
        return
    groups = result.get("final_cgroups", [])
    ack = result.get("last_acknowledged", {})
    if (len(groups) != 3 or not any(g["events"]["oom_kill"] for g in groups)
            or ack.get("objects", 0) <= 0 or ack.get("live_bytes", 0) <= 0
            or ack.get("phase") != "preload" or result["kills"]
            or "mixed_start" in result or "checker_exit" in result):
        return
    # An unrelated driver/setup/checker error must not be hidden by an OOM.
    failure = result["failure"]
    if failure != "a member was OOM-killed":
        if failure != "driver exited 1; see progress.jsonl and driver.log" or not progress:
            return
        last = progress[-1]
        if (last["phase"] != "preload-failed"
                or not last.get("failure", "").startswith("rpc error: code = Unavailable desc = ")):
            return
    if any(row["phase"] not in ("preload", "preload-failed") for row in progress):
        return
    if result["mode"] == "max":
        outcome = "capacity-limit"
    elif (result["kind"] == "etcd" and result["mode"] == "proof"
          and result["preload_bytes"] == 0
          and ack["live_bytes"] < result["memory"] * 31 // 10):
        outcome = "reference-preload-limit"
    else:
        return
    result.update(outcome=outcome, limit="cgroup-oom", termination=failure, failure=None)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", required=True)
    parser.add_argument("--driver", required=True)
    parser.add_argument("--kind", choices=["tigerstore", "etcd"], required=True)
    parser.add_argument("--mode", choices=["proof", "max", "cache"], default="proof")
    parser.add_argument("--go-memory", default="off", help="same GOMEMLIMIT for either product")
    parser.add_argument("--quota", type=int, default=8 << 30)
    parser.add_argument("--memory", type=int, default=640 << 20)
    parser.add_argument("--out", required=True)
    parser.add_argument("--seconds", type=int, default=180)
    parser.add_argument("--preload-bytes", type=int, default=0, help="nonzero is a small adapter control, not the past-RAM proof")
    parser.add_argument("--limit-seconds", type=int, default=2700)
    args = parser.parse_args()
    out = Path(args.out).resolve()
    out.mkdir(parents=True, exist_ok=False)
    nodes = []
    groups = []
    logs = []
    samples = []
    result = vars(args).copy()
    result.update(kills=0, driver_exit=None, failure=None)
    driver = None
    root = Path("/sys/fs/cgroup")
    group = root / f"past-ram-{os.getpid()}"
    initial = ",".join(f"n{i}=https://127.0.0.1:{20101 + 10*i}" for i in range(3))
    endpoints = ",".join(f"http://127.0.0.1:{20100 + 10*i}" for i in range(3))

    def launch(i):
        client = f"http://127.0.0.1:{20100 + 10*i}"
        peer = f"https://127.0.0.1:{20101 + 10*i}"
        command = [str(Path(args.server).resolve()), "--name", f"n{i}",
                   "--data-dir", str(out / f"data{i}"),
                   "--listen-client-urls", client, "--advertise-client-urls", client,
                   "--listen-peer-urls", peer, "--initial-advertise-peer-urls", peer,
                   "--initial-cluster", initial, "--initial-cluster-token", "past-ram",
                   "--peer-auto-tls", "--quota-backend-bytes", str(args.quota)]
        if args.kind == "tigerstore":
            # A positive flag overrides GOMEMLIMIT in the current server.
            # Leave it unset for the equal-GOMEMLIMIT comparison.
            if args.go_memory == "off":
                command += ["--memory-budget-bytes", str(args.memory)]
        else:
            command += ["--log-level", "warn"]
        # Membership in the cgroup precedes exec. No allocation by the server
        # escapes its limit, and restart retains the cgroup's OOM counters.
        environment = dict(os.environ, GOMEMLIMIT=args.go_memory)
        return subprocess.Popen(["sh", "-ec", 'echo $$ > "$1/cgroup.procs"; shift; exec "$@"',
                                 "past-ram", str(groups[i]), *command],
                                stdout=logs[i], stderr=subprocess.STDOUT, env=environment)

    try:
        (root / "cgroup.subtree_control").write_text("+memory +cpu +io")
        group.mkdir()
        (group / "cgroup.subtree_control").write_text("+memory +cpu +io")
        for i in range(3):
            cg = group / str(i)
            cg.mkdir()
            groups.append(cg)
            (cg / "memory.max").write_text(str(args.memory))
            (cg / "memory.swap.max").write_text("0")
            logs.append((out / f"node{i}.log").open("ab"))
            nodes.append(launch(i))
        deadline = time.monotonic() + 60
        while True:
            try:
                for i in range(3):
                    metrics(20100 + 10*i)
                break
            except (urllib.error.URLError, TimeoutError):
                if time.monotonic() > deadline or any(n.poll() is not None for n in nodes):
                    raise RuntimeError("cluster did not start")
                time.sleep(1)
        with (out / "driver.log").open("wb") as log:
            driver = subprocess.Popen([str(Path(args.driver).resolve()), "-endpoints", endpoints,
                                       "-mode", args.mode, "-bytes", str(args.preload_bytes or args.memory * 31 // 10),
                                       "-duration", f"{args.seconds}s", "-out", str(out)],
                                      stdout=log, stderr=subprocess.STDOUT)
            started = time.monotonic()
            ready = None
            restarted = False
            while driver.poll() is None:
                now = time.monotonic()
                mixed_finished = (out / "mixed-done").exists()
                sample = {"seconds": now-started, "nodes": []}
                for i, cg in enumerate(groups):
                    stat = fields(cg / "memory.stat")
                    member = {"current": int((cg / "memory.current").read_text()),
                              "peak": int((cg / "memory.peak").read_text()),
                              "events": fields(cg / "memory.events"),
                              "anon": stat["anon"], "file": stat["file"],
                              "cpu": fields(cg / "cpu.stat"), "io": (cg / "io.stat").read_text(),
                              "exit": nodes[i].poll()}
                    try:
                        member["metrics"] = metrics(20100 + 10*i)
                    except (urllib.error.URLError, TimeoutError, ConnectionError) as error:
                        member["metrics_error"] = str(error)
                    sample["nodes"].append(member)
                if (out / "last-acknowledged.json").exists():
                    sample["last_acknowledged"] = json.loads((out / "last-acknowledged.json").read_text())
                samples.append(sample)
                with (out / "samples.jsonl").open("a") as log:
                    log.write(json.dumps(sample) + "\n")
                if ready is None and (out / "ready").exists():
                    ready = now
                    result["mixed_start"] = sample
                    (out / "mixed-started").write_text("sampled\n")
                if mixed_finished and "mixed_end" not in result:
                    result["mixed_end"] = sample
                    (out / "mixed-sampled").write_text("sampled\n")
                if args.mode == "proof" and ready is not None and now-ready >= 30 and result["kills"] == 0:
                    # Kill one member, not the load generator. Other endpoints
                    # remain available. The exact selected member is recorded.
                    nodes[2].kill()
                    nodes[2].wait(timeout=30)
                    result["kills"] = 1
                    result["killed_member"] = 2
                    result["kill_seconds"] = now-started
                if result["kills"] and not restarted and now-ready >= 35:
                    nodes[2] = launch(2)
                    restarted = True
                    result["restart_seconds"] = now-started
                if any(member["events"]["oom_kill"] for member in sample["nodes"]):
                    raise RuntimeError("a member was OOM-killed")
                if now-started > args.limit_seconds:
                    result["censored"] = True
                    raise RuntimeError("runner time limit, not a store capacity result")
                time.sleep(2)
            result["driver_exit"] = driver.returncode
            if driver.returncode:
                raise RuntimeError(f"driver exited {driver.returncode}; see progress.jsonl and driver.log")
            if args.mode == "proof" and (result["kills"] != 1 or not restarted):
                raise RuntimeError("kill/restart coverage missing")
    except (OSError, RuntimeError, subprocess.TimeoutExpired) as error:
        result["failure"] = str(error)
    finally:
        if driver is not None and driver.poll() is None:
            driver.kill()
            driver.wait(timeout=30)
        for node in nodes:
            if node.poll() is None:
                node.kill()
            node.wait(timeout=30)
        if args.mode == "proof" and (out / "recorded.json").exists():
            try:
                with (out / "checker.log").open("wb") as log:
                    checked = subprocess.run([str(Path(args.driver).resolve()), "-mode", "check", "-out", str(out)],
                                             stdout=log, stderr=subprocess.STDOUT, timeout=600)
                result["checker_exit"] = checked.returncode
                if checked.returncode:
                    result["failure"] = (result["failure"] or "") + "; recorded history checks failed"
            except subprocess.TimeoutExpired as error:
                result["failure"] = (result["failure"] or "") + f"; checker process timed out: {error}"
        if (out / "last-acknowledged.json").exists():
            result["last_acknowledged"] = json.loads((out / "last-acknowledged.json").read_text())
        if samples:
            result["last_sample"] = samples[-1]
        result["final_cgroups"] = [{"events": fields(cg / "memory.events"),
                                    "peak": int((cg / "memory.peak").read_text()),
                                    "cpu": fields(cg / "cpu.stat")} for cg in groups]
        for log in logs:
            log.close()
        progress = []
        if (out / "progress.jsonl").exists():
            progress = [json.loads(line) for line in (out / "progress.jsonl").read_text().splitlines()]
        classify(result, progress)
        (out / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        summary = {"outcome": result["outcome"], "limit": result.get("limit"),
                   "termination": result.get("termination"),
                   "failure": result["failure"], "kills": result["kills"],
                   "oom_kills": [s["events"]["oom_kill"] for s in result["final_cgroups"]],
                   "peak_bytes": [s["peak"] for s in result["final_cgroups"]]}
        summary["phases"] = [row for row in progress if row["phase"] != "preload"]
        if "last_acknowledged" in result:
            summary["last_acknowledged"] = result["last_acknowledged"]
        if samples and samples[-1].get("last_acknowledged", {}).get("objects", 0):
            count = samples[-1]["last_acknowledged"]["objects"]
            summary["sampled_index_bytes_per_key"] = [
                n["metrics"]["tigerstore_index_bytes"]/count
                if "tigerstore_index_bytes" in n.get("metrics", {}) else None
                for n in samples[-1]["nodes"]]
        if "mixed_start" in result and "mixed_end" in result:
            before, after = result["mixed_start"], result["mixed_end"]
            summary["sampled_mixed_seconds"] = after["seconds"]-before["seconds"]
            summary["server_cpu_seconds"] = sum(
                b["cpu"]["usage_usec"]-a["cpu"]["usage_usec"]
                for a, b in zip(before["nodes"], after["nodes"]))/1e6
            for metric in ["tigerstore_cache_hits_total", "tigerstore_cache_misses_total"]:
                summary[metric] = sum(b.get("metrics", {}).get(metric, 0)-a.get("metrics", {}).get(metric, 0)
                                      for a, b in zip(before["nodes"], after["nodes"]))
            def read_bytes(sample):
                return sum(int(field.split("=")[1]) for n in sample["nodes"]
                           for line in n["io"].splitlines() for field in line.split()[1:]
                           if field.startswith("rbytes="))
            summary["storage_read_bytes"] = read_bytes(after)-read_bytes(before)
        (out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        # Keep the cgroups until the job exits so final memory events remain
        # inspectable. Disk data is not uploaded, only logs and measurements.
    print(json.dumps(result, indent=2))
    return 1 if result["failure"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
