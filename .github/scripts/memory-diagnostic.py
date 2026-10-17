"""Actions-only sampler for #226; does not change the soak workload or limits."""
import concurrent.futures
import datetime
import json
import pathlib
import ssl
import subprocess
import sys
import threading
import time
import urllib.request

out = pathlib.Path(sys.argv[1])
out.mkdir(parents=True, exist_ok=True)
soak_out = pathlib.Path(sys.argv[2])
stop = threading.Event()


def sample(name):
    context = None
    slot = 0
    peak = 0
    next_profile = 0
    with (out / (name + "-samples.jsonl")).open("w", buffering=1) as log:
        while not stop.is_set():
            row = {"time": datetime.datetime.now(datetime.timezone.utc).isoformat()}
            try:
                group = next(pathlib.Path("/sys/fs/cgroup/chaos").glob("soak-*-" + name), None)
                if group is None:
                    raise FileNotFoundError("node cgroup not created yet")
                for field in ("memory.current", "memory.max", "memory.stat", "memory.events"):
                    row[field] = (group / field).read_text()
                pids = (group / "cgroup.procs").read_text().split()
                if not pids:
                    raise ProcessLookupError("no process in node cgroup")
                pid = pids[0]
                row["pid"] = pid
                proc = pathlib.Path("/proc") / pid
                row["status"] = (proc / "status").read_text()
                row["smaps_rollup"] = (proc / "smaps_rollup").read_text()
                args = (proc / "cmdline").read_bytes().decode().split("\0")
                endpoint = args[args.index("--listen-client-urls") + 1]
                if context is None:
                    certs = soak_out / "certs"
                    context = ssl.create_default_context(cafile=str(certs / "ca.pem"))
                    context.load_cert_chain(str(certs / "client.pem"), str(certs / "client-key.pem"))
                with urllib.request.urlopen(endpoint + "/metrics", context=context, timeout=2) as response:
                    text = response.read().decode()
                row["metrics"] = {
                    parts[0]: parts[1]
                    for line in text.splitlines()
                    if line.startswith(("go_memstats_", "go_gc_", "process_resident_", "tigerstore_"))
                    and len(parts := line.split()) == 2
                }
                if time.monotonic() >= next_profile:
                    next_profile = time.monotonic() + 5
                    with urllib.request.urlopen(endpoint + "/debug/pprof/heap", context=context, timeout=2) as response:
                        profile = response.read()
                    stem = out / f"{name}-slot-{slot:02d}"
                    pathlib.Path(str(stem) + ".pb.gz").write_bytes(profile)
                    pathlib.Path(str(stem) + ".json").write_text(json.dumps(row))
                    slot = (slot + 1) % 12
                    heap = float(row["metrics"].get("go_memstats_heap_inuse_bytes", "0"))
                    if heap > peak:
                        peak = heap
                        (out / (name + "-peak.pb.gz")).write_bytes(profile)
                        (out / (name + "-peak.json")).write_text(json.dumps(row))
            except (OSError, ValueError) as error:
                # Faults intentionally make nodes unavailable. Keep the
                # cgroup sample and the error rather than dropping either.
                row["error"] = str(error)
            log.write(json.dumps(row) + "\n")
            stop.wait(1)


with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
    futures = [pool.submit(sample, "node" + str(i)) for i in range(1, 4)]
    try:
        result = subprocess.run(sys.argv[3:], check=False)
    finally:
        stop.set()
    for future in futures:
        future.result()
sys.exit(result.returncode)
