#!/usr/bin/env python3
"""Actions-only replay of robustness.sh's pause check against real D state."""
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile
import threading
import time

if os.environ.get("GITHUB_ACTIONS") != "true":
    sys.exit("GitHub runner required")


def run(*args):
    return subprocess.run(args, check=True, text=True, capture_output=True).stdout


def state(pid):
    return run("ps", "-p", str(pid), "-o", "stat=").strip()


out = Path("results/diag-303")
out.mkdir(parents=True, exist_ok=True)
source = Path("scripts/jepsen/robustness.sh").read_text()
# Run the production fragment unchanged, without building a database or traffic.
fragment = source.split('kill -STOP "${pids[1]}"\n', 1)[1].split('\nsleep 3\n', 1)[0]
fragment = 'kill -STOP "${pids[1]}"\n' + fragment
print("Pause fragment:\n" + fragment, flush=True)

with tempfile.TemporaryDirectory(dir=os.environ["RUNNER_TEMP"]) as tmp:
    image = Path(tmp) / "disk.img"
    mount = Path(tmp) / "disk"
    mount.mkdir()
    run("truncate", "-s", "32M", str(image))
    run("mkfs.ext4", "-q", "-F", str(image))
    run("sudo", "mount", "-o", "loop", str(image), str(mount))
    frozen = threading.Event()
    child = None
    thawer = None
    thaw_errors = []
    try:
        run("sudo", "chmod", "777", str(mount))
        run("sudo", "fsfreeze", "-f", str(mount))
        frozen.set()
        # Opening a file for writing on frozen ext4 blocks in the kernel.
        child = subprocess.Popen(["python3", "-c",
            "import sys,time; f=open(sys.argv[1], 'wb'); f.write(b'x'); "
            "f.flush(); time.sleep(30)", str(mount / "write")])
        for _ in range(50):
            if state(child.pid).startswith("D"):
                break
            time.sleep(0.1)
        else:
            raise RuntimeError("writer never entered D state")
        print(f"Before SIGSTOP: pid={child.pid} stat={state(child.pid)}", flush=True)

        def thaw():
            try:
                time.sleep(0.25)
                status = Path(f"/proc/{child.pid}/status").read_text()
                print("While frozen after SIGSTOP:\n" + "\n".join(
                    line for line in status.splitlines()
                    if re.match(r"^(State|SigPnd|ShdPnd):", line)), flush=True)
                time.sleep(0.5)
                run("sudo", "fsfreeze", "-u", str(mount))
                frozen.clear()
                print("Filesystem thawed", flush=True)
            except Exception as error:
                thaw_errors.append(error)

        thawer = threading.Thread(target=thaw)
        thawer.start()
        result = subprocess.run(["bash", "-euo", "pipefail", "-c",
            f'pids[1]={child.pid}\nRESULTS={out.resolve()}\n' + fragment], timeout=8)
        thawer.join()
        if thaw_errors:
            raise thaw_errors[0]
        for _ in range(50):
            if state(child.pid).startswith("T"):
                break
            time.sleep(0.1)
        else:
            raise RuntimeError("writer never reached T after thaw")
        print(f"After thaw: pid={child.pid} stat={state(child.pid)}; pause check exit={result.returncode}", flush=True)
    finally:
        if thawer:
            thawer.join()
        if frozen.is_set():
            run("sudo", "fsfreeze", "-u", str(mount))
        if child:
            child.send_signal(signal.SIGCONT)
            child.terminate()
            child.wait(timeout=5)
        run("sudo", "umount", str(mount))

sys.exit(result.returncode)
