#!/usr/bin/env python3
"""Fresh stock etcd process. The adapter uses only the printed API endpoint."""
import os
import socket

listeners = []
for _ in range(2):
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    listeners.append(s)
client, peer = [f"http://127.0.0.1:{s.getsockname()[1]}" for s in listeners]
for s in listeners:
    s.close()
args = [os.environ["STOCK_ETCD"],
        "--name=storage-test", "--data-dir=" + os.environ["STORAGE_TEST_DATA_DIR"],
        "--listen-client-urls=" + client, "--advertise-client-urls=" + client,
        "--listen-peer-urls=" + peer, "--initial-advertise-peer-urls=" + peer,
        "--initial-cluster=storage-test=" + peer,
        "--log-level=error"]
interval = os.environ["STORAGE_TEST_PROGRESS_INTERVAL"]
if interval != "0s":
    args.append("--watch-progress-notify-interval=" + interval)
print(client, flush=True)
os.dup2(2, 1)
os.execv(args[0], args)
