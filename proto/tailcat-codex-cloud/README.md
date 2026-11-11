# Throwaway prototype for #630: tailcat between real Codex Cloud VMs

**Can two real Codex Cloud VMs reach each other over tailcat, and at what
throughput/latency, direct or DERP?**

Answer, measured on real VMs on 2026-11-11: only relayed. The VMs have no
UDP out and no traffic between each other, so tailcat never gets a direct
path. Only a tailcat built with `ts_debug_websockets` connects, and then
only via Tailscale's shared DERP relay: about 81 ms RTT (nyc) and
3 to 7 Mbit/s. Results and evidence are in the #630 comment. Never merge.

## Files

* `cc.mjs`: host driver. Starts new-Codex-Cloud threads in the tigerstore
  environment (`thread/start`, the #476 way), runs commands with
  `command/exec`, copies small files in, passes one VM's tailcat address to
  another (`pair`) without printing it, and archives the threads. State
  (thread and environment ids) goes to `/tmp/tc630/threads.json`, outside
  the repo. No model turn, no tokens.
* `vm-build.sh`: in the VM, builds tailcat `e00cdde` three ways: stock
  (`build-tags.txt`), `+useproxy` (CONNECT through `HTTPS_PROXY`), and
  `+useproxy,ts_debug_websockets` (DERP over WebSocket when
  `TS_DEBUG_DERP_WS_CLIENT=1`).
* `vm-serve.sh <stock|proxy|ws> [region]`: in the server VM, starts the
  tcbench server and `tailcat serve 7000,perf`. The address goes to
  `/tmp/tc/addr` in the VM only.
* `vm-tput.sh bytes:streams...`: in the client VM, `tailcat forward` plus
  capped transfers.
* `udpprobe/`: STUN binding requests, UDP echo and send, raw TCP dials.
* `tcbench/`: capped TCP transfer and 64-byte echo RTT client and server.
* `pingstats.py`: summarises `tailcat ping` pong lines.

## Run

```sh
node cc.mjs start 3
for i in 0 1 2; do node cc.mjs put $i vm-build.sh /tmp/tc/vm-build.sh; done
# ... put the other files the same way, then:
node cc.mjs exec 0 'setsid nohup bash /tmp/tc/vm-build.sh >/dev/null 2>&1 </dev/null &'
node cc.mjs exec 1 'bash /tmp/tc/vm-serve.sh ws nyc'
node cc.mjs pair 1 2 'TS_DEBUG_DERP_WS_CLIENT=1 /tmp/tc/bin/tailcat-ws ping --until-direct --timeout=30s "$TC_ADDR"'
node cc.mjs pair 1 2 'bash /tmp/tc/vm-tput.sh 32000000:1 32000000:4 32000000:8'
node cc.mjs archive
```
