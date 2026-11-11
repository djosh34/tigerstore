# Throwaway prototype for #630: tailcat between Codex Cloud tasks

**Can two Codex Cloud tasks reach each other over tailcat, and at what
throughput/latency, direct or DERP?**

Not delete-on-merge code. Never merge.

## What happened

1. **Blocked on Codex itself.** This account has no Codex Cloud environment
   at all: `GET /wham/environments` and `.../by-repo/github/djosh34/tigerstore`
   (the calls `codex cloud` makes) both return `[]`, and
   `codex cloud exec --env default|universal ...` (codex-cli 0.166.0) says
   `Error: no cloud environments are available for this workspace`.
   Environments can only be created in the web UI or desktop app, so no
   Codex task ran and there are no Codex numbers.
2. **Emulated instead** (`emulate.sh`): two network namespaces with the
   egress Codex documents (no route out, only an HTTP CONNECT proxy on
   `:8088`, no UDP, no route between them), on one 8-core arm64 host.
   * Stock tailcat (`build-tags.txt` release build, commit e00cdde): fetches
     the DERP map through the proxy but dials DERP directly
     (`dial tcp6 ...:443: connect: network is unreachable`). Its address is
     dead. Release builds omit `tailscale.com/feature/useproxy`.
   * tailcat + `import _ "tailscale.com/feature/useproxy"` (`build.sh`): DERP
     works through CONNECT. Path stays `DERP(fra)` (`ping --until-direct`
     fails after 20 s). `tailcat perf` refuses the shared relay by design.
     | measure | result |
     |---|---|
     | 1 stream, 32 MB | 22.1 Mbit/s |
     | 4 streams x 8 MB | 22.1 Mbit/s total |
     | RTT 64 B, 10 s | p50 18.9, p90 19.6, p99 148, max 149 ms |
     | RTT 64 B every 100 ms, 180 s | n=1791, p50 19.0, p90 19.7, p99 30.9, max 52 ms, 0 errors, path DERP throughout |

## Files

* `build.sh`: builds stock and proxy-patched tailcat plus `connectproxy`.
* `emulate.sh up|down|run <ns> <cmd>`: the two namespaces (needs sudo).
* `probe.py`: echo/sink servers, RTT and stream clients.
* `codex-task.sh`: what each Codex task would run once an environment exists.
