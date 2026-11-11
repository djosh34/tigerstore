# THROWAWAY PROTOTYPE (#630). TCP echo server and round-trip timers.
#   echo.py server <port>
#   echo.py rtt <port> <count> <size>     one connection, <count> round trips
#   echo.py stab <port> <secs>            20 round trips/s for <secs>, reconnecting on error
import json, socket, socketserver, statistics, sys, time


class H(socketserver.BaseRequestHandler):
    def handle(self):
        self.request.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        while True:
            b = self.request.recv(65536)
            if not b:
                return
            self.request.sendall(b)


def conn(port):
    c = socket.create_connection(("127.0.0.1", port), timeout=5)
    c.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    return c


def rt(c, msg):
    t = time.perf_counter()
    c.sendall(msg)
    got = 0
    while got < len(msg):
        b = c.recv(65536)
        if not b:
            raise EOFError
        got += len(b)
    return (time.perf_counter() - t) * 1000


def pct(xs, p):
    xs = sorted(xs)
    return round(xs[min(len(xs) - 1, int(p / 100 * len(xs)))], 3)


def summary(xs):
    return {"n": len(xs), "min": round(min(xs), 3), "p50": pct(xs, 50), "p90": pct(xs, 90),
            "p99": pct(xs, 99), "p999": pct(xs, 99.9), "max": round(max(xs), 3),
            "mean": round(statistics.mean(xs), 3)}


mode = sys.argv[1]
if mode == "server":
    socketserver.ThreadingTCPServer.allow_reuse_address = True
    socketserver.ThreadingTCPServer(("127.0.0.1", int(sys.argv[2])), H).serve_forever()
elif mode == "rtt":
    port, n, size = map(int, sys.argv[2:5])
    msg = b"x" * size
    c = conn(port)
    for _ in range(100):
        rt(c, msg)
    xs = [rt(c, msg) for _ in range(n)]
    print(json.dumps({"size": size, **summary(xs)}))
elif mode == "stab":
    port, secs = int(sys.argv[2]), float(sys.argv[3])
    msg = b"x" * 64
    xs, errors, buckets, c = [], [], {}, None
    t0 = time.monotonic()
    while time.monotonic() - t0 < secs:
        tick = time.monotonic()
        try:
            if c is None:
                c = conn(port)
                c.settimeout(2)
            ms = rt(c, msg)
            xs.append(ms)
            b = int((tick - t0) // 10) * 10
            buckets[b] = max(buckets.get(b, 0), round(ms, 2))
        except Exception as e:
            errors.append({"t": round(tick - t0, 1), "err": repr(e)[:80]})
            try:
                c and c.close()
            except Exception:
                pass
            c = None
        time.sleep(max(0, 0.05 - (time.monotonic() - tick)))
    out = summary(xs) if xs else {"n": 0}
    out.update({"secs": secs, "errors": len(errors), "error_list": errors[:20],
                "over_10ms": sum(x > 10 for x in xs), "over_100ms": sum(x > 100 for x in xs),
                "max_per_10s": buckets})
    print(json.dumps(out))
