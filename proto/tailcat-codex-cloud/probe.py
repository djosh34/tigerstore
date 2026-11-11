"""Crude TCP microbench for #630, stdlib only.

  probe.py echo <port>                    echo server (RTT)
  probe.py sink <port>                    discard server, reports bytes/s
  probe.py rtt <host:port> <secs> <ms>    one held connection, a 64-byte
                                          round trip every <ms>; prints
                                          p50/p90/p99/max and every gap or
                                          spike (>3x p50) as it happens
  probe.py send <host:port> <MB> [n]      n parallel streams of <MB> each
"""
import socket, statistics, sys, threading, time


def serve(port, fn):
    s = socket.create_server(("127.0.0.1", port), reuse_port=True)
    while True:
        c, _ = s.accept()
        threading.Thread(target=fn, args=(c,), daemon=True).start()


def echo(c):
    with c:
        while d := c.recv(65536):
            c.sendall(d)


def sink(c):
    n, t = 0, time.monotonic()
    with c:
        while d := c.recv(1 << 20):
            n += len(d)
    dt = time.monotonic() - t
    print(f"sink: {n/1e6:.1f} MB in {dt:.2f}s = {n*8/dt/1e6:.1f} Mbit/s", flush=True)


def dial(hp):
    h, p = hp.rsplit(":", 1)
    c = socket.create_connection((h, int(p)))
    c.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    return c


def rtt(hp, secs, ms):
    c = dial(hp)
    msg = b"x" * 64
    rtts, end = [], time.monotonic() + secs
    while time.monotonic() < end:
        t = time.perf_counter()
        c.sendall(msg)
        got = 0
        while got < len(msg):
            got += len(c.recv(64 - got))
        r = (time.perf_counter() - t) * 1000
        rtts.append(r)
        if len(rtts) > 20 and r > 3 * statistics.median(rtts):
            print(f"spike at {time.strftime('%H:%M:%S')}: {r:.1f} ms", flush=True)
        time.sleep(max(0, ms / 1000 - r / 1000))
    q = statistics.quantiles(rtts, n=100)
    print(f"rtt n={len(rtts)} min={min(rtts):.2f} p50={q[49]:.2f} p90={q[89]:.2f} "
          f"p99={q[98]:.2f} max={max(rtts):.2f} ms, errors=0", flush=True)


def send(hp, mb, n):
    buf = b"\0" * (1 << 20)
    def one():
        c = dial(hp)
        for _ in range(mb):
            c.sendall(buf)
        c.shutdown(socket.SHUT_WR)
        c.recv(1)
        c.close()
    t = time.monotonic()
    ts = [threading.Thread(target=one) for _ in range(n)]
    [x.start() for x in ts]
    [x.join() for x in ts]
    dt = time.monotonic() - t
    print(f"send: {n}x{mb} MB in {dt:.2f}s = {n*mb*8.388608/dt:.1f} Mbit/s", flush=True)


if __name__ == "__main__":
    m, a = sys.argv[1], sys.argv[2:]
    if m == "echo":
        serve(int(a[0]), echo)
    elif m == "sink":
        serve(int(a[0]), sink)
    elif m == "rtt":
        rtt(a[0], float(a[1]), float(a[2]))
    elif m == "send":
        send(a[0], int(a[1]), int(a[2]) if len(a) > 2 else 1)
