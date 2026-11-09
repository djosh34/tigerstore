"""Parse `go tool pprof -traces` output into (value, frames leaf-first)."""
import re, sys
from collections import Counter

UNITS = {"ns": 1e-6, "us": 1e-3, "ms": 1.0, "s": 1e3, "B": 1, "kB": 1e3, "KB": 1024, "MB": 1024**2, "GB": 1024**3}

def parse_value(tok):
    m = re.match(r"^(-?[0-9.]+)([a-zA-Z]*)$", tok)
    if not m:
        raise ValueError(tok)
    v, u = float(m.group(1)), m.group(2)
    if u == "":
        return v
    return v * UNITS[u]

def parse(path):
    samples, cur, val = [], None, None
    for line in open(path):
        if line.startswith("-----------+"):
            if cur is not None:
                samples.append((val, cur))
            cur, val = [], None
            continue
        if cur is None:
            continue
        if not line.strip():
            continue
        m = re.match(r"^\s*(-?[0-9.]+[a-zA-Z]*)\s{2,}(\S.*)$", line)
        if m and val is None:
            val = parse_value(m.group(1))
            cur.append(m.group(2).replace(" (inline)", "").strip())
        elif val is not None:
            s = line.strip()
            if s.startswith("bytes:") or ":" in s.split()[0] and not s.split()[0].startswith(("github", "golang", "google", "go.", "crypto", "internal", "runtime", "sync", "net", "bufio", "main", "hash", "syscall", "os", "io", "encoding", "math", "sort", "slices", "strings", "bytes", "time", "context", "reflect", "unicode", "compress", "fmt", "strconv", "errors", "cmp", "maps", "iter", "unique", "weak", "log", "container", "path", "regexp", "expvar", "mime", "html", "text", "vendor", "type:", "gogo", "cmpbody", "indexbytebody", "memeqbody", "aeshashbody")):
                continue
            cur.append(s.replace(" (inline)", ""))
    if cur:
        samples.append((val, cur))
    return samples

if __name__ == "__main__":
    s = parse(sys.argv[1])
    tot = sum(v for v, _ in s)
    roots = Counter()
    for v, f in s:
        roots[" <- ".join(reversed(f[-3:]))] += v
    print("total", tot)
    for k, v in roots.most_common(40):
        print(f"{100*v/tot:6.2f}%  {k}")
