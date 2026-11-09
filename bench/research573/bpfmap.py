"""Generic parser for bpftrace map output with stack keys.

Returns list of (keys: list[str], frames: list[str] leaf-first, value: int)."""
import re

def parse(path, name):
    out = []
    cur = None
    for raw in open(path, errors="replace"):
        line = raw.rstrip("\n")
        if cur is None:
            if not line.startswith(f"@{name}["):
                continue
            body = line[len(name) + 2:]
            m = re.match(r"^(.*)\]: (-?\d+)$", body)
            if m:  # single-line entry (no stack, or empty stack)
                keys = [k.strip() for k in m.group(1).split(", ") if k.strip()]
                out.append((keys, [], int(m.group(2))))
                continue
            keys = [k.strip() for k in body.split(", ") if k.strip()]
            cur = (keys, [])
            continue
        m = re.match(r"^\]: (-?\d+)$", line)
        if m:
            out.append((cur[0], cur[1], int(m.group(1))))
            cur = None
            continue
        m = re.match(r"^\s*(.*?)\]: (-?\d+)$", line)
        if m and not line.startswith(" " * 4):
            # trailing key after a stack, e.g. "], comm]: N" is not produced; keep simple
            pass
        fr = re.sub(r"\+\d+$", "", line.strip())
        fr = re.sub(r"\.abi0$", "", fr)
        if fr:
            cur[1].append(fr)
    return out
