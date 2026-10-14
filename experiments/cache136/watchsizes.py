#!/usr/bin/env python3
"""Reads etcdctl JSON (get or watch) on stdin, writes keys, revisions and sizes only.
Values never leave this process."""
import base64, json, sys, time
dec = json.JSONDecoder()
buf = ""
out = sys.stdout
def kvinfo(kv):
    if not kv:
        return None
    return [base64.b64decode(kv["key"]).decode("utf-8", "replace"), kv.get("mod_revision", 0), kv.get("create_revision", 0),
            kv.get("version", 0), len(base64.b64decode(kv.get("value", "")))]
for line in sys.stdin:
    now = time.time()
    buf += line
    while True:
        s = buf.lstrip()
        if not s:
            buf = ""
            break
        try:
            obj, end = dec.raw_decode(s)
        except ValueError:
            buf = s
            break
        buf = s[end:]
        if "kvs" in obj:  # etcdctl get
            out.write(json.dumps({"snap_rev": obj["header"]["revision"], "t": now}) + "\n")
            for kv in obj.get("kvs", []):
                out.write(json.dumps({"s": kvinfo(kv)}) + "\n")
        for e in obj.get("Events") or []:
            out.write(json.dumps({"t": round(now, 4), "d": e.get("type", 0), "kv": kvinfo(e.get("kv")), "p": kvinfo(e.get("prev_kv"))}) + "\n")
    out.flush()
