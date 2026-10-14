import collections, sys
src = open("replay.py").read()
exec(src.split("budgets = [")[0].replace('print(', '(lambda *a, **k: None)('))
def run_combo(W, C):
    lru = collections.OrderedDict(); used = 0; out = []; dead = set(); q = collections.deque()
    def ins(v, sz):
        nonlocal used
        if v in lru: lru.move_to_end(v); return
        lru[v] = sz; used += sz
        while used > C and lru: _, s = lru.popitem(last=False); used -= s
    touched = set()
    for o in ops:
        k, v, sz, Lop = o[0], o[1], o[2], o[6]
        while q and Lop - q[0][0] > W:
            p, vv, s = q.popleft()
            if vv not in dead: ins(vv, s)
        if k == "w": q.append((pos[v][0], v, sz))
        elif k == "x":
            dead.add(v)
            if v in lru: used -= lru.pop(v)
        else:
            p = pos.get(v)
            if p is not None and Lop - p[0] <= W: out.append(True)
            elif v in lru: lru.move_to_end(v); out.append(True)
            else:
                out.append(False)
                if o[5] != "prev_kv": ins(v, sz)
    return out
MiB = 1 << 20
for b in (1, 2, 4, 8, 16):
    B = b * MiB
    print(f"{b:3} MiB  window B/2 + live-version LRU B/2: {summarize(run_combo(B/2, B/2))}   window B/4 + LRU 3B/4: {summarize(run_combo(B/4, 3*B/4))}")
