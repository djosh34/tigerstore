import collections, sys
exec(open("replay.py").read().split("# ---- policies")[0].replace('print(', '(lambda *a, **k: None)('))
g = collections.Counter(); gb = collections.Counter(); keys = collections.Counter()
for o in ops:
    if o[0] == "r" and o[5] in ("get", "txn_failure"):
        g[(o[5], res(o[3]))] += 1; gb[(o[5], res(o[3]))] += o[2]; keys[o[3]] += 1
for k, v in g.most_common(14): print(k, v, round(gb[k] / 1024))
print("top keys", keys.most_common(8))
# get followed by write of same version within 1 s (read-modify-write by clients)
lastget = {}
rmw = 0; prevs = 0
for o in ops:
    if o[0] == "r" and o[5] == "get": lastget[o[1]] = o[4]
    if o[0] == "r" and o[5] == "prev_kv":
        prevs += 1
        if o[1] in lastget and o[4] - lastget[o[1]] < 2: rmw += 1
print("prev_kv reads", prevs, "preceded by a point Get of the same version within 2 s", rmw)
# point reads per second by phase
