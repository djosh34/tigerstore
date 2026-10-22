from pathlib import Path
import sys

p = Path('internal/core/snapshot.go')
s = p.read_text()
anchor = '\tvar failed []uint64\n'
if sys.argv[1] == 'on':
    s = s.replace(anchor, '\tfor id := range n.snapWait {\n\t\tif n.state.members[id] == nil {\n\t\t\tdelete(n.snapWait, id)\n\t\t}\n\t}\n' + anchor, 1)
p.write_text(s)
# Capture the pin state only after the persist goroutine reports a disk
# failure. No live logging or synchronization is added.
p = Path('internal/core/node.go')
p.write_text(p.read_text() + '\nfunc (n *Node) Diag379() string { if n.cfg.ID != 1 || !n.state.removed[4] { return "" }; return fmt.Sprintf("node=%d tick=%d snapWait=%v removed=%v", n.cfg.ID, n.now, n.snapWait, n.state.removed) }\n')
p = Path('internal/node/node.go')
s = p.read_text()
old = '\t\t\t\treturn done.err\n'
new = '\t\t\t\tif diag := n.core.Diag379(); diag != "" {\n\t\t\t\t\tn.log.mu.Lock()\n\t\t\t\t\tdiag = fmt.Sprintf("%s serving=%d ckpts=%d removal=%v", diag, n.log.serving, len(n.log.ckpts), n.log.removal)\n\t\t\t\t\tn.log.mu.Unlock()\n\t\t\t\t\treturn fmt.Errorf("%w; DIAG379 %s", done.err, diag)\n\t\t\t\t}\n' + old
assert s.count(old) == 1
p.write_text(s.replace(old, new))
