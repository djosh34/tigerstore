from pathlib import Path
import sys

p = Path('internal/core/snapshot.go')
s = p.read_text()
anchor = '\tvar failed []uint64\n'
if sys.argv[1] == 'on':
    s = s.replace(anchor, '\tfor id := range n.snapWait {\n\t\tif n.state.members[id] == nil {\n\t\t\tdelete(n.snapWait, id)\n\t\t}\n\t}\n' + anchor, 1)
p.write_text(s)
# Add evidence only when a disk failure is already stopping a node after
# member 4's removal. No live logging or synchronization is added.
p = Path('internal/core/node.go')
s = p.read_text().replace('\t\tn.err = err\n', '\t\tif n.state.removed[4] && errors.Is(err, ErrDisk) {\n\t\t\terr = fmt.Errorf("%w; DIAG379 node=%d tick=%d snapWait=%v removed=%v", err, n.cfg.ID, n.now, n.snapWait, n.state.removed)\n\t\t}\n\t\tn.err = err\n', 1)
p.write_text(s)
