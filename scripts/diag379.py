from pathlib import Path
import sys

p = Path('internal/core/snapshot.go')
s = p.read_text()
anchor = '\tvar failed []uint64\n'
if sys.argv[1] == 'on':
    s = s.replace(anchor, '\tfor id := range n.snapWait {\n\t\tif n.state.members[id] == nil {\n\t\t\tdelete(n.snapWait, id)\n\t\t}\n\t}\n' + anchor, 1)
p.write_text(s)
p = Path('internal/core/checkpoint.go')
s = p.read_text().replace('\tif len(n.snapWait) > 0 {', '\tif len(n.snapWait) > 0 {\n\t\tfmt.Printf("DIAG379 checkpoint node=%d tick=%d index=%d snapWait=%v removed=%v\\n", n.cfg.ID, n.now, index, n.snapWait, n.state.removed)')
p.write_text(s)
p = Path('internal/node/node.go')
s = p.read_text().replace('\tn.log.serving++\n', '\tn.log.serving++\n\tfmt.Printf("DIAG379 serve begin node=%d to=%d serving=%d at=%s\\n", n.ID(), id, n.log.serving, time.Now())\n').replace('\tn.log.serving--\n', '\tn.log.serving--\n\tfmt.Printf("DIAG379 serve end node=%d to=%d serving=%d at=%s err=%v\\n", n.ID(), id, n.log.serving, time.Now(), err)\n')
p.write_text(s)
