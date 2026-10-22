from pathlib import Path
import sys

p = Path('internal/core/snapshot.go')
s = p.read_text()
anchor = '\tvar failed []uint64\n'
if sys.argv[1] == 'on':
    s = s.replace(anchor, '\tfor id := range n.snapWait {\n\t\tif n.state.members[id] == nil {\n\t\t\tdelete(n.snapWait, id)\n\t\t}\n\t}\n' + anchor, 1)
p.write_text(s)
