#!/usr/bin/env bash
# diag only (#300): adds the short-read dump to the product tree in $1.
set -euo pipefail
src=$1
here=$(cd "$(dirname "$0")" && pwd)
cp "$here/diag300.go.txt" "$src/internal/wal/diag300.go"
python3 - "$src" <<'PY'
import sys
from pathlib import Path
src = Path(sys.argv[1])
p = src / 'internal/node/node.go'
s = p.read_text()
old = '\treturn wal.ReadValue(r, at, name, index, off, n)\n}'
assert s.count(old) == 1, 'node.go'
s = s.replace(old, '\tb, err := wal.ReadValue(r, at, name, index, off, n)\n\tif err != nil {\n\t\tl.w.Diag300(r, at, int(n))\n\t}\n\treturn b, err\n}')
p.write_text(s)
p = src / 'internal/wal/wal.go'
s = p.read_text()
old = '\treturn ReadValue(r, at, name, index, off, n)\n}'
assert s.count(old) == 1, 'wal.go'
s = s.replace(old, '\tb, err := ReadValue(r, at, name, index, off, n)\n\tif err != nil {\n\t\tw.Diag300(r, at, int(n))\n\t}\n\treturn b, err\n}')
p.write_text(s)
PY
