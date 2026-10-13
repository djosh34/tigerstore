#!/usr/bin/env python3
"""Install the same experiment-only observation hook in each source archive."""
from pathlib import Path
import shutil
import sys

root = Path(sys.argv[1])
file = root / 'cmd/tigerstore/main.go'
text = file.read_text()
needle = '\tt, err := peer.Start(peer.Config{'
assert text.count(needle) == 1, file
text = text.replace(needle, '\tif err := bench75Observe(n); err != nil {\n\t\treturn err\n\t}\n' + needle)
file.write_text(text)
shutil.copyfile(Path(__file__).with_name('observe.go.txt'), root / 'cmd/tigerstore/bench75.go')
