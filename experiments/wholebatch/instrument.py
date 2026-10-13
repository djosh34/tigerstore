#!/usr/bin/env python3
"""Inject counting-only observability into a disposable git archive, not checkout."""
from pathlib import Path
import sys


def instrument(source):
    templates = Path(__file__).resolve().parent
    main = source / 'cmd/tigerstore/main.go'
    anchor = '\tt, err := peer.Start(peer.Config{'
    text = main.read_text()
    if text.count(anchor) != 1 or 'pre50Observe(' in text:
        raise RuntimeError('observability insertion point changed or already instrumented')
    files = [('observe.go.txt', 'cmd/tigerstore/pre50.go'),
             ('osfs.go.txt', 'internal/osfs/pre50.go')]
    for _, target in files:
        if (source / target).exists():
            raise RuntimeError(f'pre-existing instrumentation: {target}')
    main.write_text(text.replace(anchor, '\tif err := pre50Observe(n); err != nil {\n\t\treturn err\n\t}\n' + anchor))
    for template, target in files:
        (source / target).write_text((templates / template).read_text())


if __name__ == '__main__':
    instrument(Path(sys.argv[1]).resolve())
