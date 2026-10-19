#!/usr/bin/env python3
"""Build-only storage gates for chaos round 1. Usage: python3 scripts/storage-overlay.py OUT.

The overlay adds waits at actual storage boundaries. It changes no storage
result unless the controller explicitly releases a gate with an injected error.
The normal source and normal tigerstore binary have no gates.
"""
import json
import pathlib
import sys

root = pathlib.Path.cwd()
out = pathlib.Path(sys.argv[1]).resolve()
out.mkdir(parents=True, exist_ok=True)
replacements = {}


def patch(rel, edits):
    source = (root / rel).read_text()
    for old, new in edits:
        assert source.count(old) == 1, (rel, old, source.count(old))
        source = source.replace(old, new)
    dest = out / (rel.replace('/', '-'))
    dest.write_text(source)
    replacements[str(root / rel)] = str(dest)


patch('internal/osfs/osfs.go', [
    ('"errors"', '"errors"\n "strings"\n "github.com/djosh34/tigerstore/chaos/env"'),
    ('return os.Rename(filepath.Join(d.path, oldName), filepath.Join(d.path, newName))',
     '''if err := os.Rename(filepath.Join(d.path, oldName), filepath.Join(d.path, newName)); err != nil { return err }
     if strings.HasSuffix(newName, ".wal") { return env.StorageGate("rotation-rename") }
     if strings.HasPrefix(newName, "spare-") { return env.StorageGate("clean-rename") }
     return nil'''),
    ('func (f file) Sync() error {', '''func (f file) Sync() error {
     if strings.HasPrefix(filepath.Base(f.Name()), "ckpt-") {
       if err := env.StorageGate("checkpoint-sync"); err != nil { return err }
     }'''),
])
patch('internal/node/node.go', [
    ('"github.com/djosh34/tigerstore/internal/core"',
     '"github.com/djosh34/tigerstore/internal/core"\n "github.com/djosh34/tigerstore/chaos/env"'),
    ('w, rec, err := wal.Open(n.log.fs)\n\tif err != nil {\n\t\treturn err\n\t}',
     '''w, rec, err := wal.Open(n.log.fs)
     if err != nil { return err }
     if err := env.StorageGate("recovery-open"); err != nil { return errors.Join(err, w.Close()) }'''),
    ('err := core.ServeSnapshot(n.log, ckpts, first, last, w)',
     'err := core.ServeSnapshot(n.log, ckpts, first, last, env.StorageWriter{Writer: w})'),
    ('func (n *Node) staged(stage string) error {',
     '''func (n *Node) staged(stage string) error {
     if err := env.StorageGate("install-" + stage); err != nil { return err }'''),
])
patch('internal/node/clean.go', [
    ('"github.com/djosh34/tigerstore/internal/core"',
     '"github.com/djosh34/tigerstore/internal/core"\n "github.com/djosh34/tigerstore/chaos/env"'),
    ('c.n.cleaningStage("pack-synced")',
     '''c.n.cleaningStage("pack-synced")
     if err := env.StorageGate("clean-pack-synced"); err != nil { return false, err }'''),
])
(out / 'overlay.json').write_text(json.dumps({'Replace': replacements}))
print(out / 'overlay.json')
