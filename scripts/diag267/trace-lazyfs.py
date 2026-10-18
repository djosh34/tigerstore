#!/usr/bin/env python3
"""Log path-cache disagreement with the actual backing inode, without repairing it."""
from pathlib import Path
import sys
p = Path(sys.argv[1]) / 'lazyfs/src/lazyfs.cpp'
s = p.read_text()
old = '''    string inode = this_ ()->FSCache->get_original_inode (content_owner);

    if (inode.empty ()) {'''
new = '''    string inode = this_ ()->FSCache->get_original_inode (content_owner);
    if (!inode.empty () && inode != to_string (stbuf->st_ino)) {
        spdlog::warn ("[diag267] alias path={} cached_inode={} backing_inode={}",
                      path, inode, stbuf->st_ino);
    }

    if (inode.empty ()) {'''
assert s.count(old) == 1
s = s.replace(old, new)
p.write_text(s)
