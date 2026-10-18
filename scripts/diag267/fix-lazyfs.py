#!/usr/bin/env python3
"""Diagnostic patch only: serialize LazyFS namespace and path-cache updates."""
from pathlib import Path
import sys

p = Path(sys.argv[1]) / 'lazyfs/src/lazyfs.cpp'
s = p.read_text()
s = s.replace('std::shared_mutex cache_command_lock;', '''std::shared_mutex cache_command_lock;
// getattr's lstat + path-to-inode lookup/insert and rename's cache + backing
// namespace update must be one transaction. create calls getattr recursively.
std::recursive_mutex diag_namespace_lock;''', 1)
for name in ['lfs_getattr', 'lfs_rename', 'lfs_create']:
    start = s.index('int LazyFS::' + name + ' (')
    at = s.index('std::shared_lock<std::shared_mutex> lock (cache_command_lock);', start)
    at += len('std::shared_lock<std::shared_mutex> lock (cache_command_lock);')
    s = s[:at] + '\n    std::lock_guard<std::recursive_mutex> namespace_guard (diag_namespace_lock);' + s[at:]
p.write_text(s)
