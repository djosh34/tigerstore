#!/usr/bin/env python3
"""Diagnostic reference from #267 (d8a39477), not a production fix.

Make LazyFS's backing namespace and path-to-inode cache change atomically
against concurrent getattr/create/rename. create calls getattr recursively.
"""
from pathlib import Path
import sys

p = Path(sys.argv[1]) / "lazyfs/src/lazyfs.cpp"
s = p.read_text()
s = s.replace("std::shared_mutex cache_command_lock;", """std::shared_mutex cache_command_lock;
std::recursive_mutex diag_namespace_lock;""", 1)
for name in ["lfs_getattr", "lfs_rename", "lfs_create"]:
    start = s.index("int LazyFS::" + name + " (")
    at = s.index("std::shared_lock<std::shared_mutex> lock (cache_command_lock);", start)
    at += len("std::shared_lock<std::shared_mutex> lock (cache_command_lock);")
    s = s[:at] + "\n    std::lock_guard<std::recursive_mutex> namespace_guard (diag_namespace_lock);" + s[at:]
p.write_text(s)
