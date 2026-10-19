#!/usr/bin/env bash
# Builds Jepsen's LazyFS at a pinned commit under DIR (default ./lazyfs),
# with lazyfs-namespace.patch, and prints the path of its binary, for
# cmd/chaos -env -lazyfs. Needs git, cmake, g++ and the FUSE 3 headers
# (Ubuntu: cmake g++ libfuse3-dev fuse3).
#
# The patch makes getattr's path-to-inode lookup and the renames, creates
# and unlinks that change it run one at a time. Without it, a stat that
# races a rename binds the old name to the moved file's inode, and a file
# created under that name later shares the moved file's data (#267,
# #270). The pin is LazyFS's newest commit, which has the race.
#
#   scripts/lazyfs.sh [DIR]
set -euo pipefail
dir=${1:-lazyfs}
commit=fa7d32ee9710d55a7e529ac3c375f6616563c78b
patch="$(cd "$(dirname "$0")" && pwd)/lazyfs-namespace.patch"
bin="$dir/lazyfs/build/lazyfs"
if [ ! -x "$bin" ] || ! cmp -s "$patch" "$dir/.patch"; then
	rm -rf "$dir"
	git clone -q https://github.com/dsrhaslab/lazyfs.git "$dir"
	git -C "$dir" checkout -q "$commit"
	git -C "$dir" apply "$patch"
	python3 - "$dir/lazyfs/src/lazyfs.cpp" <<'PY'
from pathlib import Path
import sys
p = Path(sys.argv[1])
s = p.read_text().replace('    cfg->entry_timeout    = 0;', '    this_ ()->FSConfig->log_all_operations = true;\n    cfg->entry_timeout    = 0;')
s = s.replace('    string inode = this_ ()->FSCache->get_original_inode (path);', '''    string inode = this_ ()->FSCache->get_original_inode (path);
    struct stat diagstat;
    int diagrc = lstat(path, &diagstat);
    spdlog::info("[diag300.unlink]: path={} cached_inode={} backing_inode={} size={}", path, inode, diagrc == 0 ? diagstat.st_ino : 0, diagrc == 0 ? diagstat.st_size : -1);''')
s = s.replace('    bool locked = this_ ()->FSCache->lockItemCheckExists (inode);\n\n    if (!locked)', '''    if (inode != to_string(stbuf->st_ino))
        spdlog::error("[diag300.mapping]: path={} cached_inode={} backing_inode={}", path, inode, stbuf->st_ino);
    bool locked = this_ ()->FSCache->lockItemCheckExists (inode);

    if (!locked)''')
s = s.replace('    if (offset > (meta.size - 1))\n        return 0;', '''    if (offset > (meta.size - 1)) {
        spdlog::info("[diag300.read]: path={} offset={} request={} returned=0 meta_size={}", path, offset, size, meta.size);
        return 0;
    }''')
s = s.replace('spdlog::info ("[lazyfs.ops]: {}(path={},off={})", __FUNCTION__, path, offset);', 'spdlog::info ("[lazyfs.ops]: {}(path={},off={},flags={})", __FUNCTION__, path, offset, static_cast<int>(flags));')
s = s.replace('    res = BUF_ITERATOR;', '''    res = BUF_ITERATOR;
    spdlog::info("[diag300.read]: path={} offset={} request={} returned={} meta_size={}", path, offset, size, res, meta.size);''')
p.write_text(s)
PY
	cp "$patch" "$dir/.patch"
	(cd "$dir/libs/libpcache" && ./build.sh) >/dev/null
	(cd "$dir/lazyfs" && ./build.sh) >/dev/null
fi
echo "$(cd "$(dirname "$bin")" && pwd)/lazyfs"
