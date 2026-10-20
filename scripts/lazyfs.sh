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
	cp "$patch" "$dir/.patch"
	# CMake 4 refuses the toml11 that LazyFS fetches, which asks for a
	# CMake older than 3.5.
	export CMAKE_POLICY_VERSION_MINIMUM=3.5
	(cd "$dir/libs/libpcache" && ./build.sh) >/dev/null
	(cd "$dir/lazyfs" && ./build.sh) >/dev/null
	# LazyFS's build.sh goes on after a failed step.
	test -x "$bin" || { echo "LazyFS did not build in $dir" >&2; exit 1; }
fi
echo "$(cd "$(dirname "$bin")" && pwd)/lazyfs"
