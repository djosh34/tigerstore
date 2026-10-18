#!/usr/bin/env bash
# Builds Jepsen's LazyFS at a pinned commit under DIR (default ./lazyfs)
# and prints the path of its binary, for cmd/chaos -env -lazyfs. Needs
# git, cmake, g++ and the FUSE 3 headers (Ubuntu: cmake g++ libfuse3-dev
# fuse3).
#
#   scripts/lazyfs.sh [DIR]
set -euo pipefail
dir=${1:-lazyfs}
commit=fa7d32ee9710d55a7e529ac3c375f6616563c78b
bin="$dir/lazyfs/build/lazyfs"
if [ ! -x "$bin" ]; then
	rm -rf "$dir"
	git clone -q https://github.com/dsrhaslab/lazyfs.git "$dir"
	git -C "$dir" checkout -q "$commit"
	python3 "$(dirname "$0")/diag270/fix-lazyfs.py" "$dir"
	(cd "$dir/libs/libpcache" && ./build.sh) >/dev/null
	(cd "$dir/lazyfs" && ./build.sh) >/dev/null
fi
echo "$(cd "$(dirname "$bin")" && pwd)/lazyfs"
