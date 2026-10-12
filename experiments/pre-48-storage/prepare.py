#!/usr/bin/env python3
"""Download pinned apiserver sources, change only the test-server adapter/type."""
import difflib
import hashlib
import json
import pathlib
import re
import sys
import urllib.request
import zipfile

version, root = sys.argv[1], pathlib.Path(sys.argv[2]).resolve()
root.mkdir(parents=True, exist_ok=True)
evidence = root / "evidence"
evidence.mkdir(exist_ok=True)
script = pathlib.Path(__file__).resolve().parent
url = f"https://proxy.golang.org/k8s.io/apiserver/@v/{version}"
for suffix in ["info", "zip", "mod"]:
    urllib.request.urlretrieve(url + "." + suffix, root / ("apiserver." + suffix))
(evidence / "module.info").write_bytes((root / "apiserver.info").read_bytes())
with zipfile.ZipFile(root / "apiserver.zip") as z:
    z.extractall(root)
module = root / ("k8s.io/apiserver@" + version)
(evidence / "source-sha256.txt").write_text("".join(
    hashlib.sha256((root / ("apiserver." + ext)).read_bytes()).hexdigest() + "  apiserver." + ext + "\n"
    for ext in ["info", "zip", "mod"]))

# Every declared top-level test, benchmark and shared suite entrypoint. Dynamic
# subtests come from the unfiltered go test -json run, not a guessed regex list.
rows = []
shared_calls = set()
for f in sorted((module / "pkg/storage/etcd3").rglob("*.go")):
    for name in re.findall(r"storagetesting\.(\w+)\(", f.read_text()):
        shared_calls.add(name)
for directory in [module / "pkg/storage/etcd3", module / "pkg/storage/testing"]:
    for f in sorted(directory.rglob("*.go")):
        text = f.read_text()
        for m in re.finditer(r"^func ((?:Test|Benchmark|Fuzz|Example|Run)\w*)\(", text, re.M):
            name = m.group(1)
            if name.startswith("Run") and directory.name != "testing":
                continue
            line = text.count("\n", 0, m.start()) + 1
            rows.append({"file": str(f.relative_to(module)), "line": line, "name": name,
                         "kind": "shared helper" if name.startswith("Run") else "Go entrypoint",
                         "referenced_from_etcd3": name in shared_calls if name.startswith("Run") else None})
(evidence / "source-inventory.json").write_text(json.dumps(rows, indent=2) + "\n")

server = module / "pkg/storage/etcd3/testserver/test_server.go"
original = server.read_text()
recorders = original[original.index("\tkubernetesRecorder :="):] if "\tkubernetesRecorder :=" in original else original[original.index("\tclient.KV ="):]
recorders = recorders[:recorders.index("\n\treturn client")]
replacement = (script / "test_server.go.in").read_text().replace("\t@@RECORDERS@@", recorders)
server.write_text(replacement)
store = module / "pkg/storage/etcd3/store_test.go"
old = store.read_text()
assert old.count('"go.etcd.io/etcd/server/v3/embed"') == 1
assert old.count("*embed.Config") == 1
new = old.replace('\t"go.etcd.io/etcd/server/v3/embed"\n', "").replace("*embed.Config", "*testserver.Config")
store.write_text(new)
(evidence / "adapter.diff").write_text("".join(difflib.unified_diff(original.splitlines(True), replacement.splitlines(True), "a/testserver/test_server.go", "b/testserver/test_server.go")) +
    "".join(difflib.unified_diff(old.splitlines(True), new.splitlines(True), "a/store_test.go", "b/store_test.go")))
print(module)
