#!/usr/bin/env python3
"""Apply small #333 adapter edits AFTER runtime-explore.patch; no local builds."""
from pathlib import Path
import shutil, sys
src, root = map(Path, sys.argv[1:3])
here = Path(__file__).resolve().parent

def replace(file, old, new):
 p = src / file
 s = p.read_text()
 assert s.count(old) == 1, (file, old, s.count(old))
 p.write_text(s.replace(old, new))

replace('internal/hooks/go123/port127.go',
 'func Time_runtimeIsBubbled() bool { return false }\nfunc InternalSynctest_IsInBubble() bool { return false }',
 'func Time_runtimeIsBubbled() bool { return synctest333Active }\nfunc InternalSynctest_IsInBubble() bool { return synctest333Active }')
replace('internal/hooks/go123/port127.go',
 ' if durable { panic(gosimruntime.MissingHook("sync.runtime_SemacquireWaitGroup(durable)")) }\n',
 ' // Experiment: durable waits use the same gosim semaphore; no bubble idle accounting.\n')
names = ['Run', 'associate', 'disassociate', 'isAssociated', 'acquire', 'release', 'inBubble']
entries = ''.join(f'\t{{Pkg: "internal/synctest", Selector: "{n}"}}: {{Pkg: hooksGo123Package}},\n' for n in names)
entries += '\t{Pkg: "testing/synctest", Selector: "testingSynctestTest"}: {Pkg: hooksGo123Package},\n'
replace('internal/translate/hooks_go123.go',
 'var hooksGo123 = map[packageSelector]packageSelector{\n',
 'var hooksGo123 = map[packageSelector]packageSelector{\n' + entries)
shutil.copy(here/'gosim/synctest333.go', src/'internal/hooks/go123/synctest333.go')
shutil.copy(here/'gosim/options333.go', src/'gosimruntime/options333.go')
replace('internal/testing/testing.go',
 'func Entrypoint(match string, skip string, tests []gosimruntime.Test) bool {\n',
 'func Entrypoint(match string, skip string, tests []gosimruntime.Test) bool {\n applyTigerstore333Options()\n')
(src/'internal/testing/options333.go').write_text('''package testing
import (
 "flag"
 "github.com/jellevandenhooff/gosim/gosimruntime"
)
func applyTigerstore333Options() {
 seed, seeds := gosimruntime.Tigerstore333Options()
 if flag.Lookup("sim.seed") != nil { if err := flag.Set("sim.seed", seed); err != nil { panic(err) } }
 if flag.Lookup("sim.seeds") != nil { if err := flag.Set("sim.seeds", seeds); err != nil { panic(err) } }
}
''')

# Preserve production checks: only add a diagnostic recorder, without changing
# TestSeeds, its configuration, its scheduling, or its checker decisions.
p = root/'internal/sim/sim_test.go'
s = p.read_text()
needle = 'synctest.Test(t, func(*testing.T) { res, err = runInspect(cfg, inspect) })'
assert s.count(needle) == 1
s = s.replace(needle, needle + '\n if res != nil { t.Cleanup(func() { gosim333Record(t, res) }) }')
p.write_text(s)
(root/'internal/sim/gosim333_test.go').write_text('''package sim
import (
 "encoding/json"
 "strings"
 "testing"
 "github.com/jellevandenhooff/gosim/gosimruntime"
)
func gosim333Record(t *testing.T, r *Result) {
 h, err := r.Checkable()
 if err != nil { t.Logf("#333 record Checkable error: %v", err); return }
 b, err := json.MarshalIndent(h, "", "  ")
 if err != nil { t.Logf("#333 record marshal error: %v", err); return }
 gosimruntime.RecordTigerstore333(strings.Join(r.Trace, "\\n")+"\\n", string(b)+"\\n")
}
''')

# Later rounds add only observed fixes here.
fixes = here/'fixes.py'
if fixes.exists():
 exec(compile(fixes.read_text(), str(fixes), 'exec'))
