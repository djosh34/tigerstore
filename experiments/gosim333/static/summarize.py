import collections, csv, json, re
from pathlib import Path
root = Path('/tmp/r333-internals-artifacts')
audit = {(p,n) for p,n in re.findall(r'ERROR .*?pkg=(\S+) name=(\S+)', Path('/tmp/r333-internals-audit.log').read_text())}
lines=[]
for arch in ['arm64','amd64']:
    rs = list(csv.DictReader((root / f'inventory-{arch}.tsv').open(), delimiter='\t'))
    old = {(r['package'],r['name'],r['kind']) for r in rs if r['version']=='1.23.12'}
    missing = [r for r in rs if r['version']=='1.27.2' and r['status'] in ['missing-body','unknown-linkname']]
    def comparison(r):
        if r['package'].startswith('golang.org/'):
            return 'module-not-compared'
        return 'present' if (r['package'],r['name'],r['kind']) in old else 'absent-at-same-stdlib-path'
    with (root / f'missing-{arch}.tsv').open('w') as f:
        w=csv.DictWriter(f,fieldnames=list(rs[0])+['go123_comparison'],delimiter='\t')
        w.writeheader(); w.writerows([{**r,'go123_comparison':comparison(r)} for r in missing])
    current={(r['package'],r['name']) for r in missing}
    lines += [arch, 'missing counts: '+str(dict(collections.Counter(r['status'] for r in missing))),
        'same-path old stdlib comparison: '+str(dict(collections.Counter(comparison(r) for r in missing))),
        'audit entries absent: '+str(sorted(audit-current)), 'beyond audit: '+str(sorted(current-audit))]
    with (root / f'missing-grouped-{arch}.txt').open('w') as f:
        for pkg in sorted({r['package'] for r in missing}):
            f.write(pkg+': '+', '.join(r['name'] for r in missing if r['package']==pkg)+'\n')
(root / 'summary.txt').write_text('\n'.join(lines)+'\n')
print('\n'.join(lines))
