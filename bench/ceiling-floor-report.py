import sys,json,glob,statistics,os,collections
all=collections.defaultdict(list)
for f in glob.glob(sys.argv[1]+'/null-*.json'):
 r=json.load(open(f)); assert r['valid']; all[(r['label'],r['workload'])].append(r)
print('| server CPUs | workload | ops/s min/median/max | server us/op min/median/max | generator us/op min/median/max | server cores min/median/max | generator cores min/median/max |')
print('|---|---|---|---|---|---|---|')
for k,rs in sorted(all.items()):
 def spread(key):
  v=[r[key] for r in rs];return ' / '.join(f'{x:.3f}' for x in [min(v),statistics.median(v),max(v)])
 for r in rs:r['server_cores']=r['server_cores_by_node'][0]
 print('| '+' | '.join([*k,*[spread(key) for key in ['throughput','server_cpu_us_per_op','client_cpu_us_per_op','server_cores','client_cores']]])+' |')
