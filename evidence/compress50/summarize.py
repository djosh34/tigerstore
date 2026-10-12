import csv, pathlib, statistics, sys
p=pathlib.Path(sys.argv[1])
meta=dict(csv.reader((p/'metadata.csv').open()))
assert meta['status']=='complete', meta
rows=list(csv.DictReader((p/'benchmark.csv').open()))
groups={}
for r in rows:
    k=tuple(r[x] for x in ['dataset','codec','level','dictionary_bytes','batch_target','order'])
    groups.setdefault(k,[]).append(r)
assert all(len(g)==5 and {r['repetition'] for r in g}==set('12345') for g in groups.values())
def select(dataset,batch,order='shuffled'):
    out=[]
    for k,rr in groups.items():
        if k[0]!=dataset or int(k[4])!=batch or k[5] != ('single' if batch==1 else order): continue
        r=rr[0]
        label=r['codec']+(r['level'] if r['codec']=='zstd' else '')+(' dict' if int(r['dictionary_bytes']) else '')
        med=lambda x:statistics.median(float(v[x]) for v in rr)
        count=int(r['values']); raw=int(r['value_bytes']); comp=int(r['compressed_bytes'])
        enc=med('encode_cpu_ns_per_value')/1000; dec=med('decode_cpu_ns_per_value')/1000
        out.append((label,comp/raw,enc,dec,med('decode_cpu_ns_per_batch')/1000, (raw-comp)/count/(enc+dec),float(r['compressed_plus_dictionary_per_value_byte']),count,raw/count))
    return out
print('INVENTORY')
inv=list(csv.DictReader((p/'inventory.csv').open()))
for r in sorted([r for r in inv if r['split']=='all' and r['format']=='*' and r['size_bin']=='*'],key=lambda r:-int(r['value_bytes'])):
    print(r['resource'],r['values'],r['value_bytes'])
for r in inv:
    if r['split']=='all' and r['resource']=='*' and r['size_bin']=='*' and r['format']!='*': print('FORMAT',r)
for dataset in ['all','test80','near_1KiB_896_to_1152_test80','near_10KiB_9216_to_11264_test80','synthetic_random_1024','synthetic_random_10240','synthetic_repeated_1024','synthetic_repeated_10240']:
    print('\nDATASET',dataset)
    print('codec batch compressed/raw encode_us decode_us decode_batch_us breakeven_MB/s including_dict_ratio count mean_value')
    for batch in [1,16,64,256]:
        for v in select(dataset,batch):
            print(v[0],batch,*[round(x,5) if isinstance(x,float) else x for x in v[1:]])
print('\nORDER EFFECT test80 64')
for v in select('test80',64,'resource_sorted'): print(v)
