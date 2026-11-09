# Research #573: analysis scripts

Research only, not for main. They read the artifacts of the `research573`
workflow (`bench/research573.sh`) and print the tables posted on #573.

```
gh run download <run> -R djosh34/tigerstore
# CPU and allocations (runs 42693813022 profile1/profile2, 42696251127 sys)
go tool pprof -traces profile1/node-0.cpu > l1.traces          # node-0 = leader, node-1 = follower
python3 -I concept.py l1.traces <leader us/op from result.json server_cpu_us_per_op_by_node[0]>
python3 -I classify.py l1.traces <us/op>                        # layer x kind
go tool pprof -traces -sample_index=alloc_space   -diff_base=node-0-0.allocs node-0-1.allocs > space.traces
go tool pprof -traces -sample_index=alloc_objects -diff_base=node-0-0.allocs node-0-1.allocs > objects.traces
python3 -I allocs.py space.traces objects.traces <ops>          # by layer and site
python3 -I gcshare.py l1.traces <us/op> space.traces objects.traces <ops>
# syscalls, kernel CPU (sys mode) and memmove copies (copy mode)
python3 -I sysrun.py sys
python3 -I copyrun.py copy
```

merged.py averages two profiles per node; edit its paths.
