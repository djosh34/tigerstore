## Runner
```
CPU(s):                                  4
Model name:                              Neoverse-N2
Thread(s) per core:                      1
               total        used        free      shared  buff/cache   available
Mem:     16722010112  1786978304 12079206400    49561600  3179814912 14935031808
/dev/sda1 ext4   rw,relatime,discard,journal_async_commit,errors=remount-ro,commit=30,data=writeback
6.17.0-1022-azure
go version go1.27.2 linux/arm64
etcd Version: 3.7.2
baseline main 5d964228952d6983ba273bc2e6813770aad796c6
candidate main 5d964228952d6983ba273bc2e6813770aad796c6
generator, scripts and report: 05e551026800d9efb21df13078c5107b0891c1cc
layout (generator, leader, followers): 3 0 1 2
both stores: 3 nodes on 127.0.0.1, client and peer TLS with client certificates, fresh data directory per run
mixed-k8s: 1M preloaded keys, <=128 puts and <=60 KiB per Txn at 300 Txns/s, dump-derived sizes, all endpoints, default mix 25/10/40/15/10 plus resource watches; 3 GiB Go memory target per server
etcd only: --quota-backend-bytes 8 GiB, --log-level warn, --peer-client-cert-auth, default safe persistence
tigerstore: at least two Go processors on its one CPU (its own rule), peer certificates always verified
```
