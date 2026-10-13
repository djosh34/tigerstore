## Runner
```
CPU(s):                                  4
Model name:                              Neoverse-N2
Thread(s) per core:                      1
               total        used        free      shared  buff/cache   available
Mem:     16722046976  1755815936 11391164416    50077696  3899584512 14966231040
/dev/sda1 ext4   rw,relatime,discard,journal_async_commit,errors=remount-ro,commit=30,data=writeback
6.17.0-1022-azure
go version go1.27.2 linux/arm64
etcd Version: 3.7.2
baseline main b5f2e28ef2edd94393d62952b1568763ebc12fe2
candidate main b5f2e28ef2edd94393d62952b1568763ebc12fe2
generator, scripts and report: 92f644a3a1a8dd76d5a4675c3f35012bd8f7f379
layout (generator, leader, followers): 3 0 1 2
both stores: 3 nodes on 127.0.0.1, client and peer TLS with client certificates, fresh data directory per run
mixed-k8s: 1M preloaded keys, <=128 puts and <=60 KiB per Txn at 300 Txns/s, fitted beta sizes, all endpoints, default mix plus resource watches; both stores GOMEMLIMIT=3GiB; rate is half etcd median max
etcd only: --quota-backend-bytes 8 GiB, --log-level warn, --peer-client-cert-auth, default safe persistence
tigerstore: at least two Go processors on its one CPU (its own rule), peer certificates always verified
```
