# Per-history verdicts

All commands supplied `--nemesis kill,partition,pause --ops-per-key 50 --time-limit 90`. The upstream set and append generators ignore ops-per-key. Fault coverage requires independent process/packet observations and completed injection/healing.

| Store | Workload | Repeat | Calls | Original online result | Final workload verdict | Register keys | DROP packet hits, maximum observed | Fault coverage |
| --- | --- | ---: | ---: | --- | --- | ---: | ---: | --- |
| tigerstore | append | 1 | 995 | valid | valid |  | 26 | observed |
| tigerstore | append | 2 | 1063 | valid | valid |  | 20 | observed |
| tigerstore | register | 1 | 1325 | timeout, no aggregate result | valid | 27 | 24 | observed |
| tigerstore | register | 2 | 1098 | timeout, no aggregate result | valid | 22 | 19 | observed |
| tigerstore | set | 1 | 1202 | valid | valid |  | 34 | observed |
| tigerstore | set | 2 | 1161 | valid | valid |  | 24 | observed |
| etcd | append | 1 | 1176 | valid | valid |  | 84 | observed |
| etcd | append | 2 | 772 | valid | valid |  | 125 | observed |
| etcd | register | 1 | 825 | valid | valid | 17 | 168 | observed |
| etcd | register | 2 | 1349 | valid | valid | 27 | 84 | observed |
| etcd | set | 1 | 1371 | valid | valid |  | 64 | observed |
| etcd | set | 2 | 1401 | valid | valid |  | 99 | observed |
