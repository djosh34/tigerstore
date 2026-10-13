# Per-repetition spread

Each row is the min, median and max of per-run statistics. Percentiles are not pooled.

| Architecture | Store | Metric | Unit | n | Min | Median | Max | Workloads passed |
|---|---|---|---|---:|---:|---:|---:|---|
| ARM64 | etcd | delete.api.all.p50 | ms | 3 | 3.7361 | 3.7563 | 3.8827 | True |
| ARM64 | etcd | delete.api.all.p99 | ms | 3 | 457.9259 | 490.4667 | 593.2667 | True |
| ARM64 | etcd | delete.api.creates.count | requests | 3 | 112.0000 | 112.0000 | 112.0000 | True |
| ARM64 | etcd | delete.api.creates.rate | requests/s | 3 | 1.6531 | 1.6533 | 1.7040 | True |
| ARM64 | etcd | delete.api.deletes.count | requests | 3 | 1521.0000 | 1521.0000 | 1521.0000 | True |
| ARM64 | etcd | delete.api.deletes.rate | requests/s | 3 | 22.4491 | 22.4523 | 23.1413 | True |
| ARM64 | etcd | delete.api.http403 | responses | 3 | 9.0000 | 9.0000 | 9.0000 | True |
| ARM64 | etcd | delete.api.http404 | responses | 3 | 31.0000 | 36.0000 | 38.0000 | True |
| ARM64 | etcd | delete.api.http409 | responses | 3 | 9.0000 | 9.0000 | 9.0000 | True |
| ARM64 | etcd | delete.api.http422 | responses | 1 | 0.0000 | 0.0000 | 0.0000 | True |
| ARM64 | etcd | delete.api.http500 | responses | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| ARM64 | etcd | delete.api.requests | requests/s | 3 | 35.4375 | 36.0625 | 36.7888 | True |
| ARM64 | etcd | delete.api.resource..p50 | ms | 3 | 2.6126 | 2.6739 | 2.7455 | True |
| ARM64 | etcd | delete.api.resource..p99 | ms | 3 | 138.5000 | 177.0000 | 184.0000 | True |
| ARM64 | etcd | delete.api.resource.configmaps.p50 | ms | 3 | 2.5828 | 2.6603 | 2.6630 | True |
| ARM64 | etcd | delete.api.resource.configmaps.p99 | ms | 3 | 22.4692 | 24.5222 | 28.5500 | True |
| ARM64 | etcd | delete.api.resource.controllerrevisions.p50 | ms | 3 | 3.4615 | 3.4615 | 5.0000 | True |
| ARM64 | etcd | delete.api.resource.controllerrevisions.p99 | ms | 3 | 97.7500 | 98.2000 | 182.0000 | True |
| ARM64 | etcd | delete.api.resource.cronjobs.p50 | ms | 3 | 4.5000 | 25.0000 | 37.5000 | True |
| ARM64 | etcd | delete.api.resource.cronjobs.p99 | ms | 3 | 98.8750 | 98.8750 | 182.0000 | True |
| ARM64 | etcd | delete.api.resource.csistoragecapacities.p50 | ms | 3 | 3.2143 | 15.0000 | 18.3333 | True |
| ARM64 | etcd | delete.api.resource.csistoragecapacities.p99 | ms | 3 | 182.0000 | 182.0000 | 182.0000 | True |
| ARM64 | etcd | delete.api.resource.daemonsets.p50 | ms | 3 | 3.7500 | 4.0909 | 13.0000 | True |
| ARM64 | etcd | delete.api.resource.daemonsets.p99 | ms | 3 | 98.2000 | 98.5000 | 194.0000 | True |
| ARM64 | etcd | delete.api.resource.deployments.p50 | ms | 3 | 4.6667 | 5.0000 | 17.7273 | True |
| ARM64 | etcd | delete.api.resource.deployments.p99 | ms | 3 | 97.6667 | 172.0000 | 172.0000 | True |
| ARM64 | etcd | delete.api.resource.endpoints.p50 | ms | 3 | 36.6071 | 42.2619 | 47.8261 | True |
| ARM64 | etcd | delete.api.resource.endpoints.p99 | ms | 3 | 369.4286 | 398.6667 | 552.5000 | True |
| ARM64 | etcd | delete.api.resource.endpointslices.p50 | ms | 3 | 38.9706 | 39.7059 | 56.7568 | True |
| ARM64 | etcd | delete.api.resource.endpointslices.p99 | ms | 3 | 375.8000 | 396.0000 | 599.0000 | True |
| ARM64 | etcd | delete.api.resource.events.p50 | ms | 3 | 19.5455 | 24.0244 | 25.0000 | True |
| ARM64 | etcd | delete.api.resource.events.p99 | ms | 3 | 576.0000 | 580.8000 | 586.2857 | True |
| ARM64 | etcd | delete.api.resource.horizontalpodautoscalers.p50 | ms | 3 | 4.5000 | 5.0000 | 10.0000 | True |
| ARM64 | etcd | delete.api.resource.horizontalpodautoscalers.p99 | ms | 3 | 98.2000 | 98.5000 | 98.8750 | True |
| ARM64 | etcd | delete.api.resource.ingresses.p50 | ms | 3 | 5.0000 | 15.0000 | 18.3333 | True |
| ARM64 | etcd | delete.api.resource.ingresses.p99 | ms | 3 | 98.8750 | 182.0000 | 191.0000 | True |
| ARM64 | etcd | delete.api.resource.ipaddresses.p50 | ms | 3 | 31.2500 | 35.0000 | 35.0000 | True |
| ARM64 | etcd | delete.api.resource.ipaddresses.p99 | ms | 3 | 98.4000 | 192.0000 | 196.0000 | True |
| ARM64 | etcd | delete.api.resource.jobs.p50 | ms | 3 | 3.4615 | 4.5000 | 55.5556 | True |
| ARM64 | etcd | delete.api.resource.jobs.p99 | ms | 3 | 97.7500 | 98.2000 | 182.0000 | True |
| ARM64 | etcd | delete.api.resource.leases.p50 | ms | 3 | 2.8866 | 2.9124 | 2.9620 | True |
| ARM64 | etcd | delete.api.resource.leases.p99 | ms | 3 | 98.3750 | 99.1000 | 172.0000 | True |
| ARM64 | etcd | delete.api.resource.limitranges.p50 | ms | 3 | 4.0909 | 11.6667 | 15.0000 | True |
| ARM64 | etcd | delete.api.resource.limitranges.p99 | ms | 3 | 98.2000 | 98.7143 | 182.0000 | True |
| ARM64 | etcd | delete.api.resource.namespaces.p50 | ms | 3 | 2.7273 | 2.7727 | 2.7727 | True |
| ARM64 | etcd | delete.api.resource.namespaces.p99 | ms | 3 | 22.6000 | 22.9667 | 22.9667 | True |
| ARM64 | etcd | delete.api.resource.networkpolicies.p50 | ms | 3 | 2.8125 | 3.7500 | 4.0909 | True |
| ARM64 | etcd | delete.api.resource.networkpolicies.p99 | ms | 3 | 91.0000 | 97.0000 | 98.2000 | True |
| ARM64 | etcd | delete.api.resource.nodes.p50 | ms | 3 | 2.5000 | 5.0000 | 5.0000 | True |
| ARM64 | etcd | delete.api.resource.nodes.p99 | ms | 3 | 4.9500 | 24.6000 | 24.6000 | True |
| ARM64 | etcd | delete.api.resource.persistentvolumeclaims.p50 | ms | 3 | 4.0909 | 5.0000 | 5.0000 | True |
| ARM64 | etcd | delete.api.resource.persistentvolumeclaims.p99 | ms | 3 | 98.5000 | 182.0000 | 182.0000 | True |
| ARM64 | etcd | delete.api.resource.podcertificaterequests.p50 | ms | 3 | 3.7500 | 4.0909 | 4.5000 | True |
| ARM64 | etcd | delete.api.resource.podcertificaterequests.p99 | ms | 3 | 95.5000 | 98.2000 | 182.0000 | True |
| ARM64 | etcd | delete.api.resource.poddisruptionbudgets.p50 | ms | 3 | 3.7500 | 5.0000 | 5.0000 | True |
| ARM64 | etcd | delete.api.resource.poddisruptionbudgets.p99 | ms | 3 | 98.2000 | 98.2000 | 182.0000 | True |
| ARM64 | etcd | delete.api.resource.pods.p50 | ms | 3 | 101.3889 | 108.8235 | 153.7037 | True |
| ARM64 | etcd | delete.api.resource.pods.p99 | ms | 3 | 705.0000 | 746.5714 | 1028.7500 | True |
| ARM64 | etcd | delete.api.resource.podtemplates.p50 | ms | 3 | 3.7500 | 4.0909 | 66.6667 | True |
| ARM64 | etcd | delete.api.resource.podtemplates.p99 | ms | 3 | 98.7143 | 182.0000 | 191.0000 | True |
| ARM64 | etcd | delete.api.resource.replicasets.p50 | ms | 3 | 5.0000 | 8.8095 | 13.8889 | True |
| ARM64 | etcd | delete.api.resource.replicasets.p99 | ms | 3 | 150.0000 | 150.0000 | 300.0000 | True |
| ARM64 | etcd | delete.api.resource.replicationcontrollers.p50 | ms | 3 | 11.6667 | 25.0000 | 60.0000 | True |
| ARM64 | etcd | delete.api.resource.replicationcontrollers.p99 | ms | 3 | 182.0000 | 191.0000 | 194.0000 | True |
| ARM64 | etcd | delete.api.resource.resourceclaims.p50 | ms | 3 | 3.7500 | 4.5000 | 25.0000 | True |
| ARM64 | etcd | delete.api.resource.resourceclaims.p99 | ms | 3 | 98.5000 | 98.8750 | 182.0000 | True |
| ARM64 | etcd | delete.api.resource.resourceclaimtemplates.p50 | ms | 3 | 4.5000 | 5.0000 | 5.0000 | True |
| ARM64 | etcd | delete.api.resource.resourceclaimtemplates.p99 | ms | 3 | 98.5000 | 98.8750 | 182.0000 | True |
| ARM64 | etcd | delete.api.resource.resourcequotas.p50 | ms | 3 | 4.5000 | 5.0000 | 25.0000 | True |
| ARM64 | etcd | delete.api.resource.resourcequotas.p99 | ms | 3 | 98.5000 | 182.0000 | 191.0000 | True |
| ARM64 | etcd | delete.api.resource.rolebindings.p50 | ms | 3 | 3.7500 | 5.0000 | 25.0000 | True |
| ARM64 | etcd | delete.api.resource.rolebindings.p99 | ms | 3 | 97.7500 | 98.7143 | 191.0000 | True |
| ARM64 | etcd | delete.api.resource.roles.p50 | ms | 3 | 3.4615 | 4.0909 | 17.0000 | True |
| ARM64 | etcd | delete.api.resource.roles.p99 | ms | 3 | 98.2000 | 98.7143 | 182.0000 | True |
| ARM64 | etcd | delete.api.resource.secrets.p50 | ms | 3 | 3.0556 | 3.0556 | 3.3333 | True |
| ARM64 | etcd | delete.api.resource.secrets.p99 | ms | 3 | 95.6000 | 95.6000 | 156.0000 | True |
| ARM64 | etcd | delete.api.resource.serviceaccounts.p50 | ms | 3 | 17.5000 | 22.1429 | 62.5000 | True |
| ARM64 | etcd | delete.api.resource.serviceaccounts.p99 | ms | 3 | 98.5000 | 98.7143 | 99.2500 | True |
| ARM64 | etcd | delete.api.resource.services.p50 | ms | 3 | 33.3333 | 56.6667 | 67.8571 | True |
| ARM64 | etcd | delete.api.resource.services.p99 | ms | 3 | 192.8000 | 364.0000 | 382.0000 | True |
| ARM64 | etcd | delete.api.resource.statefulsets.p50 | ms | 3 | 3.7500 | 4.0909 | 4.5000 | True |
| ARM64 | etcd | delete.api.resource.statefulsets.p99 | ms | 3 | 182.0000 | 191.0000 | 191.0000 | True |
| ARM64 | etcd | delete.api.updates.count | requests | 3 | 177.0000 | 189.0000 | 209.0000 | True |
| ARM64 | etcd | delete.api.updates.rate | requests/s | 3 | 2.6124 | 2.8756 | 3.0852 | True |
| ARM64 | etcd | delete.api.verb.DELETE.p50 | ms | 3 | 3.2591 | 3.2842 | 3.3677 | True |
| ARM64 | etcd | delete.api.verb.DELETE.p99 | ms | 3 | 517.0400 | 550.8667 | 710.5000 | True |
| ARM64 | etcd | delete.api.verb.GET.p50 | ms | 3 | 3.5096 | 3.5552 | 3.6364 | True |
| ARM64 | etcd | delete.api.verb.GET.p99 | ms | 3 | 190.0833 | 191.1429 | 337.4286 | True |
| ARM64 | etcd | delete.api.verb.LIST.p50 | ms | 3 | 4.9062 | 31.2500 | 51.1745 | True |
| ARM64 | etcd | delete.api.verb.LIST.p99 | ms | 3 | 173.7500 | 183.4211 | 183.4737 | True |
| ARM64 | etcd | delete.api.verb.PATCH.p50 | ms | 3 | 75.0000 | 75.0000 | 116.6667 | True |
| ARM64 | etcd | delete.api.verb.PATCH.p99 | ms | 3 | 197.8000 | 198.3333 | 380.0000 | True |
| ARM64 | etcd | delete.api.verb.POST.p50 | ms | 3 | 21.1818 | 27.8409 | 29.8913 | True |
| ARM64 | etcd | delete.api.verb.POST.p99 | ms | 3 | 339.5000 | 339.5000 | 373.1111 | True |
| ARM64 | etcd | delete.api.verb.PUT.p50 | ms | 3 | 4.7664 | 9.6154 | 10.5172 | True |
| ARM64 | etcd | delete.api.verb.PUT.p99 | ms | 3 | 370.5000 | 396.0000 | 574.0000 | True |
| ARM64 | etcd | delete.cpu.all | CPU s | 3 | 14.9800 | 15.6500 | 16.0500 | True |
| ARM64 | etcd | delete.cpu.cores | cores | 3 | 0.2279 | 0.2310 | 0.2369 | True |
| ARM64 | etcd | delete.cpu.member1 | CPU s | 3 | 4.4600 | 4.6900 | 4.7800 | True |
| ARM64 | etcd | delete.cpu.member2 | CPU s | 3 | 5.7000 | 6.1200 | 6.1400 | True |
| ARM64 | etcd | delete.cpu.member3 | CPU s | 3 | 4.8200 | 4.8200 | 5.1500 | True |
| ARM64 | etcd | delete.duration | s | 3 | 65.0059 | 67.0349 | 67.0417 | True |
| ARM64 | etcd | delete.exit | exit code | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| ARM64 | etcd | delete.live_inventory | objects in tracked kinds | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| ARM64 | etcd | delete.revision.member1 | revisions | 3 | 2741.0000 | 2753.0000 | 2772.0000 | True |
| ARM64 | etcd | delete.revision.member2 | revisions | 3 | 2741.0000 | 2752.0000 | 2772.0000 | True |
| ARM64 | etcd | delete.revision.member3 | revisions | 3 | 2741.0000 | 2752.0000 | 2772.0000 | True |
| ARM64 | etcd | delete.server.external-etcd-1.etcd_mvcc_delete_total | counter increments/s | 3 | 34.0512 | 34.0577 | 35.1138 | True |
| ARM64 | etcd | delete.server.external-etcd-1.etcd_mvcc_put_total | counter increments/s | 3 | 6.4058 | 6.7731 | 6.8647 | True |
| ARM64 | etcd | delete.server.external-etcd-1.etcd_mvcc_range_total | counter increments/s | 3 | 67.2888 | 68.7518 | 71.9779 | True |
| ARM64 | etcd | delete.server.external-etcd-1.etcd_network_client_grpc_received_bytes_total | counter increments/s | 3 | 7590.7443 | 7728.3892 | 7982.9571 | True |
| ARM64 | etcd | delete.server.external-etcd-1.etcd_network_client_grpc_sent_bytes_total | counter increments/s | 3 | 27316.8711 | 48088.7451 | 54323.2823 | True |
| ARM64 | etcd | delete.server.external-etcd-1.etcd_server_proposals_applied_total | counter increments/s | 3 | 42.1249 | 42.4873 | 43.5307 | True |
| ARM64 | etcd | delete.server.external-etcd-1.etcd_server_proposals_committed_total | counter increments/s | 3 | 42.1249 | 42.4873 | 43.5307 | True |
| ARM64 | etcd | delete.server.external-etcd-1.grpc_server_handled_total | counter increments/s | 3 | 34.9811 | 35.3421 | 36.1944 | True |
| ARM64 | etcd | delete.server.external-etcd-2.etcd_mvcc_delete_total | counter increments/s | 3 | 34.0507 | 34.0564 | 35.1155 | True |
| ARM64 | etcd | delete.server.external-etcd-2.etcd_mvcc_put_total | counter increments/s | 3 | 6.4057 | 6.7735 | 6.8497 | True |
| ARM64 | etcd | delete.server.external-etcd-2.etcd_mvcc_range_total | counter increments/s | 3 | 62.8280 | 63.4575 | 80.9866 | True |
| ARM64 | etcd | delete.server.external-etcd-2.etcd_network_client_grpc_received_bytes_total | counter increments/s | 3 | 7883.9233 | 8176.5683 | 8719.8233 | True |
| ARM64 | etcd | delete.server.external-etcd-2.etcd_network_client_grpc_sent_bytes_total | counter increments/s | 3 | 28213.5802 | 30138.6814 | 41247.5348 | True |
| ARM64 | etcd | delete.server.external-etcd-2.etcd_server_proposals_applied_total | counter increments/s | 3 | 42.1243 | 42.4709 | 43.5328 | True |
| ARM64 | etcd | delete.server.external-etcd-2.etcd_server_proposals_committed_total | counter increments/s | 3 | 42.1243 | 42.4709 | 43.5328 | True |
| ARM64 | etcd | delete.server.external-etcd-2.grpc_server_handled_total | counter increments/s | 3 | 35.1429 | 35.2669 | 36.3484 | True |
| ARM64 | etcd | delete.server.external-etcd-3.etcd_mvcc_delete_total | counter increments/s | 3 | 34.0513 | 34.0531 | 35.1173 | True |
| ARM64 | etcd | delete.server.external-etcd-3.etcd_mvcc_put_total | counter increments/s | 3 | 6.3911 | 6.7738 | 6.8342 | True |
| ARM64 | etcd | delete.server.external-etcd-3.etcd_mvcc_range_total | counter increments/s | 3 | 78.8332 | 100.1224 | 100.9985 | True |
| ARM64 | etcd | delete.server.external-etcd-3.etcd_network_client_grpc_received_bytes_total | counter increments/s | 3 | 7652.5987 | 7798.0927 | 8147.6250 | True |
| ARM64 | etcd | delete.server.external-etcd-3.etcd_network_client_grpc_sent_bytes_total | counter increments/s | 3 | 39659.1802 | 64006.2482 | 79125.4782 | True |
| ARM64 | etcd | delete.server.external-etcd-3.etcd_server_proposals_applied_total | counter increments/s | 3 | 42.1103 | 42.4520 | 43.5352 | True |
| ARM64 | etcd | delete.server.external-etcd-3.etcd_server_proposals_committed_total | counter increments/s | 3 | 42.1103 | 42.4520 | 43.5352 | True |
| ARM64 | etcd | delete.server.external-etcd-3.grpc_server_handled_total | counter increments/s | 3 | 35.0698 | 35.1897 | 36.2895 | True |
| ARM64 | etcd | delete.storage.etcd_request_duration_seconds | RPC/s | 3 | 103.6114 | 104.2609 | 107.2018 | True |
| ARM64 | etcd | delete.verified_deletion_rate | objects/s | 3 | 17.6756 | 17.6774 | 18.2291 | True |
| ARM64 | etcd | delete.window | s | 3 | 65.7265 | 67.7435 | 67.7532 | True |
| ARM64 | etcd | errors.observed | records | 3 | 20.0000 | 22.0000 | 23.0000 | True |
| ARM64 | etcd | errors.workload | records | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| ARM64 | etcd | events.observations | watch notifications | 3 | 2212.0000 | 2212.0000 | 2212.0000 | True |
| ARM64 | etcd | events.unique | events | 3 | 1094.0000 | 1094.0000 | 1094.0000 | True |
| ARM64 | etcd | mixed.api.all.p50 | ms | 3 | 3.4643 | 3.5576 | 3.9759 | True |
| ARM64 | etcd | mixed.api.all.p99 | ms | 3 | 72.5769 | 77.0370 | 79.0000 | True |
| ARM64 | etcd | mixed.api.creates.count | requests | 3 | 1640.0000 | 1640.0000 | 1640.0000 | True |
| ARM64 | etcd | mixed.api.creates.rate | requests/s | 3 | 36.5794 | 36.5944 | 37.4378 | True |
| ARM64 | etcd | mixed.api.deletes.count | requests | 3 | 73.0000 | 75.0000 | 79.0000 | True |
| ARM64 | etcd | mixed.api.deletes.rate | requests/s | 3 | 1.6664 | 1.6728 | 1.7628 | True |
| ARM64 | etcd | mixed.api.http403 | responses | 3 | 14.0000 | 17.0000 | 19.0000 | True |
| ARM64 | etcd | mixed.api.http404 | responses | 3 | 6.0000 | 7.0000 | 11.0000 | True |
| ARM64 | etcd | mixed.api.http409 | responses | 3 | 14.0000 | 16.0000 | 16.0000 | True |
| ARM64 | etcd | mixed.api.http422 | responses | 1 | 0.0000 | 0.0000 | 0.0000 | True |
| ARM64 | etcd | mixed.api.http500 | responses | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| ARM64 | etcd | mixed.api.requests | requests/s | 3 | 65.5530 | 65.6021 | 66.8402 | True |
| ARM64 | etcd | mixed.api.resource..p50 | ms | 3 | 2.5321 | 2.6042 | 2.6712 | True |
| ARM64 | etcd | mixed.api.resource..p99 | ms | 3 | 9.2000 | 20.0000 | 21.8800 | True |
| ARM64 | etcd | mixed.api.resource.configmaps.p50 | ms | 3 | 3.0186 | 3.0741 | 3.3894 | True |
| ARM64 | etcd | mixed.api.resource.configmaps.p99 | ms | 3 | 48.1912 | 55.9545 | 73.1000 | True |
| ARM64 | etcd | mixed.api.resource.controllerrevisions.p50 | ms | 3 | 2.5000 | 5.0000 | 5.0000 | True |
| ARM64 | etcd | mixed.api.resource.controllerrevisions.p99 | ms | 3 | 4.9500 | 24.6000 | 99.0000 | True |
| ARM64 | etcd | mixed.api.resource.cronjobs.p50 | ms | 3 | 2.5000 | 3.3333 | 3.3333 | True |
| ARM64 | etcd | mixed.api.resource.cronjobs.p99 | ms | 3 | 4.9500 | 98.0000 | 98.0000 | True |
| ARM64 | etcd | mixed.api.resource.csistoragecapacities.p50 | ms | 3 | 2.5000 | 2.5000 | 2.5000 | True |
| ARM64 | etcd | mixed.api.resource.csistoragecapacities.p99 | ms | 3 | 4.9500 | 4.9500 | 4.9500 | True |
| ARM64 | etcd | mixed.api.resource.daemonsets.p50 | ms | 3 | 2.5000 | 2.5000 | 5.0000 | True |
| ARM64 | etcd | mixed.api.resource.daemonsets.p99 | ms | 3 | 4.9500 | 4.9500 | 196.0000 | True |
| ARM64 | etcd | mixed.api.resource.deployments.p50 | ms | 3 | 4.3750 | 4.4712 | 5.2381 | True |
| ARM64 | etcd | mixed.api.resource.deployments.p99 | ms | 3 | 83.5000 | 83.6667 | 107.0000 | True |
| ARM64 | etcd | mixed.api.resource.endpoints.p50 | ms | 3 | 6.1667 | 6.6667 | 12.2840 | True |
| ARM64 | etcd | mixed.api.resource.endpoints.p99 | ms | 3 | 83.7500 | 83.8750 | 167.2500 | True |
| ARM64 | etcd | mixed.api.resource.endpointslices.p50 | ms | 3 | 5.1923 | 9.2466 | 12.2152 | True |
| ARM64 | etcd | mixed.api.resource.endpointslices.p99 | ms | 3 | 77.8333 | 84.1250 | 167.7500 | True |
| ARM64 | etcd | mixed.api.resource.events.p50 | ms | 3 | 4.8364 | 5.4950 | 8.8525 | True |
| ARM64 | etcd | mixed.api.resource.events.p99 | ms | 3 | 49.4167 | 49.4167 | 49.5625 | True |
| ARM64 | etcd | mixed.api.resource.horizontalpodautoscalers.p50 | ms | 3 | 2.5000 | 3.3333 | 3.3333 | True |
| ARM64 | etcd | mixed.api.resource.horizontalpodautoscalers.p99 | ms | 3 | 4.9500 | 24.2000 | 98.0000 | True |
| ARM64 | etcd | mixed.api.resource.ingresses.p50 | ms | 3 | 2.5000 | 2.5000 | 3.3333 | True |
| ARM64 | etcd | mixed.api.resource.ingresses.p99 | ms | 3 | 4.9500 | 4.9500 | 98.0000 | True |
| ARM64 | etcd | mixed.api.resource.ipaddresses.p50 | ms | 3 | 2.6087 | 2.8571 | 3.0000 | True |
| ARM64 | etcd | mixed.api.resource.ipaddresses.p99 | ms | 3 | 20.2000 | 23.4000 | 23.8000 | True |
| ARM64 | etcd | mixed.api.resource.jobs.p50 | ms | 3 | 2.5000 | 2.5000 | 3.3333 | True |
| ARM64 | etcd | mixed.api.resource.jobs.p99 | ms | 3 | 4.9500 | 4.9500 | 98.0000 | True |
| ARM64 | etcd | mixed.api.resource.leases.p50 | ms | 3 | 2.5379 | 2.6562 | 2.7586 | True |
| ARM64 | etcd | mixed.api.resource.leases.p99 | ms | 3 | 11.6000 | 33.0000 | 136.0000 | True |
| ARM64 | etcd | mixed.api.resource.limitranges.p50 | ms | 3 | 2.5000 | 2.5000 | 3.0000 | True |
| ARM64 | etcd | mixed.api.resource.limitranges.p99 | ms | 3 | 4.9500 | 4.9500 | 47.0000 | True |
| ARM64 | etcd | mixed.api.resource.namespaces.p50 | ms | 3 | 2.8067 | 2.9805 | 3.0000 | True |
| ARM64 | etcd | mixed.api.resource.namespaces.p99 | ms | 3 | 24.4175 | 24.4373 | 24.6432 | True |
| ARM64 | etcd | mixed.api.resource.networkpolicies.p50 | ms | 3 | 2.5000 | 2.5000 | 5.0000 | True |
| ARM64 | etcd | mixed.api.resource.networkpolicies.p99 | ms | 3 | 4.9500 | 4.9500 | 99.0000 | True |
| ARM64 | etcd | mixed.api.resource.nodes.p50 | ms | 3 | 2.5000 | 2.5000 | 2.5000 | True |
| ARM64 | etcd | mixed.api.resource.nodes.p99 | ms | 3 | 4.9500 | 4.9500 | 4.9500 | True |
| ARM64 | etcd | mixed.api.resource.persistentvolumeclaims.p50 | ms | 3 | 2.5000 | 2.5000 | 3.3333 | True |
| ARM64 | etcd | mixed.api.resource.persistentvolumeclaims.p99 | ms | 3 | 4.9500 | 4.9500 | 196.0000 | True |
| ARM64 | etcd | mixed.api.resource.podcertificaterequests.p50 | ms | 3 | 2.5000 | 2.5000 | 2.5000 | True |
| ARM64 | etcd | mixed.api.resource.podcertificaterequests.p99 | ms | 3 | 4.9500 | 4.9500 | 4.9500 | True |
| ARM64 | etcd | mixed.api.resource.poddisruptionbudgets.p50 | ms | 3 | 2.5000 | 2.5000 | 2.5000 | True |
| ARM64 | etcd | mixed.api.resource.poddisruptionbudgets.p99 | ms | 3 | 4.9500 | 4.9500 | 4.9500 | True |
| ARM64 | etcd | mixed.api.resource.pods.p50 | ms | 3 | 5.8824 | 6.4124 | 9.2857 | True |
| ARM64 | etcd | mixed.api.resource.pods.p99 | ms | 3 | 42.8500 | 81.3750 | 82.5000 | True |
| ARM64 | etcd | mixed.api.resource.podtemplates.p50 | ms | 3 | 2.5000 | 5.0000 | 5.0000 | True |
| ARM64 | etcd | mixed.api.resource.podtemplates.p99 | ms | 3 | 4.9500 | 24.6000 | 49.0000 | True |
| ARM64 | etcd | mixed.api.resource.replicasets.p50 | ms | 3 | 11.3415 | 12.1429 | 13.5366 | True |
| ARM64 | etcd | mixed.api.resource.replicasets.p99 | ms | 3 | 89.5000 | 89.8333 | 138.0000 | True |
| ARM64 | etcd | mixed.api.resource.replicationcontrollers.p50 | ms | 3 | 2.5000 | 2.5000 | 5.0000 | True |
| ARM64 | etcd | mixed.api.resource.replicationcontrollers.p99 | ms | 3 | 4.9500 | 4.9500 | 196.0000 | True |
| ARM64 | etcd | mixed.api.resource.resourceclaims.p50 | ms | 3 | 2.5000 | 2.5000 | 2.5000 | True |
| ARM64 | etcd | mixed.api.resource.resourceclaims.p99 | ms | 3 | 4.9500 | 4.9500 | 4.9500 | True |
| ARM64 | etcd | mixed.api.resource.resourceclaimtemplates.p50 | ms | 3 | 2.5000 | 3.3333 | 5.0000 | True |
| ARM64 | etcd | mixed.api.resource.resourceclaimtemplates.p99 | ms | 3 | 4.9500 | 98.0000 | 99.0000 | True |
| ARM64 | etcd | mixed.api.resource.resourcequotas.p50 | ms | 3 | 2.5000 | 3.3333 | 5.0000 | True |
| ARM64 | etcd | mixed.api.resource.resourcequotas.p99 | ms | 3 | 4.9500 | 98.0000 | 99.0000 | True |
| ARM64 | etcd | mixed.api.resource.rolebindings.p50 | ms | 3 | 2.5000 | 2.5000 | 2.5000 | True |
| ARM64 | etcd | mixed.api.resource.rolebindings.p99 | ms | 3 | 4.9500 | 4.9500 | 4.9500 | True |
| ARM64 | etcd | mixed.api.resource.roles.p50 | ms | 3 | 2.5000 | 2.5000 | 3.3333 | True |
| ARM64 | etcd | mixed.api.resource.roles.p99 | ms | 3 | 4.9500 | 4.9500 | 98.0000 | True |
| ARM64 | etcd | mixed.api.resource.secrets.p50 | ms | 3 | 2.7344 | 2.8226 | 3.1250 | True |
| ARM64 | etcd | mixed.api.resource.secrets.p99 | ms | 3 | 22.6667 | 24.0000 | 82.5000 | True |
| ARM64 | etcd | mixed.api.resource.serviceaccounts.p50 | ms | 3 | 9.0000 | 14.0909 | 20.5556 | True |
| ARM64 | etcd | mixed.api.resource.serviceaccounts.p99 | ms | 3 | 93.0000 | 93.0000 | 96.5000 | True |
| ARM64 | etcd | mixed.api.resource.services.p50 | ms | 3 | 15.4545 | 16.5000 | 16.5000 | True |
| ARM64 | etcd | mixed.api.resource.services.p99 | ms | 3 | 93.7500 | 93.7500 | 95.8333 | True |
| ARM64 | etcd | mixed.api.resource.statefulsets.p50 | ms | 3 | 2.5000 | 2.5000 | 5.0000 | True |
| ARM64 | etcd | mixed.api.resource.statefulsets.p99 | ms | 3 | 4.9500 | 4.9500 | 99.0000 | True |
| ARM64 | etcd | mixed.api.updates.count | requests | 3 | 249.0000 | 251.0000 | 254.0000 | True |
| ARM64 | etcd | mixed.api.updates.rate | requests/s | 3 | 5.6007 | 5.6654 | 5.6842 | True |
| ARM64 | etcd | mixed.api.verb.DELETE.p50 | ms | 3 | 3.6321 | 3.7727 | 4.2021 | True |
| ARM64 | etcd | mixed.api.verb.DELETE.p99 | ms | 3 | 361.5000 | 366.8000 | 442.0000 | True |
| ARM64 | etcd | mixed.api.verb.GET.p50 | ms | 3 | 2.8586 | 2.9881 | 3.0022 | True |
| ARM64 | etcd | mixed.api.verb.GET.p99 | ms | 3 | 24.2023 | 24.5385 | 24.8851 | True |
| ARM64 | etcd | mixed.api.verb.LIST.p50 | ms | 3 | 3.0000 | 3.1053 | 3.2143 | True |
| ARM64 | etcd | mixed.api.verb.LIST.p99 | ms | 3 | 98.8889 | 99.2500 | 141.5000 | True |
| ARM64 | etcd | mixed.api.verb.PATCH.p50 | ms | 3 | 10.3086 | 11.1446 | 13.5870 | True |
| ARM64 | etcd | mixed.api.verb.PATCH.p99 | ms | 3 | 34.6250 | 46.2187 | 48.0833 | True |
| ARM64 | etcd | mixed.api.verb.POST.p50 | ms | 3 | 3.3236 | 3.3962 | 3.8568 | True |
| ARM64 | etcd | mixed.api.verb.POST.p99 | ms | 3 | 46.2119 | 46.4405 | 47.6991 | True |
| ARM64 | etcd | mixed.api.verb.PUT.p50 | ms | 3 | 5.8917 | 7.3497 | 10.8824 | True |
| ARM64 | etcd | mixed.api.verb.PUT.p99 | ms | 3 | 84.4167 | 85.4444 | 159.0000 | True |
| ARM64 | etcd | mixed.cpu.all | CPU s | 3 | 10.0300 | 10.3100 | 11.3300 | True |
| ARM64 | etcd | mixed.cpu.cores | cores | 3 | 0.2287 | 0.2299 | 0.2524 | True |
| ARM64 | etcd | mixed.cpu.member1 | CPU s | 3 | 2.9600 | 3.0900 | 3.3500 | True |
| ARM64 | etcd | mixed.cpu.member2 | CPU s | 3 | 3.8100 | 3.9700 | 4.2900 | True |
| ARM64 | etcd | mixed.cpu.member3 | CPU s | 3 | 3.2500 | 3.2600 | 3.6900 | True |
| ARM64 | etcd | mixed.duration | s | 3 | 43.1861 | 44.1957 | 44.2109 | True |
| ARM64 | etcd | mixed.exit | exit code | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| ARM64 | etcd | mixed.live_inventory | objects in tracked kinds | 3 | 1096.0000 | 1096.0000 | 1096.0000 | True |
| ARM64 | etcd | mixed.net_live_creation_rate | objects/s | 3 | 24.7903 | 24.7988 | 25.3786 | True |
| ARM64 | etcd | mixed.pod.ContainersReady.p50 | ms | 3 | 1000.0000 | 1000.0000 | 1000.0000 | True |
| ARM64 | etcd | mixed.pod.ContainersReady.p99 | ms | 3 | 1000.0000 | 2000.0000 | 2000.0000 | True |
| ARM64 | etcd | mixed.pod.ContainersStarted.p50 | ms | 3 | 1474.0000 | 1505.0000 | 1614.0000 | True |
| ARM64 | etcd | mixed.pod.ContainersStarted.p99 | ms | 3 | 1949.0000 | 2092.0000 | 2157.0000 | True |
| ARM64 | etcd | mixed.pod.Initialized.p50 | ms | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| ARM64 | etcd | mixed.pod.Initialized.p99 | ms | 3 | 0.0000 | 500.0000 | 1000.0000 | True |
| ARM64 | etcd | mixed.pod.PodReadyToStartContainers.p50 | ms | 3 | 0.0000 | 1000.0000 | 1000.0000 | True |
| ARM64 | etcd | mixed.pod.PodReadyToStartContainers.p99 | ms | 3 | 1000.0000 | 1000.0000 | 1000.0000 | True |
| ARM64 | etcd | mixed.pod.PodScheduled.p50 | ms | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| ARM64 | etcd | mixed.pod.PodScheduled.p99 | ms | 3 | 0.0000 | 0.0000 | 1000.0000 | True |
| ARM64 | etcd | mixed.pod.Ready.p50 | ms | 3 | 1000.0000 | 1000.0000 | 1000.0000 | True |
| ARM64 | etcd | mixed.pod.Ready.p99 | ms | 3 | 1000.0000 | 2000.0000 | 2000.0000 | True |
| ARM64 | etcd | mixed.revision.member1 | revisions | 3 | 2494.0000 | 2500.0000 | 2504.0000 | True |
| ARM64 | etcd | mixed.revision.member2 | revisions | 3 | 2494.0000 | 2500.0000 | 2504.0000 | True |
| ARM64 | etcd | mixed.revision.member3 | revisions | 3 | 2494.0000 | 2500.0000 | 2504.0000 | True |
| ARM64 | etcd | mixed.server.external-etcd-1.etcd_mvcc_delete_total | counter increments/s | 3 | 7.3054 | 7.3127 | 7.4799 | True |
| ARM64 | etcd | mixed.server.external-etcd-1.etcd_mvcc_put_total | counter increments/s | 3 | 48.4241 | 48.4653 | 49.3944 | True |
| ARM64 | etcd | mixed.server.external-etcd-1.etcd_mvcc_range_total | counter increments/s | 3 | 67.0853 | 71.1042 | 72.6138 | True |
| ARM64 | etcd | mixed.server.external-etcd-1.etcd_network_client_grpc_received_bytes_total | counter increments/s | 3 | 24747.9443 | 25320.6676 | 25483.9483 | True |
| ARM64 | etcd | mixed.server.external-etcd-1.etcd_network_client_grpc_sent_bytes_total | counter increments/s | 3 | 20997.2519 | 42183.2660 | 56480.8411 | True |
| ARM64 | etcd | mixed.server.external-etcd-1.etcd_server_proposals_applied_total | counter increments/s | 3 | 56.3164 | 56.3721 | 57.4900 | True |
| ARM64 | etcd | mixed.server.external-etcd-1.etcd_server_proposals_committed_total | counter increments/s | 3 | 56.3164 | 56.3721 | 57.4900 | True |
| ARM64 | etcd | mixed.server.external-etcd-1.grpc_server_handled_total | counter increments/s | 3 | 27.3286 | 27.3333 | 27.9354 | True |
| ARM64 | etcd | mixed.server.external-etcd-2.etcd_mvcc_delete_total | counter increments/s | 3 | 7.3055 | 7.3117 | 7.4794 | True |
| ARM64 | etcd | mixed.server.external-etcd-2.etcd_mvcc_put_total | counter increments/s | 3 | 48.4174 | 48.4654 | 49.3914 | True |
| ARM64 | etcd | mixed.server.external-etcd-2.etcd_mvcc_range_total | counter increments/s | 3 | 66.9741 | 67.2006 | 69.9511 | True |
| ARM64 | etcd | mixed.server.external-etcd-2.etcd_network_client_grpc_received_bytes_total | counter increments/s | 3 | 24648.1308 | 25072.8217 | 25085.5462 | True |
| ARM64 | etcd | mixed.server.external-etcd-2.etcd_network_client_grpc_sent_bytes_total | counter increments/s | 3 | 19429.6542 | 20248.3117 | 27473.9601 | True |
| ARM64 | etcd | mixed.server.external-etcd-2.etcd_server_proposals_applied_total | counter increments/s | 3 | 56.3086 | 56.3723 | 57.4865 | True |
| ARM64 | etcd | mixed.server.external-etcd-2.etcd_server_proposals_committed_total | counter increments/s | 3 | 56.3086 | 56.3723 | 57.4865 | True |
| ARM64 | etcd | mixed.server.external-etcd-2.grpc_server_handled_total | counter increments/s | 3 | 27.3964 | 27.4177 | 27.9337 | True |
| ARM64 | etcd | mixed.server.external-etcd-3.etcd_mvcc_delete_total | counter increments/s | 3 | 7.3070 | 7.3111 | 7.4805 | True |
| ARM64 | etcd | mixed.server.external-etcd-3.etcd_mvcc_put_total | counter increments/s | 3 | 48.4535 | 48.4581 | 49.3989 | True |
| ARM64 | etcd | mixed.server.external-etcd-3.etcd_mvcc_range_total | counter increments/s | 3 | 71.0823 | 79.4638 | 79.7315 | True |
| ARM64 | etcd | mixed.server.external-etcd-3.etcd_network_client_grpc_received_bytes_total | counter increments/s | 3 | 25040.1520 | 25115.2368 | 25631.9588 | True |
| ARM64 | etcd | mixed.server.external-etcd-3.etcd_network_client_grpc_sent_bytes_total | counter increments/s | 3 | 57275.7291 | 81084.4736 | 100307.5446 | True |
| ARM64 | etcd | mixed.server.external-etcd-3.etcd_server_proposals_applied_total | counter increments/s | 3 | 56.3487 | 56.3620 | 57.4952 | True |
| ARM64 | etcd | mixed.server.external-etcd-3.etcd_server_proposals_committed_total | counter increments/s | 3 | 56.3487 | 56.3620 | 57.4952 | True |
| ARM64 | etcd | mixed.server.external-etcd-3.grpc_server_handled_total | counter increments/s | 3 | 27.1117 | 27.4834 | 27.9608 | True |
| ARM64 | etcd | mixed.storage.etcd_request_duration_seconds | RPC/s | 3 | 80.3855 | 80.5969 | 82.3175 | True |
| ARM64 | etcd | mixed.window | s | 3 | 43.8060 | 44.8156 | 44.8339 | True |
| ARM64 | etcd | node.api.all.p50 | ms | 3 | 16.7220 | 19.6737 | 20.4602 | True |
| ARM64 | etcd | node.api.all.p99 | ms | 3 | 186.3235 | 261.8889 | 319.8788 | True |
| ARM64 | etcd | node.api.creates.count | requests | 3 | 483.0000 | 483.0000 | 483.0000 | True |
| ARM64 | etcd | node.api.creates.rate | requests/s | 3 | 24.7747 | 24.8684 | 25.0279 | True |
| ARM64 | etcd | node.api.deletes.count | requests | 3 | 16.0000 | 16.0000 | 16.0000 | True |
| ARM64 | etcd | node.api.deletes.rate | requests/s | 3 | 0.8207 | 0.8238 | 0.8291 | True |
| ARM64 | etcd | node.api.http403 | responses | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| ARM64 | etcd | node.api.http404 | responses | 3 | 20.0000 | 21.0000 | 23.0000 | True |
| ARM64 | etcd | node.api.http409 | responses | 3 | 2.0000 | 3.0000 | 5.0000 | True |
| ARM64 | etcd | node.api.http422 | responses | 1 | 0.0000 | 0.0000 | 0.0000 | True |
| ARM64 | etcd | node.api.http500 | responses | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| ARM64 | etcd | node.api.requests | requests/s | 3 | 67.8099 | 68.9173 | 69.1476 | True |
| ARM64 | etcd | node.api.resource..p50 | ms | 3 | 2.8704 | 3.1522 | 3.5714 | True |
| ARM64 | etcd | node.api.resource..p99 | ms | 3 | 47.4167 | 170.0000 | 342.0000 | True |
| ARM64 | etcd | node.api.resource.configmaps.p50 | ms | 3 | 15.0000 | 15.0000 | 15.0000 | True |
| ARM64 | etcd | node.api.resource.configmaps.p99 | ms | 3 | 24.8000 | 24.8000 | 24.8000 | True |
| ARM64 | etcd | node.api.resource.endpoints.p50 | ms | 3 | 5.0000 | 5.0000 | 5.0000 | True |
| ARM64 | etcd | node.api.resource.endpoints.p99 | ms | 3 | 24.6000 | 24.6000 | 24.6000 | True |
| ARM64 | etcd | node.api.resource.endpointslices.p50 | ms | 3 | 5.0000 | 5.0000 | 15.0000 | True |
| ARM64 | etcd | node.api.resource.endpointslices.p99 | ms | 3 | 24.6000 | 24.6000 | 24.8000 | True |
| ARM64 | etcd | node.api.resource.events.p50 | ms | 3 | 19.3850 | 21.2234 | 21.4851 | True |
| ARM64 | etcd | node.api.resource.events.p99 | ms | 3 | 184.6000 | 271.6667 | 330.0000 | True |
| ARM64 | etcd | node.api.resource.leases.p50 | ms | 3 | 4.3750 | 4.5000 | 13.0000 | True |
| ARM64 | etcd | node.api.resource.leases.p99 | ms | 3 | 172.0000 | 187.0000 | 346.0000 | True |
| ARM64 | etcd | node.api.resource.limitranges.p50 | ms | 3 | 2.5000 | 2.5000 | 15.0000 | True |
| ARM64 | etcd | node.api.resource.limitranges.p99 | ms | 3 | 4.9500 | 4.9500 | 24.8000 | True |
| ARM64 | etcd | node.api.resource.namespaces.p50 | ms | 3 | 2.5000 | 2.5000 | 2.5000 | True |
| ARM64 | etcd | node.api.resource.namespaces.p99 | ms | 3 | 4.9500 | 4.9500 | 4.9500 | True |
| ARM64 | etcd | node.api.resource.nodes.p50 | ms | 3 | 2.5000 | 2.5000 | 2.5000 | True |
| ARM64 | etcd | node.api.resource.nodes.p99 | ms | 3 | 4.9500 | 4.9500 | 4.9500 | True |
| ARM64 | etcd | node.api.resource.pods.p50 | ms | 3 | 16.4579 | 19.9036 | 20.9005 | True |
| ARM64 | etcd | node.api.resource.pods.p99 | ms | 3 | 188.6429 | 256.5455 | 317.0476 | True |
| ARM64 | etcd | node.api.resource.serviceaccounts.p50 | ms | 3 | 2.5000 | 5.0000 | 15.0000 | True |
| ARM64 | etcd | node.api.resource.serviceaccounts.p99 | ms | 3 | 4.9500 | 24.6000 | 24.8000 | True |
| ARM64 | etcd | node.api.updates.count | requests | 3 | 42.0000 | 43.0000 | 44.0000 | True |
| ARM64 | etcd | node.api.updates.rate | requests/s | 3 | 2.1625 | 2.2056 | 2.2800 | True |
| ARM64 | etcd | node.api.verb.DELETE.p50 | ms | 3 | 15.0000 | 15.0000 | 16.4286 | True |
| ARM64 | etcd | node.api.verb.DELETE.p99 | ms | 3 | 24.8000 | 24.8000 | 48.0000 | True |
| ARM64 | etcd | node.api.verb.GET.p50 | ms | 3 | 8.0723 | 12.0635 | 13.9655 | True |
| ARM64 | etcd | node.api.verb.GET.p99 | ms | 3 | 142.7143 | 165.5556 | 287.1429 | True |
| ARM64 | etcd | node.api.verb.LIST.p50 | ms | 3 | 5.0000 | 10.0000 | 15.0000 | True |
| ARM64 | etcd | node.api.verb.LIST.p99 | ms | 3 | 91.0000 | 91.0000 | 191.0000 | True |
| ARM64 | etcd | node.api.verb.PATCH.p50 | ms | 3 | 20.0920 | 22.1895 | 23.9809 | True |
| ARM64 | etcd | node.api.verb.PATCH.p99 | ms | 3 | 194.8000 | 248.5000 | 347.5000 | True |
| ARM64 | etcd | node.api.verb.POST.p50 | ms | 3 | 20.4874 | 22.9615 | 23.1139 | True |
| ARM64 | etcd | node.api.verb.POST.p99 | ms | 3 | 190.4474 | 274.8889 | 346.3810 | True |
| ARM64 | etcd | node.api.verb.PUT.p50 | ms | 3 | 4.3750 | 4.5000 | 13.0000 | True |
| ARM64 | etcd | node.api.verb.PUT.p99 | ms | 3 | 172.0000 | 187.0000 | 346.0000 | True |
| ARM64 | etcd | node.cpu.all | CPU s | 3 | 3.1500 | 3.2800 | 3.7000 | True |
| ARM64 | etcd | node.cpu.cores | cores | 3 | 0.1632 | 0.1682 | 0.1904 | True |
| ARM64 | etcd | node.cpu.member1 | CPU s | 3 | 0.9900 | 1.0300 | 1.1200 | True |
| ARM64 | etcd | node.cpu.member2 | CPU s | 3 | 1.1800 | 1.2700 | 1.3900 | True |
| ARM64 | etcd | node.cpu.member3 | CPU s | 3 | 0.9800 | 0.9800 | 1.1900 | True |
| ARM64 | etcd | node.duration | s | 3 | 18.6328 | 18.6921 | 18.8276 | True |
| ARM64 | etcd | node.exit | exit code | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| ARM64 | etcd | node.live_inventory | objects in tracked kinds | 3 | 1177.0000 | 1177.0000 | 1177.0000 | True |
| ARM64 | etcd | node.net_live_creation_rate | objects/s | 3 | 4.3022 | 4.3334 | 4.3472 | True |
| ARM64 | etcd | node.pod.ContainersReady.p50 | ms | 3 | 3000.0000 | 4000.0000 | 4000.0000 | True |
| ARM64 | etcd | node.pod.ContainersReady.p99 | ms | 3 | 5500.0000 | 6000.0000 | 6000.0000 | True |
| ARM64 | etcd | node.pod.ContainersStarted.p50 | ms | 3 | 3589.0000 | 4012.0000 | 4400.0000 | True |
| ARM64 | etcd | node.pod.ContainersStarted.p99 | ms | 3 | 5916.0000 | 6469.0000 | 6611.0000 | True |
| ARM64 | etcd | node.pod.Initialized.p50 | ms | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| ARM64 | etcd | node.pod.Initialized.p99 | ms | 3 | 1000.0000 | 1000.0000 | 1000.0000 | True |
| ARM64 | etcd | node.pod.PodReadyToStartContainers.p50 | ms | 3 | 2000.0000 | 2000.0000 | 2000.0000 | True |
| ARM64 | etcd | node.pod.PodReadyToStartContainers.p99 | ms | 3 | 4000.0000 | 4000.0000 | 5000.0000 | True |
| ARM64 | etcd | node.pod.PodScheduled.p50 | ms | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| ARM64 | etcd | node.pod.PodScheduled.p99 | ms | 3 | 1000.0000 | 1000.0000 | 1000.0000 | True |
| ARM64 | etcd | node.pod.Ready.p50 | ms | 3 | 3000.0000 | 4000.0000 | 4000.0000 | True |
| ARM64 | etcd | node.pod.Ready.p99 | ms | 3 | 5500.0000 | 6000.0000 | 6000.0000 | True |
| ARM64 | etcd | node.revision.member1 | revisions | 3 | 902.0000 | 907.0000 | 913.0000 | True |
| ARM64 | etcd | node.revision.member2 | revisions | 3 | 902.0000 | 907.0000 | 913.0000 | True |
| ARM64 | etcd | node.revision.member3 | revisions | 3 | 902.0000 | 907.0000 | 913.0000 | True |
| ARM64 | etcd | node.server.external-etcd-1.etcd_mvcc_delete_total | counter increments/s | 3 | 0.8202 | 0.8232 | 0.8284 | True |
| ARM64 | etcd | node.server.external-etcd-1.etcd_mvcc_put_total | counter increments/s | 3 | 45.4186 | 46.1332 | 46.1494 | True |
| ARM64 | etcd | node.server.external-etcd-1.etcd_mvcc_range_total | counter increments/s | 3 | 57.2109 | 74.2793 | 76.6298 | True |
| ARM64 | etcd | node.server.external-etcd-1.etcd_network_client_grpc_received_bytes_total | counter increments/s | 3 | 33593.1699 | 34467.1862 | 35353.3124 | True |
| ARM64 | etcd | node.server.external-etcd-1.etcd_network_client_grpc_sent_bytes_total | counter increments/s | 3 | 25137.0287 | 170559.3931 | 175296.6397 | True |
| ARM64 | etcd | node.server.external-etcd-1.etcd_server_proposals_applied_total | counter increments/s | 3 | 46.7514 | 47.4794 | 47.7443 | True |
| ARM64 | etcd | node.server.external-etcd-1.etcd_server_proposals_committed_total | counter increments/s | 3 | 46.7514 | 47.4794 | 47.7443 | True |
| ARM64 | etcd | node.server.external-etcd-1.grpc_server_handled_total | counter increments/s | 3 | 23.8370 | 24.0763 | 24.4895 | True |
| ARM64 | etcd | node.server.external-etcd-2.etcd_mvcc_delete_total | counter increments/s | 3 | 0.8202 | 0.8234 | 0.8281 | True |
| ARM64 | etcd | node.server.external-etcd-2.etcd_mvcc_put_total | counter increments/s | 3 | 45.4206 | 46.1126 | 46.1617 | True |
| ARM64 | etcd | node.server.external-etcd-2.etcd_mvcc_range_total | counter increments/s | 3 | 55.2212 | 56.0836 | 56.5571 | True |
| ARM64 | etcd | node.server.external-etcd-2.etcd_network_client_grpc_received_bytes_total | counter increments/s | 3 | 33398.2097 | 34026.1988 | 34063.7333 | True |
| ARM64 | etcd | node.server.external-etcd-2.etcd_network_client_grpc_sent_bytes_total | counter increments/s | 3 | 31645.5283 | 34711.1674 | 55838.4863 | True |
| ARM64 | etcd | node.server.external-etcd-2.etcd_server_proposals_applied_total | counter increments/s | 3 | 46.7535 | 47.4581 | 47.7570 | True |
| ARM64 | etcd | node.server.external-etcd-2.etcd_server_proposals_committed_total | counter increments/s | 3 | 46.7535 | 47.4581 | 47.7570 | True |
| ARM64 | etcd | node.server.external-etcd-2.grpc_server_handled_total | counter increments/s | 3 | 24.1690 | 24.1970 | 24.6504 | True |
| ARM64 | etcd | node.server.external-etcd-3.etcd_mvcc_delete_total | counter increments/s | 3 | 0.8203 | 0.8236 | 0.8281 | True |
| ARM64 | etcd | node.server.external-etcd-3.etcd_mvcc_put_total | counter increments/s | 3 | 45.4259 | 46.0641 | 46.1744 | True |
| ARM64 | etcd | node.server.external-etcd-3.etcd_mvcc_range_total | counter increments/s | 3 | 55.1161 | 56.1568 | 76.6484 | True |
| ARM64 | etcd | node.server.external-etcd-3.etcd_network_client_grpc_received_bytes_total | counter increments/s | 3 | 32692.9000 | 32861.9491 | 35486.5106 | True |
| ARM64 | etcd | node.server.external-etcd-3.etcd_network_client_grpc_sent_bytes_total | counter increments/s | 3 | 23677.2965 | 57356.5804 | 203249.5771 | True |
| ARM64 | etcd | node.server.external-etcd-3.etcd_server_proposals_applied_total | counter increments/s | 3 | 46.7589 | 47.4098 | 47.7186 | True |
| ARM64 | etcd | node.server.external-etcd-3.etcd_server_proposals_committed_total | counter increments/s | 3 | 46.7589 | 47.4098 | 47.7186 | True |
| ARM64 | etcd | node.server.external-etcd-3.grpc_server_handled_total | counter increments/s | 3 | 23.7896 | 24.2225 | 24.7087 | True |
| ARM64 | etcd | node.storage.etcd_request_duration_seconds | RPC/s | 3 | 70.1694 | 70.9382 | 72.2369 | True |
| ARM64 | etcd | node.window | s | 3 | 19.2985 | 19.4222 | 19.4957 | True |
| ARM64 | etcd | total.api.all.p50 | ms | 3 | 3.7521 | 3.9020 | 4.3394 | True |
| ARM64 | etcd | total.api.all.p99 | ms | 3 | 188.1171 | 193.2941 | 259.8113 | True |
| ARM64 | etcd | total.api.creates.count | requests | 3 | 2715.0000 | 2715.0000 | 2715.0000 | True |
| ARM64 | etcd | total.api.creates.rate | requests/s | 3 | 11.9151 | 11.9219 | 12.0871 | True |
| ARM64 | etcd | total.api.deletes.count | requests | 3 | 1674.0000 | 1676.0000 | 1680.0000 | True |
| ARM64 | etcd | total.api.deletes.rate | requests/s | 3 | 7.3595 | 7.3729 | 7.4526 | True |
| ARM64 | etcd | total.api.http403 | responses | 3 | 23.0000 | 26.0000 | 28.0000 | True |
| ARM64 | etcd | total.api.http404 | responses | 3 | 85.0000 | 90.0000 | 96.0000 | True |
| ARM64 | etcd | total.api.http409 | responses | 3 | 63.0000 | 63.0000 | 70.0000 | True |
| ARM64 | etcd | total.api.http422 | responses | 1 | 0.0000 | 0.0000 | 0.0000 | True |
| ARM64 | etcd | total.api.http500 | responses | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| ARM64 | etcd | total.api.requests | requests/s | 3 | 55.8670 | 56.1887 | 56.7001 | True |
| ARM64 | etcd | total.api.resource..p50 | ms | 3 | 2.6218 | 2.7047 | 2.7381 | True |
| ARM64 | etcd | total.api.resource..p99 | ms | 3 | 36.1667 | 44.3333 | 110.0000 | True |
| ARM64 | etcd | total.api.resource.configmaps.p50 | ms | 3 | 2.7874 | 2.8441 | 3.0001 | True |
| ARM64 | etcd | total.api.resource.configmaps.p99 | ms | 3 | 57.3125 | 64.1667 | 67.0205 | True |
| ARM64 | etcd | total.api.resource.controllerrevisions.p50 | ms | 3 | 3.2353 | 3.6667 | 5.0000 | True |
| ARM64 | etcd | total.api.resource.controllerrevisions.p99 | ms | 3 | 97.2500 | 98.4286 | 178.0000 | True |
| ARM64 | etcd | total.api.resource.cronjobs.p50 | ms | 3 | 4.2308 | 4.5833 | 25.0000 | True |
| ARM64 | etcd | total.api.resource.cronjobs.p99 | ms | 3 | 98.6250 | 98.7778 | 178.0000 | True |
| ARM64 | etcd | total.api.resource.csistoragecapacities.p50 | ms | 3 | 3.0556 | 5.0000 | 5.0000 | True |
| ARM64 | etcd | total.api.resource.csistoragecapacities.p99 | ms | 3 | 178.0000 | 178.0000 | 178.0000 | True |
| ARM64 | etcd | total.api.resource.daemonsets.p50 | ms | 3 | 3.6667 | 3.9286 | 5.0000 | True |
| ARM64 | etcd | total.api.resource.daemonsets.p99 | ms | 3 | 98.1667 | 178.0000 | 192.6667 | True |
| ARM64 | etcd | total.api.resource.deployments.p50 | ms | 3 | 5.6024 | 5.6289 | 10.2041 | True |
| ARM64 | etcd | total.api.resource.deployments.p99 | ms | 3 | 96.6667 | 97.2500 | 144.2857 | True |
| ARM64 | etcd | total.api.resource.endpoints.p50 | ms | 3 | 13.5222 | 14.1220 | 15.9796 | True |
| ARM64 | etcd | total.api.resource.endpoints.p99 | ms | 3 | 197.8125 | 274.5714 | 388.4000 | True |
| ARM64 | etcd | total.api.resource.endpointslices.p50 | ms | 3 | 14.7638 | 15.0000 | 16.7350 | True |
| ARM64 | etcd | total.api.resource.endpointslices.p99 | ms | 3 | 229.6000 | 290.4000 | 389.6000 | True |
| ARM64 | etcd | total.api.resource.events.p50 | ms | 3 | 10.3542 | 12.5246 | 13.9022 | True |
| ARM64 | etcd | total.api.resource.events.p99 | ms | 3 | 231.2500 | 331.8182 | 352.0833 | True |
| ARM64 | etcd | total.api.resource.horizontalpodautoscalers.p50 | ms | 3 | 3.9286 | 4.5833 | 5.0000 | True |
| ARM64 | etcd | total.api.resource.horizontalpodautoscalers.p99 | ms | 3 | 98.1667 | 98.1667 | 98.6250 | True |
| ARM64 | etcd | total.api.resource.ingresses.p50 | ms | 3 | 4.2308 | 5.0000 | 5.0000 | True |
| ARM64 | etcd | total.api.resource.ingresses.p99 | ms | 3 | 98.7778 | 178.0000 | 189.0000 | True |
| ARM64 | etcd | total.api.resource.ipaddresses.p50 | ms | 3 | 4.1667 | 4.7619 | 4.7619 | True |
| ARM64 | etcd | total.api.resource.ipaddresses.p99 | ms | 3 | 96.0000 | 180.0000 | 190.0000 | True |
| ARM64 | etcd | total.api.resource.jobs.p50 | ms | 3 | 3.2353 | 4.2308 | 5.0000 | True |
| ARM64 | etcd | total.api.resource.jobs.p99 | ms | 3 | 97.2500 | 98.1667 | 178.0000 | True |
| ARM64 | etcd | total.api.resource.leases.p50 | ms | 3 | 2.8177 | 2.8596 | 2.9355 | True |
| ARM64 | etcd | total.api.resource.leases.p99 | ms | 3 | 98.1500 | 98.6923 | 152.6000 | True |
| ARM64 | etcd | total.api.resource.limitranges.p50 | ms | 3 | 3.6765 | 4.0000 | 4.0517 | True |
| ARM64 | etcd | total.api.resource.limitranges.p99 | ms | 3 | 97.0625 | 152.0000 | 175.0000 | True |
| ARM64 | etcd | total.api.resource.namespaces.p50 | ms | 3 | 2.7892 | 2.9392 | 2.9552 | True |
| ARM64 | etcd | total.api.resource.namespaces.p99 | ms | 3 | 24.2540 | 24.2769 | 24.3619 | True |
| ARM64 | etcd | total.api.resource.networkpolicies.p50 | ms | 3 | 2.7500 | 3.6667 | 3.9286 | True |
| ARM64 | etcd | total.api.resource.networkpolicies.p99 | ms | 3 | 89.0000 | 97.8000 | 97.8000 | True |
| ARM64 | etcd | total.api.resource.nodes.p50 | ms | 3 | 2.5000 | 3.0000 | 3.0000 | True |
| ARM64 | etcd | total.api.resource.nodes.p99 | ms | 3 | 4.9500 | 23.8000 | 23.8000 | True |
| ARM64 | etcd | total.api.resource.persistentvolumeclaims.p50 | ms | 3 | 3.6667 | 4.2308 | 4.5833 | True |
| ARM64 | etcd | total.api.resource.persistentvolumeclaims.p99 | ms | 3 | 98.1667 | 178.0000 | 189.0000 | True |
| ARM64 | etcd | total.api.resource.podcertificaterequests.p50 | ms | 3 | 3.4375 | 3.6667 | 3.9286 | True |
| ARM64 | etcd | total.api.resource.podcertificaterequests.p99 | ms | 3 | 94.5000 | 97.8000 | 178.0000 | True |
| ARM64 | etcd | total.api.resource.poddisruptionbudgets.p50 | ms | 3 | 3.4375 | 4.2308 | 4.2308 | True |
| ARM64 | etcd | total.api.resource.poddisruptionbudgets.p99 | ms | 3 | 97.8000 | 97.8000 | 178.0000 | True |
| ARM64 | etcd | total.api.resource.pods.p50 | ms | 3 | 8.9892 | 11.3445 | 14.5034 | True |
| ARM64 | etcd | total.api.resource.pods.p99 | ms | 3 | 406.9524 | 455.2500 | 564.0000 | True |
| ARM64 | etcd | total.api.resource.podtemplates.p50 | ms | 3 | 3.4375 | 4.2308 | 58.3333 | True |
| ARM64 | etcd | total.api.resource.podtemplates.p99 | ms | 3 | 98.4286 | 178.0000 | 189.0000 | True |
| ARM64 | etcd | total.api.resource.replicasets.p50 | ms | 3 | 8.2422 | 9.4167 | 12.6238 | True |
| ARM64 | etcd | total.api.resource.replicasets.p99 | ms | 3 | 113.5000 | 130.0000 | 134.8750 | True |
| ARM64 | etcd | total.api.resource.replicationcontrollers.p50 | ms | 3 | 4.5833 | 11.6667 | 50.0000 | True |
| ARM64 | etcd | total.api.resource.replicationcontrollers.p99 | ms | 3 | 178.0000 | 192.6667 | 192.6667 | True |
| ARM64 | etcd | total.api.resource.resourceclaims.p50 | ms | 3 | 3.4375 | 3.9286 | 5.0000 | True |
| ARM64 | etcd | total.api.resource.resourceclaims.p99 | ms | 3 | 98.1667 | 98.6250 | 178.0000 | True |
| ARM64 | etcd | total.api.resource.resourceclaimtemplates.p50 | ms | 3 | 4.2308 | 4.5833 | 4.5833 | True |
| ARM64 | etcd | total.api.resource.resourceclaimtemplates.p99 | ms | 3 | 98.4286 | 98.6250 | 178.0000 | True |
| ARM64 | etcd | total.api.resource.resourcequotas.p50 | ms | 3 | 4.5833 | 4.5833 | 11.6667 | True |
| ARM64 | etcd | total.api.resource.resourcequotas.p99 | ms | 3 | 98.6250 | 178.0000 | 189.0000 | True |
| ARM64 | etcd | total.api.resource.rolebindings.p50 | ms | 3 | 3.4375 | 4.2308 | 5.0000 | True |
| ARM64 | etcd | total.api.resource.rolebindings.p99 | ms | 3 | 97.2500 | 98.4286 | 189.0000 | True |
| ARM64 | etcd | total.api.resource.roles.p50 | ms | 3 | 3.2353 | 3.9286 | 9.0000 | True |
| ARM64 | etcd | total.api.resource.roles.p99 | ms | 3 | 98.1667 | 98.4286 | 178.0000 | True |
| ARM64 | etcd | total.api.resource.secrets.p50 | ms | 3 | 2.7013 | 2.7381 | 2.9167 | True |
| ARM64 | etcd | total.api.resource.secrets.p99 | ms | 3 | 83.9000 | 86.5833 | 94.9167 | True |
| ARM64 | etcd | total.api.resource.serviceaccounts.p50 | ms | 3 | 15.0000 | 18.6842 | 24.0909 | True |
| ARM64 | etcd | total.api.resource.serviceaccounts.p99 | ms | 3 | 97.6429 | 97.8889 | 98.7308 | True |
| ARM64 | etcd | total.api.resource.services.p50 | ms | 3 | 4.1667 | 4.6154 | 4.6875 | True |
| ARM64 | etcd | total.api.resource.services.p99 | ms | 3 | 176.0000 | 280.0000 | 340.0000 | True |
| ARM64 | etcd | total.api.resource.statefulsets.p50 | ms | 3 | 3.6667 | 3.9286 | 3.9286 | True |
| ARM64 | etcd | total.api.resource.statefulsets.p99 | ms | 3 | 178.0000 | 189.0000 | 189.0000 | True |
| ARM64 | etcd | total.api.updates.count | requests | 3 | 4678.0000 | 4685.0000 | 4710.0000 | True |
| ARM64 | etcd | total.api.updates.rate | requests/s | 3 | 20.5299 | 20.6821 | 20.8574 | True |
| ARM64 | etcd | total.api.verb.DELETE.p50 | ms | 3 | 3.3989 | 3.4302 | 3.5363 | True |
| ARM64 | etcd | total.api.verb.DELETE.p99 | ms | 3 | 504.4800 | 542.1935 | 683.3333 | True |
| ARM64 | etcd | total.api.verb.GET.p50 | ms | 3 | 3.3522 | 3.5343 | 3.7570 | True |
| ARM64 | etcd | total.api.verb.GET.p99 | ms | 3 | 121.2105 | 141.0833 | 194.2727 | True |
| ARM64 | etcd | total.api.verb.LIST.p50 | ms | 3 | 4.0976 | 4.1103 | 4.2069 | True |
| ARM64 | etcd | total.api.verb.LIST.p99 | ms | 3 | 160.0714 | 173.4762 | 175.9130 | True |
| ARM64 | etcd | total.api.verb.PATCH.p50 | ms | 3 | 3.1856 | 3.3212 | 3.7019 | True |
| ARM64 | etcd | total.api.verb.PATCH.p99 | ms | 3 | 95.6569 | 96.2115 | 99.0625 | True |
| ARM64 | etcd | total.api.verb.POST.p50 | ms | 3 | 4.3814 | 4.6244 | 6.6547 | True |
| ARM64 | etcd | total.api.verb.POST.p99 | ms | 3 | 168.6721 | 175.6613 | 206.2667 | True |
| ARM64 | etcd | total.api.verb.PUT.p50 | ms | 3 | 8.3926 | 9.1103 | 12.2932 | True |
| ARM64 | etcd | total.api.verb.PUT.p99 | ms | 3 | 180.7561 | 196.4286 | 202.2222 | True |
| ARM64 | etcd | total.cpu.all | CPU s | 3 | 48.7500 | 50.9700 | 54.8000 | True |
| ARM64 | etcd | total.cpu.cores | cores | 3 | 0.2170 | 0.2236 | 0.2406 | True |
| ARM64 | etcd | total.cpu.member1 | CPU s | 3 | 14.5400 | 15.4100 | 16.2500 | True |
| ARM64 | etcd | total.cpu.member2 | CPU s | 3 | 18.5000 | 19.5700 | 20.6500 | True |
| ARM64 | etcd | total.cpu.member3 | CPU s | 3 | 15.7100 | 15.9900 | 17.9000 | True |
| ARM64 | etcd | total.revision.member1 | revisions | 3 | 11655.0000 | 11658.0000 | 11691.0000 | True |
| ARM64 | etcd | total.revision.member2 | revisions | 3 | 11654.0000 | 11658.0000 | 11691.0000 | True |
| ARM64 | etcd | total.revision.member3 | revisions | 3 | 11654.0000 | 11658.0000 | 11691.0000 | True |
| ARM64 | etcd | total.script | s | 3 | 284.4370 | 285.6443 | 289.1188 | True |
| ARM64 | etcd | total.server.external-etcd-1.etcd_mvcc_delete_total | counter increments/s | 3 | 11.9118 | 11.9191 | 12.0843 | True |
| ARM64 | etcd | total.server.external-etcd-1.etcd_mvcc_put_total | counter increments/s | 3 | 39.2367 | 39.4055 | 39.7871 | True |
| ARM64 | etcd | total.server.external-etcd-1.etcd_mvcc_range_total | counter increments/s | 3 | 66.0490 | 70.9794 | 72.7391 | True |
| ARM64 | etcd | total.server.external-etcd-1.etcd_network_client_grpc_received_bytes_total | counter increments/s | 3 | 24289.0982 | 24563.5886 | 25105.8871 | True |
| ARM64 | etcd | total.server.external-etcd-1.etcd_network_client_grpc_sent_bytes_total | counter increments/s | 3 | 33080.5997 | 65022.0409 | 86276.7159 | True |
| ARM64 | etcd | total.server.external-etcd-1.etcd_server_proposals_applied_total | counter increments/s | 3 | 52.1226 | 52.2773 | 52.8150 | True |
| ARM64 | etcd | total.server.external-etcd-1.etcd_server_proposals_committed_total | counter increments/s | 3 | 52.1226 | 52.2773 | 52.8150 | True |
| ARM64 | etcd | total.server.external-etcd-1.grpc_server_handled_total | counter increments/s | 3 | 27.2020 | 27.3108 | 27.5959 | True |
| ARM64 | etcd | total.server.external-etcd-2.etcd_mvcc_delete_total | counter increments/s | 3 | 11.9118 | 11.9190 | 12.0844 | True |
| ARM64 | etcd | total.server.external-etcd-2.etcd_mvcc_put_total | counter increments/s | 3 | 39.2364 | 39.4053 | 39.7873 | True |
| ARM64 | etcd | total.server.external-etcd-2.etcd_mvcc_range_total | counter increments/s | 3 | 63.6648 | 64.3833 | 70.0622 | True |
| ARM64 | etcd | total.server.external-etcd-2.etcd_network_client_grpc_received_bytes_total | counter increments/s | 3 | 25261.6173 | 25587.0858 | 25672.3954 | True |
| ARM64 | etcd | total.server.external-etcd-2.etcd_network_client_grpc_sent_bytes_total | counter increments/s | 3 | 24429.4443 | 32079.3224 | 33971.9390 | True |
| ARM64 | etcd | total.server.external-etcd-2.etcd_server_proposals_applied_total | counter increments/s | 3 | 52.1222 | 52.2770 | 52.8153 | True |
| ARM64 | etcd | total.server.external-etcd-2.etcd_server_proposals_committed_total | counter increments/s | 3 | 52.1222 | 52.2770 | 52.8153 | True |
| ARM64 | etcd | total.server.external-etcd-2.grpc_server_handled_total | counter increments/s | 3 | 27.2106 | 27.3282 | 27.6272 | True |
| ARM64 | etcd | total.server.external-etcd-3.etcd_mvcc_delete_total | counter increments/s | 3 | 11.9117 | 11.9191 | 12.0844 | True |
| ARM64 | etcd | total.server.external-etcd-3.etcd_mvcc_put_total | counter increments/s | 3 | 39.2364 | 39.4054 | 39.7872 | True |
| ARM64 | etcd | total.server.external-etcd-3.etcd_mvcc_range_total | counter increments/s | 3 | 81.9344 | 92.4422 | 95.7477 | True |
| ARM64 | etcd | total.server.external-etcd-3.etcd_network_client_grpc_received_bytes_total | counter increments/s | 3 | 24461.7381 | 24789.6227 | 25380.9601 | True |
| ARM64 | etcd | total.server.external-etcd-3.etcd_network_client_grpc_sent_bytes_total | counter increments/s | 3 | 64359.2931 | 89833.5791 | 127642.9798 | True |
| ARM64 | etcd | total.server.external-etcd-3.etcd_server_proposals_applied_total | counter increments/s | 3 | 52.1221 | 52.2771 | 52.8152 | True |
| ARM64 | etcd | total.server.external-etcd-3.etcd_server_proposals_committed_total | counter increments/s | 3 | 52.1221 | 52.2771 | 52.8152 | True |
| ARM64 | etcd | total.server.external-etcd-3.grpc_server_handled_total | counter increments/s | 3 | 27.2500 | 27.2932 | 27.5782 | True |
| ARM64 | etcd | total.storage.etcd_request_duration_seconds | RPC/s | 3 | 80.0220 | 80.3135 | 81.1413 | True |
| ARM64 | etcd | total.window | s | 3 | 224.6206 | 227.7326 | 227.8624 | True |
| ARM64 | etcd | total.workload | s | 3 | 223.6474 | 226.7327 | 226.8722 | True |
| ARM64 | etcd | update.api.all.p50 | ms | 3 | 3.4062 | 3.5819 | 4.0402 | True |
| ARM64 | etcd | update.api.all.p99 | ms | 3 | 73.8264 | 91.1962 | 93.5203 | True |
| ARM64 | etcd | update.api.creates.count | requests | 3 | 480.0000 | 480.0000 | 480.0000 | True |
| ARM64 | etcd | update.api.creates.rate | requests/s | 3 | 5.4983 | 5.4983 | 5.4988 | True |
| ARM64 | etcd | update.api.deletes.count | requests | 3 | 64.0000 | 64.0000 | 64.0000 | True |
| ARM64 | etcd | update.api.deletes.rate | requests/s | 3 | 0.7331 | 0.7331 | 0.7332 | True |
| ARM64 | etcd | update.api.http403 | responses | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| ARM64 | etcd | update.api.http404 | responses | 3 | 20.0000 | 25.0000 | 33.0000 | True |
| ARM64 | etcd | update.api.http409 | responses | 3 | 33.0000 | 37.0000 | 43.0000 | True |
| ARM64 | etcd | update.api.http422 | responses | 1 | 0.0000 | 0.0000 | 0.0000 | True |
| ARM64 | etcd | update.api.http500 | responses | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| ARM64 | etcd | update.api.requests | requests/s | 3 | 68.3733 | 68.6026 | 68.6666 | True |
| ARM64 | etcd | update.api.resource..p50 | ms | 3 | 2.6050 | 2.6411 | 2.7331 | True |
| ARM64 | etcd | update.api.resource..p99 | ms | 3 | 20.0400 | 33.8750 | 42.2500 | True |
| ARM64 | etcd | update.api.resource.configmaps.p50 | ms | 3 | 2.7672 | 2.8150 | 2.9803 | True |
| ARM64 | etcd | update.api.resource.configmaps.p99 | ms | 3 | 64.9500 | 74.4804 | 74.9732 | True |
| ARM64 | etcd | update.api.resource.deployments.p50 | ms | 3 | 7.2581 | 7.8205 | 11.5734 | True |
| ARM64 | etcd | update.api.resource.deployments.p99 | ms | 3 | 84.8000 | 115.0000 | 157.3333 | True |
| ARM64 | etcd | update.api.resource.endpoints.p50 | ms | 3 | 13.4091 | 13.4252 | 15.7857 | True |
| ARM64 | etcd | update.api.resource.endpoints.p99 | ms | 3 | 99.7500 | 149.7500 | 171.2857 | True |
| ARM64 | etcd | update.api.resource.endpointslices.p50 | ms | 3 | 13.6842 | 15.1015 | 16.3171 | True |
| ARM64 | etcd | update.api.resource.endpointslices.p99 | ms | 3 | 91.3000 | 151.1667 | 163.5000 | True |
| ARM64 | etcd | update.api.resource.events.p50 | ms | 3 | 4.2054 | 4.9318 | 7.8571 | True |
| ARM64 | etcd | update.api.resource.events.p99 | ms | 3 | 49.3929 | 79.1250 | 113.2000 | True |
| ARM64 | etcd | update.api.resource.leases.p50 | ms | 3 | 2.6078 | 2.7045 | 2.8606 | True |
| ARM64 | etcd | update.api.resource.leases.p99 | ms | 3 | 20.1600 | 24.5250 | 70.2500 | True |
| ARM64 | etcd | update.api.resource.limitranges.p50 | ms | 3 | 3.8636 | 4.2500 | 10.0000 | True |
| ARM64 | etcd | update.api.resource.limitranges.p99 | ms | 3 | 97.3333 | 183.0000 | 183.0000 | True |
| ARM64 | etcd | update.api.resource.nodes.p50 | ms | 3 | 2.5000 | 2.5000 | 2.5000 | True |
| ARM64 | etcd | update.api.resource.nodes.p99 | ms | 3 | 4.9500 | 4.9500 | 4.9500 | True |
| ARM64 | etcd | update.api.resource.pods.p50 | ms | 3 | 3.8186 | 4.5315 | 8.7387 | True |
| ARM64 | etcd | update.api.resource.pods.p99 | ms | 3 | 69.1538 | 94.3684 | 126.7273 | True |
| ARM64 | etcd | update.api.resource.replicasets.p50 | ms | 3 | 8.2663 | 9.4382 | 12.6170 | True |
| ARM64 | etcd | update.api.resource.replicasets.p99 | ms | 3 | 99.9762 | 119.8000 | 142.7143 | True |
| ARM64 | etcd | update.api.resource.secrets.p50 | ms | 3 | 2.5338 | 2.5338 | 2.6786 | True |
| ARM64 | etcd | update.api.resource.secrets.p99 | ms | 3 | 10.0000 | 10.0000 | 22.0000 | True |
| ARM64 | etcd | update.api.resource.services.p50 | ms | 3 | 2.5490 | 2.5490 | 2.7083 | True |
| ARM64 | etcd | update.api.resource.services.p99 | ms | 3 | 14.6000 | 14.6000 | 22.4000 | True |
| ARM64 | etcd | update.api.updates.count | requests | 3 | 4191.0000 | 4194.0000 | 4195.0000 | True |
| ARM64 | etcd | update.api.updates.rate | requests/s | 3 | 48.0068 | 48.0460 | 48.0527 | True |
| ARM64 | etcd | update.api.verb.DELETE.p50 | ms | 3 | 17.0000 | 20.0000 | 20.5556 | True |
| ARM64 | etcd | update.api.verb.DELETE.p99 | ms | 3 | 189.3333 | 272.0000 | 336.0000 | True |
| ARM64 | etcd | update.api.verb.GET.p50 | ms | 3 | 2.8352 | 2.9885 | 3.2088 | True |
| ARM64 | etcd | update.api.verb.GET.p99 | ms | 3 | 24.9727 | 44.4750 | 49.0000 | True |
| ARM64 | etcd | update.api.verb.LIST.p50 | ms | 3 | 3.7121 | 3.8281 | 3.8710 | True |
| ARM64 | etcd | update.api.verb.LIST.p99 | ms | 3 | 94.0000 | 151.0000 | 151.0000 | True |
| ARM64 | etcd | update.api.verb.PATCH.p50 | ms | 3 | 2.9094 | 3.0372 | 3.3458 | True |
| ARM64 | etcd | update.api.verb.PATCH.p99 | ms | 3 | 52.4667 | 68.7115 | 69.5091 | True |
| ARM64 | etcd | update.api.verb.POST.p50 | ms | 3 | 4.4295 | 5.3687 | 8.5857 | True |
| ARM64 | etcd | update.api.verb.POST.p99 | ms | 3 | 69.4286 | 95.0769 | 98.7273 | True |
| ARM64 | etcd | update.api.verb.PUT.p50 | ms | 3 | 9.2320 | 10.2115 | 12.9365 | True |
| ARM64 | etcd | update.api.verb.PUT.p99 | ms | 3 | 96.2826 | 126.3750 | 156.3704 | True |
| ARM64 | etcd | update.cpu.all | CPU s | 3 | 20.0000 | 21.1300 | 23.0600 | True |
| ARM64 | etcd | update.cpu.cores | cores | 3 | 0.2290 | 0.2420 | 0.2642 | True |
| ARM64 | etcd | update.cpu.member1 | CPU s | 3 | 5.9400 | 6.4000 | 6.8000 | True |
| ARM64 | etcd | update.cpu.member2 | CPU s | 3 | 7.5900 | 7.9700 | 8.6200 | True |
| ARM64 | etcd | update.cpu.member3 | CPU s | 3 | 6.4700 | 6.7600 | 7.6400 | True |
| ARM64 | etcd | update.duration | s | 3 | 86.5796 | 86.5805 | 86.6204 | True |
| ARM64 | etcd | update.exit | exit code | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| ARM64 | etcd | update.live_inventory | objects in tracked kinds | 3 | 1185.0000 | 1185.0000 | 1185.0000 | True |
| ARM64 | etcd | update.pod.ContainersReady.p50 | ms | 3 | 1000.0000 | 1000.0000 | 1000.0000 | True |
| ARM64 | etcd | update.pod.ContainersReady.p99 | ms | 3 | 2000.0000 | 2000.0000 | 2000.0000 | True |
| ARM64 | etcd | update.pod.ContainersStarted.p50 | ms | 3 | 1513.0000 | 1545.0000 | 1549.0000 | True |
| ARM64 | etcd | update.pod.ContainersStarted.p99 | ms | 3 | 2125.0000 | 2247.0000 | 2329.0000 | True |
| ARM64 | etcd | update.pod.Initialized.p50 | ms | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| ARM64 | etcd | update.pod.Initialized.p99 | ms | 3 | 500.0000 | 500.0000 | 500.0000 | True |
| ARM64 | etcd | update.pod.PodReadyToStartContainers.p50 | ms | 3 | 1000.0000 | 1000.0000 | 1000.0000 | True |
| ARM64 | etcd | update.pod.PodReadyToStartContainers.p99 | ms | 3 | 1000.0000 | 1000.0000 | 1500.0000 | True |
| ARM64 | etcd | update.pod.PodScheduled.p50 | ms | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| ARM64 | etcd | update.pod.PodScheduled.p99 | ms | 3 | 0.0000 | 0.0000 | 500.0000 | True |
| ARM64 | etcd | update.pod.Ready.p50 | ms | 3 | 1000.0000 | 1000.0000 | 1000.0000 | True |
| ARM64 | etcd | update.pod.Ready.p99 | ms | 3 | 2000.0000 | 2000.0000 | 2000.0000 | True |
| ARM64 | etcd | update.revision.member1 | revisions | 3 | 5489.0000 | 5490.0000 | 5501.0000 | True |
| ARM64 | etcd | update.revision.member2 | revisions | 3 | 5489.0000 | 5490.0000 | 5502.0000 | True |
| ARM64 | etcd | update.revision.member3 | revisions | 3 | 5489.0000 | 5490.0000 | 5502.0000 | True |
| ARM64 | etcd | update.server.external-etcd-1.etcd_mvcc_delete_total | counter increments/s | 3 | 0.7329 | 0.7331 | 0.7331 | True |
| ARM64 | etcd | update.server.external-etcd-1.etcd_mvcc_put_total | counter increments/s | 3 | 62.1223 | 62.1569 | 62.2871 | True |
| ARM64 | etcd | update.server.external-etcd-1.etcd_mvcc_range_total | counter increments/s | 3 | 72.3751 | 75.2796 | 82.1599 | True |
| ARM64 | etcd | update.server.external-etcd-1.etcd_network_client_grpc_received_bytes_total | counter increments/s | 3 | 36646.2084 | 37856.1229 | 37980.2680 | True |
| ARM64 | etcd | update.server.external-etcd-1.etcd_network_client_grpc_sent_bytes_total | counter increments/s | 3 | 48554.3358 | 71906.2975 | 113948.7646 | True |
| ARM64 | etcd | update.server.external-etcd-1.etcd_server_proposals_applied_total | counter increments/s | 3 | 63.6224 | 63.6576 | 63.8334 | True |
| ARM64 | etcd | update.server.external-etcd-1.etcd_server_proposals_committed_total | counter increments/s | 3 | 63.6224 | 63.6576 | 63.8334 | True |
| ARM64 | etcd | update.server.external-etcd-1.grpc_server_handled_total | counter increments/s | 3 | 23.9761 | 24.0015 | 24.0993 | True |
| ARM64 | etcd | update.server.external-etcd-2.etcd_mvcc_delete_total | counter increments/s | 3 | 0.7329 | 0.7331 | 0.7332 | True |
| ARM64 | etcd | update.server.external-etcd-2.etcd_mvcc_put_total | counter increments/s | 3 | 62.1326 | 62.1594 | 62.2907 | True |
| ARM64 | etcd | update.server.external-etcd-2.etcd_mvcc_range_total | counter increments/s | 3 | 69.8692 | 71.1337 | 71.4880 | True |
| ARM64 | etcd | update.server.external-etcd-2.etcd_network_client_grpc_received_bytes_total | counter increments/s | 3 | 39332.0224 | 39426.5380 | 40145.5467 | True |
| ARM64 | etcd | update.server.external-etcd-2.etcd_network_client_grpc_sent_bytes_total | counter increments/s | 3 | 21961.1469 | 29921.7725 | 43454.1314 | True |
| ARM64 | etcd | update.server.external-etcd-2.etcd_server_proposals_applied_total | counter increments/s | 3 | 63.6327 | 63.6601 | 63.8371 | True |
| ARM64 | etcd | update.server.external-etcd-2.etcd_server_proposals_committed_total | counter increments/s | 3 | 63.6327 | 63.6601 | 63.8371 | True |
| ARM64 | etcd | update.server.external-etcd-2.grpc_server_handled_total | counter increments/s | 3 | 23.9438 | 23.9542 | 23.9747 | True |
| ARM64 | etcd | update.server.external-etcd-3.etcd_mvcc_delete_total | counter increments/s | 3 | 0.7329 | 0.7331 | 0.7332 | True |
| ARM64 | etcd | update.server.external-etcd-3.etcd_mvcc_put_total | counter increments/s | 3 | 62.1499 | 62.1712 | 62.2911 | True |
| ARM64 | etcd | update.server.external-etcd-3.etcd_mvcc_range_total | counter increments/s | 3 | 103.4710 | 109.0000 | 113.7916 | True |
| ARM64 | etcd | update.server.external-etcd-3.etcd_network_client_grpc_received_bytes_total | counter increments/s | 3 | 37605.5657 | 37861.2148 | 39045.1310 | True |
| ARM64 | etcd | update.server.external-etcd-3.etcd_network_client_grpc_sent_bytes_total | counter increments/s | 3 | 102506.2434 | 129484.1586 | 174831.2950 | True |
| ARM64 | etcd | update.server.external-etcd-3.etcd_server_proposals_applied_total | counter increments/s | 3 | 63.6502 | 63.6719 | 63.8375 | True |
| ARM64 | etcd | update.server.external-etcd-3.etcd_server_proposals_committed_total | counter increments/s | 3 | 63.6502 | 63.6719 | 63.8375 | True |
| ARM64 | etcd | update.server.external-etcd-3.grpc_server_handled_total | counter increments/s | 3 | 23.8888 | 24.0230 | 24.0779 | True |
| ARM64 | etcd | update.storage.etcd_request_duration_seconds | RPC/s | 3 | 70.2633 | 70.4308 | 70.5728 | True |
| ARM64 | etcd | update.window | s | 3 | 87.2914 | 87.2999 | 87.3002 | True |
| ARM64 | tigerstore | delete.api.all.p50 | ms | 3 | 3.5827 | 3.6262 | 3.7223 | True |
| ARM64 | tigerstore | delete.api.all.p99 | ms | 3 | 190.4118 | 307.0417 | 339.4000 | True |
| ARM64 | tigerstore | delete.api.creates.count | requests | 3 | 112.0000 | 112.0000 | 112.0000 | True |
| ARM64 | tigerstore | delete.api.creates.rate | requests/s | 3 | 1.7036 | 1.7043 | 1.7064 | True |
| ARM64 | tigerstore | delete.api.deletes.count | requests | 3 | 1521.0000 | 1521.0000 | 1521.0000 | True |
| ARM64 | tigerstore | delete.api.deletes.rate | requests/s | 3 | 23.1350 | 23.1445 | 23.1730 | True |
| ARM64 | tigerstore | delete.api.http403 | responses | 3 | 9.0000 | 9.0000 | 9.0000 | True |
| ARM64 | tigerstore | delete.api.http404 | responses | 3 | 32.0000 | 38.0000 | 45.0000 | True |
| ARM64 | tigerstore | delete.api.http409 | responses | 3 | 9.0000 | 9.0000 | 9.0000 | True |
| ARM64 | tigerstore | delete.api.http500 | responses | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| ARM64 | tigerstore | delete.api.requests | requests/s | 3 | 36.7787 | 36.9917 | 37.3571 | True |
| ARM64 | tigerstore | delete.api.resource..p50 | ms | 3 | 2.5676 | 2.5885 | 2.6327 | True |
| ARM64 | tigerstore | delete.api.resource..p99 | ms | 3 | 23.8667 | 48.2500 | 181.0000 | True |
| ARM64 | tigerstore | delete.api.resource.configmaps.p50 | ms | 3 | 2.5496 | 2.5854 | 2.6439 | True |
| ARM64 | tigerstore | delete.api.resource.configmaps.p99 | ms | 3 | 17.9467 | 22.5630 | 57.7727 | True |
| ARM64 | tigerstore | delete.api.resource.controllerrevisions.p50 | ms | 3 | 3.2143 | 4.5000 | 25.0000 | True |
| ARM64 | tigerstore | delete.api.resource.controllerrevisions.p99 | ms | 3 | 97.0000 | 191.0000 | 191.0000 | True |
| ARM64 | tigerstore | delete.api.resource.cronjobs.p50 | ms | 3 | 4.0909 | 4.5000 | 25.0000 | True |
| ARM64 | tigerstore | delete.api.resource.cronjobs.p99 | ms | 3 | 97.7500 | 182.0000 | 182.0000 | True |
| ARM64 | tigerstore | delete.api.resource.csistoragecapacities.p50 | ms | 3 | 4.0909 | 4.0909 | 4.5000 | True |
| ARM64 | tigerstore | delete.api.resource.csistoragecapacities.p99 | ms | 3 | 98.7143 | 182.0000 | 191.0000 | True |
| ARM64 | tigerstore | delete.api.resource.daemonsets.p50 | ms | 3 | 3.4615 | 5.0000 | 5.0000 | True |
| ARM64 | tigerstore | delete.api.resource.daemonsets.p99 | ms | 3 | 98.2000 | 98.8750 | 182.0000 | True |
| ARM64 | tigerstore | delete.api.resource.deployments.p50 | ms | 3 | 4.3750 | 5.0000 | 5.0000 | True |
| ARM64 | tigerstore | delete.api.resource.deployments.p99 | ms | 3 | 95.3333 | 96.5000 | 98.2500 | True |
| ARM64 | tigerstore | delete.api.resource.endpoints.p50 | ms | 3 | 18.8776 | 19.2500 | 22.9412 | True |
| ARM64 | tigerstore | delete.api.resource.endpoints.p99 | ms | 3 | 143.0000 | 295.0000 | 348.5000 | True |
| ARM64 | tigerstore | delete.api.resource.endpointslices.p50 | ms | 3 | 18.1250 | 20.5814 | 22.2222 | True |
| ARM64 | tigerstore | delete.api.resource.endpointslices.p99 | ms | 3 | 175.4000 | 323.3333 | 395.0000 | True |
| ARM64 | tigerstore | delete.api.resource.events.p50 | ms | 3 | 17.2581 | 22.6471 | 22.7778 | True |
| ARM64 | tigerstore | delete.api.resource.events.p99 | ms | 3 | 388.0000 | 389.3333 | 568.0000 | True |
| ARM64 | tigerstore | delete.api.resource.horizontalpodautoscalers.p50 | ms | 3 | 3.2143 | 3.2143 | 5.0000 | True |
| ARM64 | tigerstore | delete.api.resource.horizontalpodautoscalers.p99 | ms | 3 | 97.0000 | 97.0000 | 98.7143 | True |
| ARM64 | tigerstore | delete.api.resource.ingresses.p50 | ms | 3 | 3.7500 | 5.0000 | 55.0000 | True |
| ARM64 | tigerstore | delete.api.resource.ingresses.p99 | ms | 3 | 98.5000 | 99.1000 | 182.0000 | True |
| ARM64 | tigerstore | delete.api.resource.ipaddresses.p50 | ms | 3 | 17.0000 | 18.3333 | 33.3333 | True |
| ARM64 | tigerstore | delete.api.resource.ipaddresses.p99 | ms | 3 | 49.0000 | 49.0000 | 368.0000 | True |
| ARM64 | tigerstore | delete.api.resource.jobs.p50 | ms | 3 | 3.0000 | 4.5000 | 5.0000 | True |
| ARM64 | tigerstore | delete.api.resource.jobs.p99 | ms | 3 | 97.0000 | 98.8750 | 182.0000 | True |
| ARM64 | tigerstore | delete.api.resource.leases.p50 | ms | 3 | 2.8385 | 2.8723 | 2.8989 | True |
| ARM64 | tigerstore | delete.api.resource.leases.p99 | ms | 3 | 89.1000 | 99.3333 | 163.6667 | True |
| ARM64 | tigerstore | delete.api.resource.limitranges.p50 | ms | 3 | 3.2143 | 3.4615 | 18.3333 | True |
| ARM64 | tigerstore | delete.api.resource.limitranges.p99 | ms | 3 | 98.8750 | 182.0000 | 182.0000 | True |
| ARM64 | tigerstore | delete.api.resource.namespaces.p50 | ms | 3 | 2.5000 | 2.5862 | 2.7778 | True |
| ARM64 | tigerstore | delete.api.resource.namespaces.p99 | ms | 3 | 4.9500 | 19.0000 | 23.0000 | True |
| ARM64 | tigerstore | delete.api.resource.networkpolicies.p50 | ms | 3 | 4.0909 | 5.0000 | 5.0000 | True |
| ARM64 | tigerstore | delete.api.resource.networkpolicies.p99 | ms | 3 | 98.7143 | 98.8750 | 182.0000 | True |
| ARM64 | tigerstore | delete.api.resource.nodes.p50 | ms | 3 | 2.5000 | 2.5000 | 2.5000 | True |
| ARM64 | tigerstore | delete.api.resource.nodes.p99 | ms | 3 | 4.9500 | 4.9500 | 4.9500 | True |
| ARM64 | tigerstore | delete.api.resource.persistentvolumeclaims.p50 | ms | 3 | 3.0000 | 3.4615 | 4.0909 | True |
| ARM64 | tigerstore | delete.api.resource.persistentvolumeclaims.p99 | ms | 3 | 97.0000 | 98.2000 | 98.2000 | True |
| ARM64 | tigerstore | delete.api.resource.podcertificaterequests.p50 | ms | 3 | 3.2143 | 3.4615 | 3.7500 | True |
| ARM64 | tigerstore | delete.api.resource.podcertificaterequests.p99 | ms | 3 | 95.5000 | 97.7500 | 98.2000 | True |
| ARM64 | tigerstore | delete.api.resource.poddisruptionbudgets.p50 | ms | 3 | 3.4615 | 4.0909 | 4.0909 | True |
| ARM64 | tigerstore | delete.api.resource.poddisruptionbudgets.p99 | ms | 3 | 97.7500 | 182.0000 | 182.0000 | True |
| ARM64 | tigerstore | delete.api.resource.pods.p50 | ms | 3 | 45.7237 | 75.0000 | 90.5000 | True |
| ARM64 | tigerstore | delete.api.resource.pods.p99 | ms | 3 | 355.3333 | 394.3529 | 394.8824 | True |
| ARM64 | tigerstore | delete.api.resource.podtemplates.p50 | ms | 3 | 3.2143 | 3.7500 | 25.0000 | True |
| ARM64 | tigerstore | delete.api.resource.podtemplates.p99 | ms | 3 | 97.7500 | 98.5000 | 182.0000 | True |
| ARM64 | tigerstore | delete.api.resource.replicasets.p50 | ms | 3 | 4.1667 | 6.0526 | 7.1053 | True |
| ARM64 | tigerstore | delete.api.resource.replicasets.p99 | ms | 3 | 93.7500 | 96.4286 | 150.0000 | True |
| ARM64 | tigerstore | delete.api.resource.replicationcontrollers.p50 | ms | 3 | 3.2143 | 3.7500 | 3.7500 | True |
| ARM64 | tigerstore | delete.api.resource.replicationcontrollers.p99 | ms | 3 | 97.7500 | 98.5000 | 194.0000 | True |
| ARM64 | tigerstore | delete.api.resource.resourceclaims.p50 | ms | 3 | 3.2143 | 3.4615 | 4.5000 | True |
| ARM64 | tigerstore | delete.api.resource.resourceclaims.p99 | ms | 3 | 97.0000 | 97.0000 | 182.0000 | True |
| ARM64 | tigerstore | delete.api.resource.resourceclaimtemplates.p50 | ms | 3 | 3.7500 | 3.7500 | 4.0909 | True |
| ARM64 | tigerstore | delete.api.resource.resourceclaimtemplates.p99 | ms | 3 | 97.0000 | 98.7143 | 182.0000 | True |
| ARM64 | tigerstore | delete.api.resource.resourcequotas.p50 | ms | 3 | 3.4615 | 3.7500 | 62.5000 | True |
| ARM64 | tigerstore | delete.api.resource.resourcequotas.p99 | ms | 3 | 98.2000 | 99.2500 | 182.0000 | True |
| ARM64 | tigerstore | delete.api.resource.rolebindings.p50 | ms | 3 | 3.2143 | 4.0909 | 25.0000 | True |
| ARM64 | tigerstore | delete.api.resource.rolebindings.p99 | ms | 3 | 98.5000 | 98.7143 | 182.0000 | True |
| ARM64 | tigerstore | delete.api.resource.roles.p50 | ms | 3 | 4.0909 | 15.0000 | 25.0000 | True |
| ARM64 | tigerstore | delete.api.resource.roles.p99 | ms | 3 | 98.7143 | 98.8750 | 191.0000 | True |
| ARM64 | tigerstore | delete.api.resource.secrets.p50 | ms | 3 | 2.6829 | 2.8205 | 3.4375 | True |
| ARM64 | tigerstore | delete.api.resource.secrets.p99 | ms | 3 | 92.6667 | 92.6667 | 178.0000 | True |
| ARM64 | tigerstore | delete.api.resource.serviceaccounts.p50 | ms | 3 | 16.4286 | 21.6667 | 55.0000 | True |
| ARM64 | tigerstore | delete.api.resource.serviceaccounts.p99 | ms | 3 | 98.5000 | 98.8750 | 99.1000 | True |
| ARM64 | tigerstore | delete.api.resource.services.p50 | ms | 3 | 27.7778 | 40.0000 | 50.0000 | True |
| ARM64 | tigerstore | delete.api.resource.services.p99 | ms | 3 | 164.0000 | 182.0000 | 385.6000 | True |
| ARM64 | tigerstore | delete.api.resource.statefulsets.p50 | ms | 3 | 3.7500 | 4.0909 | 5.0000 | True |
| ARM64 | tigerstore | delete.api.resource.statefulsets.p99 | ms | 3 | 98.8750 | 182.0000 | 182.0000 | True |
| ARM64 | tigerstore | delete.api.updates.count | requests | 3 | 190.0000 | 196.0000 | 216.0000 | True |
| ARM64 | tigerstore | delete.api.updates.rate | requests/s | 3 | 2.8900 | 2.9825 | 3.2908 | True |
| ARM64 | tigerstore | delete.api.verb.DELETE.p50 | ms | 3 | 3.1548 | 3.1600 | 3.2926 | True |
| ARM64 | tigerstore | delete.api.verb.DELETE.p99 | ms | 3 | 220.3750 | 336.3333 | 350.6190 | True |
| ARM64 | tigerstore | delete.api.verb.GET.p50 | ms | 3 | 3.0632 | 3.3136 | 3.3333 | True |
| ARM64 | tigerstore | delete.api.verb.GET.p99 | ms | 3 | 72.1250 | 144.0000 | 253.3333 | True |
| ARM64 | tigerstore | delete.api.verb.LIST.p50 | ms | 3 | 4.3370 | 4.5376 | 4.6176 | True |
| ARM64 | tigerstore | delete.api.verb.LIST.p99 | ms | 3 | 155.1429 | 160.7500 | 182.5556 | True |
| ARM64 | tigerstore | delete.api.verb.PATCH.p50 | ms | 3 | 25.0000 | 31.2500 | 35.0000 | True |
| ARM64 | tigerstore | delete.api.verb.PATCH.p99 | ms | 3 | 96.5000 | 188.0000 | 382.0000 | True |
| ARM64 | tigerstore | delete.api.verb.POST.p50 | ms | 3 | 18.7097 | 24.4118 | 24.7778 | True |
| ARM64 | tigerstore | delete.api.verb.POST.p99 | ms | 3 | 179.0000 | 197.3750 | 359.6667 | True |
| ARM64 | tigerstore | delete.api.verb.PUT.p50 | ms | 3 | 5.7692 | 7.2917 | 8.3333 | True |
| ARM64 | tigerstore | delete.api.verb.PUT.p99 | ms | 3 | 151.4000 | 310.8000 | 370.7500 | True |
| ARM64 | tigerstore | delete.cpu.all | CPU s | 3 | 9.4500 | 9.7200 | 10.3500 | True |
| ARM64 | tigerstore | delete.cpu.cores | cores | 3 | 0.1440 | 0.1479 | 0.1574 | True |
| ARM64 | tigerstore | delete.cpu.member1 | CPU s | 3 | 2.7100 | 2.7300 | 4.3500 | True |
| ARM64 | tigerstore | delete.cpu.member2 | CPU s | 3 | 2.9400 | 3.9300 | 4.0700 | True |
| ARM64 | tigerstore | delete.cpu.member3 | CPU s | 3 | 2.8100 | 2.9200 | 3.0600 | True |
| ARM64 | tigerstore | delete.duration | s | 3 | 65.0204 | 65.0277 | 65.0512 | True |
| ARM64 | tigerstore | delete.exit | exit code | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| ARM64 | tigerstore | delete.live_inventory | objects in tracked kinds | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| ARM64 | tigerstore | delete.revision.member1 | revisions | 3 | 2752.0000 | 2759.0000 | 2778.0000 | True |
| ARM64 | tigerstore | delete.revision.member2 | revisions | 3 | 2753.0000 | 2759.0000 | 2778.0000 | True |
| ARM64 | tigerstore | delete.revision.member3 | revisions | 3 | 2753.0000 | 2759.0000 | 2778.0000 | True |
| ARM64 | tigerstore | delete.storage.etcd_request_duration_seconds | RPC/s | 3 | 107.0659 | 107.7035 | 108.0492 | True |
| ARM64 | tigerstore | delete.verified_deletion_rate | objects/s | 3 | 18.2164 | 18.2230 | 18.2250 | True |
| ARM64 | tigerstore | delete.window | s | 3 | 65.6368 | 65.7174 | 65.7446 | True |
| ARM64 | tigerstore | errors.observed | records | 3 | 21.0000 | 21.0000 | 22.0000 | True |
| ARM64 | tigerstore | errors.workload | records | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| ARM64 | tigerstore | events.observations | watch notifications | 3 | 2212.0000 | 2212.0000 | 2212.0000 | True |
| ARM64 | tigerstore | events.unique | events | 3 | 1094.0000 | 1094.0000 | 1094.0000 | True |
| ARM64 | tigerstore | mixed.api.all.p50 | ms | 3 | 3.2964 | 3.3205 | 3.5241 | True |
| ARM64 | tigerstore | mixed.api.all.p99 | ms | 3 | 61.7424 | 76.7083 | 147.9535 | True |
| ARM64 | tigerstore | mixed.api.creates.count | requests | 3 | 1640.0000 | 1640.0000 | 1640.0000 | True |
| ARM64 | tigerstore | mixed.api.creates.rate | requests/s | 3 | 36.5774 | 37.4819 | 37.5002 | True |
| ARM64 | tigerstore | mixed.api.deletes.count | requests | 3 | 70.0000 | 76.0000 | 77.0000 | True |
| ARM64 | tigerstore | mixed.api.deletes.rate | requests/s | 3 | 1.5998 | 1.6950 | 1.7607 | True |
| ARM64 | tigerstore | mixed.api.http403 | responses | 3 | 15.0000 | 18.0000 | 18.0000 | True |
| ARM64 | tigerstore | mixed.api.http404 | responses | 3 | 6.0000 | 6.0000 | 9.0000 | True |
| ARM64 | tigerstore | mixed.api.http409 | responses | 3 | 14.0000 | 16.0000 | 17.0000 | True |
| ARM64 | tigerstore | mixed.api.http500 | responses | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| ARM64 | tigerstore | mixed.api.requests | requests/s | 3 | 65.2371 | 66.6904 | 67.1803 | True |
| ARM64 | tigerstore | mixed.api.resource..p50 | ms | 3 | 2.5000 | 2.5342 | 2.6027 | True |
| ARM64 | tigerstore | mixed.api.resource..p99 | ms | 3 | 4.9500 | 10.2000 | 19.9333 | True |
| ARM64 | tigerstore | mixed.api.resource.configmaps.p50 | ms | 3 | 2.9106 | 2.9348 | 3.0741 | True |
| ARM64 | tigerstore | mixed.api.resource.configmaps.p99 | ms | 3 | 44.8041 | 94.2500 | 177.3824 | True |
| ARM64 | tigerstore | mixed.api.resource.controllerrevisions.p50 | ms | 3 | 2.5000 | 2.5000 | 5.0000 | True |
| ARM64 | tigerstore | mixed.api.resource.controllerrevisions.p99 | ms | 3 | 4.9500 | 4.9500 | 196.0000 | True |
| ARM64 | tigerstore | mixed.api.resource.cronjobs.p50 | ms | 3 | 2.5000 | 2.5000 | 66.6667 | True |
| ARM64 | tigerstore | mixed.api.resource.cronjobs.p99 | ms | 3 | 4.9500 | 4.9500 | 99.3333 | True |
| ARM64 | tigerstore | mixed.api.resource.csistoragecapacities.p50 | ms | 3 | 2.5000 | 2.5000 | 2.5000 | True |
| ARM64 | tigerstore | mixed.api.resource.csistoragecapacities.p99 | ms | 3 | 4.9500 | 4.9500 | 4.9500 | True |
| ARM64 | tigerstore | mixed.api.resource.daemonsets.p50 | ms | 3 | 2.5000 | 3.3333 | 3.3333 | True |
| ARM64 | tigerstore | mixed.api.resource.daemonsets.p99 | ms | 3 | 4.9500 | 98.0000 | 98.0000 | True |
| ARM64 | tigerstore | mixed.api.resource.deployments.p50 | ms | 3 | 4.1102 | 4.2982 | 4.3636 | True |
| ARM64 | tigerstore | mixed.api.resource.deployments.p99 | ms | 3 | 83.6667 | 88.0000 | 151.5000 | True |
| ARM64 | tigerstore | mixed.api.resource.endpoints.p50 | ms | 3 | 4.3092 | 4.3151 | 5.3333 | True |
| ARM64 | tigerstore | mixed.api.resource.endpoints.p99 | ms | 3 | 34.2500 | 67.2500 | 78.6667 | True |
| ARM64 | tigerstore | mixed.api.resource.endpointslices.p50 | ms | 3 | 4.4776 | 4.5833 | 5.7143 | True |
| ARM64 | tigerstore | mixed.api.resource.endpointslices.p99 | ms | 3 | 35.0000 | 91.1429 | 92.0000 | True |
| ARM64 | tigerstore | mixed.api.resource.events.p50 | ms | 3 | 4.0430 | 4.3487 | 6.0784 | True |
| ARM64 | tigerstore | mixed.api.resource.events.p99 | ms | 3 | 44.6500 | 48.2500 | 49.6500 | True |
| ARM64 | tigerstore | mixed.api.resource.horizontalpodautoscalers.p50 | ms | 3 | 2.5000 | 3.3333 | 5.0000 | True |
| ARM64 | tigerstore | mixed.api.resource.horizontalpodautoscalers.p99 | ms | 3 | 4.9500 | 98.0000 | 196.0000 | True |
| ARM64 | tigerstore | mixed.api.resource.ingresses.p50 | ms | 3 | 2.5000 | 2.5000 | 3.3333 | True |
| ARM64 | tigerstore | mixed.api.resource.ingresses.p99 | ms | 3 | 4.9500 | 4.9500 | 98.0000 | True |
| ARM64 | tigerstore | mixed.api.resource.ipaddresses.p50 | ms | 3 | 2.5000 | 2.5000 | 2.8571 | True |
| ARM64 | tigerstore | mixed.api.resource.ipaddresses.p99 | ms | 3 | 4.9500 | 4.9500 | 23.4000 | True |
| ARM64 | tigerstore | mixed.api.resource.jobs.p50 | ms | 3 | 2.5000 | 3.3333 | 5.0000 | True |
| ARM64 | tigerstore | mixed.api.resource.jobs.p99 | ms | 3 | 4.9500 | 98.0000 | 98.0000 | True |
| ARM64 | tigerstore | mixed.api.resource.leases.p50 | ms | 3 | 2.6210 | 2.7083 | 2.8879 | True |
| ARM64 | tigerstore | mixed.api.resource.leases.p99 | ms | 3 | 83.2500 | 135.0000 | 135.0000 | True |
| ARM64 | tigerstore | mixed.api.resource.limitranges.p50 | ms | 3 | 2.5000 | 2.5000 | 2.9167 | True |
| ARM64 | tigerstore | mixed.api.resource.limitranges.p99 | ms | 3 | 4.9500 | 4.9500 | 186.0000 | True |
| ARM64 | tigerstore | mixed.api.resource.namespaces.p50 | ms | 3 | 2.8416 | 2.8683 | 2.9516 | True |
| ARM64 | tigerstore | mixed.api.resource.namespaces.p99 | ms | 3 | 23.3364 | 23.4426 | 24.0327 | True |
| ARM64 | tigerstore | mixed.api.resource.networkpolicies.p50 | ms | 3 | 2.5000 | 2.5000 | 2.5000 | True |
| ARM64 | tigerstore | mixed.api.resource.networkpolicies.p99 | ms | 3 | 4.9500 | 4.9500 | 4.9500 | True |
| ARM64 | tigerstore | mixed.api.resource.nodes.p50 | ms | 3 | 2.5000 | 2.5000 | 2.5000 | True |
| ARM64 | tigerstore | mixed.api.resource.nodes.p99 | ms | 3 | 4.9500 | 4.9500 | 4.9500 | True |
| ARM64 | tigerstore | mixed.api.resource.persistentvolumeclaims.p50 | ms | 3 | 2.5000 | 2.5000 | 2.5000 | True |
| ARM64 | tigerstore | mixed.api.resource.persistentvolumeclaims.p99 | ms | 3 | 4.9500 | 4.9500 | 4.9500 | True |
| ARM64 | tigerstore | mixed.api.resource.podcertificaterequests.p50 | ms | 3 | 3.3333 | 3.3333 | 3.3333 | True |
| ARM64 | tigerstore | mixed.api.resource.podcertificaterequests.p99 | ms | 3 | 98.0000 | 98.0000 | 196.0000 | True |
| ARM64 | tigerstore | mixed.api.resource.poddisruptionbudgets.p50 | ms | 3 | 2.5000 | 3.3333 | 3.3333 | True |
| ARM64 | tigerstore | mixed.api.resource.poddisruptionbudgets.p99 | ms | 3 | 4.9500 | 98.0000 | 196.0000 | True |
| ARM64 | tigerstore | mixed.api.resource.pods.p50 | ms | 3 | 4.6144 | 4.6603 | 6.6168 | True |
| ARM64 | tigerstore | mixed.api.resource.pods.p99 | ms | 3 | 45.5312 | 47.5179 | 75.5000 | True |
| ARM64 | tigerstore | mixed.api.resource.podtemplates.p50 | ms | 3 | 2.5000 | 2.5000 | 2.5000 | True |
| ARM64 | tigerstore | mixed.api.resource.podtemplates.p99 | ms | 3 | 4.9500 | 4.9500 | 4.9500 | True |
| ARM64 | tigerstore | mixed.api.resource.replicasets.p50 | ms | 3 | 8.2353 | 10.0000 | 11.3158 | True |
| ARM64 | tigerstore | mixed.api.resource.replicasets.p99 | ms | 3 | 42.3750 | 46.9000 | 136.0000 | True |
| ARM64 | tigerstore | mixed.api.resource.replicationcontrollers.p50 | ms | 3 | 2.5000 | 2.5000 | 5.0000 | True |
| ARM64 | tigerstore | mixed.api.resource.replicationcontrollers.p99 | ms | 3 | 4.9500 | 4.9500 | 98.0000 | True |
| ARM64 | tigerstore | mixed.api.resource.resourceclaims.p50 | ms | 3 | 2.5000 | 2.5000 | 5.0000 | True |
| ARM64 | tigerstore | mixed.api.resource.resourceclaims.p99 | ms | 3 | 4.9500 | 4.9500 | 99.0000 | True |
| ARM64 | tigerstore | mixed.api.resource.resourceclaimtemplates.p50 | ms | 3 | 2.5000 | 2.5000 | 2.5000 | True |
| ARM64 | tigerstore | mixed.api.resource.resourceclaimtemplates.p99 | ms | 3 | 4.9500 | 4.9500 | 4.9500 | True |
| ARM64 | tigerstore | mixed.api.resource.resourcequotas.p50 | ms | 3 | 2.5000 | 2.5000 | 3.3333 | True |
| ARM64 | tigerstore | mixed.api.resource.resourcequotas.p99 | ms | 3 | 4.9500 | 4.9500 | 196.0000 | True |
| ARM64 | tigerstore | mixed.api.resource.rolebindings.p50 | ms | 3 | 2.5000 | 3.3333 | 5.0000 | True |
| ARM64 | tigerstore | mixed.api.resource.rolebindings.p99 | ms | 3 | 4.9500 | 98.0000 | 98.0000 | True |
| ARM64 | tigerstore | mixed.api.resource.roles.p50 | ms | 3 | 2.5000 | 5.0000 | 5.0000 | True |
| ARM64 | tigerstore | mixed.api.resource.roles.p99 | ms | 3 | 4.9500 | 98.0000 | 98.0000 | True |
| ARM64 | tigerstore | mixed.api.resource.secrets.p50 | ms | 3 | 2.6515 | 2.6515 | 2.8226 | True |
| ARM64 | tigerstore | mixed.api.resource.secrets.p99 | ms | 3 | 21.5000 | 21.5000 | 91.2500 | True |
| ARM64 | tigerstore | mixed.api.resource.serviceaccounts.p50 | ms | 3 | 13.8889 | 15.0000 | 15.0000 | True |
| ARM64 | tigerstore | mixed.api.resource.serviceaccounts.p99 | ms | 3 | 48.8333 | 93.0000 | 96.5000 | True |
| ARM64 | tigerstore | mixed.api.resource.services.p50 | ms | 3 | 12.6471 | 14.5000 | 15.4545 | True |
| ARM64 | tigerstore | mixed.api.resource.services.p99 | ms | 3 | 93.7500 | 93.7500 | 93.7500 | True |
| ARM64 | tigerstore | mixed.api.resource.statefulsets.p50 | ms | 3 | 2.5000 | 2.5000 | 2.5000 | True |
| ARM64 | tigerstore | mixed.api.resource.statefulsets.p99 | ms | 3 | 4.9500 | 4.9500 | 4.9500 | True |
| ARM64 | tigerstore | mixed.api.updates.count | requests | 3 | 245.0000 | 245.0000 | 252.0000 | True |
| ARM64 | tigerstore | mixed.api.updates.rate | requests/s | 3 | 5.4643 | 5.5994 | 5.7622 | True |
| ARM64 | tigerstore | mixed.api.verb.DELETE.p50 | ms | 3 | 3.2456 | 3.3197 | 3.6364 | True |
| ARM64 | tigerstore | mixed.api.verb.DELETE.p99 | ms | 3 | 320.0000 | 326.0000 | 359.5000 | True |
| ARM64 | tigerstore | mixed.api.verb.GET.p50 | ms | 3 | 2.7749 | 2.7990 | 2.8903 | True |
| ARM64 | tigerstore | mixed.api.verb.GET.p99 | ms | 3 | 22.9815 | 23.1276 | 23.5189 | True |
| ARM64 | tigerstore | mixed.api.verb.LIST.p50 | ms | 3 | 3.1452 | 3.1649 | 3.2967 | True |
| ARM64 | tigerstore | mixed.api.verb.LIST.p99 | ms | 3 | 96.2500 | 160.3333 | 161.0000 | True |
| ARM64 | tigerstore | mixed.api.verb.PATCH.p50 | ms | 3 | 8.4848 | 10.1316 | 12.0886 | True |
| ARM64 | tigerstore | mixed.api.verb.PATCH.p99 | ms | 3 | 43.8500 | 45.6071 | 46.6667 | True |
| ARM64 | tigerstore | mixed.api.verb.POST.p50 | ms | 3 | 3.1703 | 3.1765 | 3.4540 | True |
| ARM64 | tigerstore | mixed.api.verb.POST.p99 | ms | 3 | 42.4022 | 61.2222 | 158.9706 | True |
| ARM64 | tigerstore | mixed.api.verb.PUT.p50 | ms | 3 | 4.6066 | 4.6734 | 5.4938 | True |
| ARM64 | tigerstore | mixed.api.verb.PUT.p99 | ms | 3 | 40.6071 | 83.5455 | 125.6000 | True |
| ARM64 | tigerstore | mixed.cpu.all | CPU s | 3 | 6.9200 | 6.9900 | 7.8900 | True |
| ARM64 | tigerstore | mixed.cpu.cores | cores | 3 | 0.1580 | 0.1597 | 0.1758 | True |
| ARM64 | tigerstore | mixed.cpu.member1 | CPU s | 3 | 1.9600 | 2.0100 | 3.2500 | True |
| ARM64 | tigerstore | mixed.cpu.member2 | CPU s | 3 | 2.2300 | 2.8100 | 2.9300 | True |
| ARM64 | tigerstore | mixed.cpu.member3 | CPU s | 3 | 2.0500 | 2.1500 | 2.4100 | True |
| ARM64 | tigerstore | mixed.duration | s | 3 | 43.1780 | 43.2142 | 44.2046 | True |
| ARM64 | tigerstore | mixed.exit | exit code | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| ARM64 | tigerstore | mixed.live_inventory | objects in tracked kinds | 3 | 1096.0000 | 1096.0000 | 1096.0000 | True |
| ARM64 | tigerstore | mixed.net_live_creation_rate | objects/s | 3 | 24.7938 | 25.3620 | 25.3833 | True |
| ARM64 | tigerstore | mixed.pod.ContainersReady.p50 | ms | 3 | 1000.0000 | 1000.0000 | 1000.0000 | True |
| ARM64 | tigerstore | mixed.pod.ContainersReady.p99 | ms | 3 | 1500.0000 | 2000.0000 | 2000.0000 | True |
| ARM64 | tigerstore | mixed.pod.ContainersStarted.p50 | ms | 3 | 1425.0000 | 1515.0000 | 1686.0000 | True |
| ARM64 | tigerstore | mixed.pod.ContainersStarted.p99 | ms | 3 | 1993.0000 | 2022.0000 | 2209.0000 | True |
| ARM64 | tigerstore | mixed.pod.Initialized.p50 | ms | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| ARM64 | tigerstore | mixed.pod.Initialized.p99 | ms | 3 | 1000.0000 | 1000.0000 | 1000.0000 | True |
| ARM64 | tigerstore | mixed.pod.PodReadyToStartContainers.p50 | ms | 3 | 1000.0000 | 1000.0000 | 1000.0000 | True |
| ARM64 | tigerstore | mixed.pod.PodReadyToStartContainers.p99 | ms | 3 | 1000.0000 | 1000.0000 | 1000.0000 | True |
| ARM64 | tigerstore | mixed.pod.PodScheduled.p50 | ms | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| ARM64 | tigerstore | mixed.pod.PodScheduled.p99 | ms | 3 | 500.0000 | 500.0000 | 500.0000 | True |
| ARM64 | tigerstore | mixed.pod.Ready.p50 | ms | 3 | 1000.0000 | 1000.0000 | 1000.0000 | True |
| ARM64 | tigerstore | mixed.pod.Ready.p99 | ms | 3 | 1500.0000 | 2000.0000 | 2000.0000 | True |
| ARM64 | tigerstore | mixed.revision.member1 | revisions | 3 | 2490.0000 | 2492.0000 | 2504.0000 | True |
| ARM64 | tigerstore | mixed.revision.member2 | revisions | 3 | 2490.0000 | 2492.0000 | 2504.0000 | True |
| ARM64 | tigerstore | mixed.revision.member3 | revisions | 3 | 2490.0000 | 2492.0000 | 2504.0000 | True |
| ARM64 | tigerstore | mixed.storage.etcd_request_duration_seconds | RPC/s | 3 | 80.0465 | 82.1402 | 82.5920 | True |
| ARM64 | tigerstore | mixed.window | s | 3 | 43.7330 | 43.7545 | 44.8365 | True |
| ARM64 | tigerstore | node.api.all.p50 | ms | 3 | 15.9031 | 16.4286 | 17.0798 | True |
| ARM64 | tigerstore | node.api.all.p99 | ms | 3 | 190.0235 | 190.1404 | 194.2927 | True |
| ARM64 | tigerstore | node.api.creates.count | requests | 3 | 483.0000 | 483.0000 | 483.0000 | True |
| ARM64 | tigerstore | node.api.creates.rate | requests/s | 3 | 23.5708 | 25.2149 | 26.4632 | True |
| ARM64 | tigerstore | node.api.deletes.count | requests | 3 | 16.0000 | 16.0000 | 16.0000 | True |
| ARM64 | tigerstore | node.api.deletes.rate | requests/s | 3 | 0.7808 | 0.8353 | 0.8766 | True |
| ARM64 | tigerstore | node.api.http403 | responses | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| ARM64 | tigerstore | node.api.http404 | responses | 3 | 20.0000 | 21.0000 | 22.0000 | True |
| ARM64 | tigerstore | node.api.http409 | responses | 3 | 3.0000 | 3.0000 | 4.0000 | True |
| ARM64 | tigerstore | node.api.http500 | responses | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| ARM64 | tigerstore | node.api.requests | requests/s | 3 | 66.4668 | 70.3721 | 73.0887 | True |
| ARM64 | tigerstore | node.api.resource..p50 | ms | 3 | 3.0208 | 3.5417 | 3.6905 | True |
| ARM64 | tigerstore | node.api.resource..p99 | ms | 3 | 84.5000 | 166.0000 | 171.0000 | True |
| ARM64 | tigerstore | node.api.resource.configmaps.p50 | ms | 3 | 2.5000 | 15.0000 | 15.0000 | True |
| ARM64 | tigerstore | node.api.resource.configmaps.p99 | ms | 3 | 4.9500 | 24.8000 | 24.8000 | True |
| ARM64 | tigerstore | node.api.resource.endpoints.p50 | ms | 3 | 2.5000 | 5.0000 | 25.0000 | True |
| ARM64 | tigerstore | node.api.resource.endpoints.p99 | ms | 3 | 4.9500 | 24.6000 | 49.5000 | True |
| ARM64 | tigerstore | node.api.resource.endpointslices.p50 | ms | 3 | 5.0000 | 5.0000 | 15.0000 | True |
| ARM64 | tigerstore | node.api.resource.endpointslices.p99 | ms | 3 | 24.6000 | 24.6000 | 24.8000 | True |
| ARM64 | tigerstore | node.api.resource.events.p50 | ms | 3 | 18.2569 | 18.6464 | 18.9474 | True |
| ARM64 | tigerstore | node.api.resource.events.p99 | ms | 3 | 190.8065 | 195.0000 | 207.5000 | True |
| ARM64 | tigerstore | node.api.resource.leases.p50 | ms | 3 | 3.9062 | 4.6667 | 12.2727 | True |
| ARM64 | tigerstore | node.api.resource.leases.p99 | ms | 3 | 46.8750 | 95.3333 | 172.0000 | True |
| ARM64 | tigerstore | node.api.resource.limitranges.p50 | ms | 3 | 2.5000 | 2.5000 | 2.5000 | True |
| ARM64 | tigerstore | node.api.resource.limitranges.p99 | ms | 3 | 4.9500 | 4.9500 | 4.9500 | True |
| ARM64 | tigerstore | node.api.resource.namespaces.p50 | ms | 3 | 2.5000 | 2.5000 | 2.5000 | True |
| ARM64 | tigerstore | node.api.resource.namespaces.p99 | ms | 3 | 4.9500 | 4.9500 | 4.9500 | True |
| ARM64 | tigerstore | node.api.resource.nodes.p50 | ms | 3 | 2.5000 | 2.5000 | 2.5000 | True |
| ARM64 | tigerstore | node.api.resource.nodes.p99 | ms | 3 | 4.9500 | 4.9500 | 4.9500 | True |
| ARM64 | tigerstore | node.api.resource.pods.p50 | ms | 3 | 15.7692 | 16.2405 | 17.1692 | True |
| ARM64 | tigerstore | node.api.resource.pods.p99 | ms | 3 | 189.8250 | 190.5385 | 193.8000 | True |
| ARM64 | tigerstore | node.api.resource.serviceaccounts.p50 | ms | 3 | 2.5000 | 2.5000 | 3.7500 | True |
| ARM64 | tigerstore | node.api.resource.serviceaccounts.p99 | ms | 3 | 4.9500 | 4.9500 | 24.4000 | True |
| ARM64 | tigerstore | node.api.updates.count | requests | 3 | 41.0000 | 44.0000 | 44.0000 | True |
| ARM64 | tigerstore | node.api.updates.rate | requests/s | 3 | 2.1472 | 2.2464 | 2.2970 | True |
| ARM64 | tigerstore | node.api.verb.DELETE.p50 | ms | 3 | 10.4545 | 13.5714 | 17.0000 | True |
| ARM64 | tigerstore | node.api.verb.DELETE.p99 | ms | 3 | 24.7091 | 24.7714 | 98.0000 | True |
| ARM64 | tigerstore | node.api.verb.GET.p50 | ms | 3 | 5.1923 | 5.3704 | 9.2857 | True |
| ARM64 | tigerstore | node.api.verb.GET.p99 | ms | 3 | 96.9706 | 99.5278 | 165.2222 | True |
| ARM64 | tigerstore | node.api.verb.LIST.p50 | ms | 3 | 4.5000 | 6.2500 | 12.7778 | True |
| ARM64 | tigerstore | node.api.verb.LIST.p99 | ms | 3 | 91.5000 | 181.0000 | 191.0000 | True |
| ARM64 | tigerstore | node.api.verb.PATCH.p50 | ms | 3 | 20.8389 | 21.8391 | 22.4194 | True |
| ARM64 | tigerstore | node.api.verb.PATCH.p99 | ms | 3 | 195.2174 | 199.1364 | 199.6154 | True |
| ARM64 | tigerstore | node.api.verb.POST.p50 | ms | 3 | 18.9000 | 19.2006 | 20.4104 | True |
| ARM64 | tigerstore | node.api.verb.POST.p99 | ms | 3 | 190.1489 | 197.9000 | 274.8889 | True |
| ARM64 | tigerstore | node.api.verb.PUT.p50 | ms | 3 | 3.9062 | 4.6667 | 12.2727 | True |
| ARM64 | tigerstore | node.api.verb.PUT.p99 | ms | 3 | 46.8750 | 95.3333 | 172.0000 | True |
| ARM64 | tigerstore | node.cpu.all | CPU s | 3 | 2.0800 | 2.1200 | 2.7300 | True |
| ARM64 | tigerstore | node.cpu.cores | cores | 3 | 0.1085 | 0.1162 | 0.1332 | True |
| ARM64 | tigerstore | node.cpu.member1 | CPU s | 3 | 0.5900 | 0.6000 | 1.1100 | True |
| ARM64 | tigerstore | node.cpu.member2 | CPU s | 3 | 0.7800 | 0.8400 | 0.8500 | True |
| ARM64 | tigerstore | node.cpu.member3 | CPU s | 3 | 0.6400 | 0.6800 | 0.8400 | True |
| ARM64 | tigerstore | node.duration | s | 3 | 17.6368 | 18.5394 | 19.8208 | True |
| ARM64 | tigerstore | node.exit | exit code | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| ARM64 | tigerstore | node.live_inventory | objects in tracked kinds | 3 | 1177.0000 | 1177.0000 | 1177.0000 | True |
| ARM64 | tigerstore | node.net_live_creation_rate | objects/s | 3 | 4.0866 | 4.3691 | 4.5927 | True |
| ARM64 | tigerstore | node.pod.ContainersReady.p50 | ms | 3 | 3000.0000 | 3000.0000 | 5000.0000 | True |
| ARM64 | tigerstore | node.pod.ContainersReady.p99 | ms | 3 | 5500.0000 | 6000.0000 | 7000.0000 | True |
| ARM64 | tigerstore | node.pod.ContainersStarted.p50 | ms | 3 | 3483.0000 | 3938.0000 | 5250.0000 | True |
| ARM64 | tigerstore | node.pod.ContainersStarted.p99 | ms | 3 | 5791.0000 | 5826.0000 | 7489.0000 | True |
| ARM64 | tigerstore | node.pod.Initialized.p50 | ms | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| ARM64 | tigerstore | node.pod.Initialized.p99 | ms | 3 | 1000.0000 | 1000.0000 | 1000.0000 | True |
| ARM64 | tigerstore | node.pod.PodReadyToStartContainers.p50 | ms | 3 | 2000.0000 | 2000.0000 | 3000.0000 | True |
| ARM64 | tigerstore | node.pod.PodReadyToStartContainers.p99 | ms | 3 | 3000.0000 | 4000.0000 | 6000.0000 | True |
| ARM64 | tigerstore | node.pod.PodScheduled.p50 | ms | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| ARM64 | tigerstore | node.pod.PodScheduled.p99 | ms | 3 | 1000.0000 | 1000.0000 | 1000.0000 | True |
| ARM64 | tigerstore | node.pod.Ready.p50 | ms | 3 | 3000.0000 | 3000.0000 | 5000.0000 | True |
| ARM64 | tigerstore | node.pod.Ready.p99 | ms | 3 | 5500.0000 | 6000.0000 | 7000.0000 | True |
| ARM64 | tigerstore | node.revision.member1 | revisions | 3 | 913.0000 | 914.0000 | 924.0000 | True |
| ARM64 | tigerstore | node.revision.member2 | revisions | 3 | 913.0000 | 914.0000 | 924.0000 | True |
| ARM64 | tigerstore | node.revision.member3 | revisions | 3 | 913.0000 | 914.0000 | 924.0000 | True |
| ARM64 | tigerstore | node.storage.etcd_request_duration_seconds | RPC/s | 3 | 68.9069 | 72.1992 | 75.7186 | True |
| ARM64 | tigerstore | node.window | s | 3 | 18.2518 | 19.1553 | 20.4914 | True |
| ARM64 | tigerstore | total.api.all.p50 | ms | 3 | 3.5388 | 3.6254 | 4.0345 | True |
| ARM64 | tigerstore | total.api.all.p99 | ms | 3 | 143.4277 | 160.7611 | 174.1741 | True |
| ARM64 | tigerstore | total.api.creates.count | requests | 3 | 2715.0000 | 2715.0000 | 2715.0000 | True |
| ARM64 | tigerstore | total.api.creates.rate | requests/s | 3 | 11.9760 | 12.1274 | 12.2088 | True |
| ARM64 | tigerstore | total.api.deletes.count | requests | 3 | 1671.0000 | 1677.0000 | 1678.0000 | True |
| ARM64 | tigerstore | total.api.deletes.rate | requests/s | 3 | 7.3974 | 7.4640 | 7.5457 | True |
| ARM64 | tigerstore | total.api.http403 | responses | 3 | 24.0000 | 27.0000 | 27.0000 | True |
| ARM64 | tigerstore | total.api.http404 | responses | 3 | 89.0000 | 97.0000 | 97.0000 | True |
| ARM64 | tigerstore | total.api.http409 | responses | 3 | 64.0000 | 66.0000 | 74.0000 | True |
| ARM64 | tigerstore | total.api.http500 | responses | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| ARM64 | tigerstore | total.api.requests | requests/s | 3 | 56.4219 | 57.0099 | 57.5367 | True |
| ARM64 | tigerstore | total.api.resource..p50 | ms | 3 | 2.5563 | 2.6522 | 2.6749 | True |
| ARM64 | tigerstore | total.api.resource..p99 | ms | 3 | 24.2667 | 42.1250 | 58.2500 | True |
| ARM64 | tigerstore | total.api.resource.configmaps.p50 | ms | 3 | 2.7145 | 2.7694 | 2.9088 | True |
| ARM64 | tigerstore | total.api.resource.configmaps.p99 | ms | 3 | 47.1627 | 76.1066 | 87.1078 | True |
| ARM64 | tigerstore | total.api.resource.controllerrevisions.p50 | ms | 3 | 3.0556 | 4.5833 | 11.6667 | True |
| ARM64 | tigerstore | total.api.resource.controllerrevisions.p99 | ms | 3 | 96.3333 | 189.0000 | 192.6667 | True |
| ARM64 | tigerstore | total.api.resource.cronjobs.p50 | ms | 3 | 3.6667 | 3.9286 | 37.5000 | True |
| ARM64 | tigerstore | total.api.resource.cronjobs.p99 | ms | 3 | 97.2500 | 178.0000 | 178.0000 | True |
| ARM64 | tigerstore | total.api.resource.csistoragecapacities.p50 | ms | 3 | 3.6667 | 3.6667 | 3.9286 | True |
| ARM64 | tigerstore | total.api.resource.csistoragecapacities.p99 | ms | 3 | 98.4286 | 178.0000 | 189.0000 | True |
| ARM64 | tigerstore | total.api.resource.daemonsets.p50 | ms | 3 | 3.2353 | 4.5833 | 4.5833 | True |
| ARM64 | tigerstore | total.api.resource.daemonsets.p99 | ms | 3 | 97.8000 | 98.7778 | 178.0000 | True |
| ARM64 | tigerstore | total.api.resource.deployments.p50 | ms | 3 | 4.3922 | 4.8492 | 8.3152 | True |
| ARM64 | tigerstore | total.api.resource.deployments.p99 | ms | 3 | 85.7000 | 93.3864 | 94.2813 | True |
| ARM64 | tigerstore | total.api.resource.endpoints.p50 | ms | 3 | 10.0000 | 10.9292 | 13.3761 | True |
| ARM64 | tigerstore | total.api.resource.endpoints.p99 | ms | 3 | 97.2593 | 174.0000 | 196.9091 | True |
| ARM64 | tigerstore | total.api.resource.endpointslices.p50 | ms | 3 | 11.0985 | 11.4561 | 13.5522 | True |
| ARM64 | tigerstore | total.api.resource.endpointslices.p99 | ms | 3 | 139.3333 | 175.0000 | 197.8333 | True |
| ARM64 | tigerstore | total.api.resource.events.p50 | ms | 3 | 8.6184 | 9.4612 | 12.8401 | True |
| ARM64 | tigerstore | total.api.resource.events.p99 | ms | 3 | 198.1707 | 220.8333 | 296.1538 | True |
| ARM64 | tigerstore | total.api.resource.horizontalpodautoscalers.p50 | ms | 3 | 3.2353 | 3.4375 | 4.2308 | True |
| ARM64 | tigerstore | total.api.resource.horizontalpodautoscalers.p99 | ms | 3 | 97.2500 | 98.4286 | 178.0000 | True |
| ARM64 | tigerstore | total.api.resource.ingresses.p50 | ms | 3 | 3.6667 | 4.2308 | 15.0000 | True |
| ARM64 | tigerstore | total.api.resource.ingresses.p99 | ms | 3 | 98.1667 | 98.9000 | 178.0000 | True |
| ARM64 | tigerstore | total.api.resource.ipaddresses.p50 | ms | 3 | 3.8462 | 4.1667 | 4.7619 | True |
| ARM64 | tigerstore | total.api.resource.ipaddresses.p99 | ms | 3 | 47.5000 | 47.5000 | 320.0000 | True |
| ARM64 | tigerstore | total.api.resource.jobs.p50 | ms | 3 | 3.0556 | 3.9286 | 5.0000 | True |
| ARM64 | tigerstore | total.api.resource.jobs.p99 | ms | 3 | 97.2500 | 98.7778 | 178.0000 | True |
| ARM64 | tigerstore | total.api.resource.leases.p50 | ms | 3 | 2.7492 | 2.8522 | 2.9893 | True |
| ARM64 | tigerstore | total.api.resource.leases.p99 | ms | 3 | 80.7500 | 98.4000 | 116.0000 | True |
| ARM64 | tigerstore | total.api.resource.limitranges.p50 | ms | 3 | 3.0488 | 3.3333 | 3.9062 | True |
| ARM64 | tigerstore | total.api.resource.limitranges.p99 | ms | 3 | 150.0000 | 152.0000 | 183.3333 | True |
| ARM64 | tigerstore | total.api.resource.namespaces.p50 | ms | 3 | 2.8182 | 2.8255 | 2.8704 | True |
| ARM64 | tigerstore | total.api.resource.namespaces.p99 | ms | 3 | 23.2286 | 23.2640 | 23.7855 | True |
| ARM64 | tigerstore | total.api.resource.networkpolicies.p50 | ms | 3 | 3.6667 | 4.2308 | 4.2308 | True |
| ARM64 | tigerstore | total.api.resource.networkpolicies.p99 | ms | 3 | 98.4286 | 98.6250 | 178.0000 | True |
| ARM64 | tigerstore | total.api.resource.nodes.p50 | ms | 3 | 2.5000 | 2.5000 | 2.5000 | True |
| ARM64 | tigerstore | total.api.resource.nodes.p99 | ms | 3 | 4.9500 | 4.9500 | 4.9500 | True |
| ARM64 | tigerstore | total.api.resource.persistentvolumeclaims.p50 | ms | 3 | 2.8947 | 3.2353 | 3.6667 | True |
| ARM64 | tigerstore | total.api.resource.persistentvolumeclaims.p99 | ms | 3 | 96.3333 | 97.8000 | 97.8000 | True |
| ARM64 | tigerstore | total.api.resource.podcertificaterequests.p50 | ms | 3 | 3.2353 | 3.4375 | 3.6667 | True |
| ARM64 | tigerstore | total.api.resource.podcertificaterequests.p99 | ms | 3 | 96.3333 | 97.8000 | 178.0000 | True |
| ARM64 | tigerstore | total.api.resource.poddisruptionbudgets.p50 | ms | 3 | 3.4375 | 3.6667 | 3.9286 | True |
| ARM64 | tigerstore | total.api.resource.poddisruptionbudgets.p99 | ms | 3 | 97.8000 | 178.0000 | 189.0000 | True |
| ARM64 | tigerstore | total.api.resource.pods.p50 | ms | 3 | 5.3917 | 7.0958 | 11.7182 | True |
| ARM64 | tigerstore | total.api.resource.pods.p99 | ms | 3 | 192.2254 | 276.1579 | 278.9231 | True |
| ARM64 | tigerstore | total.api.resource.podtemplates.p50 | ms | 3 | 3.0556 | 3.4375 | 4.5833 | True |
| ARM64 | tigerstore | total.api.resource.podtemplates.p99 | ms | 3 | 97.2500 | 98.1667 | 178.0000 | True |
| ARM64 | tigerstore | total.api.resource.replicasets.p50 | ms | 3 | 4.9523 | 7.1577 | 11.7667 | True |
| ARM64 | tigerstore | total.api.resource.replicasets.p99 | ms | 3 | 90.0238 | 97.4375 | 113.6667 | True |
| ARM64 | tigerstore | total.api.resource.replicationcontrollers.p50 | ms | 3 | 3.0556 | 3.4375 | 3.9286 | True |
| ARM64 | tigerstore | total.api.resource.replicationcontrollers.p99 | ms | 3 | 97.2500 | 98.1667 | 192.6667 | True |
| ARM64 | tigerstore | total.api.resource.resourceclaims.p50 | ms | 3 | 3.0556 | 3.2353 | 4.5833 | True |
| ARM64 | tigerstore | total.api.resource.resourceclaims.p99 | ms | 3 | 96.3333 | 96.3333 | 178.0000 | True |
| ARM64 | tigerstore | total.api.resource.resourceclaimtemplates.p50 | ms | 3 | 3.4375 | 3.4375 | 3.6667 | True |
| ARM64 | tigerstore | total.api.resource.resourceclaimtemplates.p99 | ms | 3 | 96.3333 | 98.4286 | 178.0000 | True |
| ARM64 | tigerstore | total.api.resource.resourcequotas.p50 | ms | 3 | 3.2353 | 3.6667 | 54.1667 | True |
| ARM64 | tigerstore | total.api.resource.resourcequotas.p99 | ms | 3 | 99.0833 | 178.0000 | 178.0000 | True |
| ARM64 | tigerstore | total.api.resource.rolebindings.p50 | ms | 3 | 3.0556 | 3.9286 | 25.0000 | True |
| ARM64 | tigerstore | total.api.resource.rolebindings.p99 | ms | 3 | 98.4286 | 98.6250 | 178.0000 | True |
| ARM64 | tigerstore | total.api.resource.roles.p50 | ms | 3 | 4.2308 | 4.5833 | 25.0000 | True |
| ARM64 | tigerstore | total.api.resource.roles.p99 | ms | 3 | 98.6250 | 98.6250 | 189.0000 | True |
| ARM64 | tigerstore | total.api.resource.secrets.p50 | ms | 3 | 2.6480 | 2.6480 | 2.7381 | True |
| ARM64 | tigerstore | total.api.resource.secrets.p99 | ms | 3 | 73.1667 | 83.9000 | 119.5000 | True |
| ARM64 | tigerstore | total.api.resource.serviceaccounts.p50 | ms | 3 | 15.0000 | 17.1429 | 18.1250 | True |
| ARM64 | tigerstore | total.api.resource.serviceaccounts.p99 | ms | 3 | 97.9375 | 97.9375 | 98.4091 | True |
| ARM64 | tigerstore | total.api.resource.services.p50 | ms | 3 | 3.8462 | 4.2857 | 4.4776 | True |
| ARM64 | tigerstore | total.api.resource.services.p99 | ms | 3 | 99.4118 | 140.0000 | 352.0000 | True |
| ARM64 | tigerstore | total.api.resource.statefulsets.p50 | ms | 3 | 3.4375 | 3.6667 | 4.2308 | True |
| ARM64 | tigerstore | total.api.resource.statefulsets.p99 | ms | 3 | 98.6250 | 178.0000 | 178.0000 | True |
| ARM64 | tigerstore | total.api.updates.count | requests | 3 | 4684.0000 | 4687.0000 | 4713.0000 | True |
| ARM64 | tigerstore | total.api.updates.rate | requests/s | 3 | 20.6614 | 20.9360 | 21.1935 | True |
| ARM64 | tigerstore | total.api.verb.DELETE.p50 | ms | 3 | 3.2721 | 3.2780 | 3.4297 | True |
| ARM64 | tigerstore | total.api.verb.DELETE.p99 | ms | 3 | 247.8095 | 332.2273 | 345.5909 | True |
| ARM64 | tigerstore | total.api.verb.GET.p50 | ms | 3 | 3.1970 | 3.2252 | 3.3857 | True |
| ARM64 | tigerstore | total.api.verb.GET.p99 | ms | 3 | 68.8095 | 92.5536 | 95.8000 | True |
| ARM64 | tigerstore | total.api.verb.LIST.p50 | ms | 3 | 3.7669 | 3.9195 | 3.9929 | True |
| ARM64 | tigerstore | total.api.verb.LIST.p99 | ms | 3 | 153.4167 | 153.7500 | 174.7273 | True |
| ARM64 | tigerstore | total.api.verb.PATCH.p50 | ms | 3 | 3.0219 | 3.1590 | 3.5258 | True |
| ARM64 | tigerstore | total.api.verb.PATCH.p99 | ms | 3 | 89.4474 | 92.6733 | 109.3571 | True |
| ARM64 | tigerstore | total.api.verb.POST.p50 | ms | 3 | 4.1410 | 4.2003 | 4.8147 | True |
| ARM64 | tigerstore | total.api.verb.POST.p99 | ms | 3 | 159.5522 | 159.7778 | 168.4902 | True |
| ARM64 | tigerstore | total.api.verb.PUT.p50 | ms | 3 | 5.7181 | 6.2208 | 10.4180 | True |
| ARM64 | tigerstore | total.api.verb.PUT.p99 | ms | 3 | 96.7692 | 145.9167 | 159.0455 | True |
| ARM64 | tigerstore | total.cpu.all | CPU s | 3 | 33.1700 | 33.3200 | 37.3600 | True |
| ARM64 | tigerstore | total.cpu.cores | cores | 3 | 0.1481 | 0.1498 | 0.1648 | True |
| ARM64 | tigerstore | total.cpu.member1 | CPU s | 3 | 9.3200 | 9.5200 | 15.3800 | True |
| ARM64 | tigerstore | total.cpu.member2 | CPU s | 3 | 10.5500 | 13.5300 | 13.9200 | True |
| ARM64 | tigerstore | total.cpu.member3 | CPU s | 3 | 9.8800 | 10.3200 | 11.4300 | True |
| ARM64 | tigerstore | total.revision.member1 | revisions | 3 | 11667.0000 | 11673.0000 | 11695.0000 | True |
| ARM64 | tigerstore | total.revision.member2 | revisions | 3 | 11667.0000 | 11673.0000 | 11695.0000 | True |
| ARM64 | tigerstore | total.revision.member3 | revisions | 3 | 11667.0000 | 11673.0000 | 11695.0000 | True |
| ARM64 | tigerstore | total.script | s | 3 | 296.7557 | 304.1032 | 304.2364 | True |
| ARM64 | tigerstore | total.storage.etcd_request_duration_seconds | RPC/s | 3 | 80.7092 | 81.5953 | 82.3277 | True |
| ARM64 | tigerstore | total.window | s | 3 | 222.3797 | 223.8732 | 226.7027 | True |
| ARM64 | tigerstore | total.workload | s | 3 | 221.4838 | 222.9083 | 225.7367 | True |
| ARM64 | tigerstore | update.api.all.p50 | ms | 3 | 3.1815 | 3.3171 | 3.8517 | True |
| ARM64 | tigerstore | update.api.all.p99 | ms | 3 | 67.8165 | 68.4557 | 92.8421 | True |
| ARM64 | tigerstore | update.api.creates.count | requests | 3 | 480.0000 | 480.0000 | 480.0000 | True |
| ARM64 | tigerstore | update.api.creates.rate | requests/s | 3 | 5.4986 | 5.5013 | 5.5015 | True |
| ARM64 | tigerstore | update.api.deletes.count | requests | 3 | 64.0000 | 64.0000 | 64.0000 | True |
| ARM64 | tigerstore | update.api.deletes.rate | requests/s | 3 | 0.7332 | 0.7335 | 0.7335 | True |
| ARM64 | tigerstore | update.api.http403 | responses | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| ARM64 | tigerstore | update.api.http404 | responses | 3 | 26.0000 | 28.0000 | 30.0000 | True |
| ARM64 | tigerstore | update.api.http409 | responses | 3 | 37.0000 | 38.0000 | 45.0000 | True |
| ARM64 | tigerstore | update.api.http500 | responses | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| ARM64 | tigerstore | update.api.requests | requests/s | 3 | 68.5828 | 68.5969 | 68.7789 | True |
| ARM64 | tigerstore | update.api.resource..p50 | ms | 3 | 2.5000 | 2.6025 | 2.6250 | True |
| ARM64 | tigerstore | update.api.resource..p99 | ms | 3 | 4.9500 | 20.8000 | 43.2500 | True |
| ARM64 | tigerstore | update.api.resource.configmaps.p50 | ms | 3 | 2.6918 | 2.7662 | 2.9372 | True |
| ARM64 | tigerstore | update.api.resource.configmaps.p99 | ms | 3 | 45.3750 | 47.1214 | 90.3936 | True |
| ARM64 | tigerstore | update.api.resource.deployments.p50 | ms | 3 | 4.5471 | 6.1404 | 10.9574 | True |
| ARM64 | tigerstore | update.api.resource.deployments.p99 | ms | 3 | 68.6250 | 74.1667 | 94.7333 | True |
| ARM64 | tigerstore | update.api.resource.endpoints.p50 | ms | 3 | 10.7500 | 12.0312 | 14.4245 | True |
| ARM64 | tigerstore | update.api.resource.endpoints.p99 | ms | 3 | 88.8333 | 91.6667 | 133.0000 | True |
| ARM64 | tigerstore | update.api.resource.endpointslices.p50 | ms | 3 | 11.3905 | 12.3224 | 14.1626 | True |
| ARM64 | tigerstore | update.api.resource.endpointslices.p99 | ms | 3 | 89.6429 | 91.4545 | 104.0000 | True |
| ARM64 | tigerstore | update.api.resource.events.p50 | ms | 3 | 3.8750 | 4.0789 | 8.6036 | True |
| ARM64 | tigerstore | update.api.resource.events.p99 | ms | 3 | 72.1667 | 80.2727 | 98.3000 | True |
| ARM64 | tigerstore | update.api.resource.leases.p50 | ms | 3 | 2.5636 | 2.7009 | 2.8009 | True |
| ARM64 | tigerstore | update.api.resource.leases.p99 | ms | 3 | 16.9333 | 22.3111 | 44.7500 | True |
| ARM64 | tigerstore | update.api.resource.limitranges.p50 | ms | 3 | 3.0357 | 4.2500 | 4.2500 | True |
| ARM64 | tigerstore | update.api.resource.limitranges.p99 | ms | 3 | 97.8750 | 183.0000 | 183.0000 | True |
| ARM64 | tigerstore | update.api.resource.nodes.p50 | ms | 3 | 2.5000 | 2.5000 | 2.5000 | True |
| ARM64 | tigerstore | update.api.resource.nodes.p99 | ms | 3 | 4.9500 | 4.9500 | 4.9500 | True |
| ARM64 | tigerstore | update.api.resource.pods.p50 | ms | 3 | 3.2869 | 3.5649 | 4.9563 | True |
| ARM64 | tigerstore | update.api.resource.pods.p99 | ms | 3 | 55.3125 | 64.9615 | 88.9211 | True |
| ARM64 | tigerstore | update.api.resource.replicasets.p50 | ms | 3 | 4.9257 | 7.3404 | 12.5720 | True |
| ARM64 | tigerstore | update.api.resource.replicasets.p99 | ms | 3 | 87.5625 | 99.8824 | 120.0000 | True |
| ARM64 | tigerstore | update.api.resource.secrets.p50 | ms | 3 | 2.5000 | 2.5000 | 2.6408 | True |
| ARM64 | tigerstore | update.api.resource.secrets.p99 | ms | 3 | 4.9500 | 4.9500 | 21.2500 | True |
| ARM64 | tigerstore | update.api.resource.services.p50 | ms | 3 | 2.5000 | 2.5000 | 2.6531 | True |
| ARM64 | tigerstore | update.api.resource.services.p99 | ms | 3 | 4.9500 | 4.9500 | 21.5333 | True |
| ARM64 | tigerstore | update.api.updates.count | requests | 3 | 4192.0000 | 4193.0000 | 4194.0000 | True |
| ARM64 | tigerstore | update.api.updates.rate | requests/s | 3 | 48.0444 | 48.0446 | 48.0579 | True |
| ARM64 | tigerstore | update.api.verb.DELETE.p50 | ms | 3 | 15.5882 | 16.6129 | 20.2000 | True |
| ARM64 | tigerstore | update.api.verb.DELETE.p99 | ms | 3 | 136.0000 | 184.0000 | 272.0000 | True |
| ARM64 | tigerstore | update.api.verb.GET.p50 | ms | 3 | 2.7067 | 2.7741 | 2.9816 | True |
| ARM64 | tigerstore | update.api.verb.GET.p99 | ms | 3 | 23.5400 | 24.9189 | 45.6786 | True |
| ARM64 | tigerstore | update.api.verb.LIST.p50 | ms | 3 | 3.3108 | 3.8281 | 3.8281 | True |
| ARM64 | tigerstore | update.api.verb.LIST.p99 | ms | 3 | 95.9167 | 151.0000 | 183.6667 | True |
| ARM64 | tigerstore | update.api.verb.PATCH.p50 | ms | 3 | 2.7707 | 2.8780 | 3.1986 | True |
| ARM64 | tigerstore | update.api.verb.PATCH.p99 | ms | 3 | 41.9063 | 44.1707 | 83.4057 | True |
| ARM64 | tigerstore | update.api.verb.POST.p50 | ms | 3 | 4.0000 | 4.2997 | 8.8971 | True |
| ARM64 | tigerstore | update.api.verb.POST.p99 | ms | 3 | 84.7143 | 85.0909 | 99.0000 | True |
| ARM64 | tigerstore | update.api.verb.PUT.p50 | ms | 3 | 6.4419 | 7.3050 | 11.9000 | True |
| ARM64 | tigerstore | update.api.verb.PUT.p99 | ms | 3 | 87.5968 | 91.3030 | 97.2255 | True |
| ARM64 | tigerstore | update.cpu.all | CPU s | 3 | 14.1800 | 14.4400 | 16.0200 | True |
| ARM64 | tigerstore | update.cpu.cores | cores | 3 | 0.1625 | 0.1655 | 0.1835 | True |
| ARM64 | tigerstore | update.cpu.member1 | CPU s | 3 | 3.9700 | 4.1100 | 6.5200 | True |
| ARM64 | tigerstore | update.cpu.member2 | CPU s | 3 | 4.4900 | 5.6900 | 6.0800 | True |
| ARM64 | tigerstore | update.cpu.member3 | CPU s | 3 | 4.2500 | 4.5200 | 5.0100 | True |
| ARM64 | tigerstore | update.duration | s | 3 | 86.5849 | 86.6175 | 86.6326 | True |
| ARM64 | tigerstore | update.exit | exit code | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| ARM64 | tigerstore | update.live_inventory | objects in tracked kinds | 3 | 1185.0000 | 1185.0000 | 1185.0000 | True |
| ARM64 | tigerstore | update.pod.ContainersReady.p50 | ms | 3 | 1000.0000 | 1000.0000 | 1000.0000 | True |
| ARM64 | tigerstore | update.pod.ContainersReady.p99 | ms | 3 | 2000.0000 | 2000.0000 | 2000.0000 | True |
| ARM64 | tigerstore | update.pod.ContainersStarted.p50 | ms | 3 | 1483.0000 | 1612.0000 | 1829.0000 | True |
| ARM64 | tigerstore | update.pod.ContainersStarted.p99 | ms | 3 | 2117.0000 | 2272.0000 | 2510.0000 | True |
| ARM64 | tigerstore | update.pod.Initialized.p50 | ms | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| ARM64 | tigerstore | update.pod.Initialized.p99 | ms | 3 | 0.0000 | 500.0000 | 1000.0000 | True |
| ARM64 | tigerstore | update.pod.PodReadyToStartContainers.p50 | ms | 3 | 1000.0000 | 1000.0000 | 1000.0000 | True |
| ARM64 | tigerstore | update.pod.PodReadyToStartContainers.p99 | ms | 3 | 1000.0000 | 1000.0000 | 1000.0000 | True |
| ARM64 | tigerstore | update.pod.PodScheduled.p50 | ms | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| ARM64 | tigerstore | update.pod.PodScheduled.p99 | ms | 3 | 0.0000 | 0.0000 | 1000.0000 | True |
| ARM64 | tigerstore | update.pod.Ready.p50 | ms | 3 | 1000.0000 | 1000.0000 | 1000.0000 | True |
| ARM64 | tigerstore | update.pod.Ready.p99 | ms | 3 | 2000.0000 | 2000.0000 | 2000.0000 | True |
| ARM64 | tigerstore | update.revision.member1 | revisions | 3 | 5488.0000 | 5493.0000 | 5495.0000 | True |
| ARM64 | tigerstore | update.revision.member2 | revisions | 3 | 5488.0000 | 5493.0000 | 5495.0000 | True |
| ARM64 | tigerstore | update.revision.member3 | revisions | 3 | 5488.0000 | 5493.0000 | 5495.0000 | True |
| ARM64 | tigerstore | update.storage.etcd_request_duration_seconds | RPC/s | 3 | 70.4193 | 70.4509 | 70.7951 | True |
| ARM64 | tigerstore | update.window | s | 3 | 87.2489 | 87.2522 | 87.2942 | True |
| X64 | etcd | delete.api.all.p50 | ms | 3 | 3.9004 | 4.0530 | 4.4725 | True |
| X64 | etcd | delete.api.all.p99 | ms | 3 | 378.0339 | 382.2609 | 1960.9677 | True |
| X64 | etcd | delete.api.creates.count | requests | 3 | 112.0000 | 112.0000 | 112.0000 | True |
| X64 | etcd | delete.api.creates.rate | requests/s | 3 | 1.6473 | 1.6486 | 1.7008 | True |
| X64 | etcd | delete.api.deletes.count | requests | 3 | 1521.0000 | 1521.0000 | 1521.0000 | True |
| X64 | etcd | delete.api.deletes.rate | requests/s | 3 | 22.3707 | 22.3879 | 23.0979 | True |
| X64 | etcd | delete.api.http403 | responses | 3 | 9.0000 | 9.0000 | 9.0000 | True |
| X64 | etcd | delete.api.http404 | responses | 3 | 37.0000 | 40.0000 | 46.0000 | True |
| X64 | etcd | delete.api.http409 | responses | 3 | 9.0000 | 9.0000 | 9.0000 | True |
| X64 | etcd | delete.api.http500 | responses | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| X64 | etcd | delete.api.requests | requests/s | 3 | 35.9166 | 36.0326 | 36.6287 | True |
| X64 | etcd | delete.api.resource..p50 | ms | 3 | 2.6136 | 2.6327 | 2.7982 | True |
| X64 | etcd | delete.api.resource..p99 | ms | 3 | 90.5000 | 185.0000 | 385.3333 | True |
| X64 | etcd | delete.api.resource.configmaps.p50 | ms | 3 | 2.7367 | 2.8331 | 3.2981 | True |
| X64 | etcd | delete.api.resource.configmaps.p99 | ms | 3 | 74.0938 | 88.2500 | 190.6739 | True |
| X64 | etcd | delete.api.resource.controllerrevisions.p50 | ms | 3 | 5.0000 | 18.3333 | 25.0000 | True |
| X64 | etcd | delete.api.resource.controllerrevisions.p99 | ms | 3 | 98.8750 | 191.0000 | 364.0000 | True |
| X64 | etcd | delete.api.resource.cronjobs.p50 | ms | 3 | 5.0000 | 5.0000 | 50.0000 | True |
| X64 | etcd | delete.api.resource.cronjobs.p99 | ms | 3 | 98.5000 | 98.5000 | 99.0000 | True |
| X64 | etcd | delete.api.resource.csistoragecapacities.p50 | ms | 3 | 21.0000 | 25.0000 | 68.7500 | True |
| X64 | etcd | delete.api.resource.csistoragecapacities.p99 | ms | 3 | 98.8750 | 182.0000 | 195.5000 | True |
| X64 | etcd | delete.api.resource.daemonsets.p50 | ms | 3 | 4.0909 | 4.5000 | 50.0000 | True |
| X64 | etcd | delete.api.resource.daemonsets.p99 | ms | 3 | 182.0000 | 191.0000 | 364.0000 | True |
| X64 | etcd | delete.api.resource.deployments.p50 | ms | 3 | 12.5000 | 13.8889 | 15.0000 | True |
| X64 | etcd | delete.api.resource.deployments.p99 | ms | 3 | 98.2500 | 98.4444 | 344.0000 | True |
| X64 | etcd | delete.api.resource.endpoints.p50 | ms | 3 | 31.6176 | 57.7586 | 81.2500 | True |
| X64 | etcd | delete.api.resource.endpoints.p99 | ms | 3 | 335.3333 | 355.6000 | 965.0000 | True |
| X64 | etcd | delete.api.resource.endpointslices.p50 | ms | 3 | 38.3333 | 63.6364 | 97.3214 | True |
| X64 | etcd | delete.api.resource.endpointslices.p99 | ms | 3 | 359.3333 | 374.5000 | 973.1111 | True |
| X64 | etcd | delete.api.resource.events.p50 | ms | 3 | 21.8000 | 30.8333 | 50.0000 | True |
| X64 | etcd | delete.api.resource.events.p99 | ms | 3 | 552.0000 | 752.0000 | 1130.0000 | True |
| X64 | etcd | delete.api.resource.horizontalpodautoscalers.p50 | ms | 3 | 4.0909 | 5.0000 | 55.5556 | True |
| X64 | etcd | delete.api.resource.horizontalpodautoscalers.p99 | ms | 3 | 98.2000 | 182.0000 | 182.0000 | True |
| X64 | etcd | delete.api.resource.ingresses.p50 | ms | 3 | 3.2143 | 5.0000 | 11.6667 | True |
| X64 | etcd | delete.api.resource.ingresses.p99 | ms | 3 | 98.7143 | 182.0000 | 191.0000 | True |
| X64 | etcd | delete.api.resource.ipaddresses.p50 | ms | 3 | 17.3077 | 116.6667 | 133.3333 | True |
| X64 | etcd | delete.api.resource.ipaddresses.p99 | ms | 3 | 184.0000 | 389.3333 | 392.0000 | True |
| X64 | etcd | delete.api.resource.jobs.p50 | ms | 3 | 4.0909 | 5.0000 | 57.1429 | True |
| X64 | etcd | delete.api.resource.jobs.p99 | ms | 3 | 98.7143 | 182.0000 | 194.0000 | True |
| X64 | etcd | delete.api.resource.leases.p50 | ms | 3 | 3.0278 | 3.3333 | 3.5714 | True |
| X64 | etcd | delete.api.resource.leases.p99 | ms | 3 | 99.1000 | 99.6000 | 393.3333 | True |
| X64 | etcd | delete.api.resource.limitranges.p50 | ms | 3 | 4.0909 | 4.0909 | 15.0000 | True |
| X64 | etcd | delete.api.resource.limitranges.p99 | ms | 3 | 182.0000 | 182.0000 | 194.0000 | True |
| X64 | etcd | delete.api.resource.namespaces.p50 | ms | 3 | 2.5847 | 2.6786 | 3.3152 | True |
| X64 | etcd | delete.api.resource.namespaces.p99 | ms | 3 | 18.9000 | 22.0000 | 139.0000 | True |
| X64 | etcd | delete.api.resource.networkpolicies.p50 | ms | 3 | 4.0909 | 4.5000 | 15.0000 | True |
| X64 | etcd | delete.api.resource.networkpolicies.p99 | ms | 3 | 98.5000 | 182.0000 | 364.0000 | True |
| X64 | etcd | delete.api.resource.nodes.p50 | ms | 3 | 2.5000 | 2.5000 | 5.0000 | True |
| X64 | etcd | delete.api.resource.nodes.p99 | ms | 3 | 4.9500 | 4.9500 | 49.5000 | True |
| X64 | etcd | delete.api.resource.persistentvolumeclaims.p50 | ms | 3 | 5.0000 | 5.0000 | 10.0000 | True |
| X64 | etcd | delete.api.resource.persistentvolumeclaims.p99 | ms | 3 | 98.5000 | 191.0000 | 196.4000 | True |
| X64 | etcd | delete.api.resource.podcertificaterequests.p50 | ms | 3 | 3.2143 | 4.5000 | 37.5000 | True |
| X64 | etcd | delete.api.resource.podcertificaterequests.p99 | ms | 3 | 95.5000 | 98.7143 | 194.0000 | True |
| X64 | etcd | delete.api.resource.poddisruptionbudgets.p50 | ms | 3 | 4.0909 | 5.0000 | 5.0000 | True |
| X64 | etcd | delete.api.resource.poddisruptionbudgets.p99 | ms | 3 | 182.0000 | 182.0000 | 194.0000 | True |
| X64 | etcd | delete.api.resource.pods.p50 | ms | 3 | 87.1212 | 97.1591 | 420.0000 | True |
| X64 | etcd | delete.api.resource.pods.p99 | ms | 3 | 561.4000 | 583.6667 | 2908.0952 | True |
| X64 | etcd | delete.api.resource.podtemplates.p50 | ms | 3 | 3.4615 | 4.0909 | 5.0000 | True |
| X64 | etcd | delete.api.resource.podtemplates.p99 | ms | 3 | 98.5000 | 98.7143 | 182.0000 | True |
| X64 | etcd | delete.api.resource.replicasets.p50 | ms | 3 | 8.8095 | 14.0909 | 14.5652 | True |
| X64 | etcd | delete.api.resource.replicasets.p99 | ms | 3 | 190.0000 | 350.0000 | 350.0000 | True |
| X64 | etcd | delete.api.resource.replicationcontrollers.p50 | ms | 3 | 3.7500 | 4.0909 | 55.5556 | True |
| X64 | etcd | delete.api.resource.replicationcontrollers.p99 | ms | 3 | 182.0000 | 182.0000 | 182.0000 | True |
| X64 | etcd | delete.api.resource.resourceclaims.p50 | ms | 3 | 4.0909 | 10.0000 | 62.5000 | True |
| X64 | etcd | delete.api.resource.resourceclaims.p99 | ms | 3 | 98.5000 | 182.0000 | 364.0000 | True |
| X64 | etcd | delete.api.resource.resourceclaimtemplates.p50 | ms | 3 | 3.7500 | 4.0909 | 60.0000 | True |
| X64 | etcd | delete.api.resource.resourceclaimtemplates.p99 | ms | 3 | 97.7500 | 182.0000 | 182.0000 | True |
| X64 | etcd | delete.api.resource.resourcequotas.p50 | ms | 3 | 3.4615 | 3.7500 | 5.0000 | True |
| X64 | etcd | delete.api.resource.resourcequotas.p99 | ms | 3 | 97.7500 | 182.0000 | 195.5000 | True |
| X64 | etcd | delete.api.resource.rolebindings.p50 | ms | 3 | 4.0909 | 4.5000 | 55.0000 | True |
| X64 | etcd | delete.api.resource.rolebindings.p99 | ms | 3 | 98.5000 | 99.1000 | 191.0000 | True |
| X64 | etcd | delete.api.resource.roles.p50 | ms | 3 | 3.7500 | 3.7500 | 4.5000 | True |
| X64 | etcd | delete.api.resource.roles.p99 | ms | 3 | 97.7500 | 182.0000 | 364.0000 | True |
| X64 | etcd | delete.api.resource.secrets.p50 | ms | 3 | 2.8205 | 3.4375 | 3.4375 | True |
| X64 | etcd | delete.api.resource.secrets.p99 | ms | 3 | 156.0000 | 178.0000 | 178.0000 | True |
| X64 | etcd | delete.api.resource.serviceaccounts.p50 | ms | 3 | 16.4286 | 17.0000 | 22.1429 | True |
| X64 | etcd | delete.api.resource.serviceaccounts.p99 | ms | 3 | 98.5000 | 182.0000 | 364.0000 | True |
| X64 | etcd | delete.api.resource.services.p50 | ms | 3 | 30.0000 | 90.0000 | 183.3333 | True |
| X64 | etcd | delete.api.resource.services.p99 | ms | 3 | 328.0000 | 564.0000 | 594.0000 | True |
| X64 | etcd | delete.api.resource.statefulsets.p50 | ms | 3 | 3.2143 | 3.7500 | 3.7500 | True |
| X64 | etcd | delete.api.resource.statefulsets.p99 | ms | 3 | 182.0000 | 182.0000 | 364.0000 | True |
| X64 | etcd | delete.api.updates.count | requests | 3 | 176.0000 | 204.0000 | 213.0000 | True |
| X64 | etcd | delete.api.updates.rate | requests/s | 3 | 2.6727 | 3.0004 | 3.1352 | True |
| X64 | etcd | delete.api.verb.DELETE.p50 | ms | 3 | 3.4216 | 3.5285 | 4.0026 | True |
| X64 | etcd | delete.api.verb.DELETE.p99 | ms | 3 | 432.8750 | 435.0667 | 2301.3636 | True |
| X64 | etcd | delete.api.verb.GET.p50 | ms | 3 | 3.3234 | 3.6842 | 3.9897 | True |
| X64 | etcd | delete.api.verb.GET.p99 | ms | 3 | 169.5000 | 250.6667 | 867.0000 | True |
| X64 | etcd | delete.api.verb.LIST.p50 | ms | 3 | 13.5714 | 52.9018 | 53.4768 | True |
| X64 | etcd | delete.api.verb.LIST.p99 | ms | 3 | 181.4706 | 189.3000 | 337.0000 | True |
| X64 | etcd | delete.api.verb.PATCH.p50 | ms | 3 | 43.7500 | 47.5000 | 450.0000 | True |
| X64 | etcd | delete.api.verb.PATCH.p99 | ms | 3 | 374.0000 | 374.0000 | 1237.5000 | True |
| X64 | etcd | delete.api.verb.POST.p50 | ms | 3 | 23.6275 | 34.5833 | 59.0000 | True |
| X64 | etcd | delete.api.verb.POST.p99 | ms | 3 | 339.5000 | 351.6000 | 959.6667 | True |
| X64 | etcd | delete.api.verb.PUT.p50 | ms | 3 | 4.9272 | 17.1429 | 32.6389 | True |
| X64 | etcd | delete.api.verb.PUT.p99 | ms | 3 | 342.0000 | 360.0000 | 982.5333 | True |
| X64 | etcd | delete.cpu.all | CPU s | 3 | 11.7400 | 14.2300 | 16.6600 | True |
| X64 | etcd | delete.cpu.cores | cores | 3 | 0.1727 | 0.2161 | 0.2454 | True |
| X64 | etcd | delete.cpu.member1 | CPU s | 3 | 4.3900 | 4.4300 | 4.9900 | True |
| X64 | etcd | delete.cpu.member2 | CPU s | 3 | 3.6900 | 4.3600 | 6.4200 | True |
| X64 | etcd | delete.cpu.member3 | CPU s | 3 | 3.6600 | 5.2500 | 5.4400 | True |
| X64 | etcd | delete.duration | s | 3 | 65.0300 | 67.0242 | 67.2750 | True |
| X64 | etcd | delete.exit | exit code | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| X64 | etcd | delete.live_inventory | objects in tracked kinds | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| X64 | etcd | delete.revision.member1 | revisions | 3 | 2740.0000 | 2768.0000 | 2776.0000 | True |
| X64 | etcd | delete.revision.member2 | revisions | 3 | 2740.0000 | 2768.0000 | 2776.0000 | True |
| X64 | etcd | delete.revision.member3 | revisions | 3 | 2740.0000 | 2768.0000 | 2776.0000 | True |
| X64 | etcd | delete.server.external-etcd-1.etcd_mvcc_delete_total | counter increments/s | 3 | 33.9316 | 33.9744 | 35.0263 | True |
| X64 | etcd | delete.server.external-etcd-1.etcd_mvcc_put_total | counter increments/s | 3 | 6.5741 | 6.7804 | 6.9068 | True |
| X64 | etcd | delete.server.external-etcd-1.etcd_mvcc_range_total | counter increments/s | 3 | 64.5980 | 69.5099 | 83.3525 | True |
| X64 | etcd | delete.server.external-etcd-1.etcd_network_client_grpc_received_bytes_total | counter increments/s | 3 | 7399.2695 | 8074.8096 | 8123.5791 | True |
| X64 | etcd | delete.server.external-etcd-1.etcd_network_client_grpc_sent_bytes_total | counter increments/s | 3 | 26330.5186 | 44346.8355 | 45708.9710 | True |
| X64 | etcd | delete.server.external-etcd-1.etcd_server_proposals_applied_total | counter increments/s | 3 | 42.3152 | 42.3834 | 43.3463 | True |
| X64 | etcd | delete.server.external-etcd-1.etcd_server_proposals_committed_total | counter increments/s | 3 | 42.3152 | 42.3834 | 43.3463 | True |
| X64 | etcd | delete.server.external-etcd-1.grpc_server_handled_total | counter increments/s | 3 | 35.1965 | 35.2409 | 36.1650 | True |
| X64 | etcd | delete.server.external-etcd-2.etcd_mvcc_delete_total | counter increments/s | 3 | 33.9351 | 33.9742 | 35.0309 | True |
| X64 | etcd | delete.server.external-etcd-2.etcd_mvcc_put_total | counter increments/s | 3 | 6.5749 | 6.7811 | 6.9068 | True |
| X64 | etcd | delete.server.external-etcd-2.etcd_mvcc_range_total | counter increments/s | 3 | 70.3047 | 80.4512 | 82.2121 | True |
| X64 | etcd | delete.server.external-etcd-2.etcd_network_client_grpc_received_bytes_total | counter increments/s | 3 | 7736.8594 | 8093.6009 | 8325.1144 | True |
| X64 | etcd | delete.server.external-etcd-2.etcd_network_client_grpc_sent_bytes_total | counter increments/s | 3 | 48203.3276 | 50572.8834 | 65803.2518 | True |
| X64 | etcd | delete.server.external-etcd-2.etcd_server_proposals_applied_total | counter increments/s | 3 | 42.3196 | 42.3830 | 43.3520 | True |
| X64 | etcd | delete.server.external-etcd-2.etcd_server_proposals_committed_total | counter increments/s | 3 | 42.3196 | 42.3830 | 43.3520 | True |
| X64 | etcd | delete.server.external-etcd-2.grpc_server_handled_total | counter increments/s | 3 | 35.1707 | 35.1965 | 36.1697 | True |
| X64 | etcd | delete.server.external-etcd-3.etcd_mvcc_delete_total | counter increments/s | 3 | 33.9353 | 33.9755 | 35.0387 | True |
| X64 | etcd | delete.server.external-etcd-3.etcd_mvcc_put_total | counter increments/s | 3 | 6.5764 | 6.7812 | 6.8923 | True |
| X64 | etcd | delete.server.external-etcd-3.etcd_mvcc_range_total | counter increments/s | 3 | 79.9388 | 81.8024 | 82.6833 | True |
| X64 | etcd | delete.server.external-etcd-3.etcd_network_client_grpc_received_bytes_total | counter increments/s | 3 | 7677.5729 | 8184.3895 | 8295.8136 | True |
| X64 | etcd | delete.server.external-etcd-3.etcd_network_client_grpc_sent_bytes_total | counter increments/s | 3 | 39965.2826 | 43437.1353 | 47200.0336 | True |
| X64 | etcd | delete.server.external-etcd-3.etcd_server_proposals_applied_total | counter increments/s | 3 | 42.3198 | 42.3699 | 43.3617 | True |
| X64 | etcd | delete.server.external-etcd-3.etcd_server_proposals_committed_total | counter increments/s | 3 | 42.3198 | 42.3699 | 43.3617 | True |
| X64 | etcd | delete.server.external-etcd-3.grpc_server_handled_total | counter increments/s | 3 | 35.1562 | 35.2420 | 36.1474 | True |
| X64 | etcd | delete.storage.etcd_request_duration_seconds | RPC/s | 3 | 103.9552 | 104.0207 | 107.0006 | True |
| X64 | etcd | delete.verified_deletion_rate | objects/s | 3 | 17.6143 | 17.6802 | 18.2223 | True |
| X64 | etcd | delete.window | s | 3 | 65.8501 | 67.9384 | 67.9909 | True |
| X64 | etcd | errors.observed | records | 3 | 21.0000 | 23.0000 | 25.0000 | True |
| X64 | etcd | errors.workload | records | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| X64 | etcd | events.observations | watch notifications | 3 | 2212.0000 | 2212.0000 | 2214.0000 | True |
| X64 | etcd | events.unique | events | 3 | 1094.0000 | 1094.0000 | 1095.0000 | True |
| X64 | etcd | mixed.api.all.p50 | ms | 3 | 3.8315 | 4.0296 | 4.6833 | True |
| X64 | etcd | mixed.api.all.p99 | ms | 3 | 61.8214 | 69.6875 | 369.7681 | True |
| X64 | etcd | mixed.api.creates.count | requests | 3 | 1640.0000 | 1640.0000 | 1641.0000 | True |
| X64 | etcd | mixed.api.creates.rate | requests/s | 3 | 35.8208 | 36.3780 | 36.4429 | True |
| X64 | etcd | mixed.api.deletes.count | requests | 3 | 73.0000 | 75.0000 | 79.0000 | True |
| X64 | etcd | mixed.api.deletes.rate | requests/s | 3 | 1.6222 | 1.6636 | 1.7245 | True |
| X64 | etcd | mixed.api.http403 | responses | 3 | 14.0000 | 15.0000 | 17.0000 | True |
| X64 | etcd | mixed.api.http404 | responses | 3 | 7.0000 | 9.0000 | 10.0000 | True |
| X64 | etcd | mixed.api.http409 | responses | 3 | 16.0000 | 21.0000 | 23.0000 | True |
| X64 | etcd | mixed.api.http500 | responses | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| X64 | etcd | mixed.api.requests | requests/s | 3 | 64.2416 | 65.2143 | 65.2862 | True |
| X64 | etcd | mixed.api.resource..p50 | ms | 3 | 2.6027 | 2.6370 | 2.9104 | True |
| X64 | etcd | mixed.api.resource..p99 | ms | 3 | 21.1500 | 31.0000 | 122.0000 | True |
| X64 | etcd | mixed.api.resource.configmaps.p50 | ms | 3 | 3.2439 | 3.3430 | 4.4746 | True |
| X64 | etcd | mixed.api.resource.configmaps.p99 | ms | 3 | 45.4740 | 48.3835 | 421.8333 | True |
| X64 | etcd | mixed.api.resource.controllerrevisions.p50 | ms | 3 | 2.5000 | 2.5000 | 2.5000 | True |
| X64 | etcd | mixed.api.resource.controllerrevisions.p99 | ms | 3 | 4.9500 | 4.9500 | 4.9500 | True |
| X64 | etcd | mixed.api.resource.cronjobs.p50 | ms | 3 | 2.5000 | 2.5000 | 5.0000 | True |
| X64 | etcd | mixed.api.resource.cronjobs.p99 | ms | 3 | 4.9500 | 4.9500 | 99.0000 | True |
| X64 | etcd | mixed.api.resource.csistoragecapacities.p50 | ms | 3 | 2.5000 | 3.3333 | 5.0000 | True |
| X64 | etcd | mixed.api.resource.csistoragecapacities.p99 | ms | 3 | 4.9500 | 24.2000 | 196.0000 | True |
| X64 | etcd | mixed.api.resource.daemonsets.p50 | ms | 3 | 2.5000 | 2.5000 | 5.0000 | True |
| X64 | etcd | mixed.api.resource.daemonsets.p99 | ms | 3 | 4.9500 | 4.9500 | 196.0000 | True |
| X64 | etcd | mixed.api.resource.deployments.p50 | ms | 3 | 4.9519 | 5.2326 | 12.0213 | True |
| X64 | etcd | mixed.api.resource.deployments.p99 | ms | 3 | 75.7500 | 91.4167 | 199.2500 | True |
| X64 | etcd | mixed.api.resource.endpoints.p50 | ms | 3 | 9.8077 | 10.5556 | 12.2414 | True |
| X64 | etcd | mixed.api.resource.endpoints.p99 | ms | 3 | 78.1667 | 134.5000 | 473.0000 | True |
| X64 | etcd | mixed.api.resource.endpointslices.p50 | ms | 3 | 8.7037 | 11.7857 | 12.7419 | True |
| X64 | etcd | mixed.api.resource.endpointslices.p99 | ms | 3 | 48.9583 | 95.7500 | 382.6667 | True |
| X64 | etcd | mixed.api.resource.events.p50 | ms | 3 | 4.8821 | 7.4096 | 10.3279 | True |
| X64 | etcd | mixed.api.resource.events.p99 | ms | 3 | 49.5625 | 49.8542 | 198.6667 | True |
| X64 | etcd | mixed.api.resource.horizontalpodautoscalers.p50 | ms | 3 | 3.3333 | 3.3333 | 5.0000 | True |
| X64 | etcd | mixed.api.resource.horizontalpodautoscalers.p99 | ms | 3 | 24.2000 | 24.2000 | 196.0000 | True |
| X64 | etcd | mixed.api.resource.ingresses.p50 | ms | 3 | 2.5000 | 2.5000 | 15.0000 | True |
| X64 | etcd | mixed.api.resource.ingresses.p99 | ms | 3 | 4.9500 | 4.9500 | 98.0000 | True |
| X64 | etcd | mixed.api.resource.ipaddresses.p50 | ms | 3 | 2.6087 | 3.0000 | 3.6765 | True |
| X64 | etcd | mixed.api.resource.ipaddresses.p99 | ms | 3 | 20.2000 | 23.8000 | 187.5000 | True |
| X64 | etcd | mixed.api.resource.jobs.p50 | ms | 3 | 2.5000 | 2.5000 | 2.5000 | True |
| X64 | etcd | mixed.api.resource.jobs.p99 | ms | 3 | 4.9500 | 4.9500 | 4.9500 | True |
| X64 | etcd | mixed.api.resource.leases.p50 | ms | 3 | 2.9911 | 3.1481 | 3.8333 | True |
| X64 | etcd | mixed.api.resource.leases.p99 | ms | 3 | 66.5000 | 88.6667 | 190.1429 | True |
| X64 | etcd | mixed.api.resource.limitranges.p50 | ms | 3 | 2.6923 | 2.9167 | 3.8889 | True |
| X64 | etcd | mixed.api.resource.limitranges.p99 | ms | 3 | 22.2000 | 93.0000 | 193.0000 | True |
| X64 | etcd | mixed.api.resource.namespaces.p50 | ms | 3 | 3.0099 | 3.1229 | 3.7602 | True |
| X64 | etcd | mixed.api.resource.namespaces.p99 | ms | 3 | 24.7763 | 24.8114 | 308.2500 | True |
| X64 | etcd | mixed.api.resource.networkpolicies.p50 | ms | 3 | 2.5000 | 2.5000 | 3.3333 | True |
| X64 | etcd | mixed.api.resource.networkpolicies.p99 | ms | 3 | 4.9500 | 4.9500 | 49.0000 | True |
| X64 | etcd | mixed.api.resource.nodes.p50 | ms | 3 | 2.5000 | 2.5000 | 2.5000 | True |
| X64 | etcd | mixed.api.resource.nodes.p99 | ms | 3 | 4.9500 | 4.9500 | 4.9500 | True |
| X64 | etcd | mixed.api.resource.persistentvolumeclaims.p50 | ms | 3 | 2.5000 | 3.3333 | 5.0000 | True |
| X64 | etcd | mixed.api.resource.persistentvolumeclaims.p99 | ms | 3 | 4.9500 | 98.0000 | 198.0000 | True |
| X64 | etcd | mixed.api.resource.podcertificaterequests.p50 | ms | 3 | 2.5000 | 2.5000 | 3.3333 | True |
| X64 | etcd | mixed.api.resource.podcertificaterequests.p99 | ms | 3 | 4.9500 | 4.9500 | 24.2000 | True |
| X64 | etcd | mixed.api.resource.poddisruptionbudgets.p50 | ms | 3 | 2.5000 | 3.3333 | 5.0000 | True |
| X64 | etcd | mixed.api.resource.poddisruptionbudgets.p99 | ms | 3 | 4.9500 | 196.0000 | 196.0000 | True |
| X64 | etcd | mixed.api.resource.pods.p50 | ms | 3 | 9.4444 | 9.6405 | 9.7179 | True |
| X64 | etcd | mixed.api.resource.pods.p99 | ms | 3 | 64.5000 | 76.3333 | 302.0000 | True |
| X64 | etcd | mixed.api.resource.podtemplates.p50 | ms | 3 | 2.5000 | 3.3333 | 5.0000 | True |
| X64 | etcd | mixed.api.resource.podtemplates.p99 | ms | 3 | 4.9500 | 98.0000 | 196.0000 | True |
| X64 | etcd | mixed.api.resource.replicasets.p50 | ms | 3 | 11.9444 | 12.9070 | 13.7879 | True |
| X64 | etcd | mixed.api.resource.replicasets.p99 | ms | 3 | 138.0000 | 139.0000 | 359.3333 | True |
| X64 | etcd | mixed.api.resource.replicationcontrollers.p50 | ms | 3 | 3.3333 | 3.3333 | 3.3333 | True |
| X64 | etcd | mixed.api.resource.replicationcontrollers.p99 | ms | 3 | 24.2000 | 98.0000 | 98.0000 | True |
| X64 | etcd | mixed.api.resource.resourceclaims.p50 | ms | 3 | 2.5000 | 3.3333 | 25.0000 | True |
| X64 | etcd | mixed.api.resource.resourceclaims.p99 | ms | 3 | 4.9500 | 49.0000 | 196.0000 | True |
| X64 | etcd | mixed.api.resource.resourceclaimtemplates.p50 | ms | 3 | 2.5000 | 3.3333 | 5.0000 | True |
| X64 | etcd | mixed.api.resource.resourceclaimtemplates.p99 | ms | 3 | 4.9500 | 98.0000 | 196.0000 | True |
| X64 | etcd | mixed.api.resource.resourcequotas.p50 | ms | 3 | 2.5000 | 2.5000 | 3.3333 | True |
| X64 | etcd | mixed.api.resource.resourcequotas.p99 | ms | 3 | 4.9500 | 4.9500 | 24.2000 | True |
| X64 | etcd | mixed.api.resource.rolebindings.p50 | ms | 3 | 2.5000 | 3.3333 | 3.3333 | True |
| X64 | etcd | mixed.api.resource.rolebindings.p99 | ms | 3 | 4.9500 | 24.2000 | 98.0000 | True |
| X64 | etcd | mixed.api.resource.roles.p50 | ms | 3 | 3.3333 | 3.3333 | 3.3333 | True |
| X64 | etcd | mixed.api.resource.roles.p99 | ms | 3 | 24.2000 | 98.0000 | 98.0000 | True |
| X64 | etcd | mixed.api.resource.secrets.p50 | ms | 3 | 3.0172 | 3.0172 | 4.3750 | True |
| X64 | etcd | mixed.api.resource.secrets.p99 | ms | 3 | 23.8333 | 82.5000 | 330.0000 | True |
| X64 | etcd | mixed.api.resource.serviceaccounts.p50 | ms | 3 | 12.2727 | 15.0000 | 17.0000 | True |
| X64 | etcd | mixed.api.resource.serviceaccounts.p99 | ms | 3 | 24.7455 | 93.0000 | 96.5000 | True |
| X64 | etcd | mixed.api.resource.services.p50 | ms | 3 | 15.4545 | 15.9524 | 16.0000 | True |
| X64 | etcd | mixed.api.resource.services.p99 | ms | 3 | 93.7500 | 93.7500 | 375.0000 | True |
| X64 | etcd | mixed.api.resource.statefulsets.p50 | ms | 3 | 3.3333 | 3.3333 | 3.3333 | True |
| X64 | etcd | mixed.api.resource.statefulsets.p99 | ms | 3 | 98.0000 | 98.0000 | 98.0000 | True |
| X64 | etcd | mixed.api.updates.count | requests | 3 | 246.0000 | 256.0000 | 258.0000 | True |
| X64 | etcd | mixed.api.updates.rate | requests/s | 3 | 5.3698 | 5.6887 | 5.7229 | True |
| X64 | etcd | mixed.api.verb.DELETE.p50 | ms | 3 | 3.9151 | 3.9500 | 4.0104 | True |
| X64 | etcd | mixed.api.verb.DELETE.p99 | ms | 3 | 361.5000 | 442.0000 | 834.0000 | True |
| X64 | etcd | mixed.api.verb.GET.p50 | ms | 3 | 2.9759 | 3.1066 | 3.6097 | True |
| X64 | etcd | mixed.api.verb.GET.p99 | ms | 3 | 24.6530 | 24.9059 | 261.7500 | True |
| X64 | etcd | mixed.api.verb.LIST.p50 | ms | 3 | 3.3333 | 3.4375 | 3.5714 | True |
| X64 | etcd | mixed.api.verb.LIST.p99 | ms | 3 | 99.2857 | 160.0000 | 197.3750 | True |
| X64 | etcd | mixed.api.verb.PATCH.p50 | ms | 3 | 12.7612 | 13.5393 | 13.8506 | True |
| X64 | etcd | mixed.api.verb.PATCH.p99 | ms | 3 | 49.3889 | 49.5208 | 318.6667 | True |
| X64 | etcd | mixed.api.verb.POST.p50 | ms | 3 | 3.6187 | 3.8290 | 4.8127 | True |
| X64 | etcd | mixed.api.verb.POST.p99 | ms | 3 | 43.8730 | 47.6921 | 380.1600 | True |
| X64 | etcd | mixed.api.verb.PUT.p50 | ms | 3 | 10.3947 | 11.0262 | 11.6667 | True |
| X64 | etcd | mixed.api.verb.PUT.p99 | ms | 3 | 76.3125 | 106.5000 | 416.0000 | True |
| X64 | etcd | mixed.cpu.all | CPU s | 3 | 8.7200 | 10.3300 | 11.7300 | True |
| X64 | etcd | mixed.cpu.cores | cores | 3 | 0.1902 | 0.2294 | 0.2597 | True |
| X64 | etcd | mixed.cpu.member1 | CPU s | 3 | 3.2700 | 3.2800 | 3.5400 | True |
| X64 | etcd | mixed.cpu.member2 | CPU s | 3 | 2.7900 | 3.1500 | 4.4400 | True |
| X64 | etcd | mixed.cpu.member3 | CPU s | 3 | 2.6600 | 3.7500 | 3.9000 | True |
| X64 | etcd | mixed.duration | s | 3 | 44.2487 | 44.2786 | 45.2019 | True |
| X64 | etcd | mixed.exit | exit code | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| X64 | etcd | mixed.live_inventory | objects in tracked kinds | 3 | 1096.0000 | 1096.0000 | 1096.0000 | True |
| X64 | etcd | mixed.net_live_creation_rate | objects/s | 3 | 24.2468 | 24.7524 | 24.7691 | True |
| X64 | etcd | mixed.pod.ContainersReady.p50 | ms | 3 | 1000.0000 | 1000.0000 | 2000.0000 | True |
| X64 | etcd | mixed.pod.ContainersReady.p99 | ms | 3 | 2000.0000 | 2000.0000 | 2000.0000 | True |
| X64 | etcd | mixed.pod.ContainersStarted.p50 | ms | 3 | 1882.0000 | 1970.0000 | 2147.0000 | True |
| X64 | etcd | mixed.pod.ContainersStarted.p99 | ms | 3 | 2442.0000 | 2593.0000 | 2706.0000 | True |
| X64 | etcd | mixed.pod.Initialized.p50 | ms | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| X64 | etcd | mixed.pod.Initialized.p99 | ms | 3 | 0.0000 | 1000.0000 | 1000.0000 | True |
| X64 | etcd | mixed.pod.PodReadyToStartContainers.p50 | ms | 3 | 1000.0000 | 1000.0000 | 1000.0000 | True |
| X64 | etcd | mixed.pod.PodReadyToStartContainers.p99 | ms | 3 | 1000.0000 | 1000.0000 | 1500.0000 | True |
| X64 | etcd | mixed.pod.PodScheduled.p50 | ms | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| X64 | etcd | mixed.pod.PodScheduled.p99 | ms | 3 | 0.0000 | 0.0000 | 1000.0000 | True |
| X64 | etcd | mixed.pod.Ready.p50 | ms | 3 | 1000.0000 | 1000.0000 | 2000.0000 | True |
| X64 | etcd | mixed.pod.Ready.p99 | ms | 3 | 2000.0000 | 2000.0000 | 2000.0000 | True |
| X64 | etcd | mixed.revision.member1 | revisions | 3 | 2494.0000 | 2503.0000 | 2504.0000 | True |
| X64 | etcd | mixed.revision.member2 | revisions | 3 | 2494.0000 | 2503.0000 | 2504.0000 | True |
| X64 | etcd | mixed.revision.member3 | revisions | 3 | 2494.0000 | 2503.0000 | 2504.0000 | True |
| X64 | etcd | mixed.server.external-etcd-1.etcd_mvcc_delete_total | counter increments/s | 3 | 7.1763 | 7.2605 | 7.2857 | True |
| X64 | etcd | mixed.server.external-etcd-1.etcd_mvcc_put_total | counter increments/s | 3 | 47.2240 | 48.1669 | 48.3121 | True |
| X64 | etcd | mixed.server.external-etcd-1.etcd_mvcc_range_total | counter increments/s | 3 | 65.4592 | 69.6163 | 73.3900 | True |
| X64 | etcd | mixed.server.external-etcd-1.etcd_network_client_grpc_received_bytes_total | counter increments/s | 3 | 24380.8434 | 24912.7633 | 25332.8380 | True |
| X64 | etcd | mixed.server.external-etcd-1.etcd_network_client_grpc_sent_bytes_total | counter increments/s | 3 | 17409.2730 | 55522.5449 | 65524.7972 | True |
| X64 | etcd | mixed.server.external-etcd-1.etcd_server_proposals_applied_total | counter increments/s | 3 | 55.0547 | 56.0029 | 56.2198 | True |
| X64 | etcd | mixed.server.external-etcd-1.etcd_server_proposals_committed_total | counter increments/s | 3 | 55.0547 | 56.0029 | 56.2198 | True |
| X64 | etcd | mixed.server.external-etcd-1.grpc_server_handled_total | counter increments/s | 3 | 26.8511 | 27.2046 | 27.2547 | True |
| X64 | etcd | mixed.server.external-etcd-2.etcd_mvcc_delete_total | counter increments/s | 3 | 7.1766 | 7.2610 | 7.2863 | True |
| X64 | etcd | mixed.server.external-etcd-2.etcd_mvcc_put_total | counter increments/s | 3 | 47.1821 | 48.1482 | 48.3160 | True |
| X64 | etcd | mixed.server.external-etcd-2.etcd_mvcc_range_total | counter increments/s | 3 | 69.2641 | 70.8608 | 74.1215 | True |
| X64 | etcd | mixed.server.external-etcd-2.etcd_network_client_grpc_received_bytes_total | counter increments/s | 3 | 23933.7048 | 24352.3256 | 25373.7480 | True |
| X64 | etcd | mixed.server.external-etcd-2.etcd_network_client_grpc_sent_bytes_total | counter increments/s | 3 | 33817.1044 | 47365.2419 | 75650.4784 | True |
| X64 | etcd | mixed.server.external-etcd-2.etcd_server_proposals_applied_total | counter increments/s | 3 | 55.0130 | 55.9847 | 56.2243 | True |
| X64 | etcd | mixed.server.external-etcd-2.etcd_server_proposals_committed_total | counter increments/s | 3 | 55.0130 | 55.9847 | 56.2243 | True |
| X64 | etcd | mixed.server.external-etcd-2.grpc_server_handled_total | counter increments/s | 3 | 26.8085 | 27.1401 | 27.2791 | True |
| X64 | etcd | mixed.server.external-etcd-3.etcd_mvcc_delete_total | counter increments/s | 3 | 7.1775 | 7.2618 | 7.2889 | True |
| X64 | etcd | mixed.server.external-etcd-3.etcd_mvcc_put_total | counter increments/s | 3 | 47.1880 | 48.1535 | 48.3331 | True |
| X64 | etcd | mixed.server.external-etcd-3.etcd_mvcc_range_total | counter increments/s | 3 | 69.0040 | 70.4664 | 71.7542 | True |
| X64 | etcd | mixed.server.external-etcd-3.etcd_network_client_grpc_received_bytes_total | counter increments/s | 3 | 24399.4254 | 24464.0564 | 24549.8642 | True |
| X64 | etcd | mixed.server.external-etcd-3.etcd_network_client_grpc_sent_bytes_total | counter increments/s | 3 | 28062.1425 | 43944.0592 | 50221.7248 | True |
| X64 | etcd | mixed.server.external-etcd-3.etcd_server_proposals_applied_total | counter increments/s | 3 | 55.0200 | 55.9908 | 56.2442 | True |
| X64 | etcd | mixed.server.external-etcd-3.etcd_server_proposals_committed_total | counter increments/s | 3 | 55.0200 | 55.9908 | 56.2442 | True |
| X64 | etcd | mixed.server.external-etcd-3.grpc_server_handled_total | counter increments/s | 3 | 26.7028 | 27.1430 | 27.2665 | True |
| X64 | etcd | mixed.storage.etcd_request_duration_seconds | RPC/s | 3 | 78.7577 | 79.9651 | 80.2634 | True |
| X64 | etcd | mixed.window | s | 3 | 45.0019 | 45.0821 | 45.8114 | True |
| X64 | etcd | node.api.all.p50 | ms | 3 | 20.4466 | 20.7505 | 24.0891 | True |
| X64 | etcd | node.api.all.p99 | ms | 3 | 369.9608 | 370.8889 | 544.9600 | True |
| X64 | etcd | node.api.creates.count | requests | 3 | 483.0000 | 483.0000 | 483.0000 | True |
| X64 | etcd | node.api.creates.rate | requests/s | 3 | 17.5682 | 21.4697 | 22.6279 | True |
| X64 | etcd | node.api.deletes.count | requests | 3 | 16.0000 | 16.0000 | 16.0000 | True |
| X64 | etcd | node.api.deletes.rate | requests/s | 3 | 0.5820 | 0.7112 | 0.7496 | True |
| X64 | etcd | node.api.http403 | responses | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| X64 | etcd | node.api.http404 | responses | 3 | 19.0000 | 21.0000 | 40.0000 | True |
| X64 | etcd | node.api.http409 | responses | 3 | 4.0000 | 6.0000 | 6.0000 | True |
| X64 | etcd | node.api.http500 | responses | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| X64 | etcd | node.api.requests | requests/s | 3 | 50.4857 | 60.7197 | 64.9321 | True |
| X64 | etcd | node.api.resource..p50 | ms | 3 | 3.4483 | 3.6458 | 3.9655 | True |
| X64 | etcd | node.api.resource..p99 | ms | 3 | 160.0000 | 530.0000 | 754.0000 | True |
| X64 | etcd | node.api.resource.configmaps.p50 | ms | 3 | 2.5000 | 2.5000 | 2.5000 | True |
| X64 | etcd | node.api.resource.configmaps.p99 | ms | 3 | 4.9500 | 4.9500 | 4.9500 | True |
| X64 | etcd | node.api.resource.endpoints.p50 | ms | 3 | 2.5000 | 3.7500 | 5.0000 | True |
| X64 | etcd | node.api.resource.endpoints.p99 | ms | 3 | 4.9500 | 98.5000 | 99.0000 | True |
| X64 | etcd | node.api.resource.endpointslices.p50 | ms | 3 | 3.7500 | 5.0000 | 5.0000 | True |
| X64 | etcd | node.api.resource.endpointslices.p99 | ms | 3 | 49.5000 | 49.5000 | 197.0000 | True |
| X64 | etcd | node.api.resource.events.p50 | ms | 3 | 21.3793 | 22.1166 | 32.3214 | True |
| X64 | etcd | node.api.resource.events.p99 | ms | 3 | 351.8750 | 386.9231 | 553.7500 | True |
| X64 | etcd | node.api.resource.leases.p50 | ms | 3 | 10.7143 | 20.0000 | 21.0000 | True |
| X64 | etcd | node.api.resource.leases.p99 | ms | 3 | 342.0000 | 344.0000 | 528.0000 | True |
| X64 | etcd | node.api.resource.limitranges.p50 | ms | 3 | 2.5000 | 2.5000 | 2.5000 | True |
| X64 | etcd | node.api.resource.limitranges.p99 | ms | 3 | 4.9500 | 4.9500 | 4.9500 | True |
| X64 | etcd | node.api.resource.namespaces.p50 | ms | 3 | 2.5000 | 2.5000 | 2.5000 | True |
| X64 | etcd | node.api.resource.namespaces.p99 | ms | 3 | 4.9500 | 4.9500 | 4.9500 | True |
| X64 | etcd | node.api.resource.nodes.p50 | ms | 3 | 2.5000 | 2.5000 | 2.5000 | True |
| X64 | etcd | node.api.resource.nodes.p99 | ms | 3 | 4.9500 | 4.9500 | 4.9500 | True |
| X64 | etcd | node.api.resource.pods.p50 | ms | 3 | 20.8182 | 21.8536 | 23.6246 | True |
| X64 | etcd | node.api.resource.pods.p99 | ms | 3 | 364.6667 | 382.1765 | 512.8571 | True |
| X64 | etcd | node.api.resource.serviceaccounts.p50 | ms | 3 | 4.8810 | 15.0000 | 15.0000 | True |
| X64 | etcd | node.api.resource.serviceaccounts.p99 | ms | 3 | 24.8000 | 24.8000 | 79.5000 | True |
| X64 | etcd | node.api.updates.count | requests | 3 | 44.0000 | 45.0000 | 52.0000 | True |
| X64 | etcd | node.api.updates.rate | requests/s | 3 | 1.8914 | 2.0003 | 2.0613 | True |
| X64 | etcd | node.api.verb.DELETE.p50 | ms | 3 | 15.0000 | 27.7778 | 50.0000 | True |
| X64 | etcd | node.api.verb.DELETE.p99 | ms | 3 | 24.8000 | 49.5556 | 99.0000 | True |
| X64 | etcd | node.api.verb.GET.p50 | ms | 3 | 10.7432 | 12.3826 | 14.5349 | True |
| X64 | etcd | node.api.verb.GET.p99 | ms | 3 | 315.2000 | 369.1250 | 548.4000 | True |
| X64 | etcd | node.api.verb.LIST.p50 | ms | 3 | 13.5714 | 16.2500 | 19.2857 | True |
| X64 | etcd | node.api.verb.LIST.p99 | ms | 3 | 97.5000 | 189.5000 | 574.0000 | True |
| X64 | etcd | node.api.verb.PATCH.p50 | ms | 3 | 23.0128 | 24.8438 | 25.7692 | True |
| X64 | etcd | node.api.verb.PATCH.p99 | ms | 3 | 343.2727 | 347.8333 | 541.0000 | True |
| X64 | etcd | node.api.verb.POST.p50 | ms | 3 | 23.6531 | 25.3571 | 39.8750 | True |
| X64 | etcd | node.api.verb.POST.p99 | ms | 3 | 387.4615 | 412.3333 | 544.1538 | True |
| X64 | etcd | node.api.verb.PUT.p50 | ms | 3 | 10.7143 | 20.0000 | 21.0000 | True |
| X64 | etcd | node.api.verb.PUT.p99 | ms | 3 | 342.0000 | 344.0000 | 528.0000 | True |
| X64 | etcd | node.cpu.all | CPU s | 3 | 3.1500 | 3.7300 | 4.6700 | True |
| X64 | etcd | node.cpu.cores | cores | 3 | 0.1475 | 0.1657 | 0.1696 | True |
| X64 | etcd | node.cpu.member1 | CPU s | 3 | 1.1200 | 1.1600 | 1.4700 | True |
| X64 | etcd | node.cpu.member2 | CPU s | 3 | 1.0100 | 1.1800 | 1.8000 | True |
| X64 | etcd | node.cpu.member3 | CPU s | 3 | 0.9800 | 1.4000 | 1.4300 | True |
| X64 | etcd | node.duration | s | 3 | 20.6639 | 21.7082 | 26.6253 | True |
| X64 | etcd | node.exit | exit code | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| X64 | etcd | node.live_inventory | objects in tracked kinds | 3 | 1177.0000 | 1177.0000 | 1177.0000 | True |
| X64 | etcd | node.net_live_creation_rate | objects/s | 3 | 3.0422 | 3.7313 | 3.9199 | True |
| X64 | etcd | node.pod.ContainersReady.p50 | ms | 3 | 6000.0000 | 6000.0000 | 9000.0000 | True |
| X64 | etcd | node.pod.ContainersReady.p99 | ms | 3 | 8000.0000 | 9000.0000 | 11500.0000 | True |
| X64 | etcd | node.pod.ContainersStarted.p50 | ms | 3 | 6237.0000 | 6610.0000 | 9151.0000 | True |
| X64 | etcd | node.pod.ContainersStarted.p99 | ms | 3 | 8334.0000 | 9324.0000 | 12022.0000 | True |
| X64 | etcd | node.pod.Initialized.p50 | ms | 3 | 0.0000 | 0.0000 | 1000.0000 | True |
| X64 | etcd | node.pod.Initialized.p99 | ms | 3 | 1000.0000 | 1000.0000 | 2000.0000 | True |
| X64 | etcd | node.pod.PodReadyToStartContainers.p50 | ms | 3 | 4000.0000 | 5000.0000 | 6000.0000 | True |
| X64 | etcd | node.pod.PodReadyToStartContainers.p99 | ms | 3 | 6000.0000 | 7500.0000 | 9000.0000 | True |
| X64 | etcd | node.pod.PodScheduled.p50 | ms | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| X64 | etcd | node.pod.PodScheduled.p99 | ms | 3 | 1000.0000 | 1000.0000 | 1000.0000 | True |
| X64 | etcd | node.pod.Ready.p50 | ms | 3 | 6000.0000 | 6000.0000 | 9000.0000 | True |
| X64 | etcd | node.pod.Ready.p99 | ms | 3 | 8000.0000 | 9000.0000 | 11500.0000 | True |
| X64 | etcd | node.revision.member1 | revisions | 3 | 918.0000 | 918.0000 | 930.0000 | True |
| X64 | etcd | node.revision.member2 | revisions | 3 | 918.0000 | 918.0000 | 930.0000 | True |
| X64 | etcd | node.revision.member3 | revisions | 3 | 918.0000 | 918.0000 | 930.0000 | True |
| X64 | etcd | node.server.external-etcd-1.etcd_mvcc_delete_total | counter increments/s | 3 | 0.5800 | 0.7107 | 0.7491 | True |
| X64 | etcd | node.server.external-etcd-1.etcd_mvcc_put_total | counter increments/s | 3 | 33.1314 | 40.0639 | 42.2286 | True |
| X64 | etcd | node.server.external-etcd-1.etcd_mvcc_range_total | counter increments/s | 3 | 48.4587 | 51.6856 | 54.4819 | True |
| X64 | etcd | node.server.external-etcd-1.etcd_network_client_grpc_received_bytes_total | counter increments/s | 3 | 25884.6820 | 29932.3500 | 30317.9571 | True |
| X64 | etcd | node.server.external-etcd-1.etcd_network_client_grpc_sent_bytes_total | counter increments/s | 3 | 20873.5740 | 23664.5402 | 122198.1194 | True |
| X64 | etcd | node.server.external-etcd-1.etcd_server_proposals_applied_total | counter increments/s | 3 | 34.0739 | 41.3076 | 43.4458 | True |
| X64 | etcd | node.server.external-etcd-1.etcd_server_proposals_committed_total | counter increments/s | 3 | 34.0739 | 41.3076 | 43.4458 | True |
| X64 | etcd | node.server.external-etcd-1.grpc_server_handled_total | counter increments/s | 3 | 17.8707 | 21.4089 | 22.6592 | True |
| X64 | etcd | node.server.external-etcd-2.etcd_mvcc_delete_total | counter increments/s | 3 | 0.5798 | 0.7107 | 0.7488 | True |
| X64 | etcd | node.server.external-etcd-2.etcd_mvcc_put_total | counter increments/s | 3 | 33.1214 | 40.0657 | 42.2156 | True |
| X64 | etcd | node.server.external-etcd-2.etcd_mvcc_range_total | counter increments/s | 3 | 40.4414 | 66.9390 | 71.3266 | True |
| X64 | etcd | node.server.external-etcd-2.etcd_network_client_grpc_received_bytes_total | counter increments/s | 3 | 23986.1252 | 26866.2275 | 33625.6473 | True |
| X64 | etcd | node.server.external-etcd-2.etcd_network_client_grpc_sent_bytes_total | counter increments/s | 3 | 45812.2207 | 160296.4197 | 164578.7530 | True |
| X64 | etcd | node.server.external-etcd-2.etcd_server_proposals_applied_total | counter increments/s | 3 | 34.0636 | 41.3094 | 43.4325 | True |
| X64 | etcd | node.server.external-etcd-2.etcd_server_proposals_committed_total | counter increments/s | 3 | 34.0636 | 41.3094 | 43.4325 | True |
| X64 | etcd | node.server.external-etcd-2.grpc_server_handled_total | counter increments/s | 3 | 17.9377 | 21.2766 | 23.1203 | True |
| X64 | etcd | node.server.external-etcd-3.etcd_mvcc_delete_total | counter increments/s | 3 | 0.5798 | 0.7107 | 0.7486 | True |
| X64 | etcd | node.server.external-etcd-3.etcd_mvcc_put_total | counter increments/s | 3 | 33.1217 | 40.0631 | 42.2051 | True |
| X64 | etcd | node.server.external-etcd-3.etcd_mvcc_range_total | counter increments/s | 3 | 41.7464 | 49.4349 | 51.6568 | True |
| X64 | etcd | node.server.external-etcd-3.etcd_network_client_grpc_received_bytes_total | counter increments/s | 3 | 23485.4531 | 30764.8247 | 32138.6599 | True |
| X64 | etcd | node.server.external-etcd-3.etcd_network_client_grpc_sent_bytes_total | counter increments/s | 3 | 19666.9221 | 47731.3408 | 50559.1869 | True |
| X64 | etcd | node.server.external-etcd-3.etcd_server_proposals_applied_total | counter increments/s | 3 | 34.0639 | 41.3068 | 43.4216 | True |
| X64 | etcd | node.server.external-etcd-3.etcd_server_proposals_committed_total | counter increments/s | 3 | 34.0639 | 41.3068 | 43.4216 | True |
| X64 | etcd | node.server.external-etcd-3.grpc_server_handled_total | counter increments/s | 3 | 17.8292 | 21.2752 | 22.8806 | True |
| X64 | etcd | node.storage.etcd_request_duration_seconds | RPC/s | 3 | 51.9406 | 62.4978 | 67.1340 | True |
| X64 | etcd | node.window | s | 3 | 21.3454 | 22.4968 | 27.4929 | True |
| X64 | etcd | total.api.all.p50 | ms | 3 | 4.3563 | 4.6326 | 4.8144 | True |
| X64 | etcd | total.api.all.p99 | ms | 3 | 248.2190 | 313.8876 | 567.1910 | True |
| X64 | etcd | total.api.creates.count | requests | 3 | 2715.0000 | 2715.0000 | 2716.0000 | True |
| X64 | etcd | total.api.creates.rate | requests/s | 3 | 11.3616 | 11.7836 | 11.8014 | True |
| X64 | etcd | total.api.deletes.count | requests | 3 | 1674.0000 | 1676.0000 | 1680.0000 | True |
| X64 | etcd | total.api.deletes.rate | requests/s | 3 | 7.0136 | 7.2765 | 7.2888 | True |
| X64 | etcd | total.api.http403 | responses | 3 | 23.0000 | 24.0000 | 26.0000 | True |
| X64 | etcd | total.api.http404 | responses | 3 | 91.0000 | 96.0000 | 126.0000 | True |
| X64 | etcd | total.api.http409 | responses | 3 | 79.0000 | 82.0000 | 85.0000 | True |
| X64 | etcd | total.api.http500 | responses | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| X64 | etcd | total.api.requests | requests/s | 3 | 53.8324 | 55.6211 | 55.7940 | True |
| X64 | etcd | total.api.resource..p50 | ms | 3 | 2.6945 | 2.7980 | 2.8947 | True |
| X64 | etcd | total.api.resource..p99 | ms | 3 | 95.3750 | 330.4000 | 415.0000 | True |
| X64 | etcd | total.api.resource.configmaps.p50 | ms | 3 | 3.0144 | 3.2275 | 3.5471 | True |
| X64 | etcd | total.api.resource.configmaps.p99 | ms | 3 | 105.4808 | 182.1324 | 252.2642 | True |
| X64 | etcd | total.api.resource.controllerrevisions.p50 | ms | 3 | 4.2308 | 4.5833 | 5.0000 | True |
| X64 | etcd | total.api.resource.controllerrevisions.p99 | ms | 3 | 98.6250 | 189.0000 | 356.0000 | True |
| X64 | etcd | total.api.resource.cronjobs.p50 | ms | 3 | 4.2308 | 5.0000 | 33.3333 | True |
| X64 | etcd | total.api.resource.cronjobs.p99 | ms | 3 | 98.1667 | 98.6250 | 98.7778 | True |
| X64 | etcd | total.api.resource.csistoragecapacities.p50 | ms | 3 | 11.6667 | 13.0000 | 62.5000 | True |
| X64 | etcd | total.api.resource.csistoragecapacities.p99 | ms | 3 | 98.6250 | 178.0000 | 195.6000 | True |
| X64 | etcd | total.api.resource.daemonsets.p50 | ms | 3 | 3.6667 | 4.5833 | 18.3333 | True |
| X64 | etcd | total.api.resource.daemonsets.p99 | ms | 3 | 178.0000 | 189.0000 | 356.0000 | True |
| X64 | etcd | total.api.resource.deployments.p50 | ms | 3 | 12.1981 | 12.7419 | 15.7653 | True |
| X64 | etcd | total.api.resource.deployments.p99 | ms | 3 | 98.4032 | 183.5833 | 190.3810 | True |
| X64 | etcd | total.api.resource.endpoints.p50 | ms | 3 | 16.1538 | 16.9731 | 18.0290 | True |
| X64 | etcd | total.api.resource.endpoints.p99 | ms | 3 | 198.8929 | 221.6000 | 855.0000 | True |
| X64 | etcd | total.api.resource.endpointslices.p50 | ms | 3 | 17.4775 | 18.3333 | 19.6570 | True |
| X64 | etcd | total.api.resource.endpointslices.p99 | ms | 3 | 278.2222 | 314.3333 | 881.5556 | True |
| X64 | etcd | total.api.resource.events.p50 | ms | 3 | 14.3613 | 15.3111 | 18.8095 | True |
| X64 | etcd | total.api.resource.events.p99 | ms | 3 | 332.6923 | 496.1538 | 572.9231 | True |
| X64 | etcd | total.api.resource.horizontalpodautoscalers.p50 | ms | 3 | 4.2308 | 4.5833 | 5.0000 | True |
| X64 | etcd | total.api.resource.horizontalpodautoscalers.p99 | ms | 3 | 178.0000 | 178.0000 | 178.0000 | True |
| X64 | etcd | total.api.resource.ingresses.p50 | ms | 3 | 3.0556 | 4.2308 | 13.0000 | True |
| X64 | etcd | total.api.resource.ingresses.p99 | ms | 3 | 98.4286 | 178.0000 | 189.0000 | True |
| X64 | etcd | total.api.resource.ipaddresses.p50 | ms | 3 | 4.3478 | 5.0000 | 37.5000 | True |
| X64 | etcd | total.api.resource.ipaddresses.p99 | ms | 3 | 160.0000 | 373.3333 | 379.5000 | True |
| X64 | etcd | total.api.resource.jobs.p50 | ms | 3 | 3.6667 | 4.2308 | 4.5833 | True |
| X64 | etcd | total.api.resource.jobs.p99 | ms | 3 | 98.4286 | 178.0000 | 192.6667 | True |
| X64 | etcd | total.api.resource.leases.p50 | ms | 3 | 3.1342 | 3.3846 | 3.5911 | True |
| X64 | etcd | total.api.resource.leases.p99 | ms | 3 | 139.7500 | 232.0000 | 304.4000 | True |
| X64 | etcd | total.api.resource.limitranges.p50 | ms | 3 | 4.0323 | 4.1667 | 4.2500 | True |
| X64 | etcd | total.api.resource.limitranges.p99 | ms | 3 | 183.3333 | 183.3333 | 187.2500 | True |
| X64 | etcd | total.api.resource.namespaces.p50 | ms | 3 | 2.9484 | 3.0208 | 3.6577 | True |
| X64 | etcd | total.api.resource.namespaces.p99 | ms | 3 | 24.5746 | 24.6250 | 291.0000 | True |
| X64 | etcd | total.api.resource.networkpolicies.p50 | ms | 3 | 3.9286 | 3.9286 | 4.5833 | True |
| X64 | etcd | total.api.resource.networkpolicies.p99 | ms | 3 | 98.1667 | 178.0000 | 356.0000 | True |
| X64 | etcd | total.api.resource.nodes.p50 | ms | 3 | 2.5000 | 2.5000 | 3.0000 | True |
| X64 | etcd | total.api.resource.nodes.p99 | ms | 3 | 4.9500 | 4.9500 | 48.5000 | True |
| X64 | etcd | total.api.resource.persistentvolumeclaims.p50 | ms | 3 | 4.2308 | 5.0000 | 5.0000 | True |
| X64 | etcd | total.api.resource.persistentvolumeclaims.p99 | ms | 3 | 98.1667 | 189.0000 | 196.8571 | True |
| X64 | etcd | total.api.resource.podcertificaterequests.p50 | ms | 3 | 3.2353 | 3.9286 | 4.5833 | True |
| X64 | etcd | total.api.resource.podcertificaterequests.p99 | ms | 3 | 94.5000 | 98.4286 | 192.6667 | True |
| X64 | etcd | total.api.resource.poddisruptionbudgets.p50 | ms | 3 | 4.2308 | 4.2308 | 4.5833 | True |
| X64 | etcd | total.api.resource.poddisruptionbudgets.p99 | ms | 3 | 178.0000 | 189.0000 | 194.5000 | True |
| X64 | etcd | total.api.resource.pods.p50 | ms | 3 | 12.3482 | 14.6415 | 17.1229 | True |
| X64 | etcd | total.api.resource.pods.p99 | ms | 3 | 384.0845 | 426.8800 | 1940.1667 | True |
| X64 | etcd | total.api.resource.podtemplates.p50 | ms | 3 | 3.4375 | 3.6667 | 5.0000 | True |
| X64 | etcd | total.api.resource.podtemplates.p99 | ms | 3 | 98.1667 | 98.6250 | 189.0000 | True |
| X64 | etcd | total.api.resource.replicasets.p50 | ms | 3 | 13.7603 | 15.1034 | 19.4727 | True |
| X64 | etcd | total.api.resource.replicasets.p99 | ms | 3 | 177.0714 | 195.2000 | 313.0000 | True |
| X64 | etcd | total.api.resource.replicationcontrollers.p50 | ms | 3 | 3.6667 | 3.9286 | 25.0000 | True |
| X64 | etcd | total.api.resource.replicationcontrollers.p99 | ms | 3 | 178.0000 | 178.0000 | 178.0000 | True |
| X64 | etcd | total.api.resource.resourceclaims.p50 | ms | 3 | 3.9286 | 5.0000 | 13.0000 | True |
| X64 | etcd | total.api.resource.resourceclaims.p99 | ms | 3 | 98.1667 | 189.0000 | 356.0000 | True |
| X64 | etcd | total.api.resource.resourceclaimtemplates.p50 | ms | 3 | 3.6667 | 4.2308 | 25.0000 | True |
| X64 | etcd | total.api.resource.resourceclaimtemplates.p99 | ms | 3 | 97.8000 | 178.0000 | 189.0000 | True |
| X64 | etcd | total.api.resource.resourcequotas.p50 | ms | 3 | 3.2353 | 3.4375 | 4.5833 | True |
| X64 | etcd | total.api.resource.resourcequotas.p99 | ms | 3 | 97.2500 | 178.0000 | 194.5000 | True |
| X64 | etcd | total.api.resource.rolebindings.p50 | ms | 3 | 3.9286 | 4.2308 | 15.0000 | True |
| X64 | etcd | total.api.resource.rolebindings.p99 | ms | 3 | 98.1667 | 98.9000 | 189.0000 | True |
| X64 | etcd | total.api.resource.roles.p50 | ms | 3 | 3.6667 | 3.6667 | 4.2308 | True |
| X64 | etcd | total.api.resource.roles.p99 | ms | 3 | 97.8000 | 178.0000 | 356.0000 | True |
| X64 | etcd | total.api.resource.secrets.p50 | ms | 3 | 2.7568 | 2.9167 | 3.0962 | True |
| X64 | etcd | total.api.resource.secrets.p99 | ms | 3 | 92.3750 | 119.5000 | 179.6667 | True |
| X64 | etcd | total.api.resource.serviceaccounts.p50 | ms | 3 | 9.4118 | 16.0526 | 16.6667 | True |
| X64 | etcd | total.api.resource.serviceaccounts.p99 | ms | 3 | 97.6429 | 167.0000 | 254.0000 | True |
| X64 | etcd | total.api.resource.services.p50 | ms | 3 | 4.4776 | 4.9180 | 6.2500 | True |
| X64 | etcd | total.api.resource.services.p99 | ms | 3 | 196.0000 | 480.0000 | 580.0000 | True |
| X64 | etcd | total.api.resource.statefulsets.p50 | ms | 3 | 3.2353 | 3.6667 | 3.6667 | True |
| X64 | etcd | total.api.resource.statefulsets.p99 | ms | 3 | 178.0000 | 178.0000 | 356.0000 | True |
| X64 | etcd | total.api.updates.count | requests | 3 | 4680.0000 | 4693.0000 | 4732.0000 | True |
| X64 | etcd | total.api.updates.rate | requests/s | 3 | 19.8022 | 20.3428 | 20.3609 | True |
| X64 | etcd | total.api.verb.DELETE.p50 | ms | 3 | 3.5890 | 3.6934 | 4.1585 | True |
| X64 | etcd | total.api.verb.DELETE.p99 | ms | 3 | 413.2500 | 435.7647 | 2227.2727 | True |
| X64 | etcd | total.api.verb.GET.p50 | ms | 3 | 3.6622 | 3.8516 | 3.9828 | True |
| X64 | etcd | total.api.verb.GET.p99 | ms | 3 | 190.6857 | 272.3077 | 371.6000 | True |
| X64 | etcd | total.api.verb.LIST.p50 | ms | 3 | 4.3021 | 4.4683 | 4.7567 | True |
| X64 | etcd | total.api.verb.LIST.p99 | ms | 3 | 185.5600 | 193.3200 | 306.1667 | True |
| X64 | etcd | total.api.verb.PATCH.p50 | ms | 3 | 3.7094 | 3.7393 | 4.1574 | True |
| X64 | etcd | total.api.verb.PATCH.p99 | ms | 3 | 164.4937 | 182.7869 | 261.1923 | True |
| X64 | etcd | total.api.verb.POST.p50 | ms | 3 | 6.1388 | 9.0596 | 11.5922 | True |
| X64 | etcd | total.api.verb.POST.p99 | ms | 3 | 260.1818 | 334.4651 | 423.3600 | True |
| X64 | etcd | total.api.verb.PUT.p50 | ms | 3 | 13.4717 | 13.7582 | 17.2247 | True |
| X64 | etcd | total.api.verb.PUT.p99 | ms | 3 | 194.1538 | 233.0476 | 592.6667 | True |
| X64 | etcd | total.cpu.all | CPU s | 3 | 41.6800 | 50.1200 | 56.6100 | True |
| X64 | etcd | total.cpu.cores | cores | 3 | 0.1808 | 0.2178 | 0.2368 | True |
| X64 | etcd | total.cpu.member1 | CPU s | 3 | 15.4700 | 15.8200 | 17.1600 | True |
| X64 | etcd | total.cpu.member2 | CPU s | 3 | 13.3700 | 15.3700 | 21.5300 | True |
| X64 | etcd | total.cpu.member3 | CPU s | 3 | 12.8400 | 17.9200 | 18.9300 | True |
| X64 | etcd | total.revision.member1 | revisions | 3 | 11667.0000 | 11682.0000 | 11717.0000 | True |
| X64 | etcd | total.revision.member2 | revisions | 3 | 11667.0000 | 11682.0000 | 11717.0000 | True |
| X64 | etcd | total.revision.member3 | revisions | 3 | 11667.0000 | 11682.0000 | 11717.0000 | True |
| X64 | etcd | total.script | s | 3 | 289.3032 | 294.9782 | 297.1867 | True |
| X64 | etcd | total.server.external-etcd-1.etcd_mvcc_delete_total | counter increments/s | 3 | 11.3583 | 11.7819 | 11.7970 | True |
| X64 | etcd | total.server.external-etcd-1.etcd_mvcc_put_total | counter increments/s | 3 | 37.6601 | 38.8941 | 38.8976 | True |
| X64 | etcd | total.server.external-etcd-1.etcd_mvcc_range_total | counter increments/s | 3 | 63.1216 | 68.7814 | 83.2484 | True |
| X64 | etcd | total.server.external-etcd-1.etcd_network_client_grpc_received_bytes_total | counter increments/s | 3 | 24005.9892 | 24911.7036 | 25202.2604 | True |
| X64 | etcd | total.server.external-etcd-1.etcd_network_client_grpc_sent_bytes_total | counter increments/s | 3 | 19735.7563 | 68939.0038 | 82602.6586 | True |
| X64 | etcd | total.server.external-etcd-1.etcd_server_proposals_applied_total | counter increments/s | 3 | 49.9262 | 51.6564 | 51.6723 | True |
| X64 | etcd | total.server.external-etcd-1.etcd_server_proposals_committed_total | counter increments/s | 3 | 49.9262 | 51.6564 | 51.6723 | True |
| X64 | etcd | total.server.external-etcd-1.grpc_server_handled_total | counter increments/s | 3 | 26.1429 | 27.0354 | 27.0949 | True |
| X64 | etcd | total.server.external-etcd-2.etcd_mvcc_delete_total | counter increments/s | 3 | 11.3580 | 11.7823 | 11.7968 | True |
| X64 | etcd | total.server.external-etcd-2.etcd_mvcc_put_total | counter increments/s | 3 | 37.6592 | 38.8953 | 38.8968 | True |
| X64 | etcd | total.server.external-etcd-2.etcd_mvcc_range_total | counter increments/s | 3 | 67.3364 | 70.2984 | 87.7943 | True |
| X64 | etcd | total.server.external-etcd-2.etcd_network_client_grpc_received_bytes_total | counter increments/s | 3 | 24327.2874 | 24619.2281 | 24791.1335 | True |
| X64 | etcd | total.server.external-etcd-2.etcd_network_client_grpc_sent_bytes_total | counter increments/s | 3 | 38966.9072 | 78308.6937 | 104392.4988 | True |
| X64 | etcd | total.server.external-etcd-2.etcd_server_proposals_applied_total | counter increments/s | 3 | 49.9250 | 51.6580 | 51.6712 | True |
| X64 | etcd | total.server.external-etcd-2.etcd_server_proposals_committed_total | counter increments/s | 3 | 49.9250 | 51.6580 | 51.6712 | True |
| X64 | etcd | total.server.external-etcd-2.grpc_server_handled_total | counter increments/s | 3 | 26.1464 | 27.0348 | 27.1131 | True |
| X64 | etcd | total.server.external-etcd-3.etcd_mvcc_delete_total | counter increments/s | 3 | 11.3577 | 11.7825 | 11.7966 | True |
| X64 | etcd | total.server.external-etcd-3.etcd_mvcc_put_total | counter increments/s | 3 | 37.6582 | 38.8960 | 38.8963 | True |
| X64 | etcd | total.server.external-etcd-3.etcd_mvcc_range_total | counter increments/s | 3 | 69.4024 | 72.1870 | 79.3240 | True |
| X64 | etcd | total.server.external-etcd-3.etcd_network_client_grpc_received_bytes_total | counter increments/s | 3 | 23148.4311 | 23999.9516 | 24481.9846 | True |
| X64 | etcd | total.server.external-etcd-3.etcd_network_client_grpc_sent_bytes_total | counter increments/s | 3 | 36281.5497 | 55419.5658 | 59026.6566 | True |
| X64 | etcd | total.server.external-etcd-3.etcd_server_proposals_applied_total | counter increments/s | 3 | 49.9237 | 51.6589 | 51.6706 | True |
| X64 | etcd | total.server.external-etcd-3.etcd_server_proposals_committed_total | counter increments/s | 3 | 49.9237 | 51.6589 | 51.6706 | True |
| X64 | etcd | total.server.external-etcd-3.grpc_server_handled_total | counter increments/s | 3 | 26.1457 | 27.0519 | 27.0962 | True |
| X64 | etcd | total.storage.etcd_request_duration_seconds | RPC/s | 3 | 76.7731 | 79.5326 | 79.6388 | True |
| X64 | etcd | total.window | s | 3 | 230.0566 | 230.4905 | 238.9638 | True |
| X64 | etcd | total.workload | s | 3 | 228.9570 | 229.4879 | 237.7435 | True |
| X64 | etcd | update.api.all.p50 | ms | 3 | 4.1162 | 4.2048 | 4.9067 | True |
| X64 | etcd | update.api.all.p99 | ms | 3 | 147.2360 | 183.0750 | 196.5829 | True |
| X64 | etcd | update.api.creates.count | requests | 3 | 480.0000 | 480.0000 | 480.0000 | True |
| X64 | etcd | update.api.creates.rate | requests/s | 3 | 5.4804 | 5.4921 | 5.5023 | True |
| X64 | etcd | update.api.deletes.count | requests | 3 | 64.0000 | 64.0000 | 64.0000 | True |
| X64 | etcd | update.api.deletes.rate | requests/s | 3 | 0.7307 | 0.7323 | 0.7336 | True |
| X64 | etcd | update.api.http403 | responses | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| X64 | etcd | update.api.http404 | responses | 3 | 24.0000 | 30.0000 | 30.0000 | True |
| X64 | etcd | update.api.http409 | responses | 3 | 43.0000 | 49.0000 | 51.0000 | True |
| X64 | etcd | update.api.http500 | responses | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| X64 | etcd | update.api.requests | requests/s | 3 | 68.4824 | 68.6050 | 68.8698 | True |
| X64 | etcd | update.api.resource..p50 | ms | 3 | 2.6667 | 2.8017 | 2.8261 | True |
| X64 | etcd | update.api.resource..p99 | ms | 3 | 84.0000 | 135.0000 | 170.0000 | True |
| X64 | etcd | update.api.resource.configmaps.p50 | ms | 3 | 3.0248 | 3.3337 | 3.3367 | True |
| X64 | etcd | update.api.resource.configmaps.p99 | ms | 3 | 143.9400 | 172.6000 | 241.9474 | True |
| X64 | etcd | update.api.resource.deployments.p50 | ms | 3 | 13.3721 | 14.6178 | 19.4755 | True |
| X64 | etcd | update.api.resource.deployments.p99 | ms | 3 | 119.5000 | 183.5000 | 194.5833 | True |
| X64 | etcd | update.api.resource.endpoints.p50 | ms | 3 | 14.4167 | 17.7344 | 19.4915 | True |
| X64 | etcd | update.api.resource.endpoints.p99 | ms | 3 | 177.6667 | 185.5714 | 299.5000 | True |
| X64 | etcd | update.api.resource.endpointslices.p50 | ms | 3 | 15.3659 | 18.2768 | 21.8590 | True |
| X64 | etcd | update.api.resource.endpointslices.p99 | ms | 3 | 207.3333 | 257.5000 | 286.4000 | True |
| X64 | etcd | update.api.resource.events.p50 | ms | 3 | 10.2910 | 11.5502 | 15.9910 | True |
| X64 | etcd | update.api.resource.events.p99 | ms | 3 | 96.8095 | 186.6000 | 226.4000 | True |
| X64 | etcd | update.api.resource.leases.p50 | ms | 3 | 2.8505 | 3.1989 | 3.3989 | True |
| X64 | etcd | update.api.resource.leases.p99 | ms | 3 | 24.6857 | 96.5000 | 97.6250 | True |
| X64 | etcd | update.api.resource.limitranges.p50 | ms | 3 | 5.0000 | 6.4286 | 12.5000 | True |
| X64 | etcd | update.api.resource.limitranges.p99 | ms | 3 | 95.7500 | 182.0000 | 191.5000 | True |
| X64 | etcd | update.api.resource.nodes.p50 | ms | 3 | 2.5000 | 2.5000 | 2.5000 | True |
| X64 | etcd | update.api.resource.nodes.p99 | ms | 3 | 4.9500 | 4.9500 | 4.9500 | True |
| X64 | etcd | update.api.resource.pods.p50 | ms | 3 | 4.2203 | 8.7897 | 12.8193 | True |
| X64 | etcd | update.api.resource.pods.p99 | ms | 3 | 140.4167 | 176.5000 | 186.4000 | True |
| X64 | etcd | update.api.resource.replicasets.p50 | ms | 3 | 14.6809 | 15.8929 | 21.5899 | True |
| X64 | etcd | update.api.resource.replicasets.p99 | ms | 3 | 174.7500 | 190.8182 | 284.5714 | True |
| X64 | etcd | update.api.resource.secrets.p50 | ms | 3 | 2.6408 | 2.6408 | 2.6786 | True |
| X64 | etcd | update.api.resource.secrets.p99 | ms | 3 | 21.2500 | 21.2500 | 22.0000 | True |
| X64 | etcd | update.api.resource.services.p50 | ms | 3 | 2.7660 | 2.9545 | 3.0233 | True |
| X64 | etcd | update.api.resource.services.p99 | ms | 3 | 22.9200 | 23.7000 | 43.5000 | True |
| X64 | etcd | update.api.updates.count | requests | 3 | 4186.0000 | 4190.0000 | 4193.0000 | True |
| X64 | etcd | update.api.updates.rate | requests/s | 3 | 47.8738 | 47.9411 | 47.9842 | True |
| X64 | etcd | update.api.verb.DELETE.p50 | ms | 3 | 22.0370 | 37.5000 | 47.2222 | True |
| X64 | etcd | update.api.verb.DELETE.p99 | ms | 3 | 357.3333 | 472.0000 | 472.0000 | True |
| X64 | etcd | update.api.verb.GET.p50 | ms | 3 | 3.1839 | 3.2393 | 3.6879 | True |
| X64 | etcd | update.api.verb.GET.p99 | ms | 3 | 77.2143 | 95.5385 | 175.0000 | True |
| X64 | etcd | update.api.verb.LIST.p50 | ms | 3 | 3.9062 | 4.3750 | 5.6667 | True |
| X64 | etcd | update.api.verb.LIST.p99 | ms | 3 | 187.7500 | 300.0000 | 302.0000 | True |
| X64 | etcd | update.api.verb.PATCH.p50 | ms | 3 | 3.3608 | 3.4269 | 3.7510 | True |
| X64 | etcd | update.api.verb.PATCH.p99 | ms | 3 | 128.3137 | 165.0505 | 216.8780 | True |
| X64 | etcd | update.api.verb.POST.p50 | ms | 3 | 11.2882 | 12.4306 | 16.7518 | True |
| X64 | etcd | update.api.verb.POST.p99 | ms | 3 | 99.5000 | 174.8235 | 249.1429 | True |
| X64 | etcd | update.api.verb.PUT.p50 | ms | 3 | 13.5095 | 15.3167 | 19.8539 | True |
| X64 | etcd | update.api.verb.PUT.p99 | ms | 3 | 180.0000 | 192.8382 | 242.5333 | True |
| X64 | etcd | update.cpu.all | CPU s | 3 | 17.5200 | 21.0700 | 22.6900 | True |
| X64 | etcd | update.cpu.cores | cores | 3 | 0.2008 | 0.2411 | 0.2590 | True |
| X64 | etcd | update.cpu.member1 | CPU s | 3 | 6.4600 | 6.7600 | 6.9100 | True |
| X64 | etcd | update.cpu.member2 | CPU s | 3 | 5.7200 | 6.4500 | 8.5300 | True |
| X64 | etcd | update.cpu.member3 | CPU s | 3 | 5.3400 | 7.2500 | 7.8600 | True |
| X64 | etcd | update.duration | s | 3 | 86.5672 | 86.5906 | 86.6675 | True |
| X64 | etcd | update.exit | exit code | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| X64 | etcd | update.live_inventory | objects in tracked kinds | 3 | 1185.0000 | 1185.0000 | 1185.0000 | True |
| X64 | etcd | update.pod.ContainersReady.p50 | ms | 3 | 2000.0000 | 2000.0000 | 2000.0000 | True |
| X64 | etcd | update.pod.ContainersReady.p99 | ms | 3 | 2000.0000 | 2000.0000 | 3000.0000 | True |
| X64 | etcd | update.pod.ContainersStarted.p50 | ms | 3 | 2082.0000 | 2154.0000 | 2753.0000 | True |
| X64 | etcd | update.pod.ContainersStarted.p99 | ms | 3 | 2657.0000 | 2822.0000 | 3625.0000 | True |
| X64 | etcd | update.pod.Initialized.p50 | ms | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| X64 | etcd | update.pod.Initialized.p99 | ms | 3 | 1000.0000 | 1000.0000 | 1000.0000 | True |
| X64 | etcd | update.pod.PodReadyToStartContainers.p50 | ms | 3 | 1000.0000 | 1000.0000 | 2000.0000 | True |
| X64 | etcd | update.pod.PodReadyToStartContainers.p99 | ms | 3 | 1500.0000 | 2000.0000 | 2000.0000 | True |
| X64 | etcd | update.pod.PodScheduled.p50 | ms | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| X64 | etcd | update.pod.PodScheduled.p99 | ms | 3 | 500.0000 | 500.0000 | 1000.0000 | True |
| X64 | etcd | update.pod.Ready.p50 | ms | 3 | 2000.0000 | 2000.0000 | 2000.0000 | True |
| X64 | etcd | update.pod.Ready.p99 | ms | 3 | 2000.0000 | 2000.0000 | 3000.0000 | True |
| X64 | etcd | update.revision.member1 | revisions | 3 | 5490.0000 | 5491.0000 | 5492.0000 | True |
| X64 | etcd | update.revision.member2 | revisions | 3 | 5490.0000 | 5491.0000 | 5492.0000 | True |
| X64 | etcd | update.revision.member3 | revisions | 3 | 5490.0000 | 5491.0000 | 5492.0000 | True |
| X64 | etcd | update.server.external-etcd-1.etcd_mvcc_delete_total | counter increments/s | 3 | 0.7303 | 0.7323 | 0.7335 | True |
| X64 | etcd | update.server.external-etcd-1.etcd_mvcc_put_total | counter increments/s | 3 | 61.9396 | 62.0870 | 62.2122 | True |
| X64 | etcd | update.server.external-etcd-1.etcd_mvcc_range_total | counter increments/s | 3 | 68.9515 | 80.2088 | 105.6714 | True |
| X64 | etcd | update.server.external-etcd-1.etcd_network_client_grpc_received_bytes_total | counter increments/s | 3 | 38434.3786 | 39320.8877 | 39591.9967 | True |
| X64 | etcd | update.server.external-etcd-1.etcd_network_client_grpc_sent_bytes_total | counter increments/s | 3 | 16616.6074 | 108963.8582 | 121995.1534 | True |
| X64 | etcd | update.server.external-etcd-1.etcd_server_proposals_applied_total | counter increments/s | 3 | 63.4801 | 63.5745 | 63.8283 | True |
| X64 | etcd | update.server.external-etcd-1.etcd_server_proposals_committed_total | counter increments/s | 3 | 63.4801 | 63.5745 | 63.8283 | True |
| X64 | etcd | update.server.external-etcd-1.grpc_server_handled_total | counter increments/s | 3 | 23.9519 | 23.9720 | 24.0918 | True |
| X64 | etcd | update.server.external-etcd-2.etcd_mvcc_delete_total | counter increments/s | 3 | 0.7303 | 0.7323 | 0.7334 | True |
| X64 | etcd | update.server.external-etcd-2.etcd_mvcc_put_total | counter increments/s | 3 | 61.9371 | 62.0841 | 62.2015 | True |
| X64 | etcd | update.server.external-etcd-2.etcd_mvcc_range_total | counter increments/s | 3 | 71.6248 | 78.6978 | 110.9611 | True |
| X64 | etcd | update.server.external-etcd-2.etcd_network_client_grpc_received_bytes_total | counter increments/s | 3 | 37831.3619 | 39423.0537 | 39760.3243 | True |
| X64 | etcd | update.server.external-etcd-2.etcd_network_client_grpc_sent_bytes_total | counter increments/s | 3 | 35223.8394 | 103511.2674 | 143563.6370 | True |
| X64 | etcd | update.server.external-etcd-2.etcd_server_proposals_applied_total | counter increments/s | 3 | 63.4776 | 63.5716 | 63.8172 | True |
| X64 | etcd | update.server.external-etcd-2.etcd_server_proposals_committed_total | counter increments/s | 3 | 63.4776 | 63.5716 | 63.8172 | True |
| X64 | etcd | update.server.external-etcd-2.grpc_server_handled_total | counter increments/s | 3 | 23.9709 | 23.9966 | 24.1334 | True |
| X64 | etcd | update.server.external-etcd-3.etcd_mvcc_delete_total | counter increments/s | 3 | 0.7303 | 0.7322 | 0.7333 | True |
| X64 | etcd | update.server.external-etcd-3.etcd_mvcc_put_total | counter increments/s | 3 | 61.9360 | 62.0730 | 62.1947 | True |
| X64 | etcd | update.server.external-etcd-3.etcd_mvcc_range_total | counter increments/s | 3 | 71.4651 | 77.0101 | 103.6755 | True |
| X64 | etcd | update.server.external-etcd-3.etcd_network_client_grpc_received_bytes_total | counter increments/s | 3 | 36143.5428 | 36652.2586 | 38230.3624 | True |
| X64 | etcd | update.server.external-etcd-3.etcd_network_client_grpc_sent_bytes_total | counter increments/s | 3 | 33034.9413 | 86583.1751 | 87826.7492 | True |
| X64 | etcd | update.server.external-etcd-3.etcd_server_proposals_applied_total | counter increments/s | 3 | 63.4765 | 63.5601 | 63.8103 | True |
| X64 | etcd | update.server.external-etcd-3.etcd_server_proposals_committed_total | counter increments/s | 3 | 63.4765 | 63.5601 | 63.8103 | True |
| X64 | etcd | update.server.external-etcd-3.grpc_server_handled_total | counter increments/s | 3 | 23.9962 | 24.0124 | 24.2110 | True |
| X64 | etcd | update.storage.etcd_request_duration_seconds | RPC/s | 3 | 70.4120 | 70.5158 | 70.7842 | True |
| X64 | etcd | update.window | s | 3 | 87.2370 | 87.3989 | 87.5845 | True |
| X64 | tigerstore | delete.api.all.p50 | ms | 3 | 3.5545 | 3.5905 | 3.6870 | True |
| X64 | tigerstore | delete.api.all.p99 | ms | 3 | 346.0000 | 357.3000 | 367.1803 | True |
| X64 | tigerstore | delete.api.creates.count | requests | 3 | 112.0000 | 112.0000 | 112.0000 | True |
| X64 | tigerstore | delete.api.creates.rate | requests/s | 3 | 1.6990 | 1.7032 | 1.7035 | True |
| X64 | tigerstore | delete.api.deletes.count | requests | 3 | 1521.0000 | 1521.0000 | 1521.0000 | True |
| X64 | tigerstore | delete.api.deletes.rate | requests/s | 3 | 23.0734 | 23.1298 | 23.1342 | True |
| X64 | tigerstore | delete.api.http403 | responses | 3 | 9.0000 | 9.0000 | 9.0000 | True |
| X64 | tigerstore | delete.api.http404 | responses | 3 | 35.0000 | 40.0000 | 42.0000 | True |
| X64 | tigerstore | delete.api.http409 | responses | 3 | 9.0000 | 9.0000 | 9.0000 | True |
| X64 | tigerstore | delete.api.http422 | responses | 1 | 0.0000 | 0.0000 | 0.0000 | True |
| X64 | tigerstore | delete.api.http500 | responses | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| X64 | tigerstore | delete.api.requests | requests/s | 3 | 36.4229 | 36.8008 | 37.3251 | True |
| X64 | tigerstore | delete.api.resource..p50 | ms | 3 | 2.5664 | 2.5901 | 2.6364 | True |
| X64 | tigerstore | delete.api.resource..p99 | ms | 3 | 24.3600 | 71.2500 | 92.0000 | True |
| X64 | tigerstore | delete.api.resource.configmaps.p50 | ms | 3 | 2.6064 | 2.6304 | 2.7280 | True |
| X64 | tigerstore | delete.api.resource.configmaps.p99 | ms | 3 | 44.7955 | 49.4821 | 73.7917 | True |
| X64 | tigerstore | delete.api.resource.controllerrevisions.p50 | ms | 3 | 3.4615 | 4.5000 | 25.0000 | True |
| X64 | tigerstore | delete.api.resource.controllerrevisions.p99 | ms | 3 | 97.7500 | 98.7143 | 182.0000 | True |
| X64 | tigerstore | delete.api.resource.cronjobs.p50 | ms | 3 | 3.4615 | 4.0909 | 4.5000 | True |
| X64 | tigerstore | delete.api.resource.cronjobs.p99 | ms | 3 | 97.0000 | 182.0000 | 182.0000 | True |
| X64 | tigerstore | delete.api.resource.csistoragecapacities.p50 | ms | 3 | 2.8125 | 3.2143 | 3.4615 | True |
| X64 | tigerstore | delete.api.resource.csistoragecapacities.p99 | ms | 3 | 95.5000 | 97.0000 | 97.0000 | True |
| X64 | tigerstore | delete.api.resource.daemonsets.p50 | ms | 3 | 3.4615 | 3.4615 | 3.4615 | True |
| X64 | tigerstore | delete.api.resource.daemonsets.p99 | ms | 3 | 97.0000 | 98.2000 | 182.0000 | True |
| X64 | tigerstore | delete.api.resource.deployments.p50 | ms | 3 | 2.9167 | 3.1818 | 5.0000 | True |
| X64 | tigerstore | delete.api.resource.deployments.p99 | ms | 3 | 95.3333 | 96.5000 | 172.0000 | True |
| X64 | tigerstore | delete.api.resource.endpoints.p50 | ms | 3 | 17.5000 | 22.0588 | 24.6296 | True |
| X64 | tigerstore | delete.api.resource.endpoints.p99 | ms | 3 | 199.5833 | 354.4000 | 610.0000 | True |
| X64 | tigerstore | delete.api.resource.endpointslices.p50 | ms | 3 | 17.8571 | 22.1429 | 31.2500 | True |
| X64 | tigerstore | delete.api.resource.endpointslices.p99 | ms | 3 | 198.7273 | 314.6667 | 361.6000 | True |
| X64 | tigerstore | delete.api.resource.events.p50 | ms | 3 | 20.9184 | 22.3913 | 31.7308 | True |
| X64 | tigerstore | delete.api.resource.events.p99 | ms | 3 | 352.0000 | 504.0000 | 576.0000 | True |
| X64 | tigerstore | delete.api.resource.horizontalpodautoscalers.p50 | ms | 3 | 3.2143 | 4.5000 | 4.5000 | True |
| X64 | tigerstore | delete.api.resource.horizontalpodautoscalers.p99 | ms | 3 | 98.2000 | 182.0000 | 182.0000 | True |
| X64 | tigerstore | delete.api.resource.ingresses.p50 | ms | 3 | 3.7500 | 4.0909 | 25.0000 | True |
| X64 | tigerstore | delete.api.resource.ingresses.p99 | ms | 3 | 98.2000 | 182.0000 | 194.0000 | True |
| X64 | tigerstore | delete.api.resource.ipaddresses.p50 | ms | 3 | 16.4286 | 21.0000 | 21.0000 | True |
| X64 | tigerstore | delete.api.resource.ipaddresses.p99 | ms | 3 | 48.0000 | 96.0000 | 384.0000 | True |
| X64 | tigerstore | delete.api.resource.jobs.p50 | ms | 3 | 2.8125 | 3.0000 | 56.2500 | True |
| X64 | tigerstore | delete.api.resource.jobs.p99 | ms | 3 | 91.0000 | 97.0000 | 191.0000 | True |
| X64 | tigerstore | delete.api.resource.leases.p50 | ms | 3 | 2.7041 | 2.8947 | 2.9891 | True |
| X64 | tigerstore | delete.api.resource.leases.p99 | ms | 3 | 194.0000 | 195.0000 | 326.6667 | True |
| X64 | tigerstore | delete.api.resource.limitranges.p50 | ms | 3 | 3.0000 | 3.2143 | 3.7500 | True |
| X64 | tigerstore | delete.api.resource.limitranges.p99 | ms | 3 | 95.5000 | 97.0000 | 98.2000 | True |
| X64 | tigerstore | delete.api.resource.namespaces.p50 | ms | 3 | 2.5000 | 2.5000 | 2.6316 | True |
| X64 | tigerstore | delete.api.resource.namespaces.p99 | ms | 3 | 4.9500 | 4.9500 | 21.0000 | True |
| X64 | tigerstore | delete.api.resource.networkpolicies.p50 | ms | 3 | 4.0909 | 5.0000 | 13.0000 | True |
| X64 | tigerstore | delete.api.resource.networkpolicies.p99 | ms | 3 | 182.0000 | 182.0000 | 182.0000 | True |
| X64 | tigerstore | delete.api.resource.nodes.p50 | ms | 3 | 2.5000 | 2.5000 | 5.0000 | True |
| X64 | tigerstore | delete.api.resource.nodes.p99 | ms | 3 | 4.9500 | 4.9500 | 24.6000 | True |
| X64 | tigerstore | delete.api.resource.persistentvolumeclaims.p50 | ms | 3 | 3.7500 | 4.0909 | 4.5000 | True |
| X64 | tigerstore | delete.api.resource.persistentvolumeclaims.p99 | ms | 3 | 98.2000 | 98.5000 | 98.7143 | True |
| X64 | tigerstore | delete.api.resource.podcertificaterequests.p50 | ms | 3 | 3.2143 | 3.7500 | 4.5000 | True |
| X64 | tigerstore | delete.api.resource.podcertificaterequests.p99 | ms | 3 | 97.0000 | 182.0000 | 182.0000 | True |
| X64 | tigerstore | delete.api.resource.poddisruptionbudgets.p50 | ms | 3 | 3.2143 | 3.7500 | 4.0909 | True |
| X64 | tigerstore | delete.api.resource.poddisruptionbudgets.p99 | ms | 3 | 97.7500 | 98.5000 | 191.0000 | True |
| X64 | tigerstore | delete.api.resource.pods.p50 | ms | 3 | 71.9388 | 84.6154 | 108.3333 | True |
| X64 | tigerstore | delete.api.resource.pods.p99 | ms | 3 | 544.5714 | 565.6000 | 569.3846 | True |
| X64 | tigerstore | delete.api.resource.podtemplates.p50 | ms | 3 | 3.4615 | 3.7500 | 4.5000 | True |
| X64 | tigerstore | delete.api.resource.podtemplates.p99 | ms | 3 | 97.0000 | 98.5000 | 182.0000 | True |
| X64 | tigerstore | delete.api.resource.replicasets.p50 | ms | 3 | 4.1667 | 4.1667 | 11.6667 | True |
| X64 | tigerstore | delete.api.resource.replicasets.p99 | ms | 3 | 91.6667 | 93.7500 | 95.8333 | True |
| X64 | tigerstore | delete.api.resource.replicationcontrollers.p50 | ms | 3 | 3.2143 | 3.7500 | 5.0000 | True |
| X64 | tigerstore | delete.api.resource.replicationcontrollers.p99 | ms | 3 | 97.0000 | 98.5000 | 98.7143 | True |
| X64 | tigerstore | delete.api.resource.resourceclaims.p50 | ms | 3 | 3.0000 | 4.0909 | 4.0909 | True |
| X64 | tigerstore | delete.api.resource.resourceclaims.p99 | ms | 3 | 98.5000 | 98.5000 | 182.0000 | True |
| X64 | tigerstore | delete.api.resource.resourceclaimtemplates.p50 | ms | 3 | 3.0000 | 3.7500 | 4.5000 | True |
| X64 | tigerstore | delete.api.resource.resourceclaimtemplates.p99 | ms | 3 | 95.5000 | 182.0000 | 182.0000 | True |
| X64 | tigerstore | delete.api.resource.resourcequotas.p50 | ms | 3 | 4.0909 | 4.5000 | 5.0000 | True |
| X64 | tigerstore | delete.api.resource.resourcequotas.p99 | ms | 3 | 98.5000 | 98.5000 | 98.7143 | True |
| X64 | tigerstore | delete.api.resource.rolebindings.p50 | ms | 3 | 3.4615 | 3.7500 | 4.5000 | True |
| X64 | tigerstore | delete.api.resource.rolebindings.p99 | ms | 3 | 97.7500 | 191.0000 | 191.0000 | True |
| X64 | tigerstore | delete.api.resource.roles.p50 | ms | 3 | 3.4615 | 4.0909 | 55.0000 | True |
| X64 | tigerstore | delete.api.resource.roles.p99 | ms | 3 | 97.7500 | 99.1000 | 191.0000 | True |
| X64 | tigerstore | delete.api.resource.secrets.p50 | ms | 3 | 2.6829 | 2.7500 | 3.1429 | True |
| X64 | tigerstore | delete.api.resource.secrets.p99 | ms | 3 | 89.0000 | 96.3333 | 156.0000 | True |
| X64 | tigerstore | delete.api.resource.serviceaccounts.p50 | ms | 3 | 4.5000 | 10.0000 | 13.5714 | True |
| X64 | tigerstore | delete.api.resource.serviceaccounts.p99 | ms | 3 | 98.2000 | 98.5000 | 182.0000 | True |
| X64 | tigerstore | delete.api.resource.services.p50 | ms | 3 | 20.0000 | 22.7778 | 25.0000 | True |
| X64 | tigerstore | delete.api.resource.services.p99 | ms | 3 | 164.0000 | 182.0000 | 528.0000 | True |
| X64 | tigerstore | delete.api.resource.statefulsets.p50 | ms | 3 | 3.2143 | 4.0909 | 4.5000 | True |
| X64 | tigerstore | delete.api.resource.statefulsets.p99 | ms | 3 | 91.0000 | 98.7143 | 182.0000 | True |
| X64 | tigerstore | delete.api.updates.count | requests | 3 | 169.0000 | 199.0000 | 216.0000 | True |
| X64 | tigerstore | delete.api.updates.rate | requests/s | 3 | 2.5637 | 3.0262 | 3.2853 | True |
| X64 | tigerstore | delete.api.verb.DELETE.p50 | ms | 3 | 3.1652 | 3.1914 | 3.3326 | True |
| X64 | tigerstore | delete.api.verb.DELETE.p99 | ms | 3 | 367.8077 | 384.3256 | 397.6129 | True |
| X64 | tigerstore | delete.api.verb.GET.p50 | ms | 3 | 3.2703 | 3.3333 | 3.3648 | True |
| X64 | tigerstore | delete.api.verb.GET.p99 | ms | 3 | 143.7500 | 193.0000 | 325.3333 | True |
| X64 | tigerstore | delete.api.verb.LIST.p50 | ms | 3 | 3.6854 | 4.0051 | 4.2432 | True |
| X64 | tigerstore | delete.api.verb.LIST.p99 | ms | 3 | 160.7500 | 177.5714 | 184.3000 | True |
| X64 | tigerstore | delete.api.verb.PATCH.p50 | ms | 3 | 22.1429 | 45.8333 | 50.0000 | True |
| X64 | tigerstore | delete.api.verb.PATCH.p99 | ms | 3 | 186.0000 | 191.0000 | 196.0000 | True |
| X64 | tigerstore | delete.api.verb.POST.p50 | ms | 3 | 22.7551 | 24.3478 | 36.0577 | True |
| X64 | tigerstore | delete.api.verb.POST.p99 | ms | 3 | 279.0000 | 319.3333 | 379.0000 | True |
| X64 | tigerstore | delete.api.verb.PUT.p50 | ms | 3 | 4.6667 | 8.0000 | 11.7568 | True |
| X64 | tigerstore | delete.api.verb.PUT.p99 | ms | 3 | 330.5714 | 337.0000 | 408.0000 | True |
| X64 | tigerstore | delete.cpu.all | CPU s | 3 | 7.4000 | 7.9200 | 10.6600 | True |
| X64 | tigerstore | delete.cpu.cores | cores | 3 | 0.1126 | 0.1204 | 0.1618 | True |
| X64 | tigerstore | delete.cpu.member1 | CPU s | 3 | 2.3300 | 2.9600 | 4.4000 | True |
| X64 | tigerstore | delete.cpu.member2 | CPU s | 3 | 2.1500 | 3.1000 | 3.3200 | True |
| X64 | tigerstore | delete.cpu.member3 | CPU s | 3 | 2.2700 | 2.2900 | 3.1600 | True |
| X64 | tigerstore | delete.duration | s | 3 | 65.0370 | 65.0449 | 65.0509 | True |
| X64 | tigerstore | delete.exit | exit code | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| X64 | tigerstore | delete.live_inventory | objects in tracked kinds | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| X64 | tigerstore | delete.revision.member1 | revisions | 3 | 2733.0000 | 2761.0000 | 2781.0000 | True |
| X64 | tigerstore | delete.revision.member2 | revisions | 3 | 2733.0000 | 2761.0000 | 2781.0000 | True |
| X64 | tigerstore | delete.revision.member3 | revisions | 3 | 2733.0000 | 2761.0000 | 2781.0000 | True |
| X64 | tigerstore | delete.storage.etcd_request_duration_seconds | RPC/s | 3 | 106.4320 | 107.1329 | 107.7317 | True |
| X64 | tigerstore | delete.verified_deletion_rate | objects/s | 3 | 18.2165 | 18.2182 | 18.2204 | True |
| X64 | tigerstore | delete.window | s | 3 | 65.7467 | 65.7594 | 65.9200 | True |
| X64 | tigerstore | errors.observed | records | 3 | 21.0000 | 21.0000 | 21.0000 | True |
| X64 | tigerstore | errors.workload | records | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| X64 | tigerstore | events.observations | watch notifications | 3 | 2212.0000 | 2212.0000 | 2212.0000 | True |
| X64 | tigerstore | events.unique | events | 3 | 1094.0000 | 1094.0000 | 1094.0000 | True |
| X64 | tigerstore | mixed.api.all.p50 | ms | 3 | 3.1025 | 3.2817 | 3.4126 | True |
| X64 | tigerstore | mixed.api.all.p99 | ms | 3 | 49.0483 | 75.3548 | 76.1889 | True |
| X64 | tigerstore | mixed.api.creates.count | requests | 3 | 1640.0000 | 1640.0000 | 1640.0000 | True |
| X64 | tigerstore | mixed.api.creates.rate | requests/s | 3 | 36.4325 | 36.5525 | 36.5882 | True |
| X64 | tigerstore | mixed.api.deletes.count | requests | 3 | 72.0000 | 74.0000 | 76.0000 | True |
| X64 | tigerstore | mixed.api.deletes.rate | requests/s | 3 | 1.6063 | 1.6439 | 1.6939 | True |
| X64 | tigerstore | mixed.api.http403 | responses | 3 | 16.0000 | 16.0000 | 19.0000 | True |
| X64 | tigerstore | mixed.api.http404 | responses | 3 | 7.0000 | 7.0000 | 9.0000 | True |
| X64 | tigerstore | mixed.api.http409 | responses | 3 | 16.0000 | 17.0000 | 20.0000 | True |
| X64 | tigerstore | mixed.api.http422 | responses | 1 | 0.0000 | 0.0000 | 0.0000 | True |
| X64 | tigerstore | mixed.api.http500 | responses | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| X64 | tigerstore | mixed.api.requests | requests/s | 3 | 65.0453 | 65.4795 | 65.5938 | True |
| X64 | tigerstore | mixed.api.resource..p50 | ms | 3 | 2.5342 | 2.5667 | 2.5676 | True |
| X64 | tigerstore | mixed.api.resource..p99 | ms | 3 | 17.3000 | 17.4000 | 31.5000 | True |
| X64 | tigerstore | mixed.api.resource.configmaps.p50 | ms | 3 | 2.8301 | 2.9240 | 3.0014 | True |
| X64 | tigerstore | mixed.api.resource.configmaps.p99 | ms | 3 | 44.3100 | 45.7278 | 74.5000 | True |
| X64 | tigerstore | mixed.api.resource.controllerrevisions.p50 | ms | 3 | 2.5000 | 3.3333 | 3.3333 | True |
| X64 | tigerstore | mixed.api.resource.controllerrevisions.p99 | ms | 3 | 4.9500 | 98.0000 | 196.0000 | True |
| X64 | tigerstore | mixed.api.resource.cronjobs.p50 | ms | 3 | 2.5000 | 2.5000 | 3.3333 | True |
| X64 | tigerstore | mixed.api.resource.cronjobs.p99 | ms | 3 | 4.9500 | 4.9500 | 98.0000 | True |
| X64 | tigerstore | mixed.api.resource.csistoragecapacities.p50 | ms | 3 | 2.5000 | 2.5000 | 3.3333 | True |
| X64 | tigerstore | mixed.api.resource.csistoragecapacities.p99 | ms | 3 | 4.9500 | 4.9500 | 49.0000 | True |
| X64 | tigerstore | mixed.api.resource.daemonsets.p50 | ms | 3 | 2.5000 | 2.5000 | 2.5000 | True |
| X64 | tigerstore | mixed.api.resource.daemonsets.p99 | ms | 3 | 4.9500 | 4.9500 | 4.9500 | True |
| X64 | tigerstore | mixed.api.resource.deployments.p50 | ms | 3 | 4.1393 | 4.3220 | 4.7059 | True |
| X64 | tigerstore | mixed.api.resource.deployments.p99 | ms | 3 | 87.2500 | 99.7500 | 152.0000 | True |
| X64 | tigerstore | mixed.api.resource.endpoints.p50 | ms | 3 | 3.4635 | 4.0244 | 4.8846 | True |
| X64 | tigerstore | mixed.api.resource.endpoints.p99 | ms | 3 | 48.9687 | 83.5000 | 95.5000 | True |
| X64 | tigerstore | mixed.api.resource.endpointslices.p50 | ms | 3 | 4.2230 | 4.3038 | 4.5652 | True |
| X64 | tigerstore | mixed.api.resource.endpointslices.p99 | ms | 3 | 47.1591 | 95.6667 | 96.4000 | True |
| X64 | tigerstore | mixed.api.resource.events.p50 | ms | 3 | 3.3824 | 4.1400 | 4.3856 | True |
| X64 | tigerstore | mixed.api.resource.events.p99 | ms | 3 | 41.0833 | 49.4167 | 49.5625 | True |
| X64 | tigerstore | mixed.api.resource.horizontalpodautoscalers.p50 | ms | 3 | 2.5000 | 3.3333 | 5.0000 | True |
| X64 | tigerstore | mixed.api.resource.horizontalpodautoscalers.p99 | ms | 3 | 4.9500 | 49.0000 | 99.0000 | True |
| X64 | tigerstore | mixed.api.resource.ingresses.p50 | ms | 3 | 2.5000 | 2.5000 | 5.0000 | True |
| X64 | tigerstore | mixed.api.resource.ingresses.p99 | ms | 3 | 4.9500 | 4.9500 | 98.0000 | True |
| X64 | tigerstore | mixed.api.resource.ipaddresses.p50 | ms | 3 | 2.5000 | 2.5000 | 2.8571 | True |
| X64 | tigerstore | mixed.api.resource.ipaddresses.p99 | ms | 3 | 4.9500 | 4.9500 | 23.4000 | True |
| X64 | tigerstore | mixed.api.resource.jobs.p50 | ms | 3 | 2.5000 | 2.5000 | 2.5000 | True |
| X64 | tigerstore | mixed.api.resource.jobs.p99 | ms | 3 | 4.9500 | 4.9500 | 4.9500 | True |
| X64 | tigerstore | mixed.api.resource.leases.p50 | ms | 3 | 2.5769 | 2.6587 | 2.9018 | True |
| X64 | tigerstore | mixed.api.resource.leases.p99 | ms | 3 | 18.3000 | 66.5000 | 89.1667 | True |
| X64 | tigerstore | mixed.api.resource.limitranges.p50 | ms | 3 | 2.5000 | 2.6923 | 2.7273 | True |
| X64 | tigerstore | mixed.api.resource.limitranges.p99 | ms | 3 | 4.9500 | 46.5000 | 188.0000 | True |
| X64 | tigerstore | mixed.api.resource.namespaces.p50 | ms | 3 | 2.7232 | 2.7477 | 2.7727 | True |
| X64 | tigerstore | mixed.api.resource.namespaces.p99 | ms | 3 | 23.1655 | 23.3375 | 24.6000 | True |
| X64 | tigerstore | mixed.api.resource.networkpolicies.p50 | ms | 3 | 2.5000 | 2.5000 | 3.3333 | True |
| X64 | tigerstore | mixed.api.resource.networkpolicies.p99 | ms | 3 | 4.9500 | 4.9500 | 98.0000 | True |
| X64 | tigerstore | mixed.api.resource.nodes.p50 | ms | 3 | 2.5000 | 2.5000 | 2.5000 | True |
| X64 | tigerstore | mixed.api.resource.nodes.p99 | ms | 3 | 4.9500 | 4.9500 | 4.9500 | True |
| X64 | tigerstore | mixed.api.resource.persistentvolumeclaims.p50 | ms | 3 | 2.5000 | 2.5000 | 2.5000 | True |
| X64 | tigerstore | mixed.api.resource.persistentvolumeclaims.p99 | ms | 3 | 4.9500 | 4.9500 | 4.9500 | True |
| X64 | tigerstore | mixed.api.resource.podcertificaterequests.p50 | ms | 3 | 2.5000 | 3.3333 | 5.0000 | True |
| X64 | tigerstore | mixed.api.resource.podcertificaterequests.p99 | ms | 3 | 4.9500 | 24.2000 | 99.0000 | True |
| X64 | tigerstore | mixed.api.resource.poddisruptionbudgets.p50 | ms | 3 | 2.5000 | 3.3333 | 25.0000 | True |
| X64 | tigerstore | mixed.api.resource.poddisruptionbudgets.p99 | ms | 3 | 4.9500 | 49.0000 | 99.0000 | True |
| X64 | tigerstore | mixed.api.resource.pods.p50 | ms | 3 | 4.1304 | 4.8707 | 7.3837 | True |
| X64 | tigerstore | mixed.api.resource.pods.p99 | ms | 3 | 64.5000 | 93.5000 | 151.6667 | True |
| X64 | tigerstore | mixed.api.resource.podtemplates.p50 | ms | 3 | 2.5000 | 2.5000 | 5.0000 | True |
| X64 | tigerstore | mixed.api.resource.podtemplates.p99 | ms | 3 | 4.9500 | 4.9500 | 98.0000 | True |
| X64 | tigerstore | mixed.api.resource.replicasets.p50 | ms | 3 | 7.9032 | 8.9286 | 12.6744 | True |
| X64 | tigerstore | mixed.api.resource.replicasets.p99 | ms | 3 | 46.1875 | 84.7500 | 89.5000 | True |
| X64 | tigerstore | mixed.api.resource.replicationcontrollers.p50 | ms | 3 | 2.5000 | 3.3333 | 5.0000 | True |
| X64 | tigerstore | mixed.api.resource.replicationcontrollers.p99 | ms | 3 | 4.9500 | 24.2000 | 98.0000 | True |
| X64 | tigerstore | mixed.api.resource.resourceclaims.p50 | ms | 3 | 2.5000 | 2.5000 | 2.5000 | True |
| X64 | tigerstore | mixed.api.resource.resourceclaims.p99 | ms | 3 | 4.9500 | 4.9500 | 4.9500 | True |
| X64 | tigerstore | mixed.api.resource.resourceclaimtemplates.p50 | ms | 3 | 2.5000 | 2.5000 | 2.5000 | True |
| X64 | tigerstore | mixed.api.resource.resourceclaimtemplates.p99 | ms | 3 | 4.9500 | 4.9500 | 4.9500 | True |
| X64 | tigerstore | mixed.api.resource.resourcequotas.p50 | ms | 3 | 2.5000 | 2.5000 | 3.3333 | True |
| X64 | tigerstore | mixed.api.resource.resourcequotas.p99 | ms | 3 | 4.9500 | 4.9500 | 98.0000 | True |
| X64 | tigerstore | mixed.api.resource.rolebindings.p50 | ms | 3 | 2.5000 | 2.5000 | 3.3333 | True |
| X64 | tigerstore | mixed.api.resource.rolebindings.p99 | ms | 3 | 4.9500 | 4.9500 | 98.0000 | True |
| X64 | tigerstore | mixed.api.resource.roles.p50 | ms | 3 | 2.5000 | 2.5000 | 3.3333 | True |
| X64 | tigerstore | mixed.api.resource.roles.p99 | ms | 3 | 4.9500 | 4.9500 | 24.2000 | True |
| X64 | tigerstore | mixed.api.resource.secrets.p50 | ms | 3 | 2.6515 | 2.6515 | 2.7344 | True |
| X64 | tigerstore | mixed.api.resource.secrets.p99 | ms | 3 | 21.5000 | 21.5000 | 22.6667 | True |
| X64 | tigerstore | mixed.api.resource.serviceaccounts.p50 | ms | 3 | 7.5000 | 10.0000 | 10.0000 | True |
| X64 | tigerstore | mixed.api.resource.serviceaccounts.p99 | ms | 3 | 24.6500 | 46.5000 | 46.5000 | True |
| X64 | tigerstore | mixed.api.resource.services.p50 | ms | 3 | 3.9062 | 10.0000 | 15.0000 | True |
| X64 | tigerstore | mixed.api.resource.services.p99 | ms | 3 | 93.7500 | 93.7500 | 95.8333 | True |
| X64 | tigerstore | mixed.api.resource.statefulsets.p50 | ms | 3 | 2.5000 | 3.3333 | 5.0000 | True |
| X64 | tigerstore | mixed.api.resource.statefulsets.p99 | ms | 3 | 4.9500 | 98.0000 | 196.0000 | True |
| X64 | tigerstore | mixed.api.updates.count | requests | 3 | 244.0000 | 257.0000 | 259.0000 | True |
| X64 | tigerstore | mixed.api.updates.rate | requests/s | 3 | 5.4204 | 5.7336 | 5.7726 | True |
| X64 | tigerstore | mixed.api.verb.DELETE.p50 | ms | 3 | 3.0769 | 3.3333 | 3.6792 | True |
| X64 | tigerstore | mixed.api.verb.DELETE.p99 | ms | 3 | 324.0000 | 346.6667 | 348.0000 | True |
| X64 | tigerstore | mixed.api.verb.GET.p50 | ms | 3 | 2.7154 | 2.7716 | 2.8035 | True |
| X64 | tigerstore | mixed.api.verb.GET.p99 | ms | 3 | 23.2981 | 23.7900 | 24.1250 | True |
| X64 | tigerstore | mixed.api.verb.LIST.p50 | ms | 3 | 2.9126 | 3.1915 | 3.3523 | True |
| X64 | tigerstore | mixed.api.verb.LIST.p99 | ms | 3 | 92.5000 | 98.8889 | 170.5000 | True |
| X64 | tigerstore | mixed.api.verb.PATCH.p50 | ms | 3 | 6.9355 | 10.6000 | 12.1084 | True |
| X64 | tigerstore | mixed.api.verb.PATCH.p99 | ms | 3 | 48.6250 | 49.1786 | 90.0000 | True |
| X64 | tigerstore | mixed.api.verb.POST.p50 | ms | 3 | 2.9754 | 3.1642 | 3.2825 | True |
| X64 | tigerstore | mixed.api.verb.POST.p99 | ms | 3 | 43.2885 | 44.3629 | 60.1000 | True |
| X64 | tigerstore | mixed.api.verb.PUT.p50 | ms | 3 | 4.0319 | 4.5694 | 4.9586 | True |
| X64 | tigerstore | mixed.api.verb.PUT.p99 | ms | 3 | 49.0595 | 86.7500 | 92.4167 | True |
| X64 | tigerstore | mixed.cpu.all | CPU s | 3 | 5.5400 | 5.9300 | 8.0600 | True |
| X64 | tigerstore | mixed.cpu.cores | cores | 3 | 0.1235 | 0.1320 | 0.1787 | True |
| X64 | tigerstore | mixed.cpu.member1 | CPU s | 3 | 1.7600 | 2.1800 | 3.2400 | True |
| X64 | tigerstore | mixed.cpu.member2 | CPU s | 3 | 1.6000 | 2.3300 | 2.4900 | True |
| X64 | tigerstore | mixed.cpu.member3 | CPU s | 3 | 1.6800 | 1.7600 | 2.4900 | True |
| X64 | tigerstore | mixed.duration | s | 3 | 44.2069 | 44.2541 | 44.3122 | True |
| X64 | tigerstore | mixed.exit | exit code | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| X64 | tigerstore | mixed.live_inventory | objects in tracked kinds | 3 | 1096.0000 | 1096.0000 | 1096.0000 | True |
| X64 | tigerstore | mixed.net_live_creation_rate | objects/s | 3 | 24.7336 | 24.7661 | 24.7925 | True |
| X64 | tigerstore | mixed.pod.ContainersReady.p50 | ms | 3 | 1000.0000 | 1000.0000 | 2000.0000 | True |
| X64 | tigerstore | mixed.pod.ContainersReady.p99 | ms | 3 | 2000.0000 | 2000.0000 | 2000.0000 | True |
| X64 | tigerstore | mixed.pod.ContainersStarted.p50 | ms | 3 | 1749.0000 | 1926.0000 | 2054.0000 | True |
| X64 | tigerstore | mixed.pod.ContainersStarted.p99 | ms | 3 | 2218.0000 | 2464.0000 | 2696.0000 | True |
| X64 | tigerstore | mixed.pod.Initialized.p50 | ms | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| X64 | tigerstore | mixed.pod.Initialized.p99 | ms | 3 | 0.0000 | 0.0000 | 500.0000 | True |
| X64 | tigerstore | mixed.pod.PodReadyToStartContainers.p50 | ms | 3 | 1000.0000 | 1000.0000 | 1000.0000 | True |
| X64 | tigerstore | mixed.pod.PodReadyToStartContainers.p99 | ms | 3 | 1000.0000 | 1000.0000 | 2000.0000 | True |
| X64 | tigerstore | mixed.pod.PodScheduled.p50 | ms | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| X64 | tigerstore | mixed.pod.PodScheduled.p99 | ms | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| X64 | tigerstore | mixed.pod.Ready.p50 | ms | 3 | 1000.0000 | 1000.0000 | 2000.0000 | True |
| X64 | tigerstore | mixed.pod.Ready.p99 | ms | 3 | 2000.0000 | 2000.0000 | 2000.0000 | True |
| X64 | tigerstore | mixed.revision.member1 | revisions | 3 | 2490.0000 | 2505.0000 | 2507.0000 | True |
| X64 | tigerstore | mixed.revision.member2 | revisions | 3 | 2490.0000 | 2505.0000 | 2507.0000 | True |
| X64 | tigerstore | mixed.revision.member3 | revisions | 3 | 2490.0000 | 2505.0000 | 2507.0000 | True |
| X64 | tigerstore | mixed.storage.etcd_request_duration_seconds | RPC/s | 3 | 79.7738 | 80.5610 | 80.7051 | True |
| X64 | tigerstore | mixed.window | s | 3 | 44.8232 | 44.8670 | 45.0148 | True |
| X64 | tigerstore | node.api.all.p50 | ms | 3 | 17.8947 | 18.4409 | 18.6952 | True |
| X64 | tigerstore | node.api.all.p99 | ms | 3 | 393.4444 | 436.3077 | 477.4286 | True |
| X64 | tigerstore | node.api.creates.count | requests | 3 | 483.0000 | 483.0000 | 483.0000 | True |
| X64 | tigerstore | node.api.creates.rate | requests/s | 3 | 17.6550 | 21.6413 | 22.3037 | True |
| X64 | tigerstore | node.api.deletes.count | requests | 3 | 16.0000 | 16.0000 | 16.0000 | True |
| X64 | tigerstore | node.api.deletes.rate | requests/s | 3 | 0.5848 | 0.7169 | 0.7388 | True |
| X64 | tigerstore | node.api.http403 | responses | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| X64 | tigerstore | node.api.http404 | responses | 3 | 17.0000 | 20.0000 | 22.0000 | True |
| X64 | tigerstore | node.api.http409 | responses | 3 | 5.0000 | 5.0000 | 6.0000 | True |
| X64 | tigerstore | node.api.http422 | responses | 1 | 0.0000 | 0.0000 | 0.0000 | True |
| X64 | tigerstore | node.api.http500 | responses | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| X64 | tigerstore | node.api.requests | requests/s | 3 | 50.6988 | 61.6977 | 62.9860 | True |
| X64 | tigerstore | node.api.resource..p50 | ms | 3 | 3.0357 | 3.2759 | 3.2857 | True |
| X64 | tigerstore | node.api.resource..p99 | ms | 3 | 88.5000 | 324.0000 | 366.0000 | True |
| X64 | tigerstore | node.api.resource.configmaps.p50 | ms | 3 | 2.5000 | 2.5000 | 15.0000 | True |
| X64 | tigerstore | node.api.resource.configmaps.p99 | ms | 3 | 4.9500 | 4.9500 | 24.8000 | True |
| X64 | tigerstore | node.api.resource.endpoints.p50 | ms | 3 | 2.5000 | 2.5000 | 3.7500 | True |
| X64 | tigerstore | node.api.resource.endpoints.p99 | ms | 3 | 4.9500 | 4.9500 | 394.0000 | True |
| X64 | tigerstore | node.api.resource.endpointslices.p50 | ms | 3 | 2.5000 | 3.7500 | 5.0000 | True |
| X64 | tigerstore | node.api.resource.endpointslices.p99 | ms | 3 | 4.9500 | 24.6000 | 98.5000 | True |
| X64 | tigerstore | node.api.resource.events.p50 | ms | 3 | 20.3333 | 21.5244 | 21.8987 | True |
| X64 | tigerstore | node.api.resource.events.p99 | ms | 3 | 387.8571 | 486.0000 | 526.0000 | True |
| X64 | tigerstore | node.api.resource.leases.p50 | ms | 3 | 4.1667 | 4.8333 | 17.5000 | True |
| X64 | tigerstore | node.api.resource.leases.p99 | ms | 3 | 385.0000 | 532.0000 | 542.0000 | True |
| X64 | tigerstore | node.api.resource.limitranges.p50 | ms | 3 | 2.5000 | 2.5000 | 2.5000 | True |
| X64 | tigerstore | node.api.resource.limitranges.p99 | ms | 3 | 4.9500 | 4.9500 | 4.9500 | True |
| X64 | tigerstore | node.api.resource.namespaces.p50 | ms | 3 | 2.5000 | 2.5000 | 2.5000 | True |
| X64 | tigerstore | node.api.resource.namespaces.p99 | ms | 3 | 4.9500 | 4.9500 | 4.9500 | True |
| X64 | tigerstore | node.api.resource.nodes.p50 | ms | 3 | 2.5000 | 2.5000 | 2.5000 | True |
| X64 | tigerstore | node.api.resource.nodes.p99 | ms | 3 | 4.9500 | 4.9500 | 4.9500 | True |
| X64 | tigerstore | node.api.resource.pods.p50 | ms | 3 | 17.8462 | 18.1688 | 18.4711 | True |
| X64 | tigerstore | node.api.resource.pods.p99 | ms | 3 | 392.0769 | 398.8824 | 478.5333 | True |
| X64 | tigerstore | node.api.resource.serviceaccounts.p50 | ms | 3 | 2.5000 | 2.5000 | 2.5000 | True |
| X64 | tigerstore | node.api.resource.serviceaccounts.p99 | ms | 3 | 4.9500 | 4.9500 | 4.9500 | True |
| X64 | tigerstore | node.api.updates.count | requests | 3 | 45.0000 | 46.0000 | 50.0000 | True |
| X64 | tigerstore | node.api.updates.rate | requests/s | 3 | 1.8276 | 2.0611 | 2.0780 | True |
| X64 | tigerstore | node.api.verb.DELETE.p50 | ms | 3 | 12.2727 | 13.5714 | 15.0000 | True |
| X64 | tigerstore | node.api.verb.DELETE.p99 | ms | 3 | 24.7714 | 24.8000 | 46.0000 | True |
| X64 | tigerstore | node.api.verb.GET.p50 | ms | 3 | 5.0000 | 5.8333 | 7.3973 | True |
| X64 | tigerstore | node.api.verb.GET.p99 | ms | 3 | 239.5000 | 279.1429 | 349.7778 | True |
| X64 | tigerstore | node.api.verb.LIST.p50 | ms | 3 | 10.8333 | 11.6667 | 15.0000 | True |
| X64 | tigerstore | node.api.verb.LIST.p99 | ms | 3 | 358.0000 | 380.0000 | 548.0000 | True |
| X64 | tigerstore | node.api.verb.PATCH.p50 | ms | 3 | 21.6667 | 23.9510 | 24.3103 | True |
| X64 | tigerstore | node.api.verb.PATCH.p99 | ms | 3 | 358.6667 | 374.4444 | 519.5000 | True |
| X64 | tigerstore | node.api.verb.POST.p50 | ms | 3 | 22.6250 | 23.3772 | 23.9593 | True |
| X64 | tigerstore | node.api.verb.POST.p99 | ms | 3 | 497.1111 | 497.1111 | 541.5556 | True |
| X64 | tigerstore | node.api.verb.PUT.p50 | ms | 3 | 4.1667 | 4.8333 | 17.5000 | True |
| X64 | tigerstore | node.api.verb.PUT.p99 | ms | 3 | 385.0000 | 532.0000 | 542.0000 | True |
| X64 | tigerstore | node.cpu.all | CPU s | 3 | 1.8500 | 1.9000 | 2.9300 | True |
| X64 | tigerstore | node.cpu.cores | cores | 3 | 0.0828 | 0.0877 | 0.1069 | True |
| X64 | tigerstore | node.cpu.member1 | CPU s | 3 | 0.5800 | 0.7100 | 1.1900 | True |
| X64 | tigerstore | node.cpu.member2 | CPU s | 3 | 0.5400 | 0.7700 | 0.8700 | True |
| X64 | tigerstore | node.cpu.member3 | CPU s | 3 | 0.5500 | 0.6000 | 0.8700 | True |
| X64 | tigerstore | node.duration | s | 3 | 20.9378 | 21.6045 | 26.5690 | True |
| X64 | tigerstore | node.exit | exit code | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| X64 | tigerstore | node.live_inventory | objects in tracked kinds | 3 | 1177.0000 | 1177.0000 | 1177.0000 | True |
| X64 | tigerstore | node.net_live_creation_rate | objects/s | 3 | 3.0487 | 3.7492 | 3.8686 | True |
| X64 | tigerstore | node.pod.ContainersReady.p50 | ms | 3 | 6000.0000 | 6000.0000 | 9000.0000 | True |
| X64 | tigerstore | node.pod.ContainersReady.p99 | ms | 3 | 8500.0000 | 9000.0000 | 12000.0000 | True |
| X64 | tigerstore | node.pod.ContainersStarted.p50 | ms | 3 | 6537.0000 | 6689.0000 | 9839.0000 | True |
| X64 | tigerstore | node.pod.ContainersStarted.p99 | ms | 3 | 9182.0000 | 9785.0000 | 12474.0000 | True |
| X64 | tigerstore | node.pod.Initialized.p50 | ms | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| X64 | tigerstore | node.pod.Initialized.p99 | ms | 3 | 1000.0000 | 1000.0000 | 1000.0000 | True |
| X64 | tigerstore | node.pod.PodReadyToStartContainers.p50 | ms | 3 | 4000.0000 | 4000.0000 | 5000.0000 | True |
| X64 | tigerstore | node.pod.PodReadyToStartContainers.p99 | ms | 3 | 6000.0000 | 7000.0000 | 10000.0000 | True |
| X64 | tigerstore | node.pod.PodScheduled.p50 | ms | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| X64 | tigerstore | node.pod.PodScheduled.p99 | ms | 3 | 1000.0000 | 1000.0000 | 1000.0000 | True |
| X64 | tigerstore | node.pod.Ready.p50 | ms | 3 | 6000.0000 | 6000.0000 | 9000.0000 | True |
| X64 | tigerstore | node.pod.Ready.p99 | ms | 3 | 8500.0000 | 9000.0000 | 12000.0000 | True |
| X64 | tigerstore | node.revision.member1 | revisions | 3 | 921.0000 | 927.0000 | 937.0000 | True |
| X64 | tigerstore | node.revision.member2 | revisions | 3 | 921.0000 | 927.0000 | 937.0000 | True |
| X64 | tigerstore | node.revision.member3 | revisions | 3 | 921.0000 | 927.0000 | 937.0000 | True |
| X64 | tigerstore | node.storage.etcd_request_duration_seconds | RPC/s | 3 | 52.2340 | 63.4452 | 64.3713 | True |
| X64 | tigerstore | node.window | s | 3 | 21.6556 | 22.3185 | 27.3576 | True |
| X64 | tigerstore | total.api.all.p50 | ms | 3 | 3.5207 | 3.6766 | 4.1253 | True |
| X64 | tigerstore | total.api.all.p99 | ms | 3 | 198.2667 | 200.0192 | 247.0588 | True |
| X64 | tigerstore | total.api.creates.count | requests | 3 | 2715.0000 | 2715.0000 | 2715.0000 | True |
| X64 | tigerstore | total.api.creates.rate | requests/s | 3 | 11.5277 | 11.8574 | 11.8821 | True |
| X64 | tigerstore | total.api.deletes.count | requests | 3 | 1673.0000 | 1675.0000 | 1677.0000 | True |
| X64 | tigerstore | total.api.deletes.rate | requests/s | 3 | 7.1119 | 7.3066 | 7.3393 | True |
| X64 | tigerstore | total.api.http403 | responses | 3 | 25.0000 | 25.0000 | 28.0000 | True |
| X64 | tigerstore | total.api.http404 | responses | 3 | 82.0000 | 89.0000 | 94.0000 | True |
| X64 | tigerstore | total.api.http409 | responses | 3 | 73.0000 | 80.0000 | 82.0000 | True |
| X64 | tigerstore | total.api.http422 | responses | 1 | 0.0000 | 0.0000 | 0.0000 | True |
| X64 | tigerstore | total.api.http500 | responses | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| X64 | tigerstore | total.api.requests | requests/s | 3 | 54.3522 | 56.0143 | 56.0681 | True |
| X64 | tigerstore | total.api.resource..p50 | ms | 3 | 2.5983 | 2.6357 | 2.6831 | True |
| X64 | tigerstore | total.api.resource..p99 | ms | 3 | 53.1667 | 83.1000 | 91.2500 | True |
| X64 | tigerstore | total.api.resource.configmaps.p50 | ms | 3 | 2.7284 | 2.7991 | 3.0042 | True |
| X64 | tigerstore | total.api.resource.configmaps.p99 | ms | 3 | 43.7092 | 49.9601 | 77.2266 | True |
| X64 | tigerstore | total.api.resource.controllerrevisions.p50 | ms | 3 | 3.4375 | 4.2308 | 4.5833 | True |
| X64 | tigerstore | total.api.resource.controllerrevisions.p99 | ms | 3 | 98.6250 | 178.0000 | 178.0000 | True |
| X64 | tigerstore | total.api.resource.cronjobs.p50 | ms | 3 | 3.4375 | 3.6667 | 3.9286 | True |
| X64 | tigerstore | total.api.resource.cronjobs.p99 | ms | 3 | 97.2500 | 178.0000 | 178.0000 | True |
| X64 | tigerstore | total.api.resource.csistoragecapacities.p50 | ms | 3 | 2.7500 | 3.0556 | 3.4375 | True |
| X64 | tigerstore | total.api.resource.csistoragecapacities.p99 | ms | 3 | 94.5000 | 96.3333 | 96.3333 | True |
| X64 | tigerstore | total.api.resource.daemonsets.p50 | ms | 3 | 3.2353 | 3.2353 | 3.2353 | True |
| X64 | tigerstore | total.api.resource.daemonsets.p99 | ms | 3 | 96.3333 | 97.8000 | 178.0000 | True |
| X64 | tigerstore | total.api.resource.deployments.p50 | ms | 3 | 4.4731 | 4.8645 | 11.5104 | True |
| X64 | tigerstore | total.api.resource.deployments.p99 | ms | 3 | 83.3750 | 95.5682 | 102.2500 | True |
| X64 | tigerstore | total.api.resource.endpoints.p50 | ms | 3 | 7.6590 | 11.4894 | 13.7805 | True |
| X64 | tigerstore | total.api.resource.endpoints.p99 | ms | 3 | 177.2000 | 220.0000 | 309.6000 | True |
| X64 | tigerstore | total.api.resource.endpointslices.p50 | ms | 3 | 11.0082 | 12.1888 | 14.9163 | True |
| X64 | tigerstore | total.api.resource.endpointslices.p99 | ms | 3 | 172.4375 | 188.2857 | 199.0000 | True |
| X64 | tigerstore | total.api.resource.events.p50 | ms | 3 | 7.8974 | 10.8945 | 14.1699 | True |
| X64 | tigerstore | total.api.resource.events.p99 | ms | 3 | 338.6364 | 350.0000 | 394.2308 | True |
| X64 | tigerstore | total.api.resource.horizontalpodautoscalers.p50 | ms | 3 | 3.2353 | 3.9286 | 4.5833 | True |
| X64 | tigerstore | total.api.resource.horizontalpodautoscalers.p99 | ms | 3 | 97.8000 | 178.0000 | 178.0000 | True |
| X64 | tigerstore | total.api.resource.ingresses.p50 | ms | 3 | 3.4375 | 3.6667 | 18.3333 | True |
| X64 | tigerstore | total.api.resource.ingresses.p99 | ms | 3 | 97.8000 | 178.0000 | 192.6667 | True |
| X64 | tigerstore | total.api.resource.ipaddresses.p50 | ms | 3 | 4.1667 | 4.1667 | 4.7619 | True |
| X64 | tigerstore | total.api.resource.ipaddresses.p99 | ms | 3 | 45.0000 | 90.0000 | 360.0000 | True |
| X64 | tigerstore | total.api.resource.jobs.p50 | ms | 3 | 2.7500 | 2.8947 | 5.0000 | True |
| X64 | tigerstore | total.api.resource.jobs.p99 | ms | 3 | 89.0000 | 96.3333 | 189.0000 | True |
| X64 | tigerstore | total.api.resource.leases.p50 | ms | 3 | 2.7258 | 2.9239 | 3.0762 | True |
| X64 | tigerstore | total.api.resource.leases.p99 | ms | 3 | 235.3333 | 264.8000 | 281.0000 | True |
| X64 | tigerstore | total.api.resource.limitranges.p50 | ms | 3 | 3.0357 | 3.5294 | 3.6429 | True |
| X64 | tigerstore | total.api.resource.limitranges.p99 | ms | 3 | 87.2500 | 176.0000 | 183.0000 | True |
| X64 | tigerstore | total.api.resource.namespaces.p50 | ms | 3 | 2.7057 | 2.7057 | 2.7261 | True |
| X64 | tigerstore | total.api.resource.namespaces.p99 | ms | 3 | 22.9125 | 22.9125 | 24.1879 | True |
| X64 | tigerstore | total.api.resource.networkpolicies.p50 | ms | 3 | 3.6667 | 4.2308 | 9.0000 | True |
| X64 | tigerstore | total.api.resource.networkpolicies.p99 | ms | 3 | 178.0000 | 178.0000 | 178.0000 | True |
| X64 | tigerstore | total.api.resource.nodes.p50 | ms | 3 | 2.5000 | 2.5000 | 3.0000 | True |
| X64 | tigerstore | total.api.resource.nodes.p99 | ms | 3 | 4.9500 | 4.9500 | 23.8000 | True |
| X64 | tigerstore | total.api.resource.persistentvolumeclaims.p50 | ms | 3 | 3.4375 | 3.6667 | 3.9286 | True |
| X64 | tigerstore | total.api.resource.persistentvolumeclaims.p99 | ms | 3 | 97.8000 | 98.1667 | 98.4286 | True |
| X64 | tigerstore | total.api.resource.podcertificaterequests.p50 | ms | 3 | 3.2353 | 3.9286 | 3.9286 | True |
| X64 | tigerstore | total.api.resource.podcertificaterequests.p99 | ms | 3 | 97.8000 | 178.0000 | 178.0000 | True |
| X64 | tigerstore | total.api.resource.poddisruptionbudgets.p50 | ms | 3 | 3.2353 | 3.4375 | 4.5833 | True |
| X64 | tigerstore | total.api.resource.poddisruptionbudgets.p99 | ms | 3 | 97.2500 | 98.1667 | 189.0000 | True |
| X64 | tigerstore | total.api.resource.pods.p50 | ms | 3 | 6.8301 | 8.4563 | 11.9794 | True |
| X64 | tigerstore | total.api.resource.pods.p99 | ms | 3 | 374.7073 | 390.4000 | 392.8684 | True |
| X64 | tigerstore | total.api.resource.podtemplates.p50 | ms | 3 | 3.2353 | 3.4375 | 4.5833 | True |
| X64 | tigerstore | total.api.resource.podtemplates.p99 | ms | 3 | 96.3333 | 98.4286 | 178.0000 | True |
| X64 | tigerstore | total.api.resource.replicasets.p50 | ms | 3 | 6.2121 | 8.6986 | 15.7443 | True |
| X64 | tigerstore | total.api.resource.replicasets.p99 | ms | 3 | 87.7692 | 134.3750 | 156.3333 | True |
| X64 | tigerstore | total.api.resource.replicationcontrollers.p50 | ms | 3 | 3.2353 | 3.9286 | 4.2308 | True |
| X64 | tigerstore | total.api.resource.replicationcontrollers.p99 | ms | 3 | 96.3333 | 98.4286 | 98.4286 | True |
| X64 | tigerstore | total.api.resource.resourceclaims.p50 | ms | 3 | 2.8947 | 3.6667 | 3.6667 | True |
| X64 | tigerstore | total.api.resource.resourceclaims.p99 | ms | 3 | 98.1667 | 98.1667 | 178.0000 | True |
| X64 | tigerstore | total.api.resource.resourceclaimtemplates.p50 | ms | 3 | 2.8947 | 3.4375 | 3.9286 | True |
| X64 | tigerstore | total.api.resource.resourceclaimtemplates.p99 | ms | 3 | 94.5000 | 178.0000 | 178.0000 | True |
| X64 | tigerstore | total.api.resource.resourcequotas.p50 | ms | 3 | 3.6667 | 3.9286 | 4.5833 | True |
| X64 | tigerstore | total.api.resource.resourcequotas.p99 | ms | 3 | 98.1667 | 98.1667 | 98.6250 | True |
| X64 | tigerstore | total.api.resource.rolebindings.p50 | ms | 3 | 3.2353 | 3.4375 | 4.2308 | True |
| X64 | tigerstore | total.api.resource.rolebindings.p99 | ms | 3 | 97.2500 | 189.0000 | 189.0000 | True |
| X64 | tigerstore | total.api.resource.roles.p50 | ms | 3 | 3.4375 | 3.6667 | 5.0000 | True |
| X64 | tigerstore | total.api.resource.roles.p99 | ms | 3 | 97.2500 | 98.9000 | 189.0000 | True |
| X64 | tigerstore | total.api.resource.secrets.p50 | ms | 3 | 2.5801 | 2.5968 | 2.7013 | True |
| X64 | tigerstore | total.api.resource.secrets.p99 | ms | 3 | 59.7500 | 86.5833 | 89.8333 | True |
| X64 | tigerstore | total.api.resource.serviceaccounts.p50 | ms | 3 | 6.1111 | 9.1667 | 9.6667 | True |
| X64 | tigerstore | total.api.resource.serviceaccounts.p99 | ms | 3 | 96.7000 | 97.2500 | 167.0000 | True |
| X64 | tigerstore | total.api.resource.services.p50 | ms | 3 | 3.4884 | 3.7037 | 4.2857 | True |
| X64 | tigerstore | total.api.resource.services.p99 | ms | 3 | 99.2857 | 140.0000 | 386.6667 | True |
| X64 | tigerstore | total.api.resource.statefulsets.p50 | ms | 3 | 3.2353 | 3.9286 | 4.2308 | True |
| X64 | tigerstore | total.api.resource.statefulsets.p99 | ms | 3 | 94.5000 | 98.4286 | 189.0000 | True |
| X64 | tigerstore | total.api.updates.count | requests | 3 | 4668.0000 | 4707.0000 | 4728.0000 | True |
| X64 | tigerstore | total.api.updates.rate | requests/s | 3 | 19.8200 | 20.6000 | 20.6489 | True |
| X64 | tigerstore | total.api.verb.DELETE.p50 | ms | 3 | 3.2887 | 3.3041 | 3.4791 | True |
| X64 | tigerstore | total.api.verb.DELETE.p99 | ms | 3 | 363.7455 | 378.4783 | 388.6471 | True |
| X64 | tigerstore | total.api.verb.GET.p50 | ms | 3 | 3.1861 | 3.2708 | 3.3306 | True |
| X64 | tigerstore | total.api.verb.GET.p99 | ms | 3 | 117.8182 | 123.6667 | 209.0667 | True |
| X64 | tigerstore | total.api.verb.LIST.p50 | ms | 3 | 3.5660 | 3.5778 | 3.9496 | True |
| X64 | tigerstore | total.api.verb.LIST.p99 | ms | 3 | 161.5000 | 178.7647 | 186.0000 | True |
| X64 | tigerstore | total.api.verb.PATCH.p50 | ms | 3 | 3.0994 | 3.2048 | 3.6453 | True |
| X64 | tigerstore | total.api.verb.PATCH.p99 | ms | 3 | 96.6475 | 124.4634 | 140.2143 | True |
| X64 | tigerstore | total.api.verb.POST.p50 | ms | 3 | 3.9447 | 4.2267 | 4.7753 | True |
| X64 | tigerstore | total.api.verb.POST.p99 | ms | 3 | 273.4815 | 296.7027 | 304.8421 | True |
| X64 | tigerstore | total.api.verb.PUT.p50 | ms | 3 | 4.9785 | 8.0575 | 13.2193 | True |
| X64 | tigerstore | total.api.verb.PUT.p99 | ms | 3 | 173.0476 | 176.5625 | 191.5588 | True |
| X64 | tigerstore | total.cpu.all | CPU s | 3 | 26.4400 | 28.4600 | 37.8400 | True |
| X64 | tigerstore | total.cpu.cores | cores | 3 | 0.1154 | 0.1245 | 0.1606 | True |
| X64 | tigerstore | total.cpu.member1 | CPU s | 3 | 8.4800 | 10.4000 | 15.3000 | True |
| X64 | tigerstore | total.cpu.member2 | CPU s | 3 | 7.7100 | 10.9900 | 11.9300 | True |
| X64 | tigerstore | total.cpu.member3 | CPU s | 3 | 8.0500 | 8.3300 | 11.5500 | True |
| X64 | tigerstore | total.revision.member1 | revisions | 3 | 11661.0000 | 11699.0000 | 11719.0000 | True |
| X64 | tigerstore | total.revision.member2 | revisions | 3 | 11661.0000 | 11699.0000 | 11719.0000 | True |
| X64 | tigerstore | total.revision.member3 | revisions | 3 | 11661.0000 | 11699.0000 | 11719.0000 | True |
| X64 | tigerstore | total.script | s | 3 | 308.8839 | 314.7645 | 318.5093 | True |
| X64 | tigerstore | total.storage.etcd_request_duration_seconds | RPC/s | 3 | 77.6326 | 80.0717 | 80.0886 | True |
| X64 | tigerstore | total.window | s | 3 | 228.4951 | 228.9714 | 235.5196 | True |
| X64 | tigerstore | total.workload | s | 3 | 227.4368 | 227.9534 | 234.3923 | True |
| X64 | tigerstore | update.api.all.p50 | ms | 3 | 3.2417 | 3.4073 | 4.1869 | True |
| X64 | tigerstore | update.api.all.p99 | ms | 3 | 57.6750 | 85.6392 | 109.3906 | True |
| X64 | tigerstore | update.api.creates.count | requests | 3 | 480.0000 | 480.0000 | 480.0000 | True |
| X64 | tigerstore | update.api.creates.rate | requests/s | 3 | 5.4896 | 5.4951 | 5.4980 | True |
| X64 | tigerstore | update.api.deletes.count | requests | 3 | 64.0000 | 64.0000 | 64.0000 | True |
| X64 | tigerstore | update.api.deletes.rate | requests/s | 3 | 0.7319 | 0.7327 | 0.7331 | True |
| X64 | tigerstore | update.api.http403 | responses | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| X64 | tigerstore | update.api.http404 | responses | 3 | 20.0000 | 20.0000 | 26.0000 | True |
| X64 | tigerstore | update.api.http409 | responses | 3 | 42.0000 | 46.0000 | 51.0000 | True |
| X64 | tigerstore | update.api.http422 | responses | 1 | 0.0000 | 0.0000 | 0.0000 | True |
| X64 | tigerstore | update.api.http500 | responses | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| X64 | tigerstore | update.api.requests | requests/s | 3 | 68.4844 | 68.5282 | 68.5971 | True |
| X64 | tigerstore | update.api.resource..p50 | ms | 3 | 2.5806 | 2.5826 | 2.6423 | True |
| X64 | tigerstore | update.api.resource..p99 | ms | 3 | 18.6000 | 18.7500 | 42.5000 | True |
| X64 | tigerstore | update.api.resource.configmaps.p50 | ms | 3 | 2.7260 | 2.8055 | 3.1074 | True |
| X64 | tigerstore | update.api.resource.configmaps.p99 | ms | 3 | 38.6136 | 46.5539 | 87.2021 | True |
| X64 | tigerstore | update.api.resource.deployments.p50 | ms | 3 | 4.9248 | 8.1132 | 14.4521 | True |
| X64 | tigerstore | update.api.resource.deployments.p99 | ms | 3 | 73.8000 | 95.0313 | 95.2941 | True |
| X64 | tigerstore | update.api.resource.endpoints.p50 | ms | 3 | 9.4144 | 13.6066 | 17.3140 | True |
| X64 | tigerstore | update.api.resource.endpoints.p99 | ms | 3 | 79.9000 | 99.9750 | 133.0000 | True |
| X64 | tigerstore | update.api.resource.endpointslices.p50 | ms | 3 | 10.9659 | 13.9080 | 18.4591 | True |
| X64 | tigerstore | update.api.resource.endpointslices.p99 | ms | 3 | 75.7500 | 128.2500 | 164.2500 | True |
| X64 | tigerstore | update.api.resource.events.p50 | ms | 3 | 3.8339 | 4.3574 | 13.0870 | True |
| X64 | tigerstore | update.api.resource.events.p99 | ms | 3 | 56.6000 | 85.3750 | 97.6071 | True |
| X64 | tigerstore | update.api.resource.leases.p50 | ms | 3 | 2.6293 | 2.7455 | 3.0198 | True |
| X64 | tigerstore | update.api.resource.leases.p99 | ms | 3 | 24.5400 | 34.7500 | 139.0000 | True |
| X64 | tigerstore | update.api.resource.limitranges.p50 | ms | 3 | 3.2143 | 11.6667 | 12.5000 | True |
| X64 | tigerstore | update.api.resource.limitranges.p99 | ms | 3 | 45.5000 | 183.0000 | 194.0000 | True |
| X64 | tigerstore | update.api.resource.nodes.p50 | ms | 3 | 2.5000 | 2.5000 | 2.5000 | True |
| X64 | tigerstore | update.api.resource.nodes.p99 | ms | 3 | 4.9500 | 4.9500 | 4.9500 | True |
| X64 | tigerstore | update.api.resource.pods.p50 | ms | 3 | 3.4645 | 3.6284 | 4.8842 | True |
| X64 | tigerstore | update.api.resource.pods.p99 | ms | 3 | 77.1154 | 83.3333 | 146.7368 | True |
| X64 | tigerstore | update.api.resource.replicasets.p50 | ms | 3 | 6.9672 | 8.5976 | 17.6400 | True |
| X64 | tigerstore | update.api.resource.replicasets.p99 | ms | 3 | 87.5000 | 149.3750 | 166.1667 | True |
| X64 | tigerstore | update.api.resource.secrets.p50 | ms | 3 | 2.5000 | 2.5000 | 2.5000 | True |
| X64 | tigerstore | update.api.resource.secrets.p99 | ms | 3 | 4.9500 | 4.9500 | 4.9500 | True |
| X64 | tigerstore | update.api.resource.services.p50 | ms | 3 | 2.5000 | 2.5490 | 2.6531 | True |
| X64 | tigerstore | update.api.resource.services.p99 | ms | 3 | 4.9500 | 14.6000 | 21.5333 | True |
| X64 | tigerstore | update.api.updates.count | requests | 3 | 4191.0000 | 4193.0000 | 4196.0000 | True |
| X64 | tigerstore | update.api.updates.rate | requests/s | 3 | 47.9311 | 48.0018 | 48.0616 | True |
| X64 | tigerstore | update.api.verb.DELETE.p50 | ms | 3 | 15.2564 | 21.1905 | 34.6154 | True |
| X64 | tigerstore | update.api.verb.DELETE.p99 | ms | 3 | 184.0000 | 193.6000 | 272.0000 | True |
| X64 | tigerstore | update.api.verb.GET.p50 | ms | 3 | 2.7371 | 2.7426 | 3.1287 | True |
| X64 | tigerstore | update.api.verb.GET.p99 | ms | 3 | 23.7824 | 24.9742 | 76.7778 | True |
| X64 | tigerstore | update.api.verb.LIST.p50 | ms | 3 | 3.3784 | 4.0323 | 4.2241 | True |
| X64 | tigerstore | update.api.verb.LIST.p99 | ms | 3 | 175.0000 | 175.5000 | 183.3333 | True |
| X64 | tigerstore | update.api.verb.PATCH.p50 | ms | 3 | 2.8257 | 2.9202 | 3.3133 | True |
| X64 | tigerstore | update.api.verb.PATCH.p99 | ms | 3 | 36.9302 | 46.7192 | 87.2459 | True |
| X64 | tigerstore | update.api.verb.POST.p50 | ms | 3 | 4.0122 | 4.4898 | 13.5915 | True |
| X64 | tigerstore | update.api.verb.POST.p99 | ms | 3 | 88.6000 | 91.8571 | 134.0000 | True |
| X64 | tigerstore | update.api.verb.PUT.p50 | ms | 3 | 6.4128 | 9.7187 | 16.0123 | True |
| X64 | tigerstore | update.api.verb.PUT.p99 | ms | 3 | 77.6136 | 121.0667 | 150.6667 | True |
| X64 | tigerstore | update.cpu.all | CPU s | 3 | 11.3000 | 12.3200 | 15.7200 | True |
| X64 | tigerstore | update.cpu.cores | cores | 3 | 0.1294 | 0.1411 | 0.1797 | True |
| X64 | tigerstore | update.cpu.member1 | CPU s | 3 | 3.6700 | 4.4000 | 6.2900 | True |
| X64 | tigerstore | update.cpu.member2 | CPU s | 3 | 3.3100 | 4.5300 | 5.1900 | True |
| X64 | tigerstore | update.cpu.member3 | CPU s | 3 | 3.4600 | 3.5900 | 4.9000 | True |
| X64 | tigerstore | update.duration | s | 3 | 86.5700 | 86.6322 | 86.6452 | True |
| X64 | tigerstore | update.exit | exit code | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| X64 | tigerstore | update.live_inventory | objects in tracked kinds | 3 | 1185.0000 | 1185.0000 | 1185.0000 | True |
| X64 | tigerstore | update.pod.ContainersReady.p50 | ms | 3 | 2000.0000 | 2000.0000 | 2000.0000 | True |
| X64 | tigerstore | update.pod.ContainersReady.p99 | ms | 3 | 2000.0000 | 2000.0000 | 3000.0000 | True |
| X64 | tigerstore | update.pod.ContainersStarted.p50 | ms | 3 | 2075.0000 | 2107.0000 | 2794.0000 | True |
| X64 | tigerstore | update.pod.ContainersStarted.p99 | ms | 3 | 2500.0000 | 2704.0000 | 3804.0000 | True |
| X64 | tigerstore | update.pod.Initialized.p50 | ms | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| X64 | tigerstore | update.pod.Initialized.p99 | ms | 3 | 500.0000 | 500.0000 | 1000.0000 | True |
| X64 | tigerstore | update.pod.PodReadyToStartContainers.p50 | ms | 3 | 1000.0000 | 1000.0000 | 2000.0000 | True |
| X64 | tigerstore | update.pod.PodReadyToStartContainers.p99 | ms | 3 | 1000.0000 | 2000.0000 | 2000.0000 | True |
| X64 | tigerstore | update.pod.PodScheduled.p50 | ms | 3 | 0.0000 | 0.0000 | 0.0000 | True |
| X64 | tigerstore | update.pod.PodScheduled.p99 | ms | 3 | 500.0000 | 500.0000 | 1000.0000 | True |
| X64 | tigerstore | update.pod.Ready.p50 | ms | 3 | 2000.0000 | 2000.0000 | 2000.0000 | True |
| X64 | tigerstore | update.pod.Ready.p99 | ms | 3 | 2000.0000 | 2000.0000 | 3000.0000 | True |
| X64 | tigerstore | update.revision.member1 | revisions | 3 | 5486.0000 | 5493.0000 | 5494.0000 | True |
| X64 | tigerstore | update.revision.member2 | revisions | 3 | 5486.0000 | 5493.0000 | 5494.0000 | True |
| X64 | tigerstore | update.revision.member3 | revisions | 3 | 5486.0000 | 5493.0000 | 5494.0000 | True |
| X64 | tigerstore | update.storage.etcd_request_duration_seconds | RPC/s | 3 | 70.3255 | 70.4430 | 70.5414 | True |
| X64 | tigerstore | update.window | s | 3 | 87.3046 | 87.3509 | 87.4380 | True |
