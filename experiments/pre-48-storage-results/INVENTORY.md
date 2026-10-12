# Storage test inventory
Every declared Go test below runs in the unfiltered suite. No test requires an embedded etcd object, Raft/WAL inspection, or Membership RPC. The API classifications describe the unmodified test setup, not a skip list. Dynamic subtest names and results are in the per-version `test-results.tsv` evidence.
`API` means an isolated external etcd endpoint. `Status` means the apiserver store constructor also starts an asynchronous Maintenance.Status version check. `local` means no etcd RPC. Compaction uses KV.Compact, not a Membership or Maintenance RPC.

## Kubernetes 1.35

### `pkg/storage/etcd3/compact_test.go`

| Name | Dependency |
| --- | --- |
| `TestCompact` | API |
| `TestCompactIntervalZero` | API |
| `TestCompactConflict` | API |

### `pkg/storage/etcd3/event_test.go`

| Name | Dependency |
| --- | --- |
| `TestParseEvent` | local |

### `pkg/storage/etcd3/healthcheck_test.go`

| Name | Dependency |
| --- | --- |
| `TestEtcdHealthCheck` | local |

### `pkg/storage/etcd3/lease_manager_test.go`

| Name | Dependency |
| --- | --- |
| `TestGetReuseDurationSeconds` | local |

### `pkg/storage/etcd3/linearized_read_test.go`

| Name | Dependency |
| --- | --- |
| `TestLinearizedReadRevisionInvariant` | API, Status |

### `pkg/storage/etcd3/metrics/metrics_test.go`

| Name | Dependency |
| --- | --- |
| `TestRecordDecodeError` | local |
| `TestRecordEtcdRequest` | local |
| `TestStorageSizeCollector` | local |
| `TestUpdateStoreStats` | local |
| `TestDeleteStoreStats` | local |

### `pkg/storage/etcd3/preflight/checks_test.go`

| Name | Dependency |
| --- | --- |
| `TestParseServerURIGood` | local |
| `TestParseServerURIGoodUnix` | local |
| `TestParseServerURIBad` | local |
| `TestEtcdConnection` | local |
| `TestCheckEtcdServersEmpty` | local |
| `TestCheckEtcdServersUri` | local |
| `TestCheckEtcdServers` | local |
| `TestPollCheckServer` | local |

### `pkg/storage/etcd3/stats_test.go`

| Name | Dependency |
| --- | --- |
| `TestStatsCache` | local |

### `pkg/storage/etcd3/store_test.go`

| Name | Dependency |
| --- | --- |
| `TestCreate` | API, Status |
| `TestCreateWithTTL` | API, Status |
| `TestCreateWithKeyExist` | API, Status |
| `TestGet` | API, Status |
| `TestUnconditionalDelete` | API, Status |
| `TestConditionalDelete` | API, Status |
| `TestDeleteWithSuggestion` | API, Status |
| `TestDeleteWithSuggestionAndConflict` | API, Status |
| `TestDeleteWithSuggestionOfDeletedObject` | API, Status |
| `TestValidateDeletionWithSuggestion` | API, Status |
| `TestValidateDeletionWithOnlySuggestionValid` | API, Status |
| `TestDeleteWithConflict` | API, Status |
| `TestPreconditionalDeleteWithSuggestion` | API, Status |
| `TestPreconditionalDeleteWithSuggestionPass` | API, Status |
| `TestListPaging` | API, Status |
| `TestGetListNonRecursive` | API, Status |
| `TestGetListRecursivePrefix` | API, Status |
| `TestKeySchema` | API, Status |
| `TestGuaranteedUpdate` | API, Status |
| `TestGuaranteedUpdateWithTTL` | API, Status |
| `TestGuaranteedUpdateChecksStoredData` | API, Status |
| `TestGuaranteedUpdateWithConflict` | API, Status |
| `TestGuaranteedUpdateWithSuggestionAndConflict` | API, Status |
| `TestTransformationFailure` | API, Status |
| `TestList` | API, Status |
| `TestConsistentList` | API, Status |
| `TestCompactRevision` | API, Status |
| `TestListContinuation` | API, Status |
| `TestListPaginationRareObject` | API, Status |
| `TestListContinuationWithFilter` | API, Status |
| `TestNamespaceScopedList` | API, Status |
| `TestListInconsistentContinuation` | API, Status |
| `TestListResourceVersionMatch` | API, Status |
| `TestStats` | API, Status |
| `TestPrefix` | API, Status |
| `Test_growSlice` | local |
| `TestLeaseMaxObjectCount` | API, Status |
| `TestPrepareKey` | API, Status |
| `TestInvalidKeys` | API, Status |
| `BenchmarkStore_GetList` | Benchmark, inventoried but not executed |
| `BenchmarkStoreListCreate` | Benchmark, inventoried but not executed |
| `BenchmarkStoreList` | Benchmark, inventoried but not executed |
| `TestGetCurrentResourceVersion` | API, Status |
| `BenchmarkStoreStats` | Benchmark, inventoried but not executed |
| `BenchmarkStatsCacheCleanKeys` | Benchmark, inventoried but not executed |
| `TestPrefixGetKeys` | API, Status |
| `TestPrefixStats` | API, Status |

### `pkg/storage/etcd3/watcher_test.go`

| Name | Dependency |
| --- | --- |
| `TestWatch` | API, Status |
| `TestClusterScopedWatch` | API, Status |
| `TestNamespaceScopedWatch` | API, Status |
| `TestDeleteTriggerWatch` | API, Status |
| `TestWatchFromZero` | API, Status |
| `TestWatchFromNoneZero` | API, Status |
| `TestDelayedWatchDelivery` | API, Status |
| `TestWatchError` | API, Status |
| `TestWatchContextCancel` | API, Status |
| `TestWatcherTimeout` | API, Status |
| `TestWatchDeleteEventObjectHaveLatestRV` | API, Status |
| `TestWatchInitializationSignal` | API, Status |
| `TestProgressNotify` | API, Status |
| `TestWatchWithUnsafeDelete` | API, Status |
| `TestWatchDispatchBookmarkEvents` | API, Status |
| `TestSendInitialEventsBackwardCompatibility` | API, Status |
| `TestEtcdWatchSemantics` | API, Status |
| `TestEtcdWatchSemanticsWithConcurrentDecode` | API, Status |
| `TestEtcdWatchSemanticInitialEventsExtended` | API, Status |
| `TestWatchListMatchSingle` | API, Status |
| `TestWatchErrorEventIsBlockingFurtherEvent` | API, Status |
| `TestWatchErrorIncorrectConfiguration` | API, Status |
| `TestTooLargeResourceVersionErrorForWatchList` | API, Status |
| `TestWatchChanSync` | API, Status |

### Shared storage contract helpers
These functions are library entrypoints, not standalone Go tests. The etcd3 wrappers invoke all the contract helpers below except the optional bookmark-RV helper. The recorder and assertions stay unchanged.
| Helper | Invoked by upstream etcd3? |
| --- | --- |
| `RunBenchmarkStoreListCreate` | yes; benchmark not executed |
| `RunBenchmarkStoreList` | yes; benchmark not executed |
| `RunBenchmarkStoreStats` | yes; benchmark not executed |
| `RunTestCreate` | yes |
| `RunTestCreateWithTTL` | yes |
| `RunTestCreateWithKeyExist` | yes |
| `RunTestGet` | yes |
| `RunTestUnconditionalDelete` | yes |
| `RunTestConditionalDelete` | yes |
| `RunTestDeleteWithSuggestion` | yes |
| `RunTestDeleteWithSuggestionAndConflict` | yes |
| `RunTestDeleteWithConflict` | yes |
| `RunTestDeleteWithSuggestionOfDeletedObject` | yes |
| `RunTestValidateDeletionWithSuggestion` | yes |
| `RunTestValidateDeletionWithOnlySuggestionValid` | yes |
| `RunTestPreconditionalDeleteWithSuggestion` | yes |
| `RunTestPreconditionalDeleteWithOnlySuggestionPass` | yes |
| `RunTestList` | yes |
| `RunTestConsistentList` | yes |
| `RunTestGetListNonRecursive` | yes |
| `RunTestGetListRecursivePrefix` | yes |
| `RunTestListContinuation` | yes |
| `RunTestListPaginationRareObject` | yes |
| `RunTestListContinuationWithFilter` | yes |
| `RunTestListInconsistentContinuation` | yes |
| `RunTestListResourceVersionMatch` | yes |
| `RunTestGuaranteedUpdate` | yes |
| `RunTestGuaranteedUpdateWithTTL` | yes |
| `RunTestGuaranteedUpdateChecksStoredData` | yes |
| `RunTestGuaranteedUpdateWithConflict` | yes |
| `RunTestGuaranteedUpdateWithSuggestionAndConflict` | yes |
| `RunTestTransformationFailure` | yes |
| `RunTestStats` | yes |
| `RunTestListPaging` | yes |
| `RunTestNamespaceScopedList` | yes |
| `RunTestCompactRevision` | yes |
| `RunTestKeySchema` | yes |
| `RunTestWatch` | yes |
| `RunTestWatchFromZero` | yes |
| `RunTestDeleteTriggerWatch` | yes |
| `RunTestWatchFromNonZero` | yes |
| `RunTestDelayedWatchDelivery` | yes |
| `RunTestWatchError` | yes |
| `RunTestWatchWithUnsafeDelete` | yes |
| `RunTestWatchContextCancel` | yes |
| `RunTestWatcherTimeout` | yes |
| `RunTestWatchDeleteEventObjectHaveLatestRV` | yes |
| `RunTestWatchInitializationSignal` | yes |
| `RunOptionalTestProgressNotify` | yes |
| `RunTestClusterScopedWatch` | yes |
| `RunTestNamespaceScopedWatch` | yes |
| `RunTestWatchDispatchBookmarkEvents` | yes |
| `RunTestOptionalWatchBookmarksWithCorrectResourceVersion` | no; cacher-only optional bookmark contract |
| `RunSendInitialEventsBackwardCompatibility` | yes |
| `RunWatchSemantics` | yes |
| `RunWatchSemanticInitialEventsExtended` | yes |
| `RunWatchListMatchSingle` | yes |
| `RunWatchErrorIsBlockingFurtherEvents` | yes |

### Adjacent feature and storage factory inventory
These are siblings of `etcd3`, not subpackages of the requested etcd3 tree. They were inspected but not included in this baseline command. Feature tests mock Status. Factory tests need their separate endpoint/TLS configuration adapter. This is an explicit scope boundary, not a claim that they passed.
| Source | Tests |
| --- | --- |
| `pkg/storage/feature/feature_support_checker_test.go` | `TestSupports`, `TestSupportsRequestWatchProgress` |
| `pkg/storage/storagebackend/factory/etcd3_test.go` | `Test_atomicLastError` |
| `pkg/storage/storagebackend/factory/factory_test.go` | `TestCreateHealthcheck`, `TestCreateReadycheck`, `TestRateLimitHealthcheck`, `TestTimeTravelHealthcheck` |
| `pkg/storage/storagebackend/factory/tls_test.go` | `TestTLSConnection` |

## Kubernetes 1.36

### `pkg/storage/etcd3/compact_test.go`

| Name | Dependency |
| --- | --- |
| `TestCompact` | API |
| `TestCompactIntervalZero` | API |
| `TestCompactConflict` | API |

### `pkg/storage/etcd3/event_test.go`

| Name | Dependency |
| --- | --- |
| `TestParseEvent` | local |

### `pkg/storage/etcd3/healthcheck_test.go`

| Name | Dependency |
| --- | --- |
| `TestEtcdHealthCheck` | local |

### `pkg/storage/etcd3/lease_manager_test.go`

| Name | Dependency |
| --- | --- |
| `TestGetReuseDurationSeconds` | local |

### `pkg/storage/etcd3/linearized_read_test.go`

| Name | Dependency |
| --- | --- |
| `TestLinearizedReadRevisionInvariant` | API, Status |

### `pkg/storage/etcd3/metrics/metrics_test.go`

| Name | Dependency |
| --- | --- |
| `TestRecordDecodeError` | local |
| `TestRecordEtcdEvent` | local |
| `TestRecordEtcdBookmark` | local |
| `TestRecordEtcdRequest` | local |
| `TestStorageSizeCollector` | local |
| `TestUpdateStoreStats` | local |
| `TestDeleteStoreStats` | local |

### `pkg/storage/etcd3/preflight/checks_test.go`

| Name | Dependency |
| --- | --- |
| `TestParseServerURIGood` | local |
| `TestParseServerURIGoodUnix` | local |
| `TestParseServerURIBad` | local |
| `TestEtcdConnection` | local |
| `TestCheckEtcdServersEmpty` | local |
| `TestCheckEtcdServersUri` | local |
| `TestCheckEtcdServers` | local |
| `TestPollCheckServer` | local |

### `pkg/storage/etcd3/stats_test.go`

| Name | Dependency |
| --- | --- |
| `TestStatsCache` | local |

### `pkg/storage/etcd3/store_test.go`

| Name | Dependency |
| --- | --- |
| `TestCreate` | API, Status |
| `TestCreateWithTTL` | API, Status |
| `TestCreateWithKeyExist` | API, Status |
| `TestGet` | API, Status |
| `TestUnconditionalDelete` | API, Status |
| `TestConditionalDelete` | API, Status |
| `TestDeleteWithSuggestion` | API, Status |
| `TestDeleteWithSuggestionAndConflict` | API, Status |
| `TestDeleteWithSuggestionOfDeletedObject` | API, Status |
| `TestValidateDeletionWithSuggestion` | API, Status |
| `TestValidateDeletionWithOnlySuggestionValid` | API, Status |
| `TestDeleteWithConflict` | API, Status |
| `TestPreconditionalDeleteWithSuggestion` | API, Status |
| `TestPreconditionalDeleteWithSuggestionPass` | API, Status |
| `TestListPaging` | API, Status |
| `TestGetListNonRecursive` | API, Status |
| `TestGetListRecursivePrefix` | API, Status |
| `TestKeySchema` | API, Status |
| `TestGuaranteedUpdate` | API, Status |
| `TestGuaranteedUpdateWithTTL` | API, Status |
| `TestGuaranteedUpdateChecksStoredData` | API, Status |
| `TestGuaranteedUpdateWithConflict` | API, Status |
| `TestGuaranteedUpdateWithSuggestionAndConflict` | API, Status |
| `TestTransformationFailure` | API, Status |
| `TestList` | API, Status |
| `TestConsistentList` | API, Status |
| `TestCompactRevision` | API, Status |
| `TestListContinuation` | API, Status |
| `TestListPaginationRareObject` | API, Status |
| `TestListContinuationWithFilter` | API, Status |
| `TestNamespaceScopedList` | API, Status |
| `TestListInconsistentContinuation` | API, Status |
| `TestListResourceVersionMatch` | API, Status |
| `TestStats` | API, Status |
| `TestPrefix` | API, Status |
| `Test_growSlice` | local |
| `TestLeaseMaxObjectCount` | API, Status |
| `TestPrepareKey` | API, Status |
| `TestInvalidKeys` | API, Status |
| `BenchmarkStore_GetList` | Benchmark, inventoried but not executed |
| `BenchmarkStoreListCreate` | Benchmark, inventoried but not executed |
| `BenchmarkStoreList` | Benchmark, inventoried but not executed |
| `TestGetCurrentResourceVersion` | API, Status |
| `BenchmarkStoreStats` | Benchmark, inventoried but not executed |
| `BenchmarkStatsCacheCleanKeys` | Benchmark, inventoried but not executed |
| `TestPrefixGetKeys` | API, Status |
| `TestPrefixStats` | API, Status |

### `pkg/storage/etcd3/watcher_test.go`

| Name | Dependency |
| --- | --- |
| `TestWatch` | API, Status |
| `TestClusterScopedWatch` | API, Status |
| `TestNamespaceScopedWatch` | API, Status |
| `TestDeleteTriggerWatch` | API, Status |
| `TestWatchFromZero` | API, Status |
| `TestWatchFromNoneZero` | API, Status |
| `TestDelayedWatchDelivery` | API, Status |
| `TestWatchError` | API, Status |
| `TestWatchContextCancel` | API, Status |
| `TestWatcherTimeout` | API, Status |
| `TestWatchDeleteEventObjectHaveLatestRV` | API, Status |
| `TestWatchInitializationSignal` | API, Status |
| `TestProgressNotify` | API, Status |
| `TestWatchWithUnsafeDelete` | API, Status |
| `TestWatchDispatchBookmarkEvents` | API, Status |
| `TestSendInitialEventsBackwardCompatibility` | API, Status |
| `TestEtcdWatchSemantics` | API, Status |
| `TestEtcdWatchSemanticsWithConcurrentDecode` | API, Status |
| `TestEtcdWatchSemanticInitialEventsExtended` | API, Status |
| `TestWatchListMatchSingle` | API, Status |
| `TestWatchErrorEventIsBlockingFurtherEvent` | API, Status |
| `TestWatchErrorIncorrectConfiguration` | API, Status |
| `TestTooLargeResourceVersionErrorForWatchList` | API, Status |
| `TestWatchChanSync` | API, Status |

### Shared storage contract helpers
These functions are library entrypoints, not standalone Go tests. The etcd3 wrappers invoke all the contract helpers below except the optional bookmark-RV helper. The recorder and assertions stay unchanged.
| Helper | Invoked by upstream etcd3? |
| --- | --- |
| `RunBenchmarkStoreListCreate` | yes; benchmark not executed |
| `RunBenchmarkStoreList` | yes; benchmark not executed |
| `RunBenchmarkStoreStats` | yes; benchmark not executed |
| `RunTestCreate` | yes |
| `RunTestCreateWithTTL` | yes |
| `RunTestCreateWithKeyExist` | yes |
| `RunTestGet` | yes |
| `RunTestUnconditionalDelete` | yes |
| `RunTestConditionalDelete` | yes |
| `RunTestDeleteWithSuggestion` | yes |
| `RunTestDeleteWithSuggestionAndConflict` | yes |
| `RunTestDeleteWithConflict` | yes |
| `RunTestDeleteWithSuggestionOfDeletedObject` | yes |
| `RunTestValidateDeletionWithSuggestion` | yes |
| `RunTestValidateDeletionWithOnlySuggestionValid` | yes |
| `RunTestPreconditionalDeleteWithSuggestion` | yes |
| `RunTestPreconditionalDeleteWithOnlySuggestionPass` | yes |
| `RunTestList` | yes |
| `RunTestConsistentList` | yes |
| `RunTestGetListNonRecursive` | yes |
| `RunTestGetListRecursivePrefix` | yes |
| `RunTestListContinuation` | yes |
| `RunTestListPaginationRareObject` | yes |
| `RunTestListContinuationWithFilter` | yes |
| `RunTestListInconsistentContinuation` | yes |
| `RunTestListResourceVersionMatch` | yes |
| `RunTestGuaranteedUpdate` | yes |
| `RunTestGuaranteedUpdateWithTTL` | yes |
| `RunTestGuaranteedUpdateChecksStoredData` | yes |
| `RunTestGuaranteedUpdateWithConflict` | yes |
| `RunTestGuaranteedUpdateWithSuggestionAndConflict` | yes |
| `RunTestTransformationFailure` | yes |
| `RunTestStats` | yes |
| `RunTestListPaging` | yes |
| `RunTestNamespaceScopedList` | yes |
| `RunTestCompactRevision` | yes |
| `RunTestKeySchema` | yes |
| `RunTestWatch` | yes |
| `RunTestWatchFromZero` | yes |
| `RunTestDeleteTriggerWatch` | yes |
| `RunTestWatchFromNonZero` | yes |
| `RunTestDelayedWatchDelivery` | yes |
| `RunTestWatchError` | yes |
| `RunTestWatchWithUnsafeDelete` | yes |
| `RunTestWatchContextCancel` | yes |
| `RunTestWatcherTimeout` | yes |
| `RunTestWatchDeleteEventObjectHaveLatestRV` | yes |
| `RunTestWatchInitializationSignal` | yes |
| `RunOptionalTestProgressNotify` | yes |
| `RunTestClusterScopedWatch` | yes |
| `RunTestNamespaceScopedWatch` | yes |
| `RunTestWatchDispatchBookmarkEvents` | yes |
| `RunTestOptionalWatchBookmarksWithCorrectResourceVersion` | no; cacher-only optional bookmark contract |
| `RunSendInitialEventsBackwardCompatibility` | yes |
| `RunWatchSemantics` | yes |
| `RunWatchSemanticInitialEventsExtended` | yes |
| `RunWatchListMatchSingle` | yes |
| `RunWatchErrorIsBlockingFurtherEvents` | yes |

### Adjacent feature and storage factory inventory
These are siblings of `etcd3`, not subpackages of the requested etcd3 tree. They were inspected but not included in this baseline command. Feature tests mock Status. Factory tests need their separate endpoint/TLS configuration adapter. This is an explicit scope boundary, not a claim that they passed.
| Source | Tests |
| --- | --- |
| `pkg/storage/feature/feature_support_checker_test.go` | `TestSupports`, `TestSupportsRequestWatchProgress` |
| `pkg/storage/storagebackend/factory/etcd3_test.go` | `Test_atomicLastError` |
| `pkg/storage/storagebackend/factory/factory_test.go` | `TestCreateHealthcheck`, `TestCreateReadycheck`, `TestRateLimitHealthcheck`, `TestTimeTravelHealthcheck` |
| `pkg/storage/storagebackend/factory/tls_test.go` | `TestTLSConnection` |

## Kubernetes 1.37

### `pkg/storage/etcd3/block_logger_test.go`

| Name | Dependency |
| --- | --- |
| `TestBlockLogger` | local |

### `pkg/storage/etcd3/compact_test.go`

| Name | Dependency |
| --- | --- |
| `TestCompact` | API |
| `TestCompactIntervalZero` | API |
| `TestCompactConflict` | API |

### `pkg/storage/etcd3/corrupt_obj_deleter_test.go`

| Name | Dependency |
| --- | --- |
| `TestAggregatedStorageErrorMessage` | local |

### `pkg/storage/etcd3/event_test.go`

| Name | Dependency |
| --- | --- |
| `TestParseEvent` | local |

### `pkg/storage/etcd3/healthcheck_test.go`

| Name | Dependency |
| --- | --- |
| `TestEtcdHealthCheck` | local |

### `pkg/storage/etcd3/lease_manager_test.go`

| Name | Dependency |
| --- | --- |
| `TestGetReuseDurationSeconds` | local |

### `pkg/storage/etcd3/linearized_read_test.go`

| Name | Dependency |
| --- | --- |
| `TestLinearizedReadRevisionInvariant` | API, Status |

### `pkg/storage/etcd3/metrics/metrics_test.go`

| Name | Dependency |
| --- | --- |
| `TestRecordDecodeError` | local |
| `TestRecordEtcdEvent` | local |
| `TestRecordEtcdBookmark` | local |
| `TestRecordEtcdRequest` | local |
| `TestStorageSizeCollector` | local |
| `TestUpdateStoreStats` | local |
| `TestDeleteStoreStats` | local |

### `pkg/storage/etcd3/preflight/checks_test.go`

| Name | Dependency |
| --- | --- |
| `TestParseServerURIGood` | local |
| `TestParseServerURIGoodUnix` | local |
| `TestParseServerURIBad` | local |
| `TestEtcdConnection` | local |
| `TestCheckEtcdServersEmpty` | local |
| `TestCheckEtcdServersUri` | local |
| `TestCheckEtcdServers` | local |
| `TestPollCheckServer` | local |

### `pkg/storage/etcd3/stats_test.go`

| Name | Dependency |
| --- | --- |
| `TestStatsCache` | local |

### `pkg/storage/etcd3/store_test.go`

| Name | Dependency |
| --- | --- |
| `TestCreate` | API, Status |
| `TestCreateWithTTL` | API, Status |
| `TestCreateWithKeyExist` | API, Status |
| `TestGet` | API, Status |
| `TestUnconditionalDelete` | API, Status |
| `TestConditionalDelete` | API, Status |
| `TestDeleteWithSuggestion` | API, Status |
| `TestDeleteWithSuggestionAndConflict` | API, Status |
| `TestDeleteWithSuggestionOfDeletedObject` | API, Status |
| `TestValidateDeletionWithSuggestion` | API, Status |
| `TestValidateDeletionWithOnlySuggestionValid` | API, Status |
| `TestDeleteWithConflict` | API, Status |
| `TestDeleteWithConflictAndMissingExpectedTransformOrDecodeError` | API, Status |
| `TestDeleteWithConflictAndExpectedTransformError` | API, Status |
| `TestDeleteWithConflictAndExpectedDecodeError` | API, Status |
| `TestDeleteWithSuggestionAndMissingExpectedTransformOrDecodeFailure` | API, Status |
| `TestPreconditionalDeleteWithSuggestion` | API, Status |
| `TestPreconditionalDeleteWithSuggestionPass` | API, Status |
| `TestListPaging` | API, Status |
| `TestGetListNonRecursive` | API, Status |
| `TestGetListRecursivePrefix` | API, Status |
| `TestKeySchema` | API, Status |
| `TestGetListWithErrorAggregation` | API, Status |
| `TestGetListWithoutErrorAggregation` | API, Status |
| `TestGuaranteedUpdate` | API, Status |
| `TestGuaranteedUpdateWithTTL` | API, Status |
| `TestGuaranteedUpdateChecksStoredData` | API, Status |
| `TestGuaranteedUpdateWithConflict` | API, Status |
| `TestGuaranteedUpdateWithSuggestionAndConflict` | API, Status |
| `TestTransformationFailure` | API, Status |
| `TestList` | API, Status |
| `TestGetListStreamFallsBackToPaginated` | API, Status |
| `TestGetListStreamMultiChunk` | API, Status |
| `TestListMetrics` | API, Status |
| `TestConsistentList` | API, Status |
| `TestCompactRevision` | API, Status |
| `TestListContinuation` | API, Status |
| `TestListPaginationRareObject` | API, Status |
| `TestListContinuationWithFilter` | API, Status |
| `TestNamespaceScopedList` | API, Status |
| `TestListInconsistentContinuation` | API, Status |
| `TestListResourceVersionMatch` | API, Status |
| `TestStats` | API, Status |
| `TestPrefix` | API, Status |
| `Test_growSlice` | local |
| `TestLeaseMaxObjectCount` | API, Status |
| `TestPrepareKey` | API, Status |
| `TestInvalidKeys` | API, Status |
| `BenchmarkStore_GetList` | Benchmark, inventoried but not executed |
| `BenchmarkStoreWriteThroughput` | Benchmark, inventoried but not executed |
| `BenchmarkStoreList` | Benchmark, inventoried but not executed |
| `TestGetCurrentResourceVersion` | API, Status |
| `BenchmarkStoreStats` | Benchmark, inventoried but not executed |
| `BenchmarkStatsCacheCleanKeys` | Benchmark, inventoried but not executed |
| `TestPrefixGetKeys` | API, Status |
| `TestPrefixStats` | API, Status |

### `pkg/storage/etcd3/watcher_test.go`

| Name | Dependency |
| --- | --- |
| `TestWatch` | API, Status |
| `TestWatchErrorIncorrectConfiguration` | API, Status |
| `TestTooLargeResourceVersionErrorForWatchList` | API, Status |
| `TestWatchChanSync` | API, Status |
| `TestWatchChanSyncStreamMatchesPaginated` | API, Status |
| `TestWatchChanSyncStreamFallsBackToPaginated` | API, Status |
| `TestWatchChanSyncStreamCompactionError` | API, Status |
| `TestWatchChanSyncStreamMetrics` | API, Status |
| `TestWatchWithShardSelector` | API, Status |

### `pkg/storage/testing/store_benchmarks_test.go`

| Name | Dependency |
| --- | --- |
| `TestWatchLatencyTimestampRoundTrip` | local |
| `TestWatchLatencyTracker_RecordWriteAndHandleEvent` | local |

### Shared storage contract helpers
These functions are library entrypoints, not standalone Go tests. The etcd3 wrappers invoke all the contract helpers below except the optional bookmark-RV helper. The recorder and assertions stay unchanged.
| Helper | Invoked by upstream etcd3? |
| --- | --- |
| `RunBenchmarkWriteThroughput` | yes; benchmark not executed |
| `RunBenchmarkStoreList` | yes; benchmark not executed |
| `RunBenchmarkStoreStats` | yes; benchmark not executed |
| `RunTestCreate` | yes |
| `RunTestCreateWithTTL` | yes |
| `RunTestCreateWithKeyExist` | yes |
| `RunTestGet` | yes |
| `RunTestUnconditionalDelete` | yes |
| `RunTestConditionalDelete` | yes |
| `RunTestDeleteWithSuggestion` | yes |
| `RunTestDeleteWithSuggestionAndConflict` | yes |
| `RunTestDeleteWithConflict` | yes |
| `RunTestDeleteWithSuggestionOfDeletedObject` | yes |
| `RunTestValidateDeletionWithSuggestion` | yes |
| `RunTestValidateDeletionWithOnlySuggestionValid` | yes |
| `RunTestPreconditionalDeleteWithSuggestion` | yes |
| `RunTestPreconditionalDeleteWithOnlySuggestionPass` | yes |
| `RunTestList` | yes |
| `RunTestDeleteWithConflictAndMissingExpectedTransformOrDecodeError` | yes |
| `RunTestDeleteExpectedTransformOrDecodeError` | yes |
| `RunTestDeleteWithSuggestionAndMissingExpectedTransformOrDecodeError` | yes |
| `RunTestConsistentList` | yes |
| `RunTestGetListNonRecursive` | yes |
| `RunTestGetListRecursivePrefix` | yes |
| `RunTestGetListWithErrorAggregation` | yes |
| `RunTestGetListWithoutErrorAggregation` | yes |
| `RunTestListContinuation` | yes |
| `RunTestListPaginationRareObject` | yes |
| `RunTestListContinuationWithFilter` | yes |
| `RunTestListInconsistentContinuation` | yes |
| `RunTestListResourceVersionMatch` | yes |
| `RunTestGuaranteedUpdate` | yes |
| `RunTestGuaranteedUpdateWithTTL` | yes |
| `RunTestGuaranteedUpdateChecksStoredData` | yes |
| `RunTestGuaranteedUpdateWithConflict` | yes |
| `RunTestGuaranteedUpdateWithSuggestionAndConflict` | yes |
| `RunTestTransformationFailure` | yes |
| `RunTestStats` | yes |
| `RunTestListPaging` | yes |
| `RunTestNamespaceScopedList` | yes |
| `RunTestCompactRevision` | yes |
| `RunTestKeySchema` | yes |
| `RunTestWatch` | yes |
| `RunTestWatchFromZero` | yes |
| `RunTestDeleteTriggerWatch` | yes |
| `RunTestWatchFromNonZero` | yes |
| `RunTestDelayedWatchDelivery` | yes |
| `RunTestWatchError` | yes |
| `RunTestWatchWithUnsafeDelete` | yes |
| `RunTestWatchContextCancel` | yes |
| `RunTestWatcherTimeout` | yes |
| `RunTestWatchDeleteEventObjectHaveLatestRV` | yes |
| `RunTestWatchInitializationSignal` | yes |
| `RunOptionalTestProgressNotify` | yes |
| `RunTestClusterScopedWatch` | yes |
| `RunTestNamespaceScopedWatch` | yes |
| `RunTestWatchDispatchBookmarkEvents` | yes |
| `RunTestOptionalWatchBookmarksWithCorrectResourceVersion` | no; cacher-only optional bookmark contract |
| `RunSendInitialEventsBackwardCompatibility` | yes |
| `RunWatchSemantics` | yes |
| `RunWatchSemanticInitialEventsExtended` | yes |
| `RunWatchListMatchSingle` | yes |
| `RunWatchErrorIsBlockingFurtherEvents` | yes |

### Adjacent feature and storage factory inventory
These are siblings of `etcd3`, not subpackages of the requested etcd3 tree. They were inspected but not included in this baseline command. Feature tests mock Status. Factory tests need their separate endpoint/TLS configuration adapter. This is an explicit scope boundary, not a claim that they passed.
| Source | Tests |
| --- | --- |
| `pkg/storage/feature/feature_support_checker_test.go` | `TestSupports`, `TestSupportsRequestWatchProgress`, `TestMarkUnsupported` |
| `pkg/storage/storagebackend/factory/etcd3_test.go` | `Test_atomicLastError` |
| `pkg/storage/storagebackend/factory/factory_test.go` | `TestCreateHealthcheck`, `TestCreateReadycheck`, `TestRateLimitHealthcheck`, `TestTimeTravelHealthcheck` |
| `pkg/storage/storagebackend/factory/tls_test.go` | `TestTLSConnection` |

## Subpackage coverage
- `pkg/storage/etcd3` runs the storage implementation tests and shared interface contracts.
- `pkg/storage/etcd3/metrics` runs every metric test, with fake monitors and no real member API.
- `pkg/storage/etcd3/preflight` runs every URI/reachability test. It tests bad addresses and empty server lists, not etcd membership.
- `pkg/storage/etcd3/testing`, `pkg/storage/etcd3/testing/testingcert`, and `pkg/storage/etcd3/testserver` contain helpers or certificate fixtures and have no test files.
- `pkg/storage/testing` has no standalone Go tests in 1.35 and 1.36. In 1.37 it also runs the two watch-latency utility tests. The shared store/watch contracts execute through the etcd3 wrappers in all three versions.
- A package-level `skip` JSON event accompanied by `[no test files]` is Go package reporting, not an executed test calling `t.Skip`. Per-test skips are counted only on JSON events with a `Test` field.

## The one shared contract not invoked by etcd3
`RunTestOptionalWatchBookmarksWithCorrectResourceVersion` asks for `AllowWatchBookmarks=true` without `ProgressNotify`. Its own comment says the feature is implemented in the watch cache layer and is optional. Its upstream callers are `pkg/storage/cacher/cacher_test.go`, at lines 478 in v0.35.0 and v0.36.0, and 526 in v0.37.0. Etcd3 does not produce periodic API-client bookmarks merely because `AllowWatchBookmarks` is true. Its `RunTestWatchDispatchBookmarkEvents(..., false)` invocation tests that distinction. This helper is not an embedded-etcd or membership dependency and was not removed by the adapter. Cacher tests are outside this experiment.

## Tests with server configuration requirements
Only `TestProgressNotify` and `TestWatchDispatchBookmarkEvents` in 1.35/1.36, and `TestWatch/ProgressNotify` and `TestWatch/WatchDispatchBookmarkEvents` in 1.37, set `WatchProgressNotifyInterval=1s`. The factory forwards this as `--watch-progress-notify-interval=1s`. This is lifecycle configuration, not an etcd-internal assertion or Member RPC. All other setup calls request the stock default 10-minute interval.

## Embedded setup removed
The original `RunEtcd` uses `embed.NewConfig`, `embed.StartEtcd`, `Server.ReadyNotify`, `Server.Stop`, `Etcd.Err`, `Etcd.Close`, and `Server.Cluster().ClientURLs`. These are test setup internals, not test assertions. The adapter replaces them with an external process factory, endpoint line, ordinary KV readiness call, and process cleanup. The sole `*embed.Config` signature in `store_test.go` becomes `*testserver.Config`. Assertions, test bodies, shared suites, feature gates, recorders, and timeouts stay upstream. `go list -deps -test` records the selected test dependencies and contains no `go.etcd.io/etcd/server/v3` package.
