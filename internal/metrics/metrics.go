// Package metrics holds the Prometheus metrics of one member under etcd
// v3.7.2's names, help texts and buckets, in a registry of the member's
// own, so that tests can run several members in one process. The node
// driver, the peer transport and the server observe into them; the
// values the core keeps are read from a core.Metrics at each scrape.
//
// Some of etcd's metrics name parts tigerstore does not have: bbolt's
// commit phases, the v2 store, the gRPC proxy's cache and v2 snapshot
// marshalling. They are registered, so that a scrape has every name
// etcd's has, and stay at zero. docs/design.md says which tigerstore part
// each other metric measures.
package metrics

import (
	"errors"
	"runtime"
	"strconv"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/djosh34/tigerstore/internal/core"
)

// ServerVersion is the version etcd_server_version reports.
const ServerVersion = core.ServerVersion

// Feature is a feature gate for etcd_server_feature_enabled.
type Feature struct {
	Name, Stage string
	On          bool
}

// Set is the metrics of one member.
type Set struct {
	Registry *prometheus.Registry

	// The log is tigerstore's WAL: WALFsync times its fsyncs, WALWrite
	// its writes, and WALWriteBytes counts the bytes written.
	WALFsync, WALWrite prometheus.Histogram
	WALWriteBytes      prometheus.Gauge
	// BackendCommit is etcd's backend commit, which makes applied writes
	// durable in its database. The log is tigerstore's database, so the
	// nearest is a sync of the log: it times each, from the write of the
	// first group of records it makes durable to its end.
	BackendCommit prometheus.Histogram
	// Defrag times Maintenance.Defragment's cleaning pass while
	// DefragInflight is 1. BackendSnapshot times Maintenance.Snapshot.
	Defrag          prometheus.Histogram
	DefragInflight  prometheus.Gauge
	BackendSnapshot prometheus.Histogram
	// SnapSave times the writing of a checkpoint and SnapFsync its
	// fsyncs; SnapDBSave times the install of a leader's snapshot and
	// SnapDBFsync its fsyncs, while SnapshotApplyInProgress is 1.
	SnapSave, SnapFsync, SnapDBSave, SnapDBFsync prometheus.Histogram
	SnapshotApplyInProgress                      prometheus.Gauge
	// IndexCompactionPause times the applies that compact the index, and
	// DBCompactionLast is the unix time of the last.
	IndexCompactionPause prometheus.Histogram
	DBCompactionLast     prometheus.Gauge

	// Apply times applied writes by op, Range served ranges, Hash and
	// HashRev Maintenance.Hash and HashKV. SlowApplies counts the applies
	// over 100 ms, etcd's --warning-apply-duration.
	Apply, Range  *prometheus.HistogramVec
	Hash, HashRev prometheus.Histogram
	SlowApplies   prometheus.Counter
	// Request times client requests by type, as etcd's EtcdServer does.
	Request *prometheus.HistogramVec

	// Health counts /health answers; HealthCheck and HealthChecks are
	// the checks of /livez and /readyz.
	HealthSuccess, HealthFailures prometheus.Counter
	HealthCheck                   *prometheus.GaugeVec
	HealthChecks                  *prometheus.CounterVec

	// ClientRequests counts calls by kind and client API version, and
	// StreamFailures the watch and keepalive streams that failed.
	ClientRequests, StreamFailures *prometheus.CounterVec
	// The watch send loop's times: WatchStream per response with events,
	// WatchStreamPerEvent per event of it, WatchControl per control
	// response and WatchProgress per progress notification.
	WatchStream, WatchStreamPerEvent, WatchControl, WatchProgress prometheus.Histogram

	// LearnerPromotes counts the promotions this member served and
	// LearnerPromoteFailures those it refused, by reason.
	LearnerPromotes        prometheus.Counter
	LearnerPromoteFailures *prometheus.CounterVec

	// The traffic with peers, by peer ID.
	ActivePeers                                *prometheus.GaugeVec
	DisconnectedPeers                          *prometheus.CounterVec
	PeerSent, PeerReceived                     *prometheus.CounterVec
	PeerSentFailures, PeerReceivedFailures     *prometheus.CounterVec
	SnapshotSend, SnapshotSendFailures         *prometheus.CounterVec
	SnapshotSendInflights                      *prometheus.GaugeVec
	SnapshotSendSeconds                        *prometheus.HistogramVec
	SnapshotReceive, SnapshotReceiveFailures   *prometheus.CounterVec
	SnapshotReceiveInflights                   *prometheus.GaugeVec
	SnapshotReceiveSeconds, PeerRoundTripTimes *prometheus.HistogramVec
	// HeartbeatSent and HeartbeatReceived are the Unix times of the last
	// Raft heartbeat written to and read from each peer, for a failure's
	// report (#241).
	HeartbeatSent, HeartbeatReceived *prometheus.GaugeVec

	// FeatureEnabled holds the feature gates.
	FeatureEnabled *prometheus.GaugeVec

	node atomic.Pointer[core.Metrics]
}

func histogram(namespace, subsystem, name, help string, buckets []float64) prometheus.Histogram {
	return prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: namespace, Subsystem: subsystem, Name: name, Help: help, Buckets: buckets})
}

func counter(namespace, subsystem, name, help string) prometheus.Counter {
	return prometheus.NewCounter(prometheus.CounterOpts{Namespace: namespace, Subsystem: subsystem, Name: name, Help: help})
}

func gauge(namespace, subsystem, name, help string) prometheus.Gauge {
	return prometheus.NewGauge(prometheus.GaugeOpts{Namespace: namespace, Subsystem: subsystem, Name: name, Help: help})
}

func counterVec(subsystem, name, help string, labels ...string) *prometheus.CounterVec {
	return prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "etcd", Subsystem: subsystem, Name: name, Help: help}, labels)
}

func gaugeVec(subsystem, name, help string, labels ...string) *prometheus.GaugeVec {
	return prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "etcd", Subsystem: subsystem, Name: name, Help: help}, labels)
}

func histogramVec(subsystem, name, help string, buckets []float64, labels ...string) *prometheus.HistogramVec {
	return prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "etcd", Subsystem: subsystem, Name: name, Help: help, Buckets: buckets}, labels)
}

// exp2 is count buckets from start, each twice the last.
func exp2(start float64, count int) []float64 {
	return prometheus.ExponentialBuckets(start, 2, count)
}

// backendCommitHelp is etcd's help text of
// etcd_disk_backend_commit_duration_seconds, and what tigerstore times.
const backendCommitHelp = "The latency distributions of commit called by backend. " +
	"tigerstore's database is its log: this is a log sync, from the write of the first records it makes durable to its end."

// New returns the metrics of a member with the feature gates features.
func New(features []Feature) (*Set, error) {
	s := &Set{
		Registry: prometheus.NewRegistry(),

		WALFsync:      histogram("etcd", "disk", "wal_fsync_duration_seconds", "The latency distributions of fsync called by WAL.", exp2(0.001, 14)),
		WALWrite:      histogram("etcd", "disk", "wal_write_duration_seconds", "The latency distributions of write called by WAL.", exp2(0.001, 14)),
		WALWriteBytes: gauge("etcd", "disk", "wal_write_bytes_total", "Total number of bytes written in WAL."),
		BackendCommit: histogram("etcd", "disk", "backend_commit_duration_seconds", backendCommitHelp, exp2(0.001, 14)),

		Defrag:          histogram("etcd", "disk", "backend_defrag_duration_seconds", "The latency distribution of backend defragmentation.", exp2(.1, 13)),
		DefragInflight:  gauge("etcd", "disk", "defrag_inflight", "Whether or not defrag is active on the member. 1 means active, 0 means not."),
		BackendSnapshot: histogram("etcd", "disk", "backend_snapshot_duration_seconds", "The latency distribution of backend snapshots.", exp2(.01, 17)),

		SnapSave:                histogram("etcd_debugging", "snap", "save_total_duration_seconds", "The total latency distributions of save called by snapshot.", exp2(0.001, 14)),
		SnapFsync:               histogram("etcd", "snap", "fsync_duration_seconds", "The latency distributions of fsync called by snap.", exp2(0.001, 14)),
		SnapDBSave:              histogram("etcd", "snap_db", "save_total_duration_seconds", "The total latency distributions of v3 snapshot save", exp2(0.1, 10)),
		SnapDBFsync:             histogram("etcd", "snap_db", "fsync_duration_seconds", "The latency distributions of fsyncing .snap.db file", exp2(0.001, 14)),
		SnapshotApplyInProgress: gauge("etcd", "server", "snapshot_apply_in_progress_total", "1 if the server is applying the incoming snapshot. 0 if none."),

		IndexCompactionPause: histogram("etcd_debugging", "mvcc", "index_compaction_pause_duration_milliseconds", "Bucketed histogram of index compaction pause duration.", exp2(0.5, 14)),
		DBCompactionLast:     gauge("etcd_debugging", "mvcc", "db_compaction_last", "The unix time of the last db compaction. Resets to 0 on start."),

		Apply:       histogramVec("server", "apply_duration_seconds", "The latency distributions of v2 apply called by backend.", exp2(0.0001, 20), "version", "op", "success"),
		Range:       histogramVec("server", "range_duration_seconds", "The latency distributions of txn.Range", exp2(0.0001, 20), "success"),
		Hash:        histogram("etcd", "mvcc", "hash_duration_seconds", "The latency distribution of storage hash operation.", exp2(.01, 15)),
		HashRev:     histogram("etcd", "mvcc", "hash_rev_duration_seconds", "The latency distribution of storage hash by revision operation.", exp2(.01, 15)),
		SlowApplies: counter("etcd", "server", "slow_apply_total", "The total number of slow apply requests (likely overloaded from slow disk)."),
		Request:     histogramVec("server", "request_duration_seconds", "Response latency distribution in seconds for each type.", exp2(0.001, 14), "type", "success"),

		HealthSuccess:  counter("etcd", "server", "health_success", "The total number of successful health checks"),
		HealthFailures: counter("etcd", "server", "health_failures", "The total number of failed health checks"),
		HealthCheck:    gaugeVec("server", "healthcheck", "The result of each kind of healthcheck.", "type", "name"),
		HealthChecks:   counterVec("server", "healthchecks_total", "The total number of each kind of healthcheck.", "type", "name", "status"),

		ClientRequests:      counterVec("server", "client_requests_total", "The total number of client requests per client version.", "type", "client_api_version"),
		StreamFailures:      counterVec("network", "server_stream_failures_total", "The total number of stream failures from the local server.", "Type", "API"),
		WatchStream:         histogram("etcd_debugging", "server", "watch_send_loop_watch_stream_duration_seconds", "The total duration in seconds of running through the send loop watch stream response all events.", exp2(0.001, 14)),
		WatchStreamPerEvent: histogram("etcd_debugging", "server", "watch_send_loop_watch_stream_duration_per_event_seconds", "The average duration in seconds of running through the send loop watch stream response, per event.", exp2(0.001, 14)),
		WatchControl:        histogram("etcd_debugging", "server", "watch_send_loop_control_stream_duration_seconds", "The total duration in seconds of running through the send loop control stream response.", exp2(0.001, 14)),
		WatchProgress:       histogram("etcd_debugging", "server", "watch_send_loop_progress_duration_seconds", "The total duration in seconds of running through the progress loop control stream response.", exp2(0.001, 14)),

		LearnerPromotes:        counter("etcd", "server", "learner_promote_successes", "The total number of successful learner promotions while this member is leader."),
		LearnerPromoteFailures: counterVec("server", "learner_promote_failures", "The total number of failed learner promotions (likely learner not ready) while this member is leader.", "Reason"),

		ActivePeers:              gaugeVec("network", "active_peers", "The current number of active peer connections.", "Local", "Remote"),
		DisconnectedPeers:        counterVec("network", "disconnected_peers_total", "The total number of disconnected peers.", "Local", "Remote"),
		PeerSent:                 counterVec("network", "peer_sent_bytes_total", "The total number of bytes sent to peers.", "To"),
		PeerReceived:             counterVec("network", "peer_received_bytes_total", "The total number of bytes received from peers.", "From"),
		PeerSentFailures:         counterVec("network", "peer_sent_failures_total", "The total number of send failures from peers.", "To"),
		PeerReceivedFailures:     counterVec("network", "peer_received_failures_total", "The total number of receive failures from peers.", "From"),
		SnapshotSend:             counterVec("network", "snapshot_send_success", "Total number of successful snapshot sends", "To"),
		SnapshotSendInflights:    gaugeVec("network", "snapshot_send_inflights_total", "Total number of inflight snapshot sends", "To"),
		SnapshotSendFailures:     counterVec("network", "snapshot_send_failures", "Total number of snapshot send failures", "To"),
		SnapshotSendSeconds:      histogramVec("network", "snapshot_send_total_duration_seconds", "Total latency distributions of v3 snapshot sends", exp2(0.1, 10), "To"),
		SnapshotReceive:          counterVec("network", "snapshot_receive_success", "Total number of successful snapshot receives", "From"),
		SnapshotReceiveInflights: gaugeVec("network", "snapshot_receive_inflights_total", "Total number of inflight snapshot receives", "From"),
		SnapshotReceiveFailures:  counterVec("network", "snapshot_receive_failures", "Total number of snapshot receive failures", "From"),
		SnapshotReceiveSeconds:   histogramVec("network", "snapshot_receive_total_duration_seconds", "Total latency distributions of v3 snapshot receives", exp2(0.1, 10), "From"),
		PeerRoundTripTimes:       histogramVec("network", "peer_round_trip_time_seconds", "Round-Trip-Time histogram between peers", exp2(0.0001, 16), "To"),
		HeartbeatSent:            prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "tigerstore_network_heartbeat_sent_time_seconds", Help: "The Unix time of the last Raft heartbeat written to each peer."}, []string{"To"}),
		HeartbeatReceived:        prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "tigerstore_network_heartbeat_received_time_seconds", Help: "The Unix time of the last Raft heartbeat read from each peer."}, []string{"From"}),

		FeatureEnabled: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "etcd_server_feature_enabled", Help: "Whether or not a feature is enabled. 1 is enabled, 0 is not."}, []string{"name", "stage"}),
	}
	for _, f := range features {
		v := 0.0
		if f.On {
			v = 1
		}
		s.FeatureEnabled.WithLabelValues(f.Name, f.Stage).Set(v)
	}
	cs := []prometheus.Collector{
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		s.WALFsync, s.WALWrite, s.WALWriteBytes, s.BackendCommit, s.Defrag, s.DefragInflight, s.BackendSnapshot,
		s.SnapSave, s.SnapFsync, s.SnapDBSave, s.SnapDBFsync, s.SnapshotApplyInProgress,
		s.IndexCompactionPause, s.DBCompactionLast,
		s.Apply, s.Range, s.Hash, s.HashRev, s.SlowApplies, s.Request,
		s.HealthSuccess, s.HealthFailures, s.HealthCheck, s.HealthChecks,
		s.ClientRequests, s.StreamFailures, s.WatchStream, s.WatchStreamPerEvent, s.WatchControl, s.WatchProgress,
		s.LearnerPromotes, s.LearnerPromoteFailures,
		s.ActivePeers, s.DisconnectedPeers, s.PeerSent, s.PeerReceived, s.PeerSentFailures, s.PeerReceivedFailures,
		s.SnapshotSend, s.SnapshotSendInflights, s.SnapshotSendFailures, s.SnapshotSendSeconds,
		s.SnapshotReceive, s.SnapshotReceiveInflights, s.SnapshotReceiveFailures, s.SnapshotReceiveSeconds, s.PeerRoundTripTimes,
		s.HeartbeatSent, s.HeartbeatReceived,
		s.FeatureEnabled,
		&nodeCollector{s: s},
	}
	cs = append(cs, absent()...)
	var errs []error
	for _, c := range cs {
		errs = append(errs, s.Registry.Register(c))
	}
	return s, errors.Join(errs...)
}

// SetNode keeps the core's metrics for the next scrape.
func (s *Set) SetNode(m *core.Metrics) { s.node.Store(m) }

// absent are etcd's metrics of parts tigerstore does not have, at zero.
func absent() []prometheus.Collector {
	return []prometheus.Collector{
		histogram("etcd_debugging", "disk", "backend_commit_rebalance_duration_seconds", "The latency distributions of commit.rebalance called by bboltdb backend.", exp2(0.001, 14)),
		histogram("etcd_debugging", "disk", "backend_commit_spill_duration_seconds", "The latency distributions of commit.spill called by bboltdb backend.", exp2(0.001, 14)),
		histogram("etcd_debugging", "disk", "backend_commit_write_duration_seconds", "The latency distributions of commit.write called by bboltdb backend.", exp2(0.001, 14)),
		histogram("etcd_debugging", "snap", "save_marshalling_duration_seconds", "The marshalling cost distributions of save called by snapshot.", exp2(0.001, 14)),
		histogram("etcd_debugging", "mvcc", "db_compaction_pause_duration_milliseconds", "Bucketed histogram of db compaction pause duration.", exp2(1, 13)),
		histogram("etcd_debugging", "mvcc", "db_compaction_total_duration_milliseconds", "Bucketed histogram of db compaction total duration.", exp2(100, 14)),
		gauge("etcd_debugging", "store", "watchers", "Count of currently active watchers."),
		counter("etcd_debugging", "store", "watch_requests_total", "Total number of incoming watch requests (new or reestablished)."),
		counter("etcd_debugging", "store", "expires_total", "Total number of expired keys."),
		gauge("etcd", "grpc_proxy", "cache_hits_total", "Total number of cache hits"),
		gauge("etcd", "grpc_proxy", "cache_misses_total", "Total number of cache misses"),
		gauge("etcd", "grpc_proxy", "cache_keys_total", "Total number of keys/ranges cached"),
		counter("etcd", "grpc_proxy", "events_coalescing_total", "Total number of events coalescing"),
		gauge("etcd", "grpc_proxy", "watchers_coalescing_total", "Total number of current watchers coalescing"),
		gauge("etcd", "mvcc", "db_open_read_transactions", "The number of currently open read transactions"),
	}
}

// nodeCollector reports the metrics read from the core at a scrape.
type nodeCollector struct{ s *Set }

type nodeMetric struct {
	desc *prometheus.Desc
	typ  prometheus.ValueType
	// value gives the metric's value; labeled ones use each instead.
	value func(m *core.Metrics) float64
	each  func(m *core.Metrics, emit func(v float64, labels ...string))
}

func desc(name, help string, labels ...string) *prometheus.Desc {
	return prometheus.NewDesc(name, help, labels, nil)
}

func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

func hexID(id uint64) string { return strconv.FormatUint(id, 16) }

var nodeMetrics = []nodeMetric{
	{
		desc: desc("etcd_server_has_leader", "Whether or not a leader exists. 1 is existence, 0 is not."), typ: prometheus.GaugeValue,
		value: func(m *core.Metrics) float64 { return b2f(m.HasLeader) },
	},
	{
		desc: desc("etcd_server_is_leader", "Whether or not this member is a leader. 1 if is, 0 otherwise."), typ: prometheus.GaugeValue,
		value: func(m *core.Metrics) float64 { return b2f(m.IsLeader) },
	},
	{
		desc: desc("etcd_server_is_learner", "Whether or not this member is a learner. 1 if is, 0 otherwise."), typ: prometheus.GaugeValue,
		value: func(m *core.Metrics) float64 { return b2f(m.IsLearner) },
	},
	{
		desc: desc("etcd_server_leader_changes_seen_total", "The number of leader changes seen."), typ: prometheus.CounterValue,
		value: func(m *core.Metrics) float64 { return float64(m.LeaderChanges) },
	},
	{
		desc: desc("etcd_server_proposals_committed_total", "The total number of consensus proposals committed."), typ: prometheus.GaugeValue,
		value: func(m *core.Metrics) float64 { return float64(m.Committed) },
	},
	{
		desc: desc("etcd_server_proposals_applied_total", "The total number of consensus proposals applied."), typ: prometheus.GaugeValue,
		value: func(m *core.Metrics) float64 { return float64(m.Applied) },
	},
	{
		desc: desc("etcd_server_proposals_pending", "The current number of pending proposals to commit."), typ: prometheus.GaugeValue,
		value: func(m *core.Metrics) float64 { return float64(m.ProposalsPending) },
	},
	{
		desc: desc("etcd_server_proposals_failed_total", "The total number of failed proposals seen."), typ: prometheus.CounterValue,
		value: func(m *core.Metrics) float64 { return float64(m.ProposalsFailed) },
	},
	{
		desc: desc("etcd_server_read_indexes_failed_total", "The total number of failed read indexes seen."), typ: prometheus.CounterValue,
		value: func(m *core.Metrics) float64 { return float64(m.ReadIndexFailed) },
	},
	{
		desc: desc("etcd_server_slow_read_indexes_total", "The total number of pending read indexes not in sync with leader's or timed out read index requests."), typ: prometheus.CounterValue,
		value: func(m *core.Metrics) float64 { return float64(m.SlowReadIndexes) },
	},
	{
		desc: desc("etcd_server_heartbeat_send_failures_total", "The total number of leader heartbeat send failures (likely overloaded from slow disk)."), typ: prometheus.CounterValue,
		value: func(m *core.Metrics) float64 { return float64(m.HeartbeatSendFailures) },
	},
	{
		desc: desc("tigerstore_server_slow_commits_total", "The total number of times the leader's oldest entry waited three election timeouts to commit."), typ: prometheus.CounterValue,
		value: func(m *core.Metrics) float64 { return float64(m.SlowCommits) },
	},
	{
		desc: desc("etcd_server_quota_backend_bytes", "Current backend storage quota size in bytes."), typ: prometheus.GaugeValue,
		value: func(m *core.Metrics) float64 { return float64(m.QuotaBytes) },
	},
	{
		desc: desc("etcd_debugging_server_lease_expired_total", "The total number of expired leases."), typ: prometheus.CounterValue,
		value: func(m *core.Metrics) float64 { return float64(m.LeaseExpired) },
	},
	{
		desc: desc("etcd_server_id", "Server or member ID in hexadecimal format. 1 for 'server_id' label with current ID.", "server_id"), typ: prometheus.GaugeValue,
		each: func(m *core.Metrics, emit func(float64, ...string)) { emit(1, hexID(m.ID)) },
	},
	{
		desc: desc("etcd_server_version", "Which version is running. 1 for 'server_version' label with current version.", "server_version"), typ: prometheus.GaugeValue,
		each: func(_ *core.Metrics, emit func(float64, ...string)) { emit(1, ServerVersion) },
	},
	{
		desc: desc("etcd_server_go_version", "Which Go version server is running with. 1 for 'server_go_version' label with current version.", "server_go_version"), typ: prometheus.GaugeValue,
		each: func(_ *core.Metrics, emit func(float64, ...string)) { emit(1, runtime.Version()) },
	},
	{
		desc: desc("etcd_cluster_version", "Which version is running. 1 for 'cluster_version' label with current cluster version", "cluster_version"), typ: prometheus.GaugeValue,
		each: func(m *core.Metrics, emit func(float64, ...string)) {
			if v := majorMinor(m.EtcdVersion); v != "" {
				emit(1, v)
			}
		},
	},
	{
		desc: desc("etcd_network_known_peers", "The current number of known peers.", "Local", "Remote"), typ: prometheus.GaugeValue,
		each: func(m *core.Metrics, emit func(float64, ...string)) {
			for _, id := range m.Members {
				emit(1, hexID(m.ID), hexID(id))
			}
		},
	},
	{
		desc: desc("etcd_debugging_auth_revision", "The current revision of auth store."), typ: prometheus.GaugeValue,
		value: func(m *core.Metrics) float64 { return float64(m.AuthRevision) },
	},
	{
		desc: desc("etcd_debugging_lease_granted_total", "The total number of granted leases."), typ: prometheus.CounterValue,
		value: func(m *core.Metrics) float64 { return float64(m.LeaseGranted) },
	},
	{
		desc: desc("etcd_debugging_lease_revoked_total", "The total number of revoked leases."), typ: prometheus.CounterValue,
		value: func(m *core.Metrics) float64 { return float64(m.LeaseRevoked) },
	},
	{
		desc: desc("etcd_debugging_lease_renewed_total", "The number of renewed leases seen by the leader."), typ: prometheus.CounterValue,
		value: func(m *core.Metrics) float64 { return float64(m.LeaseRenewed) },
	},
	{
		desc: desc("etcd_mvcc_put_total", "Total number of puts seen by this member."), typ: prometheus.CounterValue,
		value: func(m *core.Metrics) float64 { return float64(m.Puts) },
	},
	{
		desc: desc("etcd_mvcc_delete_total", "Total number of deletes seen by this member."), typ: prometheus.CounterValue,
		value: func(m *core.Metrics) float64 { return float64(m.Deletes) },
	},
	{
		desc: desc("etcd_mvcc_range_total", "Total number of ranges seen by this member."), typ: prometheus.CounterValue,
		value: func(m *core.Metrics) float64 { return float64(m.Ranges) },
	},
	{
		desc: desc("etcd_mvcc_txn_total", "Total number of txns seen by this member."), typ: prometheus.CounterValue,
		value: func(m *core.Metrics) float64 { return float64(m.Txns) },
	},
	{
		desc: desc("etcd_debugging_mvcc_total_put_size_in_bytes", "The total size of put kv pairs seen by this member."), typ: prometheus.GaugeValue,
		value: func(m *core.Metrics) float64 { return float64(m.PutBytes) },
	},
	{
		desc: desc("etcd_debugging_mvcc_db_compaction_keys_total", "Total number of db keys compacted."), typ: prometheus.CounterValue,
		value: func(m *core.Metrics) float64 { return float64(m.Compacted) },
	},
	{
		desc: desc("etcd_debugging_mvcc_keys_total", "Total number of keys."), typ: prometheus.GaugeValue,
		value: func(m *core.Metrics) float64 { return float64(m.Keys) },
	},
	{
		desc: desc("etcd_debugging_mvcc_current_revision", "The current revision of store."), typ: prometheus.GaugeValue,
		value: func(m *core.Metrics) float64 { return float64(m.Rev) },
	},
	{
		desc: desc("etcd_debugging_mvcc_compact_revision", "The revision of the last compaction in store."), typ: prometheus.GaugeValue,
		value: func(m *core.Metrics) float64 { return float64(m.CompactRev) },
	},
	{
		desc: desc("etcd_mvcc_db_total_size_in_bytes", "Total size of the underlying database physically allocated in bytes."), typ: prometheus.GaugeValue,
		value: func(m *core.Metrics) float64 { return float64(m.DbSize) },
	},
	{
		desc: desc("etcd_mvcc_db_total_size_in_use_in_bytes", "Total size of the underlying database logically in use in bytes."), typ: prometheus.GaugeValue,
		value: func(m *core.Metrics) float64 { return float64(m.DbSizeInUse) },
	},
	{
		desc: desc("etcd_debugging_mvcc_watch_stream_total", "Total number of watch streams."), typ: prometheus.GaugeValue,
		value: func(m *core.Metrics) float64 { return float64(m.WatchStreams) },
	},
	{
		desc: desc("etcd_debugging_mvcc_watcher_total", "Total number of watchers."), typ: prometheus.GaugeValue,
		value: func(m *core.Metrics) float64 { return float64(m.Watchers) },
	},
	{
		desc: desc("etcd_debugging_mvcc_slow_watcher_total", "Total number of unsynced slow watchers."), typ: prometheus.GaugeValue,
		value: func(m *core.Metrics) float64 { return float64(m.HeldWatchers) },
	},
	{
		desc: desc("etcd_debugging_mvcc_pending_events_total", "Total number of pending events to be sent."), typ: prometheus.GaugeValue,
		value: func(m *core.Metrics) float64 { return float64(m.HeldEvents) },
	},
	{
		desc: desc("etcd_debugging_mvcc_events_total", "Total number of events sent by this member."), typ: prometheus.CounterValue,
		value: func(m *core.Metrics) float64 { return float64(m.Events) },
	},
	{
		desc: desc("etcd_debugging_server_alarms", "Alarms for every member in cluster. 1 for 'server_id' label with current ID. 2 for 'alarm_type' label with type of this alarm", "server_id", "alarm_type"), typ: prometheus.GaugeValue,
		each: func(m *core.Metrics, emit func(float64, ...string)) {
			for _, g := range m.AlarmGauges {
				emit(float64(g.Value), hexID(g.MemberID), g.Alarm.String())
			}
		},
	},
	{
		desc: desc("etcd_debugging_store_reads_total", "Total number of reads action by (get/getRecursive), local to this member.", "action"), typ: prometheus.CounterValue,
		each: func(m *core.Metrics, emit func(float64, ...string)) {
			if m.StoreGets > 0 {
				emit(float64(m.StoreGets), "getRecursive")
			}
		},
	},
	{
		desc: desc("etcd_debugging_store_writes_total", "Total number of writes (e.g. set/compareAndDelete) seen by this member.", "action"), typ: prometheus.CounterValue,
		each: func(m *core.Metrics, emit func(float64, ...string)) {
			if m.StoreCreates > 0 {
				emit(float64(m.StoreCreates), "create")
			}
			if m.StoreSets > 0 {
				emit(float64(m.StoreSets), "set")
			}
		},
	},
	{
		desc: desc("os_fd_used", "The number of used file descriptors."), typ: prometheus.GaugeValue,
		value: func(*core.Metrics) float64 { return float64(fdUsed()) },
	},
	{
		desc: desc("os_fd_limit", "The file descriptor limit."), typ: prometheus.GaugeValue,
		value: func(*core.Metrics) float64 { return float64(FDLimit()) },
	},
}

// leaseTTLs is etcd_debugging_lease_ttl_total, which nodeCollector
// builds from the core's bucket counts.
var leaseTTLs = desc("etcd_debugging_lease_ttl_total", "Bucketed histogram of lease TTLs.")

func (c *nodeCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, m := range nodeMetrics {
		ch <- m.desc
	}
	ch <- leaseTTLs
}

func (c *nodeCollector) Collect(ch chan<- prometheus.Metric) {
	m := c.s.node.Load()
	if m == nil {
		return
	}
	for _, nm := range nodeMetrics {
		emit := func(v float64, labels ...string) {
			metric, err := prometheus.NewConstMetric(nm.desc, nm.typ, v, labels...)
			if err != nil {
				metric = prometheus.NewInvalidMetric(nm.desc, err)
			}
			ch <- metric
		}
		if nm.each != nil {
			nm.each(m, emit)
		} else {
			emit(nm.value(m))
		}
	}
	buckets := make(map[float64]uint64, core.LeaseTTLBuckets)
	var count uint64
	for i, n := range m.LeaseTTLs {
		count += uint64(n)
		if i < core.LeaseTTLBuckets {
			buckets[float64(uint64(1)<<i)] = count
		}
	}
	h, err := prometheus.NewConstHistogram(leaseTTLs, count, float64(m.LeaseTTLSum), buckets)
	if err != nil {
		h = prometheus.NewInvalidMetric(leaseTTLs, err)
	}
	ch <- h
}

// majorMinor is the major.minor of a version such as 3.7.0, as
// etcd_cluster_version reports it.
func majorMinor(v string) string {
	dots := 0
	for i := range len(v) {
		if v[i] == '.' {
			if dots++; dots == 2 {
				return v[:i]
			}
		}
	}
	return v
}

// Node returns the core's metrics of the last scrape, nil if there are
// none.
func (s *Set) Node() *core.Metrics { return s.node.Load() }
