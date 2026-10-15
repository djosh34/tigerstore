package metrics

import (
	"fmt"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/core"
)

// TestNodeMetrics checks the metrics read from the core at a scrape:
// labeled gauges with etcd's labels and the lease TTL histogram built
// from the core's buckets.
func TestNodeMetrics(t *testing.T) {
	s, err := New([]Feature{{Name: "LeaseCheckpoint", Stage: "ALPHA", On: true}})
	if err != nil {
		t.Fatal(err)
	}
	m := &core.Metrics{
		ID: 0x8e9e05c52164694d, Members: []uint64{0x8e9e05c52164694d, 0x91bc3c398fb3c146},
		EtcdVersion: "3.7.0", IsLeader: true, HasLeader: true,
	}
	m.AlarmGauges = []core.AlarmGauge{{MemberID: 0x91bc3c398fb3c146, Alarm: pb.AlarmType_NOSPACE, Value: 1}, {MemberID: 0x8e9e05c52164694d, Alarm: pb.AlarmType_CORRUPT}}
	m.StoreGets, m.StoreSets = 2, 3
	// Leases of 1, 3 and 60 seconds: buckets le=1, le=4 and le=64.
	m.LeaseTTLs[0], m.LeaseTTLs[2], m.LeaseTTLs[6] = 1, 1, 1
	m.LeaseTTLSum = 64
	s.SetNode(m)
	want := `
# HELP etcd_cluster_version Which version is running. 1 for 'cluster_version' label with current cluster version
# TYPE etcd_cluster_version gauge
etcd_cluster_version{cluster_version="3.7"} 1
# HELP etcd_debugging_server_alarms Alarms for every member in cluster. 1 for 'server_id' label with current ID. 2 for 'alarm_type' label with type of this alarm
# TYPE etcd_debugging_server_alarms gauge
etcd_debugging_server_alarms{alarm_type="CORRUPT",server_id="8e9e05c52164694d"} 0
etcd_debugging_server_alarms{alarm_type="NOSPACE",server_id="91bc3c398fb3c146"} 1
# HELP etcd_debugging_store_reads_total Total number of reads action by (get/getRecursive), local to this member.
# TYPE etcd_debugging_store_reads_total counter
etcd_debugging_store_reads_total{action="getRecursive"} 2
# HELP etcd_debugging_store_writes_total Total number of writes (e.g. set/compareAndDelete) seen by this member.
# TYPE etcd_debugging_store_writes_total counter
etcd_debugging_store_writes_total{action="set"} 3
# HELP etcd_network_known_peers The current number of known peers.
# TYPE etcd_network_known_peers gauge
etcd_network_known_peers{Local="8e9e05c52164694d",Remote="8e9e05c52164694d"} 1
etcd_network_known_peers{Local="8e9e05c52164694d",Remote="91bc3c398fb3c146"} 1
# HELP etcd_server_feature_enabled Whether or not a feature is enabled. 1 is enabled, 0 is not.
# TYPE etcd_server_feature_enabled gauge
etcd_server_feature_enabled{name="LeaseCheckpoint",stage="ALPHA"} 1
# HELP etcd_server_id Server or member ID in hexadecimal format. 1 for 'server_id' label with current ID.
# TYPE etcd_server_id gauge
etcd_server_id{server_id="8e9e05c52164694d"} 1
# HELP etcd_server_is_leader Whether or not this member is a leader. 1 if is, 0 otherwise.
# TYPE etcd_server_is_leader gauge
etcd_server_is_leader 1
`
	names := []string{"etcd_cluster_version", "etcd_debugging_server_alarms", "etcd_debugging_store_reads_total", "etcd_debugging_store_writes_total", "etcd_network_known_peers", "etcd_server_feature_enabled", "etcd_server_id", "etcd_server_is_leader"}
	if err := testutil.GatherAndCompare(s.Registry, strings.NewReader(want), names...); err != nil {
		t.Fatal(err)
	}
	ttl := `
# HELP etcd_debugging_lease_ttl_total Bucketed histogram of lease TTLs.
# TYPE etcd_debugging_lease_ttl_total histogram
`
	var b strings.Builder
	b.WriteString(ttl)
	for i := range core.LeaseTTLBuckets {
		var n int
		switch {
		case i >= 6:
			n = 3
		case i >= 2:
			n = 2
		default:
			n = 1
		}
		fmt.Fprintf(&b, "etcd_debugging_lease_ttl_total_bucket{le=\"%d\"} %d\n", uint64(1)<<i, n)
	}
	b.WriteString("etcd_debugging_lease_ttl_total_bucket{le=\"+Inf\"} 3\netcd_debugging_lease_ttl_total_sum 64\netcd_debugging_lease_ttl_total_count 3\n")
	if err := testutil.GatherAndCompare(s.Registry, strings.NewReader(b.String()), "etcd_debugging_lease_ttl_total"); err != nil {
		t.Fatal(err)
	}
	// Without the core's metrics, they are left out.
	s.SetNode(nil)
	if n, err := testutil.GatherAndCount(s.Registry, "etcd_server_id"); err != nil || n != 0 {
		t.Fatalf("%d series of etcd_server_id without the core's metrics, %v", n, err)
	}
}

func TestMajorMinor(t *testing.T) {
	for in, want := range map[string]string{"3.7.0": "3.7", "3.6.1": "3.6", "3.7": "3.7", "": ""} {
		if got := majorMinor(in); got != want {
			t.Errorf("majorMinor(%q) = %q, want %q", in, got, want)
		}
	}
}
