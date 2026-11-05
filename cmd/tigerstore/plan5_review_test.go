package main

import "testing"

// The rolling-upgrade harness changes peer URLs per node. Old binaries
// persist the resulting different cluster IDs before any new binary runs.
func TestPlan5ReviewPersistedClusterIDs(t *testing.T) {
	keys := []string{
		"a=https://a:2380,b=https://proxy-ab:2380,c=https://proxy-ac:2380",
		"a=https://proxy-ba:2380,b=https://b:2380,c=https://proxy-bc:2380",
		"a=https://proxy-ca:2380,b=https://proxy-cb:2380,c=https://c:2380",
	}
	ids := map[uint64]bool{}
	for i, key := range keys {
		ms, err := parseCluster(key)
		if err != nil {
			t.Fatal(err)
		}
		id := clusterIdentity(clusterSpec{members: ms, self: i, token: "etcd-cluster", key: key})
		dir := t.TempDir()
		if err := saveIdentity(dir, id); err != nil {
			t.Fatal(err)
		}
		got, started, err := readIdentity(dir)
		if err != nil || !started {
			t.Fatalf("read identity: started=%v, err=%v", started, err)
		}
		ids[got.cluster] = true
		t.Logf("node %d keeps cluster ID %x on restart", i+1, got.cluster)
	}
	if len(ids) != 3 {
		t.Fatalf("expected three different persisted cluster IDs, got %d", len(ids))
	}
}
