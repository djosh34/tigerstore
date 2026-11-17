package main

import (
	"cmp"
	"encoding/json"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	"go.etcd.io/etcd/api/v3/authpb"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"golang.org/x/crypto/bcrypt"

	"github.com/djosh34/tigerstore/internal/node"
	"github.com/djosh34/tigerstore/internal/wl"
)

// upgradeFixture is what the previous release answered before its
// snapshot was saved (testdata/recordmain).
type upgradeFixture struct {
	Rev, Compacted int64
	Reads          map[int64][]fixtureKV
	Leases         map[int64]int64
	AuthRevision   uint64
	Users          map[string][]string
	Roles          map[string][]fixturePerm
}

type fixtureKV struct {
	Key, Value                    string
	Create, Mod, Version, LeaseID int64
}

type fixturePerm struct {
	Type     int32
	Key, End string
}

// The offline upgrade takes a snapshot that v0.1.0 saved after puts,
// overwrites, deletes, Txns, leases, a compaction and auth
// (testdata/v010.snapshot, recorded by testdata/recordmain). Main's own
// restore makes a data dir of it, core replays its log, and the store it
// hands internal/wl must answer as v0.1.0 did: every key at every revision
// from the compacted one to the head, the leases with their TTLs, the
// users, roles and auth revision, and the users' passwords. wl must then
// take it as a new node's data dir.
func TestTheUpgradeKeepsWhatThePreviousReleaseHeld(t *testing.T) {
	var want upgradeFixture
	b, err := os.ReadFile(filepath.Join("testdata", "v010.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &want); err != nil {
		t.Fatal(err)
	}
	snapshot := filepath.Join("testdata", "v010.snapshot")
	members := []*pb.Member{{ID: 1, Name: "n1", PeerURLs: []string{"https://127.0.0.1:2380"}}}
	dir := t.TempDir()
	info, err := node.Restore(snapshot, filepath.Join(dir, "main"), members, false)
	if err != nil {
		t.Fatal(err)
	}
	if info.Revision != want.Rev {
		t.Fatalf("the snapshot is at revision %d, want %d", info.Revision, want.Rev)
	}
	s, err := readMain(filepath.Join(dir, "main", "wal"), members[0].ID, info)
	if err != nil {
		t.Fatal(err)
	}
	if s.Rev != want.Rev || s.CompactRev != want.Compacted {
		t.Fatalf("the store is at revision %d, compacted at %d; want %d and %d", s.Rev, s.CompactRev, want.Rev, want.Compacted)
	}
	var keys []wl.SnapKey
	for k, err := range s.Keys {
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, k)
	}
	for rev := want.Compacted; rev <= want.Rev; rev++ {
		if got := readAt(keys, rev); !reflect.DeepEqual(got, want.Reads[rev]) {
			t.Errorf("a read at revision %d:\n got  %v\n want %v", rev, got, want.Reads[rev])
		}
	}
	if !reflect.DeepEqual(s.Leases, want.Leases) {
		t.Errorf("the leases are %v, want %v", s.Leases, want.Leases)
	}
	checkAuth(t, s.Auth, want)

	// The whole upgrade, as `tigerstore snapshot restore` runs it.
	got, compacted, err := restore(snapshot, filepath.Join(dir, "n1.etcd"), members, false)
	if err != nil {
		t.Fatal(err)
	}
	if got.Revision != want.Rev || compacted != want.Compacted {
		t.Fatalf("the restore is at revision %d, compacted at %d; want %d and %d", got.Revision, compacted, want.Rev, want.Compacted)
	}
}

// readAt reads every key of keys at revision rev, as a range from the
// first key does: each key's newest state at or before rev, unless a
// deletion.
func readAt(keys []wl.SnapKey, rev int64) []fixtureKV {
	out := []fixtureKV{}
	for _, k := range keys {
		var at *wl.SnapState
		for i := range k.States {
			if s := &k.States[i]; s.Mod <= rev && (at == nil || s.Mod > at.Mod) {
				at = s
			}
		}
		if at == nil || at.Deleted {
			continue
		}
		out = append(out, fixtureKV{string(k.Key), string(at.Value), at.Create, at.Mod, at.Version, at.Lease})
	}
	slices.SortFunc(out, func(a, b fixtureKV) int { return cmp.Compare(a.Key, b.Key) })
	return out
}

func checkAuth(t *testing.T, a wl.SnapAuth, want upgradeFixture) {
	t.Helper()
	if !a.Enabled || a.Revision != want.AuthRevision {
		t.Errorf("auth on %v at revision %d, want on at %d", a.Enabled, a.Revision, want.AuthRevision)
	}
	users := map[string][]string{}
	for _, u := range a.Users {
		users[string(u.Name)] = u.Roles
	}
	if !reflect.DeepEqual(users, want.Users) {
		t.Errorf("the users are %v, want %v", users, want.Users)
	}
	for _, u := range a.Users {
		if string(u.Name) == "alice" && bcrypt.CompareHashAndPassword(u.Password, []byte("alicepw")) != nil {
			t.Error("alice's password no longer checks")
		}
	}
	roles := map[string][]fixturePerm{}
	for _, r := range a.Roles {
		ps := []fixturePerm{}
		for _, p := range r.KeyPermission {
			ps = append(ps, fixturePerm{int32(p.PermType), string(p.Key), string(p.RangeEnd)})
		}
		roles[string(r.Name)] = ps
	}
	// etcd's RoleGet of root answers with read and write over every key,
	// which no role stores (authStore.RoleGet).
	wantRoles := maps.Clone(want.Roles)
	wantRoles["root"] = slices.DeleteFunc(slices.Clone(wantRoles["root"]), func(p fixturePerm) bool {
		return p == fixturePerm{Type: int32(authpb.READWRITE), Key: "", End: "\x00"}
	})
	if !reflect.DeepEqual(roles, wantRoles) {
		t.Errorf("the roles are %v, want %v", roles, wantRoles)
	}
}
