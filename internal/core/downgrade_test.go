package core

import (
	"testing"

	"go.etcd.io/etcd/api/v3/authpb"
)

// TestHashCoversReplicatedState changes one part of the replicated state
// at a time that the index does not hold; each change must change Hash.
func TestHashCoversReplicatedState(t *testing.T) {
	base := NewState(1, nil).Hash()
	for name, change := range map[string]func(*State){
		"cluster version":   func(st *State) { st.etcdVersion = "3.6.0" },
		"downgrade enabled": func(st *State) { st.downgradeEnabled = true },
		"downgrade target":  func(st *State) { st.downgradeTarget = "3.6.0" },
		"empty role":        func(st *State) { st.auth.roles["r"] = &authpb.Role{Name: []byte("r")} },
	} {
		st := NewState(1, nil)
		change(st)
		if st.Hash() == base {
			t.Errorf("%s does not change the hash", name)
		}
	}
}
