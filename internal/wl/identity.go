package wl

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
)

// Identity is a node's Raft ID, its cluster's ID and the members it
// started with, which it keeps in <data-dir>/member from its first start
// on, so that a restart depends neither on its cluster flags nor on
// discovery. First is whether the node bootstrapped the cluster, as one
// of its first members. The file goes through the seam (sys.go), so the
// simulator crashes nodes while they write it.
type Identity struct {
	ID, Cluster uint64
	Members     []Member
	First       bool
}

// identityJSON is the identity file's format.
type identityJSON struct {
	ID      uint64       `json:"id"`
	Cluster uint64       `json:"cluster"`
	Members []memberJSON `json:"members"`
	First   bool         `json:"first,omitempty"`
}

type memberJSON struct {
	ID       uint64   `json:"id"`
	Name     string   `json:"name"`
	PeerURLs []string `json:"peerURLs"`
}

// identityFile is where a data dir keeps the node's identity.
func identityFile(dataDir string) string { return filepath.Join(dataDir, "member") }

// ReadIdentity returns the identity dataDir keeps, and whether it keeps
// one. sys is the simulator's, nil in the real binary (Config.Sys).
func ReadIdentity(sys Sys, dataDir string) (Identity, bool, error) {
	env, err := newSysEnv(&Config{Sys: sys})
	if err != nil {
		return Identity{}, false, err
	}
	b, err := env.readFile(identityFile(dataDir))
	if errors.Is(err, fs.ErrNotExist) {
		return Identity{}, false, nil
	}
	if err != nil {
		return Identity{}, false, err
	}
	var j identityJSON
	if err := json.Unmarshal(b, &j); err != nil {
		return Identity{}, false, fmt.Errorf("%s: %w", identityFile(dataDir), err)
	}
	id := Identity{ID: j.ID, Cluster: j.Cluster, First: j.First}
	for _, m := range j.Members {
		id.Members = append(id.Members, Member{ID: m.ID, Name: m.Name, PeerURLs: m.PeerURLs})
	}
	return id, true, nil
}

// SaveIdentity writes dataDir's identity file durably: a temporary file,
// synced, renamed over it, and the directory synced.
func SaveIdentity(sys Sys, dataDir string, id Identity) error {
	env, err := newSysEnv(&Config{Sys: sys})
	if err != nil {
		return err
	}
	j := identityJSON{ID: id.ID, Cluster: id.Cluster, First: id.First}
	for _, m := range id.Members {
		j.Members = append(j.Members, memberJSON{ID: m.ID, Name: m.Name, PeerURLs: m.PeerURLs})
	}
	b, err := json.Marshal(j)
	if err != nil {
		return err
	}
	if err := env.mkdirAll(dataDir); err != nil {
		return err
	}
	tmp := identityFile(dataDir) + ".tmp"
	if err := writeFileSynced(env, tmp, append(b, '\n')); err != nil {
		return err
	}
	if err := env.rename(tmp, identityFile(dataDir)); err != nil {
		return err
	}
	return syncDir(env, dataDir)
}
