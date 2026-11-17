package core

import (
	"bufio"
	"bytes"
	"fmt"
	"testing"

	"go.etcd.io/etcd/api/v3/authpb"
	"google.golang.org/protobuf/proto"
)

// TestCovers checks the merged permission spans against etcd's interval
// tree: a key, a range, a prefix and the open range from a key.
func TestCovers(t *testing.T) {
	perms := []*authpb.Permission{
		{PermType: authpb.Permission_READ, Key: []byte("c")},
		{PermType: authpb.Permission_READWRITE, Key: []byte("a"), RangeEnd: []byte("c")},
		{PermType: authpb.Permission_WRITE, Key: []byte("x"), RangeEnd: []byte{0}},
		{PermType: authpb.Permission_READ, Key: []byte("m"), RangeEnd: []byte("o")},
		{PermType: authpb.Permission_READ, Key: []byte("n"), RangeEnd: []byte("p")},
	}
	a := newAuthState()
	a.roles["r"] = &authpb.Role{Name: []byte("r"), KeyPermission: perms}
	a.users["u"] = &authpb.User{Name: []byte("u"), Roles: []string{"r"}}
	a.rebuild()
	p := a.perms["u"]
	for _, c := range []struct {
		key, end string
		write    bool
		want     bool
	}{
		{"a", "", false, true},
		{"b", "", true, true},
		{"c", "", false, true},
		{"c", "", true, false},
		// [a, c) and the key c touch: [a, c\x00) is readable as one.
		{"a", "c\x00", false, true},
		{"a", "d", false, false},
		{"a", "c", true, true},
		{"a", "c\x00", true, false},
		// [m, o) and [n, p) overlap.
		{"m", "p", false, true},
		{"m", "p\x00", false, false},
		{"l", "n", false, false},
		{"x", "\x00", true, true},
		{"zzz", "", true, true},
		{"w", "\x00", true, false},
		{"x", "\x00", false, false},
	} {
		spans := p.read
		if c.write {
			spans = p.write
		}
		var end []byte
		if c.end != "" {
			end = []byte(c.end)
		}
		if got := covers(spans, []byte(c.key), end); got != c.want {
			t.Errorf("covers(%q, %q, write %v) = %v", c.key, c.end, c.write, got)
		}
	}
}

// TestBigRoleInCheckpoint reads back a checkpoint header with a role far
// above the bound of a name or URL: a role grows with every permission
// granted, and its checkpoint must stay readable.
func TestBigRoleInCheckpoint(t *testing.T) {
	a := newAuthState()
	ro := &authpb.Role{Name: []byte("big")}
	for i := range 5000 {
		ro.KeyPermission = append(ro.KeyPermission, &authpb.Permission{PermType: authpb.Permission_READ, Key: fmt.Appendf(nil, "/registry/key-%05d", i)})
	}
	a.roles["big"] = ro
	b, err := (&ckptHeader{auth: a}).encode()
	if err != nil {
		t.Fatal(err)
	}
	h, err := readHeader(bufio.NewReader(bytes.NewReader(b)))
	if err != nil {
		t.Fatal(err)
	}
	if got := h.auth.roles["big"]; !proto.Equal(got, ro) {
		t.Fatalf("role after the round trip has %d permissions, want %d", len(got.GetKeyPermission()), len(ro.KeyPermission))
	}
	// A damaged length past the bytes that follow fails without a huge
	// buffer.
	if _, err := readHeader(bufio.NewReader(bytes.NewReader(b[:len(b)/2]))); err == nil {
		t.Fatal("a cut header read without an error")
	}
}
