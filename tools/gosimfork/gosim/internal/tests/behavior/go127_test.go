//go:build sim

package behavior

import (
	"context"
	"embed"
	"hash/maphash"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/pprof"
	"slices"
	"sync"
	"testing"
	"time"
	"weak"

	"github.com/jellevandenhooff/gosim"
)

// Tests for what the Go 1.27 port added or changed.

func TestTempDirAndSetenv(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GOSIM_TEST_VAR", "set")
	if got := os.Getenv("GOSIM_TEST_VAR"); got != "set" {
		t.Errorf("got %q, want set", got)
	}
}

func TestSyncMapRange(t *testing.T) {
	var m sync.Map
	keys := []any{"a", 1, new(int), "b"}
	for i, k := range keys {
		m.Store(k, i)
	}
	var seen []int
	m.Range(func(k, v any) bool {
		seen = append(seen, v.(int))
		return true
	})
	// Every key hashes to 0, so Range visits the newest key first.
	if !slices.Equal(seen, []int{3, 2, 1, 0}) {
		t.Errorf("got %v", seen)
	}
	if v, ok := m.Load(keys[2]); !ok || v != 2 {
		t.Errorf("got %v %v", v, ok)
	}
	if !m.CompareAndSwap("a", 0, 10) {
		t.Error("CompareAndSwap failed")
	}
}

func TestWeakPointer(t *testing.T) {
	p := new(int)
	w := weak.Make(p)
	runtime.GC()
	if w.Value() != p || weak.Make(p) != w {
		t.Error("weak pointer lost its value")
	}
	runtime.AddCleanup(p, func(int) { t.Error("cleanup ran") }, 0)
}

func TestPprofLabels(t *testing.T) {
	ctx := pprof.WithLabels(context.Background(), pprof.Labels("b", "2", "a", "1"))
	ctx = pprof.WithLabels(ctx, pprof.Labels("a", "3"))
	var got []string
	pprof.ForLabels(ctx, func(k, v string) bool {
		got = append(got, k+"="+v)
		return true
	})
	if !slices.Equal(got, []string{"a=3", "b=2"}) {
		t.Errorf("got %v", got)
	}
	pprof.Do(ctx, pprof.Labels("c", "4"), func(ctx context.Context) {
		if v, ok := pprof.Label(ctx, "c"); !ok || v != "4" {
			t.Errorf("got %q %v", v, ok)
		}
	})
}

func TestMapLiteralAddress(t *testing.T) {
	m := &map[string]int{"a": 1}
	(*m)["b"] = 2
	if len(*m) != 2 {
		t.Errorf("got %v", *m)
	}
}

type namedMap map[string]int

func TestMapsOnNamedMap(t *testing.T) {
	m := namedMap{"a": 1}
	c := make(namedMap)
	for k, v := range m {
		c[k] = v
	}
	if !reflect.DeepEqual(m, c) {
		t.Errorf("got %v", c)
	}
	if v, ok := reflect.TypeAssert[namedMap](reflect.ValueOf(m)); !ok || len(v) != 1 {
		t.Errorf("got %v %v", v, ok)
	}
}

//go:embed testdata/embed.txt
var embedded string

func TestEmbed(t *testing.T) {
	if embedded != "embedded by TestEmbed\n" {
		t.Errorf("got %q", embedded)
	}
}

func TestTestFuncAsValue(t *testing.T) {
	t.Run("embed", TestEmbed)
}

func TestWriteArtifact(t *testing.T) {
	gosim.WriteArtifact("artifact.txt", []byte("hello"))
}

func TestUnimplementedHook(t *testing.T) {
	if os.Getenv("TESTINGFAIL") != "1" {
		t.Skip()
	}
	defer func() {
		recover()
		t.Error("recovered from an unimplemented hook")
	}()
	reflect.MapOf(reflect.TypeFor[string](), reflect.TypeFor[int]())
}

func TestFatalUnlock(t *testing.T) {
	if os.Getenv("TESTINGFAIL") != "1" {
		t.Skip()
	}
	defer func() {
		recover()
		t.Error("recovered from a fatal error")
	}()
	var mu sync.Mutex
	mu.Unlock()
}

func TestOpenRoot(t *testing.T) {
	f, err := os.Open("/")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Error(err)
	}
	if err := f.Close(); err != nil {
		t.Error(err)
	}
	if info, err := os.Stat("/"); err != nil || !info.IsDir() {
		t.Errorf("got %v %v", info, err)
	}
}

func TestLoopbackListener(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	go func() {
		c, err := l.Accept()
		if err != nil {
			t.Error(err)
			return
		}
		c.Write([]byte("hi"))
		c.Close()
	}()
	c, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(c)
	if err != nil || string(b) != "hi" {
		t.Errorf("got %q %v", b, err)
	}
}

//go:embed testdata/embed.txt
var embeddedBytes []byte

type namedBytes []byte

//go:embed testdata/embed.txt
var embeddedNamedBytes namedBytes

//go:embed testdata/embed.txt
var embeddedFS embed.FS

// TestEmbedBytesPerRun changes embedded variables; the meta test checks that
// the next run sees their embedded values again.
func TestEmbedBytesPerRun(t *testing.T) {
	if string(embeddedBytes) != "embedded by TestEmbed\n" || string(embeddedNamedBytes) != "embedded by TestEmbed\n" || embedded != "embedded by TestEmbed\n" {
		t.Errorf("got %q, %q, %q", embeddedBytes, embeddedNamedBytes, embedded)
	}
	if _, err := embeddedFS.ReadFile("testdata/embed.txt"); err != nil {
		t.Error(err)
	}
	embeddedBytes[0] = 'X'
	embeddedNamedBytes[0] = 'X'
	embedded = "changed"
	embeddedFS = embed.FS{}
}

func TestContextInCleanup(t *testing.T) {
	t.Cleanup(func() {
		select {
		case <-t.Context().Done():
		default:
			t.Error("the context is not canceled during cleanup")
		}
	})
}

func TestRemovedTempDir(t *testing.T) {
	if err := os.Remove(t.TempDir()); err != nil {
		t.Fatal(err)
	}
}

func TestCloseUnacceptedConnection(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	c, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMaphashComparable(t *testing.T) {
	if os.Getenv("TESTINGFAIL") != "1" {
		t.Skip()
	}
	maphash.Comparable(maphash.MakeSeed(), "key")
}
