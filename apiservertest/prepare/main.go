// Command prepare changes a copy of the k8s.io/apiserver module so that
// its etcd3 storage tests run against an external server instead of an
// embedded etcd. It replaces pkg/storage/etcd3/testserver/test_server.go
// with test_server.go.tmpl, keeping the version's own client recorders,
// and points the one *embed.Config in store_test.go at the new Config.
// No test or assertion changes.
//
//	go run ./apiservertest/prepare <module dir>
package main

import (
	_ "embed"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

//go:embed test_server.go.tmpl
var template string

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: prepare <k8s.io/apiserver module dir>")
		os.Exit(2)
	}
	if err := prepare(os.Args[1]); err != nil {
		fmt.Fprintf(os.Stderr, "prepare: %v\n", err)
		os.Exit(1)
	}
}

func prepare(module string) error {
	server := filepath.Join(module, "pkg/storage/etcd3/testserver/test_server.go")
	b, err := os.ReadFile(server)
	if err != nil {
		return err
	}
	recorders, err := clientRecorders(string(b))
	if err != nil {
		return err
	}
	if err := os.WriteFile(server, []byte(strings.Replace(template, "\t@@RECORDERS@@", recorders, 1)), 0o644); err != nil {
		return err
	}
	store := filepath.Join(module, "pkg/storage/etcd3/store_test.go")
	b, err = os.ReadFile(store)
	if err != nil {
		return err
	}
	s := string(b)
	const embedImport = "\t\"go.etcd.io/etcd/server/v3/embed\"\n"
	if strings.Count(s, embedImport) != 1 || strings.Count(s, "*embed.Config") != 1 {
		return errors.New("store_test.go: expected one embed import and one *embed.Config")
	}
	s = strings.Replace(s, embedImport, "", 1)
	s = strings.Replace(s, "*embed.Config", "*testserver.Config", 1)
	return os.WriteFile(store, []byte(s), 0o644)
}

// clientRecorders returns the end of the upstream RunEtcd that wraps the
// client's KV, Watcher and Kubernetes interfaces in the tests' recorders,
// up to its return. Versions differ: 1.37 starts with kubernetesRecorder,
// earlier ones with client.KV.
func clientRecorders(src string) (string, error) {
	start := strings.Index(src, "\tkubernetesRecorder :=")
	if start < 0 {
		start = strings.Index(src, "\tclient.KV =")
	}
	if start < 0 {
		return "", errors.New("test_server.go: no client recorders found")
	}
	end := strings.Index(src[start:], "\n\treturn client")
	if end < 0 {
		return "", errors.New("test_server.go: no return after the recorders")
	}
	return src[start : start+end], nil
}
