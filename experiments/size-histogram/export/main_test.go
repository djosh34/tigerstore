package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestResources(t *testing.T) {
	for key, want := range map[string]string{
		"/registry/pods/ns/name":                            "pods",
		"/registry/services/specs/ns/name":                  "services",
		"/registry/services/endpoints/ns/name":              "endpoints",
		"/registry/deployments/ns/name":                     "deployments",
		"/registry/discovery.k8s.io/endpointslices/ns/name": "endpointslices",
		"/registry/apiregistration.k8s.io/apiservices/name": "apiservices",
		"/registry/rbac.authorization.k8s.io/roles/ns/name": "roles",
		"/registry/credential/name":                         "other",
		"/credential":                                       "other",
	} {
		if got := resource([]byte(key)); got != want {
			t.Fatalf("resource classification mismatch: got %s, want %s", got, want)
		}
	}
}

func TestOnlyLengths(t *testing.T) {
	input := map[string]any{"count": 3, "kvs": []map[string][]byte{
		{"key": []byte("/registry/secrets/private-name/hidden"), "value": []byte("cluster-credential")},
		{"key": []byte("/registry/credential/hidden"), "value": []byte{0, 255, 0}},
		{"key": []byte("/registry/pods/private-name/hidden"), "value": {}},
	}}
	data, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := export(bytes.NewReader(data), &out); err != nil {
		t.Fatal(err)
	}
	if want := "resource,value_bytes\nsecrets,18\nother,3\npods,0\n"; out.String() != want {
		t.Fatal("export did not match the length-only schema")
	}
}

func TestRejectWithoutOutput(t *testing.T) {
	for _, data := range []string{
		`cluster-credential`,
		`{"count":1,"kvs":[{"key":"credential","value":"invalid!"}]}`,
		`{"count":0,"kvs":[]}`,
		`{"count":2,"kvs":[{"key":"aw==","value":""}]}`,
		`{"count":1,"more":true,"kvs":[{"key":"aw==","value":""}]}`,
		`{"count":1,"kvs":[{"key":"","value":""}]}`,
		`{"count":2,"kvs":[{"key":"aw=="},{"key":"aw=="}]}`,
		`{"count":1,"kvs":[{"key":"aw=="}]} {}`,
	} {
		var out bytes.Buffer
		err := export(strings.NewReader(data), &out)
		if err == nil || out.Len() != 0 || strings.Contains(err.Error(), "credential") {
			t.Fatal("invalid input was not rejected safely")
		}
	}
}
