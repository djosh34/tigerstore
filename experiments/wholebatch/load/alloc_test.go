package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestSampleAllocations(t *testing.T) {
	for _, body := range []string{
		`{"mallocs":123,"bytes":456}`,
		"# HELP ignored\ngo_memstats_mallocs_total 1.23e2\ngo_memstats_alloc_bytes_total 456\n",
	} {
		t.Run(body, func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if _, err := fmt.Fprint(w, body); err != nil {
					t.Error(err)
				}
			}))
			defer s.Close()
			got, err := sampleAllocations(s.URL + "," + s.URL)
			want := []allocations{{123, 456}, {123, 456}}
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("got %v, %v; want %v", got, err, want)
			}
		})
	}
}

func TestSampleAllocationsRejectsMissingCounters(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := fmt.Fprint(w, "go_memstats_mallocs_total 123\n"); err != nil {
			t.Error(err)
		}
	}))
	defer s.Close()
	if _, err := sampleAllocations(s.URL); err == nil {
		t.Fatal("accepted missing byte counter")
	}
}
