package main

import "testing"

func TestParseDiskstats(t *testing.T) {
	stats := []byte(`   8       0 sda 100 0 800 10 2000 30 40000 50 0 60 70 0 0 0 0 9 1
   8       1 sda1 90 0 700 9 1900 29 39000 49 0 59 69 0 0 0 0 0 0
`)
	n, err := parseDiskstats(stats, "sda1")
	if err != nil || n != 39000*512 {
		t.Fatalf("got %d, %v", n, err)
	}
	if _, err := parseDiskstats(stats, "sdb"); err == nil {
		t.Fatal("no error for a missing device")
	}
}
