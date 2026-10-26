// Copyright 2012 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found at https://go.googlesource.com/go/+/refs/heads/master/LICENSE.

package race_test

// from go/src/runtime/race/testdata/io_test.go

// XXX: broken
/*
func TestNoRaceIOFile(t *testing.T) {
	if !gosimruntime.IsDetgo() {
		setupRealDisk(s)
	}
	x := 0
	path := "." // t.TempDir()
	fname := filepath.Join(path, "data")
	go func() {
		x = 42
		f, _ := os.OpenFile(fname, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
		f.Write([]byte("done"))
		f.Close()
	}()
	for {
		f, err := os.OpenFile(fname, os.O_RDONLY, 0)
		if err != nil {
			time.Sleep(1e6)
			continue
		}
		buf := make([]byte, 100)
		count, err := f.Read(buf)
		if count == 0 {
			time.Sleep(1e6)
			continue
		}
		break
	}
	_ = x
}
*/

// XXX: not implemented
/*
var (
	regHandler  sync.Once
	handlerData int
)

func TestNoRaceIOHttp(t *testing.T) {
	regHandler.Do(func() {
		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			handlerData++
			fmt.Fprintf(w, "test")
			handlerData++
		})
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	defer ln.Close()
	go http.Serve(ln, nil)
	handlerData++
	_, err = http.Get("http://" + ln.Addr().String())
	if err != nil {
		t.Fatalf("http.Get: %v", err)
	}
	handlerData++
	_, err = http.Get("http://" + ln.Addr().String())
	if err != nil {
		t.Fatalf("http.Get: %v", err)
	}
	handlerData++
}
*/
