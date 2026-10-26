//go:build sim && skip

package race_test

import (
	"fmt"
	"testing"

	"github.com/jellevandenhooff/gosim/internal/simulation/bridge"
)

// TODO: needs to be updated for syscallabi

func TestNoRaceOS1DetgoOnly(t *testing.T) {
	var backend struct{}
	os := bridge.NewBridge()
	go os.Run()

	op := func(_ struct{}, req string) string {
		return req
	}

	done := make(chan struct{}, 2)

	go func() {
		bridge.Invoke(os, backend, op, "a")

		done <- struct{}{}
	}()

	go func() {
		bridge.Invoke(os, backend, op, "b")

		done <- struct{}{}
	}()

	<-done
	<-done

	// os.Stop()
}

// XXX: add test that uses os as a data store (and thus sync mechanism) and show
// that it triggers the race detector still

func TestNoRaceOS2DetgoOnly(t *testing.T) {
	var backend struct{}
	os := bridge.NewBridge()
	go os.Run()

	op := func(_ struct{}, req string) string {
		return req
	}

	resp := bridge.Invoke(os, backend, op, "hello")
	if resp != "hello" {
		t.Error(resp)
	}

	// os.Stop()
}

/*
func TestNoRaceOS3DetgoOnly(t *testing.T) {
	var backend struct{}
	os := bridge.NewOS()
	go os.Work()

	op := func(_ struct{}, req string) (string, error) {
		return "", io.EOF
	}

	resp, err := bridge.Invoke(os, backend, op, "hello")
	if err != io.EOF {
		t.Error(err)
	}
	if resp != "" {
		t.Error(resp)
	}

	os.Stop()
}
*/

func TestNoRaceOS4DetgoOnly(t *testing.T) {
	var backend struct{}
	os := bridge.NewBridge()
	go os.Run()

	type Req struct {
		In, Out bridge.ByteSliceView
	}

	op := func(_ struct{}, req Req) struct{} {
		buffer := get(req.In)
		for i := range buffer {
			buffer[i] = buffer[i] + 1
		}
		req.Out.Write(buffer)
		return struct{}{}
	}

	in := make([]byte, 4)
	for i := range in {
		in[i] = byte(i)
	}
	out := make([]byte, 4)

	bridge.Invoke(os, backend, op, Req{In: bridge.ByteSliceView{Ptr: in}, Out: bridge.ByteSliceView{Ptr: out}})
	if len(out) != 4 {
		t.Error(len(out))
	}
	for i := range out {
		if out[i] != in[i]+1 {
			t.Error(i, out[i])
		}
	}

	// os.Stop()
}

func TestNoRaceOS5DetgoOnly(t *testing.T) {
	var backend struct{}
	os := bridge.NewBridge()
	go os.Run()

	type Req struct {
		A int
		B int
	}

	type Resp struct {
		A string
		B string
	}

	op := func(_ struct{}, req Req) Resp {
		return Resp{
			A: fmt.Sprint(req.A),
			B: fmt.Sprint(req.B),
		}
	}

	resp := bridge.Invoke(os, backend, op, Req{A: 1, B: 2})
	if resp.A != "1" || resp.B != "2" {
		t.Error(resp)
	}

	// os.Stop()
}

func TestRaceOS2DetgoOnly(t *testing.T) {
	var backend struct{}
	os := bridge.NewBridge()
	go os.Run()

	type Req struct {
		In, Out bridge.ByteSliceView
	}

	op := func(_ struct{}, req Req) struct{} {
		buffer := make([]byte, req.In.Len())
		copy(buffer, req.In.Ptr) // instead of req.In.Read
		for i := range buffer {
			buffer[i] = buffer[i] + 1
		}
		req.Out.Write(buffer)
		return struct{}{}
	}

	in := make([]byte, 4)
	for i := range in {
		in[i] = byte(i)
	}
	out := make([]byte, 4)

	bridge.Invoke(os, backend, op, Req{In: bridge.ByteSliceView{Ptr: in}, Out: bridge.ByteSliceView{Ptr: out}})
	if len(out) != 4 {
		t.Error(len(out))
	}
	for i := range out {
		if out[i] != in[i]+1 {
			t.Error(i, out[i])
		}
	}

	// os.Stop()
}

func get(b bridge.ByteSliceView) []byte {
	copy := make([]byte, b.Len())
	b.Read(copy)
	return copy
}

func TestRaceOS3DetgoOnly(t *testing.T) {
	var backend struct{}
	os := bridge.NewBridge()
	go os.Run()

	type Req struct {
		In, Out bridge.ByteSliceView
	}

	op := func(_ struct{}, req Req) struct{} {
		buffer := get(req.In)
		for i := range buffer {
			buffer[i] = buffer[i] + 1
		}
		copy(req.Out.Ptr, buffer) // instead of req.Out.Write
		return struct{}{}
	}

	in := make([]byte, 4)
	for i := range in {
		in[i] = byte(i)
	}
	out := make([]byte, 4)

	bridge.Invoke(os, backend, op, Req{In: bridge.ByteSliceView{Ptr: in}, Out: bridge.ByteSliceView{Ptr: out}})
	if len(out) != 4 {
		t.Error(len(out))
	}
	for i := range out {
		if out[i] != in[i]+1 {
			t.Error(i, out[i])
		}
	}

	// os.Stop()
}

func TestRaceOS1DetgoOnly(t *testing.T) {
	var backend struct{}
	os := bridge.NewBridge()
	go os.Run()

	var x int
	_ = x

	op := func(_ struct{}, req string) string {
		x = 2
		return req
	}

	x = 1

	resp := bridge.Invoke(os, backend, op, "hello")
	if resp != "hello" {
		t.Error(resp)
	}

	// os.Stop()
}

/*
func TestRaceOS4DetgoOnly(t *testing.T) {
	var backend struct{}
	os := bridge.NewOS()
	go os.Work()

	op := func(_ struct{}, req string) (string, error) {
		return "", errors.New(req)
	}

	resp, err := bridge.InvokeNoerr(os, backend, op, "hello")
	if err == nil || err.Error() != "hello" {
		t.Error(err)
	}
	if resp != "" {
		t.Error(resp)
	}

	os.Stop()
}
*/
