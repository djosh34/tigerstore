package server

import (
	"bytes"
	"io"
	"net"
	"reflect"
	"testing"
)

type pre47H2Event struct {
	kind                       string
	conn, stream, value, flags uint64
}

func pre47H2Collector(events *[]pre47H2Event) func(string, uint64, uint64, uint64, uint64) {
	return func(kind string, conn, stream, value, flags uint64) {
		*events = append(*events, pre47H2Event{kind, conn, stream, value, flags})
	}
}

func pre47H2Frame(kind, flags byte, stream uint32, payload []byte) []byte {
	n := len(payload)
	b := []byte{byte(n >> 16), byte(n >> 8), byte(n), kind, flags, byte(stream >> 24), byte(stream >> 16), byte(stream >> 8), byte(stream)}
	return append(b, payload...)
}

func TestPre47H2FragmentedFrames(t *testing.T) {
	settings := []byte{
		0, 3, 0, 0, 0, 16,
		0, 4, 0, 1, 0, 0,
		0, 5, 0, 0, 64, 0,
		0, 1, 0, 0, 16, 0, // Unrequested setting is discarded.
	}
	frames := pre47H2Frame(4, 0, 0, settings)
	frames = append(frames, pre47H2Frame(4, 1, 0, nil)...)
	frames = append(frames, pre47H2Frame(8, 0, 0, []byte{0x80, 0, 0, 32})...)
	frames = append(frames, pre47H2Frame(8, 0, 9, []byte{0, 0, 1, 0})...)
	frames = append(frames, pre47H2Frame(0, 1, 9, []byte("discard this message body"))...)
	frames = append(frames, pre47H2Frame(0, 1, 11, nil)...)
	frames = append(frames, pre47H2Frame(1, 4, 13, []byte("discard these headers"))...)
	frames = append(frames, pre47H2Frame(3, 0, 13, []byte{0, 0, 0, 8})...)
	frames = append(frames, pre47H2Frame(7, 0, 0, []byte{0, 0, 0, 13, 0, 0, 0, 0, 'x', 'y'})...)
	for _, inbound := range []bool{false, true} {
		for _, chunk := range []int{1, 2, 5, 9, 17, 4096} {
			var events []pre47H2Event
			p := pre47H2Parser{connID: 7, inbound: inbound, emit: pre47H2Collector(&events)}
			wire := frames
			prefix := "h2_out_"
			if inbound {
				prefix = "h2_in_"
				p.prefaceLeft = 24
				wire = append([]byte("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"), frames...)
			}
			for len(wire) > 0 {
				n := min(chunk, len(wire))
				p.feed(wire[:n])
				wire = wire[n:]
			}
			want := []pre47H2Event{
				{prefix + "settings_streams", 7, 0, 16, 0},
				{prefix + "settings_window", 7, 0, 65536, 0},
				{prefix + "settings_frame", 7, 0, 16384, 0},
				{prefix + "window_update", 7, 0, 32, 0},
				{prefix + "window_update", 7, 9, 256, 0},
				{prefix + "data", 7, 9, uint64(len("discard this message body")), 1},
				{prefix + "data", 7, 11, 0, 1},
				{prefix + "rst", 7, 13, 8, 0},
				{prefix + "goaway", 7, 13, 0, 0},
			}
			if !reflect.DeepEqual(events, want) {
				t.Fatalf("inbound=%t chunk=%d: got %#v, want %#v", inbound, chunk, events, want)
			}
			if p.headerN != 0 || p.remaining != 0 || p.prefaceLeft != 0 {
				t.Fatalf("inbound=%t chunk=%d: parser did not finish", inbound, chunk)
			}
		}
	}
}

func TestPre47H2DiscardsLargeData(t *testing.T) {
	var events []pre47H2Event
	p := pre47H2Parser{connID: 1, emit: pre47H2Collector(&events)}
	// The largest encodable frame must still use only the fixed buffers.
	p.feed([]byte{255, 255, 255, 0, 0, 0, 0, 0, 1})
	block := bytes.Repeat([]byte{0xaa}, 1024)
	for left := 0xffffff; left > 0; {
		n := min(left, len(block))
		p.feed(block[:n])
		left -= n
	}
	if p.field != [8]byte{} || p.fieldN != 0 {
		t.Fatal("DATA payload was retained")
	}
	if want := []pre47H2Event{{"h2_out_data", 1, 1, 0xffffff, 0}}; !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %#v, want %#v", events, want)
	}
}

type pre47H2ShortConn struct {
	net.Conn
	input  []byte
	output []byte
}

func (c *pre47H2ShortConn) Read(p []byte) (int, error) {
	if len(c.input) == 0 {
		return 0, io.EOF
	}
	n := copy(p, c.input[:min(2, len(c.input))])
	c.input = c.input[n:]
	if len(c.input) == 0 {
		return n, io.EOF
	}
	return n, nil
}

func (c *pre47H2ShortConn) Write(p []byte) (int, error) {
	n := min(3, len(p))
	c.output = append(c.output, p[:n]...)
	return n, nil
}

func TestPre47H2PartialIO(t *testing.T) {
	frame := pre47H2Frame(0, 1, 3, []byte("discard"))
	raw := &pre47H2ShortConn{input: append([]byte("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"), frame...)}
	var events []pre47H2Event
	c := &pre47H2Conn{
		Conn: raw,
		in:   pre47H2Parser{connID: 2, inbound: true, prefaceLeft: 24, emit: pre47H2Collector(&events)},
		out:  pre47H2Parser{connID: 2, emit: pre47H2Collector(&events)},
	}
	buf := make([]byte, 16)
	for {
		_, err := c.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	for left := frame; len(left) > 0; {
		n, err := c.Write(left)
		if err != nil {
			t.Fatal(err)
		}
		left = left[n:]
	}
	if !bytes.Equal(raw.output, frame) {
		t.Fatal("wrapper changed output bytes")
	}
	want := []pre47H2Event{{"h2_in_data", 2, 3, 7, 1}, {"h2_out_data", 2, 3, 7, 1}}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %#v, want %#v", events, want)
	}
}
