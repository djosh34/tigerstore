package server

import (
	"encoding/binary"

	"golang.org/x/net/http2"
)

// frame is one HTTP/2 frame the connection read. Its payload is a slice
// of the connection's input buffer, valid until the next frame is read:
// what a handler keeps of it, it copies.
type frame struct {
	typ   http2.FrameType
	flags http2.Flags
	id    uint32
	// length is the payload's length on the wire, padding included, which
	// flow control counts.
	length int
	// data is the payload without its padding and, for HEADERS, without
	// its priority fields: the data of DATA, the header block fragment of
	// HEADERS and CONTINUATION, the settings of SETTINGS, and the fixed
	// fields of the other types.
	data []byte
}

func (f *frame) has(flag http2.Flags) bool { return f.flags&flag != 0 }

// parseFrame reads the frame that b holds whole, its header and payload,
// and checks it as RFC 9113 asks, as x/net's Framer does. contID is the
// stream whose header block waits for a CONTINUATION frame, or 0. A
// broken rule is an http2.ConnectionError, or an http2.StreamError for
// one that ends only its stream.
func parseFrame(b []byte, contID uint32) (frame, error) {
	f := frame{
		typ:    http2.FrameType(b[3]),
		flags:  http2.Flags(b[4]),
		id:     binary.BigEndian.Uint32(b[5:9]) & (1<<31 - 1),
		length: len(b) - frameHeaderLen,
		data:   b[frameHeaderLen:],
	}
	// A header block is one HEADERS frame and the CONTINUATION frames of
	// its stream right after it, with nothing in between (RFC 9113 6.10).
	if contID != 0 && (f.typ != http2.FrameContinuation || f.id != contID) ||
		contID == 0 && f.typ == http2.FrameContinuation {
		return f, http2.ConnectionError(http2.ErrCodeProtocol)
	}
	switch f.typ {
	case http2.FrameData:
		if f.id == 0 {
			return f, http2.ConnectionError(http2.ErrCodeProtocol)
		}
		if f.has(http2.FlagDataPadded) {
			return f, f.unpad()
		}
	case http2.FrameHeaders:
		if f.id == 0 {
			return f, http2.ConnectionError(http2.ErrCodeProtocol)
		}
		var pad int
		if f.has(http2.FlagHeadersPadded) {
			if len(f.data) < 1 {
				return f, http2.ConnectionError(http2.ErrCodeFrameSize)
			}
			pad, f.data = int(f.data[0]), f.data[1:]
		}
		if f.has(http2.FlagHeadersPriority) {
			// The stream dependency and weight, which the server ignores.
			if len(f.data) < 5 {
				return f, http2.ConnectionError(http2.ErrCodeFrameSize)
			}
			f.data = f.data[5:]
		}
		if pad > len(f.data) {
			return f, http2.ConnectionError(http2.ErrCodeProtocol)
		}
		f.data = f.data[:len(f.data)-pad]
	case http2.FrameContinuation:
		// Its stream is the HEADERS frame's, checked above, never 0.
	case http2.FramePriority:
		if f.id == 0 {
			return f, http2.ConnectionError(http2.ErrCodeProtocol)
		}
		if f.length != 5 {
			return f, http2.ConnectionError(http2.ErrCodeFrameSize)
		}
	case http2.FrameRSTStream:
		if f.length != 4 {
			return f, http2.ConnectionError(http2.ErrCodeFrameSize)
		}
		if f.id == 0 {
			return f, http2.ConnectionError(http2.ErrCodeProtocol)
		}
	case http2.FrameSettings:
		if f.has(http2.FlagSettingsAck) && f.length > 0 {
			return f, http2.ConnectionError(http2.ErrCodeFrameSize)
		}
		if f.id != 0 {
			return f, http2.ConnectionError(http2.ErrCodeProtocol)
		}
		if f.length%6 != 0 {
			return f, http2.ConnectionError(http2.ErrCodeFrameSize)
		}
	case http2.FramePing:
		if f.length != 8 {
			return f, http2.ConnectionError(http2.ErrCodeFrameSize)
		}
		if f.id != 0 {
			return f, http2.ConnectionError(http2.ErrCodeProtocol)
		}
	case http2.FrameGoAway:
		if f.id != 0 {
			return f, http2.ConnectionError(http2.ErrCodeProtocol)
		}
		if f.length < 8 {
			return f, http2.ConnectionError(http2.ErrCodeFrameSize)
		}
	case http2.FrameWindowUpdate:
		if f.length != 4 {
			return f, http2.ConnectionError(http2.ErrCodeFrameSize)
		}
		if f.increment() == 0 {
			if f.id == 0 {
				return f, http2.ConnectionError(http2.ErrCodeProtocol)
			}
			return f, http2.StreamError{StreamID: f.id, Code: http2.ErrCodeProtocol}
		}
	case http2.FramePushPromise:
		// Only a server pushes (RFC 9113 8.4).
		return f, http2.ConnectionError(http2.ErrCodeProtocol)
	case http2.FramePriorityUpdate:
		// RFC 9218's priority signal, which the server ignores once it
		// passed x/net's checks.
		if f.id != 0 {
			return f, http2.ConnectionError(http2.ErrCodeProtocol)
		}
		if f.length < 4 {
			return f, http2.ConnectionError(http2.ErrCodeFrameSize)
		}
		if binary.BigEndian.Uint32(f.data)&(1<<31-1) == 0 {
			return f, http2.ConnectionError(http2.ErrCodeProtocol)
		}
	}
	// Frames of an unknown type are ignored (RFC 9113 5.5).
	return f, nil
}

// unpad drops the pad length and padding of a padded DATA frame.
func (f *frame) unpad() error {
	if len(f.data) < 1 {
		return http2.ConnectionError(http2.ErrCodeFrameSize)
	}
	pad := int(f.data[0])
	f.data = f.data[1:]
	if pad > len(f.data) {
		return http2.ConnectionError(http2.ErrCodeProtocol)
	}
	f.data = f.data[:len(f.data)-pad]
	return nil
}

// increment is a WINDOW_UPDATE frame's window size increment.
func (f *frame) increment() uint32 {
	return binary.BigEndian.Uint32(f.data) & (1<<31 - 1)
}

// setting is the i-th setting of a SETTINGS frame.
func (f *frame) setting(i int) http2.Setting {
	p := f.data[i*6:]
	return http2.Setting{ID: http2.SettingID(binary.BigEndian.Uint16(p)), Val: binary.BigEndian.Uint32(p[2:])}
}

// ping is a PING frame's opaque data.
func (f *frame) ping() [8]byte {
	return [8]byte(f.data)
}
