package check

import (
	"fmt"
	"reflect"
	"strings"
)

// Diagnostic JSON is search work too: failed candidate orders can render
// multi-megabyte range answers over and over. The bounded encoder escapes
// strings and copies output in at most 4 KiB chunks, including a single
// arbitrarily large scalar. Accounting uses 64-byte units; the stop hook
// is polled every 1,024 ticks (at most about 4 MiB of encoded JSON).
const (
	diagnosticBytesPerStep = 2048
	diagnosticPollBytes    = 64
)

type witnessJSONWriter struct {
	w   *witness
	b   strings.Builder
	buf [4096]byte
	n   int
}

func (d *witnessJSONWriter) Write(p []byte) (int, error) {
	if d.w.stopped {
		return 0, ErrWitnessTimeout
	}
	written := 0
	for len(p) > 0 {
		part := p[:min(len(p), 4096)]
		r := d.w.search
		if len(part) > r.diagnosticLeft {
			chunks := (len(part) - r.diagnosticLeft + diagnosticPollBytes - 1) / diagnosticPollBytes
			if r.chargeItems(int64(chunks), workUnitsPerStep/(diagnosticBytesPerStep/diagnosticPollBytes)) {
				return written, ErrWitnessTimeout
			}
			r.diagnosticLeft += chunks * diagnosticPollBytes
		}
		r.diagnosticLeft -= len(part)
		_, _ = d.b.Write(part)
		written += len(part)
		p = p[len(part):]
	}
	return written, nil
}

func (w *witness) jsonOf(v any) string {
	if w.search == nil {
		return jsonOf(v)
	}
	if w.stopped {
		return ""
	}
	d := &witnessJSONWriter{w: w}
	if err := d.value(reflect.ValueOf(v)); err != nil {
		if w.stopped {
			return ""
		}
		return fmt.Sprintf("%+v", v)
	}
	if err := d.flush(); err != nil {
		return ""
	}
	return d.b.String()
}

func (w *witness) outputOf(o outcome) string {
	if w.stopped {
		return ""
	}
	if o.err != nil {
		return fmt.Sprintf("error %q", o.err.Message)
	}
	return w.jsonOf(o.resp)
}
