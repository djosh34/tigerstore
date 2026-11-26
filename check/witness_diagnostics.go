package check

import (
	"encoding/json"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"strings"
)

// Diagnostic JSON is search work too: failed candidate orders can render
// multi-megabyte range answers over and over. Streaming keeps the encoder's
// normal 4 KiB flush points visible to both accounting and cancellation.
const (
	diagnosticBytesPerStep = 2048
	diagnosticPollBytes    = 64
)

type witnessJSONWriter struct {
	w *witness
	b strings.Builder
}

func (d *witnessJSONWriter) Write(p []byte) (int, error) {
	if d.w.stopped {
		return 0, ErrWitnessTimeout
	}
	bytes := len(p)
	r := d.w.search
	if bytes > r.diagnosticLeft {
		chunks := (bytes - r.diagnosticLeft + diagnosticPollBytes - 1) / diagnosticPollBytes
		if r.chargeItems(int64(chunks), workUnitsPerStep/(diagnosticBytesPerStep/diagnosticPollBytes)) {
			return 0, ErrWitnessTimeout
		}
		r.diagnosticLeft += chunks * diagnosticPollBytes
	}
	r.diagnosticLeft -= bytes
	return d.b.Write(p)
}

func (w *witness) jsonOf(v any) string {
	if w.search == nil {
		return jsonOf(v)
	}
	if w.stopped {
		return ""
	}
	d := &witnessJSONWriter{w: w}
	if err := jsonv2.MarshalWrite(d, v, json.DefaultOptionsV1()); err != nil {
		if w.stopped || errors.Is(err, ErrWitnessTimeout) {
			return ""
		}
		return fmt.Sprintf("%+v", v)
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
