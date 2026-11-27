package check

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/djosh34/tigerstore/history"
)

// The history's JSON schema uses ordinary tagged structs, pointers, slices
// and scalar fields. Walk that schema without handing an unbounded string
// to json.Marshal: even MarshalWrite escapes a whole scalar before Write.
func (d *witnessJSONWriter) value(v reflect.Value) error {
	if !v.IsValid() {
		return d.raw("null")
	}
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface:
		if v.IsNil() {
			return d.raw("null")
		}
		return d.value(v.Elem())
	case reflect.String:
		return d.quoted(v.String())
	case reflect.Bool:
		if v.Bool() {
			return d.raw("true")
		}
		return d.raw("false")
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return d.integer(v.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		var b [20]byte
		return d.bytes(strconv.AppendUint(b[:0], v.Uint(), 10))
	case reflect.Struct:
		if err := d.raw("{"); err != nil {
			return err
		}
		comma := false
		for i := range v.NumField() {
			field := v.Type().Field(i)
			if field.PkgPath != "" {
				continue
			}
			name, options, _ := strings.Cut(field.Tag.Get("json"), ",")
			if name == "-" || options == "omitempty" && jsonEmpty(v.Field(i)) {
				continue
			}
			if name == "" {
				name = field.Name
			}
			if comma {
				if err := d.raw(","); err != nil {
					return err
				}
			}
			comma = true
			if err := d.quoted(name); err != nil {
				return err
			}
			if err := d.raw(":"); err != nil {
				return err
			}
			if err := d.value(v.Field(i)); err != nil {
				return err
			}
		}
		return d.raw("}")
	case reflect.Slice, reflect.Array:
		if v.Kind() == reflect.Slice && v.IsNil() {
			return d.raw("null")
		}
		if err := d.raw("["); err != nil {
			return err
		}
		// Range answers dominate diagnostic volume. Avoid reflection and
		// per-field metadata allocations for each of their key-values.
		if kvs, ok := reflect.TypeAssert[[]history.KeyValue](v); ok {
			for i, kv := range kvs {
				if i > 0 {
					if err := d.raw(","); err != nil {
						return err
					}
				}
				if err := d.keyValue(kv); err != nil {
					return err
				}
			}
		} else {
			for i := range v.Len() {
				if i > 0 {
					if err := d.raw(","); err != nil {
						return err
					}
				}
				if err := d.value(v.Index(i)); err != nil {
					return err
				}
			}
		}
		return d.raw("]")
	case reflect.Float32, reflect.Float64:
		// No composite or string falls back to an unbounded encoder.
		// Floats are scalar and json.Marshal preserves v1's NaN checks.
		b, err := json.Marshal(v.Interface())
		if err != nil {
			return err
		}
		return d.bytes(b)
	case reflect.Invalid:
		return d.raw("null")
	case reflect.Complex64, reflect.Complex128, reflect.Chan, reflect.Func, reflect.Map, reflect.UnsafePointer:
		return fmt.Errorf("unsupported diagnostic JSON type %s", v.Type())
	}
	return fmt.Errorf("unknown diagnostic JSON type %s", v.Type())
}

func jsonEmpty(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Interface, reflect.Pointer:
		return v.IsZero()
	case reflect.Invalid, reflect.Complex64, reflect.Complex128, reflect.Chan, reflect.Func, reflect.Struct, reflect.UnsafePointer:
		return false
	}
	return false
}

func (d *witnessJSONWriter) keyValue(kv history.KeyValue) error {
	if err := d.raw(`{"key":`); err != nil {
		return err
	}
	if err := d.quoted(kv.Key); err != nil {
		return err
	}
	if kv.Value != "" {
		if err := d.raw(`,"value":`); err != nil {
			return err
		}
		if err := d.quoted(kv.Value); err != nil {
			return err
		}
	}
	for _, field := range []struct {
		name string
		n    int64
	}{
		{`,"createRevision":`, kv.CreateRevision},
		{`,"modRevision":`, kv.ModRevision},
		{`,"version":`, kv.Version},
		{`,"lease":`, kv.Lease},
	} {
		if field.name == `,"lease":` && field.n == 0 {
			continue
		}
		if err := d.raw(field.name); err != nil {
			return err
		}
		if err := d.integer(field.n); err != nil {
			return err
		}
	}
	return d.raw("}")
}

func (d *witnessJSONWriter) integer(n int64) error {
	var b [20]byte
	return d.bytes(strconv.AppendInt(b[:0], n, 10))
}

func (d *witnessJSONWriter) flush() error {
	_, err := d.Write(d.buf[:d.n])
	d.n = 0
	return err
}

func (d *witnessJSONWriter) raw(s string) error {
	for len(s) > 0 {
		n := copy(d.buf[d.n:4096], s)
		d.n += n
		s = s[n:]
		if d.n == 4096 {
			if err := d.flush(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (d *witnessJSONWriter) bytes(p []byte) error {
	for len(p) > 0 {
		n := copy(d.buf[d.n:4096], p)
		d.n += n
		p = p[n:]
		if d.n == 4096 {
			if err := d.flush(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (d *witnessJSONWriter) quoted(s string) error {
	const hex = "0123456789abcdef"
	// Reserve space for a six-byte escape and the closing quote, so
	// even an escape straddling a boundary never builds a >4 KiB chunk.
	if d.n > 4096-8 {
		if err := d.flush(); err != nil {
			return err
		}
	}
	b := append(d.buf[:d.n], '"')
	for i := 0; i < len(s); {
		if len(b) > 4096-8 {
			d.n = len(b)
			if err := d.flush(); err != nil {
				return err
			}
			b = d.buf[:0]
		}
		c := s[i]
		i++
		switch {
		case c == '"' || c == '\\':
			b = append(b, '\\', c)
		case c == '\n':
			b = append(b, '\\', 'n')
		case c == '\r':
			b = append(b, '\\', 'r')
		case c == '\t':
			b = append(b, '\\', 't')
		case c == '\b':
			b = append(b, '\\', 'b')
		case c == '\f':
			b = append(b, '\\', 'f')
		case c < 0x20 || c == '<' || c == '>' || c == '&':
			b = append(b, '\\', 'u', '0', '0', hex[c>>4], hex[c&15])
		case c < utf8.RuneSelf:
			b = append(b, c)
		default:
			r, size := utf8.DecodeRuneInString(s[i-1:])
			switch {
			case r == utf8.RuneError && size == 1:
				b = append(b, "\ufffd"...)
			case r == '\u2028' || r == '\u2029':
				b = append(b, '\\', 'u', '2', '0', '2', hex[r&15])
			default:
				b = append(b, s[i-1:i-1+size]...)
			}
			i += size - 1
		}
	}
	b = append(b, '"')
	d.n = len(b)
	if d.n >= 4096 {
		return d.flush()
	}
	return nil
}
