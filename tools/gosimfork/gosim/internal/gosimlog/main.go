package gosimlog

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"
)

type Stackframe struct {
	File     string `json:"file"`
	Function string `json:"function"`
	Line     int    `json:"line"`
}

func collectJsonKeys(typ reflect.Type) map[string]int {
	if typ.Kind() != reflect.Struct {
		panic("bad kind " + typ.Kind().String())
	}

	known := make(map[string]int)

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if field.Anonymous {
			panic("anonymous field " + field.Name)
		}
		value, ok := field.Tag.Lookup("json")
		if !ok {
			panic("bad struct tag " + field.Tag)
		}
		if strings.Contains(value, ",") {
			panic("bad struct tag value " + value)
		}
		known[value] = i
	}

	return known
}

func unmarshalWithExtraFields(data []byte, value any, extra *[]UnknownField, known map[string]int) error {
	reflectValue := reflect.ValueOf(value).Elem()
	if reflectValue.Kind() != reflect.Struct {
		return errors.New("expected ptr to struct value")
	}

	d := json.NewDecoder(bytes.NewReader(data))
	tok, err := d.Token()
	if err != nil {
		return err
	}
	if tok != json.Delim('{') {
		return errors.New("expected {")
	}
	for d.More() {
		tok, err = d.Token()
		if err != nil {
			return err
		}
		key, ok := tok.(string)
		if !ok {
			return fmt.Errorf("expected string key, got %q", tok)
		}
		if idx, ok := known[key]; ok {
			target := reflectValue.Field(idx).Addr().Interface()
			if err := d.Decode(target); err != nil {
				return err
			}
		} else {
			var value json.RawMessage
			if err := d.Decode(&value); err != nil {
				return err
			}

			// decode base64 binary in values and make it easier to read
			// TODO: make this less hacky, it would be nice to support
			// "types" broadly (time, references to steps/machines/FDs,
			// binary data, other stuff) and format them nicely
			const base64Prefix = `"base64:`
			if len(value) > 0 && bytes.HasPrefix(value, []byte(base64Prefix)) {
				if maybeBytes, err := base64.StdEncoding.AppendDecode(nil, value[len(base64Prefix):len(value)-1]); err == nil {
					*extra = append(*extra, UnknownField{Key: key, Value: strconv.QuoteToASCII(string(maybeBytes))})
					continue
				}
			}
			*extra = append(*extra, UnknownField{Key: key, Value: string(value)})
		}
	}
	tok, err = d.Token()
	if err != nil {
		return err
	}
	if tok != json.Delim('}') {
		return errors.New("expected }")
	}
	return nil
}

type UnknownField struct {
	Key   string
	Value string
}

type Log struct {
	Index int `json:"-"`

	Time        time.Time     `json:"time"`
	Level       slog.Level    `json:"level"`
	Msg         string        `json:"msg"`
	Source      *Stackframe   `json:"source"`
	Step        int           `json:"step"`
	RelatedStep int           `json:"relatedStep"`
	Stackframes []*Stackframe `json:"stackframes"`
	Machine     string        `json:"machine"`
	Goroutine   int           `json:"goroutine"`
	TraceKind   string        `json:"traceKind"`

	Unknown []UnknownField `json:"-"` // for extra fields
}

// sync.Once this?
var knownLogInfo = collectJsonKeys(reflect.TypeFor[Log]())

func (l *Log) UnmarshalJSON(b []byte) error {
	type Plain Log
	return unmarshalWithExtraFields(b, (*Plain)(l), &l.Unknown, knownLogInfo)
}

func ParseLog(logs []byte) []*Log {
	var out []*Log

	for _, line := range bytes.Split(logs, []byte("\n")) {
		var log Log
		if err := json.Unmarshal(line, &log); err != nil {
			// TODO: Is this ok?
			continue
		}
		out = append(out, &log)
	}

	return out
}

// Stack captures a stack with runtime.Callers and return a stackframes
// slog.Attr. If base is non-zero, Stack will skip frames until encountering
// base as a PC, which is helpful for calling Stack in a slog handler.
func Stack(skip int, base uintptr) slog.Attr {
	var stackRaw [256]uintptr
	n := runtime.Callers(skip+1, stackRaw[:])
	return StackFor(stackRaw[:n], base)
}

// StackFor returns a stackframes attr for a stack captured with runtime.Callers.
func StackFor(stack []uintptr, base uintptr) slog.Attr {
	var frames []Stackframe
	found := false
	if base == 0 {
		found = true
	} else {
		if idx := slices.Index(stack, base); idx != -1 {
			stack = stack[idx:]
			found = true
		}
	}
	if len(stack) > 0 {
		framesIter := runtime.CallersFrames(stack)
		for {
			frame, more := framesIter.Next()
			if !found && frame.PC == base {
				found = true
			}
			if found {
				frames = append(frames, Stackframe{
					Function: frame.Function,
					File:     frame.File,
					Line:     frame.Line,
				})
			}
			if !more {
				break
			}
		}
	}
	return slog.Any("stackframes", frames)
}
