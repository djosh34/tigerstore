// MIT License
//
// # Copyright (c) 2017 Olivier Poitrey
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//
// Based on https://github.com/rs/zerolog/blob/master/console.go.
package prettylog

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mattn/go-isatty"
)

const (
	colorBlack = iota + 30
	colorRed
	colorGreen
	colorYellow
	colorBlue
	colorMagenta
	colorCyan
	colorWhite

	colorBold     = 1
	colorDarkGray = 90
)

// algorithm:
// - print well known fields in fixed order with standard formatters
// - print other fields in input order (XXX: sorted for now)
// - for fields that are big (original json >80 characters?) print them on separate lines (XXX: traceback only for now)

type Writer struct {
	out       io.Writer
	formatter formatter
}

// NewWriter creates and initializes a new ConsoleWriter.
func NewWriter(out io.Writer) *Writer {
	w := Writer{
		out: out,
	}

	noColor := (os.Getenv("NO_COLOR") != "") || os.Getenv("TERM") == "dumb" ||
		(!isatty.IsTerminal(os.Stdout.Fd()) && !isatty.IsCygwinTerminal(os.Stdout.Fd()))
	noColor = noColor && !(os.Getenv("FORCE_COLOR") != "")
	w.formatter = formatter{noColor: noColor}

	return &w
}

var writePool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 0, 1024))
	},
}

// Write transforms the JSON input with formatters and appends to w.Out.
func (w *Writer) Write(p []byte) (n int, err error) {
	buf := writePool.Get().(*bytes.Buffer)
	defer func() {
		buf.Reset()
		writePool.Put(buf)
	}()

	i := 0
	for i < len(p) && p[i] == ' ' {
		i++
	}
	prefix := p[:i]
	p = p[i:]

	var evt map[string]interface{}
	d := json.NewDecoder(bytes.NewReader(p))
	d.UseNumber()
	err = d.Decode(&evt)
	if err != nil {
		// XXX: test that this is output
		w.out.Write(prefix)
		w.out.Write(p)
		return n, fmt.Errorf("cannot decode event: %s", err)
	}
	// XXX: what happens with stuff after the message? fix and test

	for _, p := range []string{
		"step",
		"machine",
		// "goroutine",
		slog.TimeKey,
		slog.LevelKey,
		slog.SourceKey,
		slog.MessageKey,
	} {
		w.writePart(buf, evt, p)
	}

	w.writeFields(evt, buf)

	err = buf.WriteByte('\n')
	if err != nil {
		return n, err
	}

	// XXX: clean up somehow
	first := true
	buffer := buf.Bytes()
	for {
		idx := bytes.IndexByte(buffer, '\n')
		if idx == -1 {
			break
		}
		w.out.Write(prefix)
		if !first {
			w.out.Write([]byte("    "))
		}
		w.out.Write(buffer[:idx+1])
		first = false
		buffer = buffer[idx+1:]
	}
	if len(buffer) > 0 {
		w.out.Write(prefix)
		if !first {
			w.out.Write([]byte("    "))
		}
		w.out.Write(buffer)
	}

	return len(p), err
}

func jsonMarshal(v interface{}, indent bool) ([]byte, error) {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	if indent {
		encoder.SetIndent("    ", "  ")
	}
	if err := encoder.Encode(v); err != nil {
		return nil, err
	}
	b := buf.Bytes()
	if len(b) > 0 {
		// Remove trailing \n which is added by Encode.
		return b[:len(b)-1], nil
	}
	return b, nil
}

// needsQuote returns true when the string s should be quoted in output.
func needsQuote(s string) bool {
	for i := range s {
		if s[i] < 0x20 || s[i] > 0x7e || s[i] == ' ' || s[i] == '\\' || s[i] == '"' {
			return true
		}
	}
	return false
}

const errorKey = "err"

// writeFields appends formatted key-value pairs to buf.
func (w Writer) writeFields(evt map[string]interface{}, buf *bytes.Buffer) {
	fields := make([]string, 0, len(evt))
	for field := range evt {
		// XXX: support skipping more?
		switch field {
		case "step", "machine", "goroutine", slog.LevelKey, slog.TimeKey, slog.MessageKey, slog.SourceKey:
			continue
		}
		fields = append(fields, field)
	}
	sort.Strings(fields)

	// Move the "error" field to the front
	ei := sort.Search(len(fields), func(i int) bool { return fields[i] >= errorKey })
	if ei < len(fields) && fields[ei] == errorKey {
		fields = append(slices.Insert(fields[:ei], 0, errorKey), fields[ei+1:]...)
	}

	for _, field := range fields {
		if buf.Len() > 0 {
			buf.WriteByte(' ')
		}
		buf.WriteString(w.formatter.fieldName(field))

		if field == "traceback" {
			b, err := jsonMarshal(evt[field], true)
			if err != nil {
				fmt.Fprintf(buf, w.formatter.colorize("[error: %v]", colorRed), err)
			} else {
				buf.WriteString(w.formatter.fieldValue(field, b))
			}
			continue
		}

		switch value := evt[field].(type) {
		case string:
			if strings.HasPrefix(value, "base64:") {
				bytes, err := base64.StdEncoding.AppendDecode(nil, []byte(value[len("base64:"):]))
				if err == nil {
					value = string(bytes)
				}
			}
			if needsQuote(value) {
				buf.WriteString(w.formatter.fieldValue(field, strconv.Quote(value)))
			} else {
				buf.WriteString(w.formatter.fieldValue(field, value))
			}
		case json.Number:
			buf.WriteString(w.formatter.fieldValue(field, value))
		default:
			b, err := jsonMarshal(value, false)
			if err != nil {
				fmt.Fprintf(buf, w.formatter.colorize("[error: %v]", colorRed), err)
			} else {
				buf.WriteString(w.formatter.fieldValue(field, b))
			}
		}
	}
}

var pad = "             " // hope you don't need more :)

func padLeft(s string, n int) string {
	if len(s) >= n {
		return s
	}
	return pad[:n-len(s)] + s
}

func padRight(s string, n int) string {
	if len(s) >= n {
		return s
	}
	return s + pad[:n-len(s)]
}

// writePart appends a formatted part to buf.
func (w Writer) writePart(buf *bytes.Buffer, evt map[string]interface{}, p string) {
	var s string
	switch p {
	case slog.LevelKey:
		s = w.formatter.level(evt[p])
	case slog.TimeKey:
		s = w.formatter.timestamp(evt[p])
	case slog.MessageKey:
		// XXX: this level is a string so will cause type problems?
		s = w.formatter.message(evt[slog.LevelKey], evt[p])
	case slog.SourceKey:
		s = w.formatter.caller(evt[p])
	case "machine":
		s = padRight(fmt.Sprintf("%s/%s", evt["machine"], evt["goroutine"]), 10)
	case "step":
		s = padLeft(fmt.Sprint(evt[p]), 5)
	default:
		s = w.formatter.fieldValue(p, evt[p])
	}

	if len(s) > 0 {
		if buf.Len() > 0 {
			buf.WriteByte(' ') // Write space only if not the first part
		}
		buf.WriteString(s)
	}
}

type formatter struct {
	noColor bool
}

// colorize returns the string s wrapped in ANSI code c, unless disabled is true or c is 0.
func (f *formatter) colorize(s interface{}, c ...int) string {
	if len(c) == 0 || (len(c) == 1 && c[0] == 0) || f.noColor {
		// XXX: %s or %v?
		return fmt.Sprintf("%s", s)
	}
	for _, c := range c {
		s = fmt.Sprintf("\x1b[%dm%v\x1b[0m", c, s)
	}
	return s.(string)
}

const timeFormat = "15:04:05.000"

func (f *formatter) timestamp(i interface{}) string {
	if s, ok := i.(string); ok {
		ts, err := time.ParseInLocation(time.RFC3339Nano, s, time.UTC)
		if err != nil {
			// ignore
		} else {
			i = ts.In(time.UTC).Format(timeFormat)
		}
	}
	return f.colorize(i, colorDarkGray)
}

var levelColors = map[slog.Level]int{
	slog.LevelDebug: colorMagenta,
	slog.LevelInfo:  colorGreen,
	slog.LevelWarn:  colorYellow,
	slog.LevelError: colorRed,
}

// formattedLevels are used by ConsoleWriter's consoleDefaultFormatLevel
// for a short level name.
var formattedLevels = map[slog.Level]string{
	slog.LevelDebug: "DBG",
	slog.LevelInfo:  "INF",
	slog.LevelWarn:  "WRN",
	slog.LevelError: "ERR",
}

func (f *formatter) level(i interface{}) string {
	var l string
	if ll, ok := i.(string); ok {
		var level slog.Level
		level.UnmarshalText([]byte(ll))
		fl, ok := formattedLevels[level]
		if ok {
			l = f.colorize(fl, levelColors[level])
		} else {
			l = strings.ToUpper(ll)[0:3]
		}
	} else {
		if i == nil {
			l = "???"
		} else {
			l = strings.ToUpper(fmt.Sprintf("%s", i))[0:3]
		}
	}
	return l
}

func (f *formatter) caller(i interface{}) string {
	m, ok := i.(map[string]any)
	if !ok {
		return ""
	}

	// fn, _ := m["function"].(string)
	file, _ := m["file"].(string)
	line, _ := m["line"].(json.Number)

	name := path.Base(file)
	dir := path.Base(path.Dir(file))
	file = fmt.Sprintf("%s/%s:%s", dir, name, line)

	c := file
	if len(c) > 0 {
		/*
			if cwd, err := os.Getwd(); err == nil {
				if rel, err := filepath.Rel(cwd, c); err == nil {
					c = rel
				}
			}
		*/
		c = f.colorize(c, colorDarkGray) + f.colorize(" >", colorCyan)
	}
	return c
}

func (f *formatter) message(level interface{}, i interface{}) string {
	if i == nil || i == "" {
		return ""
	}
	switch level {
	case slog.LevelInfo, slog.LevelWarn, slog.LevelError:
		return f.colorize(fmt.Sprintf("%s", i), colorBold)
	default:
		return fmt.Sprintf("%s", i)
	}
}

func (f *formatter) fieldName(i interface{}) string {
	return f.colorize(fmt.Sprintf("%s=", i), colorCyan)
}

func (f *formatter) fieldValue(field string, i interface{}) string {
	if field == errorKey {
		return f.colorize(fmt.Sprintf("%s", i), colorBold, colorRed)
	} else {
		return fmt.Sprintf("%s", i)
	}
}
