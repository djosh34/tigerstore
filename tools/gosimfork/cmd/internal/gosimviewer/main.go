package gosimviewer

import (
	"bytes"
	"embed"
	"encoding/json"
	"fmt"
	"html"
	"html/template"
	"log"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/jellevandenhooff/gosim/internal/gosimlog"
	"github.com/jellevandenhooff/gosim/internal/gosimtool"
)

type Server struct {
	Base string

	Logs       []*gosimlog.Log
	LogsByStep map[int]*gosimlog.Log
}

const timeFormat = "15:04:05.000"

var levelColors = map[slog.Level]string{
	slog.LevelDebug: "gray",
	slog.LevelInfo:  "green",
	slog.LevelWarn:  "orange",
	slog.LevelError: "red",
}

var formattedLevels = map[slog.Level]string{
	slog.LevelDebug: "DBG",
	slog.LevelInfo:  "INF",
	slog.LevelWarn:  "WRN",
	slog.LevelError: "ERR",
}

var funcs = template.FuncMap{
	"Source": func(frame *gosimlog.Stackframe) (template.HTML, error) {
		// TODO: use a template instead
		parent := filepath.Base(filepath.Dir(frame.File))
		filename := filepath.Base(frame.File)
		shortFile := filepath.Join(parent, filename)

		var buf bytes.Buffer
		if err := templates.ExecuteTemplate(&buf, "_filelink.html.tmpl", map[string]any{
			"File":      frame.File,
			"Line":      frame.Line,
			"ShortFile": shortFile,
		}); err != nil {
			return "", err
		}
		return template.HTML(buf.String()), nil
	},
	"ShortValue": func(value string) string {
		n := 0
		for idx := range value {
			n++
			if n >= 120 {
				return value[:idx] + "…"
			}
		}
		return value
	},
	"Time": func(time time.Time) string {
		return time.Format(timeFormat)
	},
	"Level": func(level slog.Level) template.HTML {
		color, ok := levelColors[level]
		if !ok {
			color = "black"
		}
		levelText, ok := formattedLevels[level]
		if !ok {
			levelText = html.EscapeString(level.String())
		}

		return template.HTML(fmt.Sprintf(`<span style="color: %s">%s</span>`, color, levelText))
	},
}

//go:embed *.html.tmpl
var files embed.FS

var templates *template.Template

func init() {
	// in an init() so that funcs can refer to templates
	templates = template.Must(template.New("").Funcs(funcs).ParseFS(files, "*"))
}

func filter(logs []*gosimlog.Log, f func(*gosimlog.Log) bool) []*gosimlog.Log {
	var filtered []*gosimlog.Log
	for _, log := range logs {
		if f(log) {
			filtered = append(filtered, log)
		}
	}
	return filtered
}

func (s *Server) orRelated(f func(*gosimlog.Log) bool) func(*gosimlog.Log) bool {
	return func(l *gosimlog.Log) bool {
		if f(l) {
			return true
		}
		if l.RelatedStep != 0 {
			if related, ok := s.LogsByStep[l.RelatedStep]; ok {
				if f(related) {
					return true
				}
			}
		}
		return false
	}
}

func (s *Server) Index(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	focusStr := q.Get("focus")
	levelStr := q.Get("level")
	syscallsStr := q.Get("syscalls")

	logs := s.Logs

	var syscalls bool
	if syscallsStr != "" {
		var err error
		syscalls, err = strconv.ParseBool(syscallsStr)
		if err != nil {
			http.Error(w, fmt.Sprintf("parsing syscalls: %s", err.Error()), http.StatusBadRequest)
			return
		}
	}

	logs = filter(logs, func(l *gosimlog.Log) bool {
		if l.TraceKind == "syscall" && !syscalls {
			return false
		}
		return true
	})

	level := slog.LevelInfo
	if levelStr != "" {
		if err := level.UnmarshalText([]byte(levelStr)); err != nil {
			http.Error(w, fmt.Sprintf("parsing level: %s", err.Error()), http.StatusBadRequest)
			return
		}
	}

	logs = filter(logs, func(l *gosimlog.Log) bool {
		return l.Level >= level
	})

	if focusStr != "" {
		kind, arg, _ := strings.Cut(focusStr, ":")

		switch kind {
		case "goroutine":
			goroutine, err := strconv.Atoi(arg)
			if err != nil {
				http.Error(w, fmt.Sprintf("parsing goroutine: %s", err.Error()), http.StatusBadRequest)
				return
			}
			logs = filter(logs, s.orRelated(func(l *gosimlog.Log) bool {
				return l.Goroutine == goroutine
			}))

		case "machine":
			logs = filter(logs, s.orRelated(func(l *gosimlog.Log) bool {
				return l.Machine == arg
			}))

		default:
			http.Error(w, fmt.Sprintf("unknown kind %q", kind), http.StatusBadRequest)
		}
	}

	truncated := false
	if limit := 1000; len(logs) > limit {
		logs = logs[:limit]
		truncated = true
	}

	if err := templates.ExecuteTemplate(w, "index.html.tmpl", map[string]any{
		"Logs":      logs,
		"Focus":     focusStr,
		"LogLevels": []slog.Level{slog.LevelDebug, slog.LevelInfo, slog.LevelWarn, slog.LevelError},
		"LogLevel":  level,
		"Syscalls":  syscalls,
		"Truncated": truncated,
	}); err != nil {
		http.Error(w, fmt.Sprintf("writing template: %s", err.Error()), http.StatusInternalServerError)
		return
	}
}

func (s *Server) Stack(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	indexStr := q.Get("index")
	indexIdx, err := strconv.Atoi(indexStr)
	if err != nil {
		// TODO: handle
	}

	var foundLog *gosimlog.Log

	for _, log := range s.Logs {
		if indexIdx == log.Index {
			foundLog = log
		}
	}
	// TODO: handle not found

	if err := templates.ExecuteTemplate(w, "stack.html.tmpl", map[string]any{
		"Log": foundLog,
	}); err != nil {
		http.Error(w, fmt.Sprintf("writing template: %s", err.Error()), http.StatusInternalServerError)
		return
	}
}

func (s *Server) Related(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	indexStr := q.Get("index")
	indexIdx, err := strconv.Atoi(indexStr)
	if err != nil {
		// TODO: handle
	}

	var foundLog *gosimlog.Log

	for _, log := range s.Logs {
		if indexIdx == log.Index {
			foundLog = log
		}
	}

	var logs []*gosimlog.Log
	if foundLog != nil {
		logs = filter(s.Logs, func(l *gosimlog.Log) bool {
			return l.Step == foundLog.Step || (l.RelatedStep != 0 && l.RelatedStep == foundLog.Step) || (foundLog.RelatedStep != 0 && l.Step == foundLog.RelatedStep)
		})
	}
	// TODO: handle not found

	if err := templates.ExecuteTemplate(w, "related.html.tmpl", map[string]any{
		"Logs": logs,
	}); err != nil {
		http.Error(w, fmt.Sprintf("writing template: %s", err.Error()), http.StatusInternalServerError)
		return
	}
}

func (s *Server) Viewer(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	file := q.Get("file")
	lineStr := q.Get("line")
	lineIdx, err := strconv.Atoi(lineStr)
	if err != nil {
		// TODO: handle
	}

	if !strings.HasPrefix(file, "translated/") {
		http.Error(w, fmt.Sprintf("file %q does not have translated/ prefix", file), http.StatusBadRequest)
		return
	}

	p := filepath.Join(s.Base, strings.TrimPrefix(file, "translated/"))
	f, err := os.ReadFile(p)
	if err != nil {
		http.Error(w, fmt.Sprintf("could not read file %q: %s", file, err), http.StatusInternalServerError)
		return
	}

	type Line struct {
		Line     int
		Text     string
		Selected bool
	}
	var lines []Line
	for i, line := range bytes.Split(f, []byte("\n")) {
		lines = append(lines, Line{
			Line:     i + 1,
			Text:     strings.ReplaceAll(string(line), "\t", "    "),
			Selected: i+1 == lineIdx,
		})
	}

	if err := templates.ExecuteTemplate(w, "code.html.tmpl", map[string]any{
		"File":  file,
		"Line":  lineIdx,
		"Lines": lines,
	}); err != nil {
		http.Error(w, fmt.Sprintf("writing template: %s", err.Error()), http.StatusInternalServerError)
		return
	}
}

func Viewer(logs string) {
	gomoddir, err := gosimtool.FindGoModDir()
	if err != nil {
		log.Fatalf("could not find go mod dir: %s", err)
	}
	cfg := gosimtool.BuildConfig{
		GOOS:   "linux",
		GOARCH: runtime.GOARCH,
		Race:   false, // TODO: somehow get this from the log
	}
	s := &Server{
		// TODO: somehow get this from the log (and other packages as well)
		Base: filepath.Join(gomoddir, gosimtool.OutputDirectory, "translated", cfg.AsDirname()),

		LogsByStep: make(map[int]*gosimlog.Log),
	}

	f, err := os.ReadFile(logs)
	if err != nil {
		log.Fatalf("opening logs: %s", err)
	}
	lines := bytes.Split(f, []byte("\n"))
	for i, line := range lines {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}

		var parsed gosimlog.Log
		if err := json.Unmarshal(line, &parsed); err != nil {
			log.Printf("could not parse %q: %s", string(line), err)
			continue
		}
		parsed.Index = i

		s.Logs = append(s.Logs, &parsed)
		if parsed.Step != 0 {
			s.LogsByStep[parsed.Step] = &parsed
		}
	}

	http.HandleFunc("GET /{$}", s.Index)
	http.HandleFunc("GET /viewer", s.Viewer)
	http.HandleFunc("GET /stack", s.Stack)
	http.HandleFunc("GET /related", s.Related)

	log.Printf("running server on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}
