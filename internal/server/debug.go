package server

import (
	"net/http"
	"net/http/pprof"
	"runtime"
	"strings"
)

// pprofMux serves the Go profiles under /debug/pprof/ as etcd's
// debugutil.PProfHandlers does with --enable-pprof.
func pprofMux() *http.ServeMux {
	// Only when nothing set it: 1 out of 5 mutex events, on average.
	if runtime.SetMutexProfileFraction(-1) == 0 {
		runtime.SetMutexProfileFraction(5)
	}
	const prefix = "/debug/pprof"
	mux := http.NewServeMux()
	mux.HandleFunc(prefix+"/", pprof.Index)
	mux.HandleFunc(prefix+"/profile", pprof.Profile)
	mux.HandleFunc(prefix+"/symbol", pprof.Symbol)
	mux.HandleFunc(prefix+"/cmdline", pprof.Cmdline)
	mux.HandleFunc(prefix+"/trace", pprof.Trace)
	for _, name := range []string{"heap", "goroutine", "threadcreate", "block", "mutex"} {
		mux.Handle(prefix+"/"+name, pprof.Handler(name))
	}
	return mux
}

// isPprof reports whether a path is one of pprofMux's, including the
// prefix without its slash, which the mux redirects.
func isPprof(path string) bool {
	return path == "/debug/pprof" || strings.HasPrefix(path, "/debug/pprof/")
}
