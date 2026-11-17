package wl

import (
	"context"
	"encoding/json"
	"errors"
	"expvar"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	etcdmetrics "github.com/djosh34/tigerstore/internal/metrics"
)

// The client port's HTTP endpoints, as etcd v3.7.2 serves them
// (embed/serve.go and etcdserver/api/etcdhttp): an access controller in
// front of every request, the gateway under /v3/, /version, /health,
// /readyz, /livez, /metrics, /debug/vars, and with --enable-pprof
// /debug/pprof/.

// httpAPI serves the client port's HTTP requests.
type httpAPI struct {
	m       *nodeMetrics
	gateway http.Handler
	prom    http.Handler
	cors    map[string]struct{}
	hosts   map[string]struct{}
	pprof   bool
	// raftStatus is the node's Raft status in JSON, as etcd's
	// /debug/vars shows it; read makes a health check's read.
	raftStatus func() []byte
	read       func(serializable bool) error
	// set takes the health checks' counts.
	set *etcdmetrics.Set
}

const (
	textPlain = "text/plain; charset=utf-8"
	appJSON   = "application/json"
	// healthTimeout is etcd's request timeout, which its health reads
	// wait for at most.
	healthTimeout = 7 * time.Second
)

// ServeHTTP is etcd's accessController: it moves /v3beta/ to /v3/,
// checks the Host of a request without TLS against the whitelist, adds
// the CORS headers, answers OPTIONS, and serves the endpoint.
func (a *httpAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if rest, ok := strings.CutPrefix(r.URL.Path, "/v3beta/"); ok {
		r.URL.Path = "/v3/" + rest
	}
	if r.TLS == nil {
		if host := hostname(r); !allowed(a.hosts, host) {
			http.Error(w, errCVE20185702(host), http.StatusMisdirectedRequest)
			return
		}
	}
	if allowed(a.cors, "*") {
		addCORSHeader(w, "*")
	} else if origin := r.Header.Get("Origin"); allowed(a.cors, origin) {
		addCORSHeader(w, origin)
	}
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	switch p := r.URL.Path; {
	case strings.HasPrefix(p, "/v3/"):
		a.gateway.ServeHTTP(w, r)
	case p == "/metrics":
		a.prom.ServeHTTP(w, r)
	case p == "/debug/vars":
		a.serveVars(w, r)
	case a.pprof && strings.HasPrefix(p, "/debug/pprof"):
		a.servePprof(w, r)
	case p == "/version":
		a.get(w, r, a.version)
	case p == "/health":
		a.get(w, r, a.health)
	default:
		cp, ok := checkPaths[p]
		if !ok {
			http.NotFound(w, r)
			return
		}
		a.get(w, r, func(ctx context.Context, q url.Values) (int, string, string) { return a.runChecks(ctx, cp, q) })
	}
}

// metricsHandler serves what etcd serves on --listen-metrics-urls:
// /metrics, /health, /livez and /readyz, without the access controller.
func (a *httpAPI) metricsHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/metrics":
			a.prom.ServeHTTP(w, r)
		case "/health":
			a.get(w, r, a.health)
		default:
			cp, ok := checkPaths[r.URL.Path]
			if !ok {
				http.NotFound(w, r)
				return
			}
			a.get(w, r, func(ctx context.Context, q url.Values) (int, string, string) { return a.runChecks(ctx, cp, q) })
		}
	})
}

// get serves an endpoint that takes only GET, as etcd's do: other methods
// get 405 with Allow: GET.
func (a *httpAPI) get(w http.ResponseWriter, r *http.Request, answer func(context.Context, url.Values) (int, string, string)) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	code, ctype, body := answer(r.Context(), r.URL.Query())
	if code != http.StatusOK {
		http.Error(w, strings.TrimSuffix(body, "\n"), code)
		return
	}
	w.Header().Set("Content-Type", ctype)
	if ctype == textPlain {
		w.Header().Set("X-Content-Type-Options", "nosniff")
	}
	_, _ = w.Write([]byte(body))
}

func (a *httpAPI) version(context.Context, url.Values) (int, string, string) {
	return http.StatusOK, appJSON, `{"etcdserver":"3.7.2","etcdcluster":"3.7.0","storage":"3.7.0"}`
}

// health is etcd's: an alarm not excluded fails it; without
// ?serializable=true it needs a leader; then it reads one key,
// linearizable unless serializable was asked for.
func (a *httpAPI) health(ctx context.Context, q url.Values) (int, string, string) {
	code, body := a.checkHealth(ctx, q)
	if code == http.StatusOK {
		a.set.HealthSuccess.Inc()
	} else {
		a.set.HealthFailures.Inc()
	}
	return code, textPlain, body
}

func (a *httpAPI) checkHealth(ctx context.Context, q url.Values) (int, string) {
	for _, s := range a.alarms() {
		name := s[strings.LastIndex(s, ":")+1:]
		if slices.Contains(q["exclude"], name) {
			continue
		}
		reason := "ALARM UNKNOWN"
		switch name {
		case "NOSPACE":
			reason = "ALARM NOSPACE"
		case "CORRUPT":
			reason = "ALARM CORRUPT"
		}
		return http.StatusServiceUnavailable, `{"health":"false","reason":"` + reason + `"}`
	}
	serializable := q.Get("serializable") == "true"
	if !serializable && a.m.hasLeader.Load() != 1 {
		return http.StatusServiceUnavailable, `{"health":"false","reason":"RAFT NO LEADER"}`
	}
	if err := a.healthRead(ctx, serializable); err != nil {
		return http.StatusServiceUnavailable, `{"health":"false","reason":"RANGE ERROR:` + err.Error() + `"}`
	}
	return http.StatusOK, `{"health":"true","reason":""}`
}

func (a *httpAPI) alarms() []string {
	if p := a.m.alarms.Load(); p != nil {
		return *p
	}
	return nil
}

// healthRead is the read of etcd's health checks, made on the loop.
func (a *httpAPI) healthRead(_ context.Context, serializable bool) error {
	return a.read(serializable)
}

// namedCheck is one check of /readyz or /livez; a nil error passes.
type namedCheck struct {
	name  string
	check func(a *httpAPI, ctx context.Context) error
}

var (
	livezChecks  = []namedCheck{{"serializable_read", serializableRead}}
	readyzChecks = []namedCheck{
		{"data_corruption", noCorruptAlarm},
		{"serializable_read", serializableRead},
		{"linearizable_read", linearizableRead},
		{"non_learner", nonLearner},
	}
)

// checkPath is what one path of /readyz or /livez runs.
type checkPath struct {
	checks []namedCheck
	root   bool   // only the root path takes ?exclude
	kind   string // livez or readyz, for etcd_server_healthcheck
}

// checkPaths are the paths of /readyz and /livez.
var checkPaths = func() map[string]checkPath {
	paths := map[string]checkPath{}
	for root, checks := range map[string][]namedCheck{"/livez": livezChecks, "/readyz": readyzChecks} {
		kind := strings.TrimPrefix(root, "/")
		paths[root] = checkPath{checks: checks, root: true, kind: kind}
		for _, c := range checks {
			paths[root+"/"+c.name] = checkPath{checks: []namedCheck{c}, kind: kind}
		}
	}
	return paths
}()

// runChecks answers one path of /readyz or /livez, running its checks in
// etcd's order. An excluded name that is not a check is ignored, as in
// etcd.
func (a *httpAPI) runChecks(ctx context.Context, p checkPath, q url.Values) (int, string, string) {
	var lines strings.Builder
	failed := false
	for _, c := range p.checks {
		if p.root && slices.Contains(q["exclude"], c.name) {
			continue
		}
		status := "success"
		if err := c.check(a, ctx); err != nil {
			failed, status = true, "error"
			fmt.Fprintf(&lines, "[-]%s failed: %v\n", c.name, err)
			a.set.HealthCheck.WithLabelValues(p.kind, c.name).Set(0)
		} else {
			fmt.Fprintf(&lines, "[+]%s ok\n", c.name)
			a.set.HealthCheck.WithLabelValues(p.kind, c.name).Set(1)
		}
		a.set.HealthChecks.WithLabelValues(p.kind, c.name, status).Inc()
	}
	if failed {
		return http.StatusServiceUnavailable, textPlain, lines.String()
	}
	body := "ok\n"
	if q.Has("verbose") {
		body = lines.String() + body
	}
	return http.StatusOK, textPlain, body
}

func noCorruptAlarm(a *httpAPI, _ context.Context) error {
	for _, s := range a.alarms() {
		if strings.HasSuffix(s, "alarm:CORRUPT") {
			return errors.New("alarm activated: CORRUPT")
		}
	}
	return nil
}

func nonLearner(a *httpAPI, _ context.Context) error {
	if a.m.learner.Load() {
		return errors.New("not supported for learner")
	}
	return nil
}

func serializableRead(a *httpAPI, ctx context.Context) error { return a.healthRead(ctx, true) }

func linearizableRead(a *httpAPI, ctx context.Context) error { return a.healthRead(ctx, false) }

// serveVars is etcd's /debug/vars: expvar's variables, with the node's
// file descriptor limit and its Raft status.
func (a *httpAPI) serveVars(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	vars := map[string]json.RawMessage{}
	expvar.Do(func(kv expvar.KeyValue) { vars[kv.Key] = json.RawMessage(kv.Value.String()) })
	vars["file_descriptor_limit"] = json.RawMessage(strconv.FormatUint(fdLimit(), 10))
	vars["raft.status"] = json.RawMessage("null")
	if b := a.raftStatus(); b != nil {
		vars["raft.status"] = b
	}
	b, err := json.MarshalIndent(vars, "", "")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_, _ = w.Write(b)
}

// servePprof is net/http/pprof under /debug/pprof/, as etcd registers it.
func (a *httpAPI) servePprof(w http.ResponseWriter, r *http.Request) {
	switch strings.TrimPrefix(r.URL.Path, "/debug/pprof/") {
	case "cmdline":
		pprof.Cmdline(w, r)
	case "profile":
		pprof.Profile(w, r)
	case "symbol":
		pprof.Symbol(w, r)
	case "trace":
		pprof.Trace(w, r)
	default:
		pprof.Index(w, r)
	}
}

// allowed is etcd's OriginAllowed and IsHostWhitelisted: an empty list or
// one with "*" allows all.
func allowed(list map[string]struct{}, v string) bool {
	if len(list) == 0 {
		return true
	}
	if _, ok := list["*"]; ok {
		return true
	}
	_, ok := list[v]
	return ok
}

// hostname is etcd's httputil.GetHostname: the request's Host without
// its port.
func hostname(r *http.Request) string {
	if host, _, err := net.SplitHostPort(r.Host); err == nil {
		return host
	}
	return r.Host
}

// addCORSHeader is etcd's.
func addCORSHeader(w http.ResponseWriter, origin string) {
	w.Header().Add("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
	w.Header().Add("Access-Control-Allow-Origin", origin)
	w.Header().Add("Access-Control-Allow-Headers", "accept, content-type, authorization")
}

// errCVE20185702 is etcd's answer to a Host that is not whitelisted.
func errCVE20185702(host string) string {
	return fmt.Sprintf(`
etcd received your request, but the Host header was unrecognized.

To fix this, choose one of the following options:
- Enable TLS, then any HTTPS request will be allowed.
- Add the hostname you want to use to the whitelist in settings.
  - e.g. etcd --host-whitelist %q

This requirement has been added to help prevent "DNS Rebinding" attacks (CVE-2018-5702).
`, host)
}
