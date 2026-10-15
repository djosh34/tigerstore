//go:build difftest

package difftest

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// gatewayJSON normalizes a gateway answer: revisions become relative to
// the case, IDs and terms go, and keys and values are decoded, without
// the case's prefix.
func (r *run) gatewayJSON(v any) any {
	switch v := v.(type) {
	case map[string]any:
		out := map[string]any{}
		for k, x := range v {
			switch k {
			case "cluster_id", "member_id", "raft_term", "ID", "id":
				continue
			case "revision", "create_revision", "mod_revision", "start_revision":
				if s, ok := x.(string); ok {
					if n, err := strconv.ParseInt(s, 10, 64); err == nil {
						out[k] = r.rel(n)
						continue
					}
				}
			case "key", "value", "range_end":
				if s, ok := x.(string); ok {
					if b, err := base64.StdEncoding.DecodeString(s); err == nil {
						out[k] = strings.TrimPrefix(string(b), r.prefix)
						continue
					}
				}
			}
			out[k] = r.gatewayJSON(x)
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, x := range v {
			out[i] = r.gatewayJSON(x)
		}
		return out
	}
	return v
}

// b64 is a key of the case or a value, as the gateway's JSON has bytes.
func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

// gateway records a gateway call's status, content type, CORS origin and
// normalized JSON lines. A stream's response is read for at most a
// second or lines lines.
func (r *run) gateway(do string, hc *http.Client, method, path, body string, lines int) {
	r.t.Helper()
	ctx, cancel := context.WithTimeout(r.ctx, time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, r.c.url+path, strings.NewReader(body))
	if err != nil {
		r.t.Fatal(err)
	}
	resp, err := hc.Do(req)
	if err != nil {
		r.t.Fatalf("%s: %v", do, err)
	}
	defer resp.Body.Close() //nolint:errcheck // closing a test response
	var got []any
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(nil, 1<<20)
	for len(got) < lines && sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var v any
		if err := json.Unmarshal(line, &v); err != nil {
			got = append(got, string(line))
			continue
		}
		got = append(got, r.gatewayJSON(v))
	}
	r.record(do, struct {
		Status      int    `json:"status"`
		ContentType string `json:"contentType"`
		CORS        string `json:"cors,omitempty"`
		Body        []any  `json:"body,omitempty"`
	}{resp.StatusCode, resp.Header.Get("Content-Type"), resp.Header.Get("Access-Control-Allow-Origin"), got}, nil)
}

// gatewayCase calls etcd's gRPC gateway over HTTP/2 and HTTP/1.1.
func gatewayCase(r *run) {
	k := b64(string(r.key("a")))
	put := `{"key":"` + k + `","value":"` + b64("1") + `","prev_kv":true}`
	r.gateway("put", r.c.http2, http.MethodPost, "/v3/kv/put", put, 1)
	r.gateway("put again over HTTP/1.1", r.c.http1, http.MethodPost, "/v3/kv/put", put, 1)
	r.gateway("range", r.c.http2, http.MethodPost, "/v3/kv/range", `{"key":"`+k+`"}`, 1)
	r.gateway("range under /v3beta/", r.c.http1, http.MethodPost, "/v3beta/kv/range", `{"key":"`+k+`","count_only":true}`, 1)
	r.gateway("range with an unknown field", r.c.http2, http.MethodPost, "/v3/kv/range", `{"key":"`+k+`","nope":1}`, 1)
	r.gateway("range without a key", r.c.http2, http.MethodPost, "/v3/kv/range", `{}`, 1)
	r.gateway("range with bad JSON", r.c.http2, http.MethodPost, "/v3/kv/range", `{`, 1)
	r.gateway("GET of range", r.c.http2, http.MethodGet, "/v3/kv/range", ``, 1)
	r.gateway("unknown path", r.c.http2, http.MethodPost, "/v3/kv/nope", `{}`, 1)
	r.gateway("OPTIONS", r.c.http2, http.MethodOptions, "/v3/kv/range", ``, 1)
	r.gateway("txn", r.c.http1, http.MethodPost, "/v3/kv/txn",
		`{"compare":[{"key":"`+k+`","target":"VALUE","result":"EQUAL","value":"`+b64("1")+`"}],"success":[{"request_range":{"key":"`+k+`"}}]}`, 1)
	r.gateway("delete", r.c.http2, http.MethodPost, "/v3/kv/deleterange", `{"key":"`+k+`"}`, 1)
	r.gateway("watch from the first put", r.c.http2, http.MethodPost, "/v3/watch",
		`{"create_request":{"key":"`+k+`","start_revision":"`+strconv.FormatInt(r.abs(1), 10)+`"}}`, 2)
	r.gateway("watch over HTTP/1.1", r.c.http1, http.MethodPost, "/v3/watch",
		`{"create_request":{"key":"`+k+`","start_revision":"`+strconv.FormatInt(r.abs(1), 10)+`"}}`, 2)
	r.gateway("member list status", r.c.http2, http.MethodPost, "/v3/cluster/member/list", `{}`, 0)
	r.gateway("alarm list", r.c.http2, http.MethodPost, "/v3/maintenance/alarm", `{}`, 1)
	r.gateway("auth status", r.c.http2, http.MethodPost, "/v3/auth/status", `{}`, 1)
}
