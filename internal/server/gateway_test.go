package server_test

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// gatewayClient is an HTTPS client of node id's client port with cert.
func (c *cluster) gatewayClient(cert tls.Certificate, h2 bool) *http.Client {
	tr := &http.Transport{TLSClientConfig: &tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: c.ca.Pool, ServerName: "localhost"}, ForceAttemptHTTP2: h2}
	if !h2 {
		tr.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
	}
	return &http.Client{Transport: tr, Timeout: 10 * time.Second}
}

// post sends a gateway request and returns its status and body.
func post(t *testing.T, hc *http.Client, url, body string, header ...string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	resp, err := hc.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck // closing a test response
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(b)
}

// TestGateway runs etcd's gRPC gateway on tigerstore: JSON calls over
// HTTP/2 and HTTP/1.1, a watch stream, and, with auth on, a token in the
// Authorization header and the refusal of a certificate with a name.
func TestGateway(t *testing.T) {
	c := newCluster(t)
	named, _, _, err := c.ca.Issue("client", 83)
	if err != nil {
		t.Fatal(err)
	}
	anonymous, _, _, err := c.ca.Issue("", 84)
	if err != nil {
		t.Fatal(err)
	}
	base := "https://" + c.running[1].addr
	k := base64.StdEncoding.EncodeToString([]byte("gw"))
	v := base64.StdEncoding.EncodeToString([]byte("1"))
	for _, h2 := range []bool{true, false} {
		hc := c.gatewayClient(named, h2)
		var code int
		var body string
		for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(50 * time.Millisecond) {
			code, body = post(t, hc, base+"/v3/kv/put", `{"key":"`+k+`","value":"`+v+`"}`)
			if code == http.StatusOK || time.Now().After(deadline) {
				break
			}
		}
		if code != http.StatusOK || !strings.Contains(body, `"revision"`) {
			t.Fatalf("put over h2 %v: %d %s", h2, code, body)
		}
		code, body = post(t, hc, base+"/v3/kv/range", `{"key":"`+k+`"}`)
		if code != http.StatusOK || !strings.Contains(body, `"value":"`+v+`"`) {
			t.Fatalf("range over h2 %v: %d %s", h2, code, body)
		}
		code, body = post(t, hc, base+"/v3/kv/range", `{}`)
		if code != http.StatusBadRequest || !strings.Contains(body, "key is not provided") {
			t.Fatalf("range without a key over h2 %v: %d %s", h2, code, body)
		}
	}

	// A watch stream: the created response, then the event.
	req, err := http.NewRequest(http.MethodPost, base+"/v3/watch", strings.NewReader(`{"create_request":{"key":"`+k+`","start_revision":"1"}}`))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := c.gatewayClient(named, true).Do(req.WithContext(ctx))
	if err != nil {
		t.Fatal(err)
	}
	sc := bufio.NewScanner(resp.Body)
	var lines []string
	for len(lines) < 2 && sc.Scan() {
		lines = append(lines, sc.Text())
	}
	cancel()
	_ = resp.Body.Close()
	if len(lines) != 2 || !strings.Contains(lines[0], `"created":true`) || !strings.Contains(lines[1], `"events"`) {
		t.Fatalf("watch: %q", lines)
	}

	// Auth: root with the root role, enabled through the gateway.
	hc := c.gatewayClient(anonymous, true)
	for _, step := range []struct{ path, body string }{
		{"/v3/auth/user/add", `{"name":"root","password":"pw"}`},
		{"/v3/auth/role/add", `{"name":"root"}`},
		{"/v3/auth/user/grant", `{"user":"root","role":"root"}`},
		{"/v3/auth/enable", `{}`},
	} {
		if code, body := post(t, hc, base+step.path, step.body); code != http.StatusOK {
			t.Fatalf("%s: %d %s", step.path, code, body)
		}
	}
	for deadline := time.Now().Add(5 * time.Second); !c.nodes[1].AuthEnabled(); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("auth is not on")
		}
	}
	code, body := post(t, hc, base+"/v3/kv/range", `{"key":"`+k+`"}`)
	if code != http.StatusBadRequest || !strings.Contains(body, "user name is empty") {
		t.Fatalf("range without a token: %d %s", code, body)
	}
	code, body = post(t, hc, base+"/v3/auth/authenticate", `{"name":"root","password":"pw"}`)
	var auth struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal([]byte(body), &auth); code != http.StatusOK || err != nil || auth.Token == "" {
		t.Fatalf("authenticate: %d %s", code, body)
	}
	if code, body := post(t, hc, base+"/v3/kv/range", `{"key":"`+k+`"}`, "Authorization", auth.Token); code != http.StatusOK {
		t.Fatalf("range with the token: %d %s", code, body)
	}
	code, body = post(t, c.gatewayClient(named, true), base+"/v3/kv/range", `{"key":"`+k+`"}`, "Authorization", auth.Token)
	if code != http.StatusBadRequest || !strings.Contains(body, "CommonName of client sending a request against gateway will be ignored") {
		t.Fatalf("a named certificate with auth on: %d %s", code, body)
	}
}
