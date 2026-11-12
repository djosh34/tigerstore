package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// errNoMetric is a /metrics page without the leader gauge, and
// errLateMetrics a read that ended too long after the moment it stands for.
var (
	errNoMetric    = errors.New("metric missing")
	errLateMetrics = errors.New("/metrics read too late")
)

// nodeMetrics is what one node's /metrics says at one moment: the bytes it
// has sent to its peers so far, and whether it leads.
type nodeMetrics struct {
	peerSent float64
	leader   bool
}

// windowMetrics reads every endpoint's /metrics at the start and at the
// stop of the window, both given as now() times. Each read must end within
// slack of the moment it stands for, or its counters would count load from
// outside the window. One read before the start opens the connections the
// two reads in the window use, so those pay no TLS handshake.
func windowMetrics(endpoints []string, start, stop int64, slack time.Duration) ([2][]nodeMetrics, error) {
	var out [2][]nodeMetrics
	client, scheme := metricsClient()
	defer client.CloseIdleConnections()
	if _, err := sampleMetrics(client, scheme, endpoints, math.MaxInt64); err != nil {
		return out, err
	}
	for i, at := range []int64{start, stop} {
		time.Sleep(time.Duration(at - now()))
		var err error
		if out[i], err = sampleMetrics(client, scheme, endpoints, at+int64(slack)); err != nil {
			return out, err
		}
	}
	return out, nil
}

// metricsClient is an HTTP/1.1 client of its own, apart from the load's
// connections, with one connection kept per endpoint.
func metricsClient() (*http.Client, string) {
	transport := &http.Transport{}
	scheme := "http"
	if clientTLS != nil {
		transport.TLSClientConfig = clientTLS.Clone()
		transport.TLSClientConfig.NextProtos = []string{"http/1.1"}
		scheme = "https"
	}
	return &http.Client{Timeout: 5 * time.Second, Transport: transport}, scheme
}

// sampleMetrics reads every endpoint's /metrics at once, in the order
// given. Reads that end after deadline, a now() time, are too late.
func sampleMetrics(client *http.Client, scheme string, endpoints []string, deadline int64) ([]nodeMetrics, error) {
	out := make([]nodeMetrics, len(endpoints))
	errs := make([]error, len(endpoints))
	var wg sync.WaitGroup
	for i, ep := range endpoints {
		wg.Go(func() { out[i], errs[i] = fetchMetrics(client, scheme+"://"+ep+"/metrics") })
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	if now() > deadline {
		return nil, errLateMetrics
	}
	return out, nil
}

func fetchMetrics(client *http.Client, url string) (nodeMetrics, error) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		return nodeMetrics{}, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nodeMetrics{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nodeMetrics{}, fmt.Errorf("%s: %s", url, resp.Status)
	}
	m, err := parseMetrics(resp.Body)
	if err != nil {
		return m, fmt.Errorf("%s: %w", url, err)
	}
	return m, nil
}

// parseMetrics reads the Prometheus text format: it sums
// etcd_network_peer_sent_bytes_total over its peers and reads
// etcd_server_is_leader.
func parseMetrics(r io.Reader) (nodeMetrics, error) {
	var m nodeMetrics
	sawLeader := false
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		name, rest, ok := strings.Cut(line, " ")
		if !ok || strings.HasPrefix(line, "#") {
			continue
		}
		name, _, _ = strings.Cut(name, "{")
		if name != "etcd_network_peer_sent_bytes_total" && name != "etcd_server_is_leader" {
			continue
		}
		// A label value may hold a space, so the value is the last field.
		v, err := strconv.ParseFloat(rest[strings.LastIndexByte(rest, ' ')+1:], 64)
		if err != nil {
			return m, fmt.Errorf("%s: %w", name, err)
		}
		if name == "etcd_server_is_leader" {
			m.leader, sawLeader = v == 1, true
		} else {
			m.peerSent += v
		}
	}
	if err := sc.Err(); err != nil {
		return m, err
	}
	if !sawLeader {
		return m, fmt.Errorf("etcd_server_is_leader: %w", errNoMetric)
	}
	return m, nil
}

// peerBytesPerOp is each node's peer bytes sent between two samples per
// operation, the node that led at the end first, then the others in the
// order of the endpoints. A cluster with no leader at the end, or two,
// has nothing to report.
func peerBytesPerOp(from, to []nodeMetrics, ops int64) []float64 {
	if ops == 0 || len(from) != len(to) {
		return nil
	}
	lead := -1
	for i, m := range to {
		if m.leader {
			if lead >= 0 {
				return nil
			}
			lead = i
		}
	}
	if lead < 0 {
		return nil
	}
	per := func(i int) float64 { return (to[i].peerSent - from[i].peerSent) / float64(ops) }
	out := []float64{per(lead)}
	for i := range to {
		if i != lead {
			out = append(out, per(i))
		}
	}
	return out
}
