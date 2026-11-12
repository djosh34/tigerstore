package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// errNoMetric is a /metrics page without the leader gauge.
var errNoMetric = errors.New("metric missing")

// nodeMetrics is what one node's /metrics says at one moment: the bytes it
// has sent to its peers so far, and whether it leads.
type nodeMetrics struct {
	peerSent float64
	leader   bool
}

// sampleMetrics reads every endpoint's /metrics, in the order given, over
// HTTP/1.1 connections of its own, apart from the load's.
func sampleMetrics(endpoints []string) ([]nodeMetrics, error) {
	transport := &http.Transport{DisableKeepAlives: true}
	scheme := "http"
	if clientTLS != nil {
		transport.TLSClientConfig = clientTLS.Clone()
		transport.TLSClientConfig.NextProtos = []string{"http/1.1"}
		scheme = "https"
	}
	client := &http.Client{Timeout: 5 * time.Second, Transport: transport}
	out := make([]nodeMetrics, len(endpoints))
	for i, ep := range endpoints {
		m, err := fetchMetrics(client, scheme+"://"+ep+"/metrics")
		if err != nil {
			return nil, err
		}
		out[i] = m
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
