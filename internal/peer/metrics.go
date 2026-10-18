package peer

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

// counters are the metrics of the traffic with one peer, labeled as
// etcd's rafthttp labels them, with the peers' IDs in hexadecimal.
type counters struct {
	sent, sendFailures, received, receiveFailures, disconnected prometheus.Counter
	active, heartbeatSent, heartbeatReceived                    prometheus.Gauge
}

func (t *TCP) countersOf(id uint64) *counters {
	m := t.cfg.Metrics
	local, remote := strconv.FormatUint(t.cfg.ID, 16), strconv.FormatUint(id, 16)
	return &counters{
		sent:              m.PeerSent.WithLabelValues(remote),
		sendFailures:      m.PeerSentFailures.WithLabelValues(remote),
		received:          m.PeerReceived.WithLabelValues(remote),
		receiveFailures:   m.PeerReceivedFailures.WithLabelValues(remote),
		disconnected:      m.DisconnectedPeers.WithLabelValues(local, remote),
		active:            m.ActivePeers.WithLabelValues(local, remote),
		heartbeatSent:     m.HeartbeatSent.WithLabelValues(remote),
		heartbeatReceived: m.HeartbeatReceived.WithLabelValues(remote),
	}
}
