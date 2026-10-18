//go:build chaosenv

package env

import (
	"net"
	"strings"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/internal/peer"
	"github.com/djosh34/tigerstore/internal/testcert"
	"go.etcd.io/raft/v3/raftpb"
)

// Run inside a private network namespace: NewNetem changes lo's root qdisc.
// This is a diagnostic reference, not a test intended for merging.
func TestDiag241NetemHeal(t *testing.T) {
	for _, fault := range []string{"corrupt 5%", "reorder 50% 50%", "loss 20%"} {
		t.Run(fault, func(t *testing.T) {
			n, err := NewNetem(map[int]time.Duration{19001: 2 * time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := n.Close(); err != nil {
					t.Error(err)
				}
			})
			if err := n.Impair(19001, fault); err != nil {
				t.Fatal(err)
			}
			before, err := n.Stats(19001)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("impaired: %s", before.Qdisc)
			if err := n.Heal(19001); err != nil {
				t.Fatal(err)
			}
			after, err := n.Stats(19001)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("healed: %s", after.Qdisc)
			for _, option := range []string{" loss ", " corrupt ", " reorder "} {
				if strings.Contains(after.Qdisc, option) {
					t.Errorf("Heal retained %q: %s", strings.TrimSpace(option), after.Qdisc)
				}
			}
		})
	}
}

// Control: unlike corrupt/reorder, tc resets loss even when it is omitted.
func TestDiag241NetemHealRestoresDelivery(t *testing.T) {
	ln, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	port := ln.LocalAddr().(*net.UDPAddr).Port
	n, err := NewNetem(map[int]time.Duration{port: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := n.Close(); err != nil {
			t.Error(err)
		}
	})
	c, err := net.DialUDP("udp4", nil, ln.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	probe := func(label string) error {
		t.Helper()
		if _, err := c.Write([]byte(label)); err != nil {
			return err
		}
		if err := ln.SetReadDeadline(time.Now().Add(200 * time.Millisecond)); err != nil {
			return err
		}
		var buf [128]byte
		_, _, err := ln.ReadFromUDP(buf[:])
		return err
	}
	if err := probe("before"); err != nil {
		t.Fatalf("healthy link: %v", err)
	}
	if err := n.Impair(port, "loss 100%"); err != nil {
		t.Fatal(err)
	}
	if err := probe("during"); err == nil {
		t.Fatal("100% loss did not take effect")
	} else if e, ok := err.(net.Error); !ok || !e.Timeout() {
		t.Fatalf("loss probe setup: %v", err)
	}
	if err := n.Heal(port); err != nil {
		t.Fatal(err)
	}
	st, err := n.Stats(port)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("healed: %s", st.Qdisc)
	if err := probe("after"); err != nil {
		t.Fatalf("healed link still loses packets: %v", err)
	}
}

// Keep the real TLS/framing/heartbeat path, but make the retained corruption
// deterministic. Do not send during the impairment: after a real Heal there
// is no TCP retransmission backlog to confuse this regression.
func TestDiag241NetemHealRestoresPeerHeartbeats(t *testing.T) {
	ca, err := testcert.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	cert, _, _, err := ca.Issue("node", 2)
	if err != nil {
		t.Fatal(err)
	}
	lns := make([]net.Listener, 2)
	peers := map[uint64]string{}
	for i := range lns {
		lns[i], err = net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer lns[i].Close()
		peers[uint64(i+1)] = lns[i].Addr().String()
	}
	got := make(chan *raftpb.Message, 32)
	trs := make([]*peer.TCP, 2)
	for i := range trs {
		trs[i], err = peer.Start(peer.Config{
			ID: uint64(i + 1), Listeners: []net.Listener{lns[i]}, Peers: peers,
			Certificate: cert, CA: ca.Pool,
			Deliver:     func(m *raftpb.Message) { got <- m },
			Unreachable: func(uint64) {}, Logf: t.Logf,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer trs[i].Close()
	}
	probe := func(label string) bool {
		t.Helper()
		tick := time.NewTicker(20 * time.Millisecond)
		defer tick.Stop()
		deadline := time.NewTimer(2 * time.Second)
		defer deadline.Stop()
		for {
			select {
			case <-tick.C:
				trs[0].Send(&raftpb.Message{
					Type: raftpb.MsgHeartbeat.Enum(), From: new(uint64(1)), To: new(uint64(2)),
					Term: new(uint64(2)), Context: []byte(label),
				})
			case m := <-got:
				if string(m.Context) == label {
					return true
				}
			case <-deadline.C:
				return false
			}
		}
	}
	if !probe("before") {
		t.Fatal("no heartbeat on the healthy connection")
	}
	port := lns[1].Addr().(*net.TCPAddr).Port
	n, err := NewNetem(map[int]time.Duration{port: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	if err := n.Impair(port, "corrupt 100%"); err != nil {
		t.Fatal(err)
	}
	if err := n.Heal(port); err != nil {
		t.Fatal(err)
	}
	st, err := n.Stats(port)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("healed: %s", st.Qdisc)
	if !probe("after") {
		t.Fatal("no peer heartbeat in 2s after Heal; small writes did not force a reconnect")
	}
}
