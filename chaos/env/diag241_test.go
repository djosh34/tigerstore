//go:build chaosenv

package env

import (
	"net"
	"strings"
	"testing"
	"time"
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

// A stronger loss setting turns the same retained-option bug into a
// deterministic packet-delivery failure rather than a probabilistic wait.
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
