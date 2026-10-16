package env

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Netem puts each of a set of loopback ports behind a netem qdisc of its
// own: a prio qdisc on lo sends the packets to and from each port to its
// band, and every other packet to band 1, which stays a plain FIFO. Each
// port has a base delay, which applies in both directions, and can get
// loss, reordering or corruption on top.
type Netem struct {
	bands map[int]int
	base  map[int]time.Duration
}

// NewNetem sets up netem for ports, with a base delay for each.
func NewNetem(base map[int]time.Duration) (*Netem, error) {
	if len(base) > 15 {
		return nil, fmt.Errorf("netem: %d ports, at most 15", len(base))
	}
	n := &Netem{bands: map[int]int{}, base: base}
	// Every priority to band 1 unless a filter says otherwise.
	args := []string{"tc", "qdisc", "add", "dev", "lo", "root", "handle", "1:", "prio", "bands", "16", "priomap"}
	for range 16 {
		args = append(args, "0")
	}
	if _, err := sudo(args...); err != nil {
		return nil, err
	}
	band := 2
	for port := range base {
		n.bands[port] = band
		band++
	}
	for port, b := range n.bands {
		if _, err := sudo("tc", "qdisc", "add", "dev", "lo", "parent", fmt.Sprintf("1:%d", b), "handle", fmt.Sprintf("%d:", b*10), "netem", "delay", ms(base[port])); err != nil {
			return nil, errors.Join(err, n.Close())
		}
		for _, dir := range []string{"dport", "sport"} {
			if _, err := sudo("tc", "filter", "add", "dev", "lo", "parent", "1:", "protocol", "ip", "prio", "1", "u32", "match", "ip", dir, strconv.Itoa(port), "0xffff", "flowid", fmt.Sprintf("1:%d", b)); err != nil {
				return nil, errors.Join(err, n.Close())
			}
		}
	}
	return n, nil
}

func ms(d time.Duration) string { return fmt.Sprintf("%.3fms", float64(d)/float64(time.Millisecond)) }

// Impair adds impairment to port's base delay, in netem's words, for
// example "loss 5%" or "reorder 25% 50%" or "corrupt 1%". Reordering needs
// a delay, which the base delay gives.
func (n *Netem) Impair(port int, impairment string) error {
	b, ok := n.bands[port]
	if !ok {
		return fmt.Errorf("netem: port %d is not set up", port)
	}
	args := []string{"tc", "qdisc", "change", "dev", "lo", "parent", fmt.Sprintf("1:%d", b), "handle", fmt.Sprintf("%d:", b*10), "netem", "delay", ms(max(n.base[port], time.Millisecond))}
	args = append(args, strings.Fields(impairment)...)
	_, err := sudo(args...)
	return err
}

// Heal takes port back to its base delay.
func (n *Netem) Heal(port int) error {
	b, ok := n.bands[port]
	if !ok {
		return fmt.Errorf("netem: port %d is not set up", port)
	}
	_, err := sudo("tc", "qdisc", "change", "dev", "lo", "parent", fmt.Sprintf("1:%d", b), "handle", fmt.Sprintf("%d:", b*10), "netem", "delay", ms(n.base[port]))
	return err
}

// NetemStats are the counters of one port's qdisc.
type NetemStats struct {
	Packets int64  `json:"packets"`
	Dropped int64  `json:"dropped"`
	Qdisc   string `json:"qdisc"`
}

var netemSent = regexp.MustCompile(`Sent \d+ bytes (\d+) pkt \(dropped (\d+)`)

// Stats reads port's counters.
func (n *Netem) Stats(port int) (NetemStats, error) {
	b, ok := n.bands[port]
	if !ok {
		return NetemStats{}, fmt.Errorf("netem: port %d is not set up", port)
	}
	out, err := sudo("tc", "-s", "qdisc", "show", "dev", "lo")
	if err != nil {
		return NetemStats{}, err
	}
	head := fmt.Sprintf("qdisc netem %d:", b*10)
	lines := strings.Split(out, "\n")
	for i, line := range lines {
		if !strings.HasPrefix(line, head) || i+1 >= len(lines) {
			continue
		}
		st := NetemStats{Qdisc: strings.TrimSpace(line)}
		m := netemSent.FindStringSubmatch(lines[i+1])
		if m == nil {
			return NetemStats{}, fmt.Errorf("netem: no counters for port %d in %q", port, lines[i+1])
		}
		var errP, errD error
		st.Packets, errP = strconv.ParseInt(m[1], 10, 64)
		st.Dropped, errD = strconv.ParseInt(m[2], 10, 64)
		return st, errors.Join(errP, errD)
	}
	return NetemStats{}, fmt.Errorf("netem: no qdisc for port %d in %q", port, out)
}

// Close removes every qdisc from lo.
func (n *Netem) Close() error {
	_, err := sudo("tc", "qdisc", "del", "dev", "lo", "root")
	return err
}
