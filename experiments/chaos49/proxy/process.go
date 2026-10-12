package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
)

// Linux /proc accounting isolates the proxy. CLK_TCK is queried rather than
// assumed. Deltas have one-tick resolution; report raw ticks as well as ns.
type relayProcess struct {
	cmd   *exec.Cmd
	addr  string
	hz    int64
	drain chan struct{}
}

func startRelay(upstream string, seed uint64) (*relayProcess, error) {
	hzText, e := exec.Command("getconf", "CLK_TCK").Output()
	if e != nil {
		return nil, e
	}
	hz, e := strconv.ParseInt(strings.TrimSpace(string(hzText)), 10, 64)
	if e != nil || hz <= 0 {
		return nil, fmt.Errorf("invalid CLK_TCK %q", hzText)
	}
	executable, e := os.Executable()
	if e != nil {
		return nil, e
	}
	cmd := exec.Command(executable, "-command", "serve", "-mode", "none", "-upstream", upstream, "-seed", strconv.FormatUint(seed, 10))
	cmd.Stderr = os.Stderr
	pipe, e := cmd.StdoutPipe()
	if e != nil {
		return nil, e
	}
	if e = cmd.Start(); e != nil {
		return nil, e
	}
	r := bufio.NewReader(pipe)
	first, e := r.ReadBytes('\n')
	if e != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return nil, e
	}
	var line struct {
		Address string `json:"address"`
	}
	if e = json.Unmarshal(first, &line); e != nil || line.Address == "" {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return nil, fmt.Errorf("invalid relay line %q", first)
	}
	p := &relayProcess{cmd: cmd, addr: line.Address, hz: hz, drain: make(chan struct{})}
	go func() { _, _ = io.Copy(io.Discard, r); close(p.drain) }()
	return p, nil
}
func (p *relayProcess) ticks() (int64, error) {
	if p == nil {
		return 0, nil
	}
	b, e := os.ReadFile(fmt.Sprintf("/proc/%d/stat", p.cmd.Process.Pid))
	if e != nil {
		return 0, e
	}
	end := strings.LastIndex(string(b), ")")
	if end < 0 {
		return 0, fmt.Errorf("invalid proc stat")
	}
	fields := strings.Fields(string(b)[end+1:])
	if len(fields) < 13 {
		return 0, fmt.Errorf("short proc stat")
	}
	u, e := strconv.ParseInt(fields[11], 10, 64)
	if e != nil {
		return 0, e
	}
	s, e := strconv.ParseInt(fields[12], 10, 64)
	if e != nil {
		return 0, e
	}
	return u + s, nil
}
func (p *relayProcess) stop() error {
	if p == nil {
		return nil
	}
	if e := p.cmd.Process.Signal(syscall.SIGTERM); e != nil {
		return e
	}
	<-p.drain
	return p.cmd.Wait()
}
func affinity(pid int) string {
	b, e := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if e != nil {
		return "unavailable: " + e.Error()
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "Cpus_allowed_list:") {
			return strings.TrimSpace(strings.TrimPrefix(line, "Cpus_allowed_list:"))
		}
	}
	return "unavailable"
}
func tickSample(p *relayProcess) int64 {
	v, e := p.ticks()
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
	return v
}
