package main

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// Optional measurement-only collector. Both rate-enabled runtime overlays are
// archived; these cumulative endpoints are bracketed so pprof can subtract the
// pre-window baseline. The trace is two seconds within the measured window.
func serverOffCPUProfile(scheme string, endpoints []string, start, stop int64) <-chan error {
	done := make(chan error, 1)
	dir := os.Getenv("FETCH_PROFILE_OFFCPU_DIR")
	if dir == "" {
		done <- nil
		return done
	}
	if len(endpoints) == 0 {
		done <- errors.New("off-CPU profile needs a leader endpoint")
		return done
	}
	go func() {
		client, _ := metricsClient()
		client.Timeout = 20 * time.Second
		defer client.CloseIdleConnections()
		done <- collectOffCPU(client, scheme+"://"+endpoints[0], dir, start, stop)
	}()
	return done
}

func collectOffCPU(client *http.Client, endpoint, dir string, start, stop int64) error {
	if stop-start < int64(5*time.Second) {
		return errors.New("off-CPU diagnostic needs a window of at least five seconds")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	get := func(path, filename string) error {
		resp, err := client.Get(endpoint + path)
		if err != nil {
			return err
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("off-CPU %s: %s", path, resp.Status)
		}
		f, err := os.Create(filepath.Join(dir, filename))
		if err != nil {
			return err
		}
		_, err = io.Copy(f, resp.Body)
		closeErr := f.Close()
		if err != nil {
			return err
		}
		return closeErr
	}
	snapshot := func(label string) error {
		for _, kind := range []string{"block", "mutex"} {
			if err := get("/debug/pprof/"+kind, label+"."+kind+".pprof"); err != nil {
				return err
			}
		}
		return get("/metrics", label+".metrics")
	}
	time.Sleep(time.Duration(start - now()))
	if err := snapshot("before"); err != nil {
		return err
	}
	time.Sleep(time.Duration(start + int64(2*time.Second) - now()))
	if err := get("/debug/pprof/trace?seconds=2", "execution.trace"); err != nil {
		return err
	}
	time.Sleep(time.Duration(stop - now()))
	return snapshot("after")
}
