package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

// Optional diagnostic only. The request spans the unchanged measured window;
// its result is collected after the two normal metric samples. No gate uses a
// profiled run. Client TLS and leader placement are those of the actual load.
func serverProfile(scheme string, endpoints []string, start, stop int64) <-chan error {
	done := make(chan error, 1)
	path := os.Getenv("FETCH_PROFILE_LEADER")
	if path == "" {
		done <- nil
		return done
	}
	if len(endpoints) == 0 {
		done <- fmt.Errorf("leader profile needs a metric endpoint")
		return done
	}
	go func() {
		client, _ := metricsClient()
		client.Timeout = time.Duration(stop-start) + 10*time.Second
		defer client.CloseIdleConnections()
		time.Sleep(time.Duration(start - now()))
		url := fmt.Sprintf("%s://%s/debug/pprof/profile?seconds=%d", scheme, endpoints[0], (stop-start)/int64(time.Second))
		resp, err := client.Get(url)
		if err != nil {
			done <- err
			return
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			done <- fmt.Errorf("leader profile: %s", resp.Status)
			return
		}
		f, err := os.Create(path)
		if err != nil {
			done <- err
			return
		}
		_, err = io.Copy(f, resp.Body)
		if closeErr := f.Close(); err == nil {
			err = closeErr
		}
		done <- err
	}()
	return done
}
