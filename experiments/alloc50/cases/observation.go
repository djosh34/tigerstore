package main

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

func observation(debugURL, path string, timeout time.Duration) ([]byte, error) {
	client := &http.Client{Timeout: timeout}
	r, err := client.Get(debugURL + path)
	if err != nil {
		return nil, err
	}
	b, readErr := io.ReadAll(r.Body)
	closeErr := r.Body.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return nil, err
	}
	if r.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("observation %s: %s", path, r.Status)
	}
	return b, nil
}

func captureGoroutines(debugURL, out, phase string) (string, error) {
	b, err := observation(debugURL, "/debug/goroutines", 3*time.Second)
	if err != nil {
		return "", err
	}
	file := out + "." + phase + ".goroutines.txt"
	if err := os.WriteFile(file, b, 0o644); err != nil {
		return "", err
	}
	return file, nil
}

func recordSender(report map[string]any, j senderSnapshot) {
	report["slow_sender"] = j
	report["slow_attempted_puts"] = j.Attempted
	report["slow_sent_headers"] = j.Headers
	report["slow_sent_data"] = j.Data
	report["slow_data_bytes"] = j.DataBytes
}
