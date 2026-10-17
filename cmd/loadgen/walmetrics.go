package main

import (
	"bufio"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func sampleWAL(endpoints string) (float64, error) {
	client := &http.Client{Timeout: 2 * time.Second}
	var total float64
	for _, endpoint := range strings.Split(endpoints, ",") {
		resp, err := client.Get(endpoint + "/metrics")
		if err != nil {
			return 0, err
		}
		var value float64
		found := false
		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			fields := strings.Fields(scanner.Text())
			if len(fields) == 2 && fields[0] == "etcd_disk_wal_write_bytes_total" {
				value, err = strconv.ParseFloat(fields[1], 64)
				found = true
				break
			}
		}
		err = errors.Join(err, scanner.Err(), resp.Body.Close())
		if err != nil {
			return 0, err
		}
		if resp.StatusCode != http.StatusOK || !found {
			return 0, fmt.Errorf("no WAL write counter at %s: HTTP %d", endpoint, resp.StatusCode)
		}
		total += value
	}
	return total, nil
}
