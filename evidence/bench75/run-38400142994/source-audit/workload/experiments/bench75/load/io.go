package main

import (
	"errors"
	"strconv"
	"strings"
)

func parseIO(b []byte) (uint64, uint64, error) {
	counts := map[string]uint64{}
	for line := range strings.SplitSeq(string(b), "\n") {
		key, value, found := strings.Cut(line, ":")
		if !found || (key != "syscr" && key != "syscw") {
			continue
		}
		if _, duplicate := counts[key]; duplicate {
			return 0, 0, errors.New("duplicate proc io counter")
		}
		v, err := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
		if err != nil {
			return 0, 0, err
		}
		counts[key] = v
	}
	r, hasRead := counts["syscr"]
	w, hasWrite := counts["syscw"]
	if !hasRead || !hasWrite {
		return 0, 0, errors.New("missing proc io counter")
	}
	return r, w, nil
}
