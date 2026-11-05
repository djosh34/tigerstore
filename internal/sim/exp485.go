package sim

import (
	"os"
	"reflect"
	"strconv"
	"strings"
)

// exp485Duration is the length in seconds of the phase with faults that
// EXP485_DURATION sets for every DefaultConfig (research #485, exp branch
// only), 0 when it is not set.
func exp485Duration() int64 {
	d, err := strconv.ParseInt(os.Getenv("EXP485_DURATION"), 10, 64)
	if err != nil || d <= 0 {
		return 0
	}
	return d
}

// exp485Faults applies EXP485_FAULTS to a DefaultConfig: "none" turns
// every fault off, and a number k spaces every periodic fault (the
// fields named ...Every) k times further apart.
func exp485Faults(cfg *Config) {
	v := os.Getenv("EXP485_FAULTS")
	if v == "" {
		return
	}
	if v == "none" {
		cfg.Faults = Faults{}
		return
	}
	k, err := strconv.ParseInt(v, 10, 64)
	if err != nil || k <= 1 {
		return
	}
	f := reflect.ValueOf(&cfg.Faults).Elem()
	for i := range f.NumField() {
		if strings.HasSuffix(f.Type().Field(i).Name, "Every") && f.Field(i).Kind() == reflect.Int64 {
			f.Field(i).SetInt(f.Field(i).Int() * k)
		}
	}
}
