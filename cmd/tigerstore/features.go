package main

import (
	"fmt"
	"strconv"
	"strings"
)

// featureGates are etcd v3.7.2's server feature gates
// (server/features/etcd_features.go), each with whether it is alpha and
// its default, set with --feature-gates as etcd parses it
// (pkg/featuregate).
type featureGates map[string]bool

var gateSpecs = map[string]struct{ alpha, on bool }{
	"StopGRPCServiceOnDefrag":      {alpha: true},
	"InitialCorruptCheck":          {alpha: true},
	"CompactHashCheck":             {alpha: true},
	"TxnModeWriteWithSharedBuffer": {on: true},
	"LeaseCheckpoint":              {alpha: true},
	"LeaseCheckpointPersist":       {alpha: true},
	"SetMemberLocalAddr":           {alpha: true},
	"FastLeaseKeepAlive":           {on: true},
	"PriorityRequest":              {alpha: true},
}

func newFeatureGates() featureGates {
	g := featureGates{}
	for name, spec := range gateSpecs {
		g[name] = spec.on
	}
	return g
}

// Set parses "key1=value1,key2=value2". AllAlpha and AllBeta set every
// alpha or beta gate; a gate named on its own wins over them.
func (g featureGates) Set(value string) error {
	set := map[string]bool{}
	for s := range strings.SplitSeq(value, ",") {
		if s == "" {
			continue
		}
		k, v, ok := strings.Cut(s, "=")
		k = strings.TrimSpace(k)
		if !ok {
			return fmt.Errorf("missing bool value for %s", k)
		}
		v = strings.TrimSpace(v)
		b, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("invalid value of %s=%s, err: %w", k, v, err)
		}
		if _, known := gateSpecs[k]; !known && k != "AllAlpha" && k != "AllBeta" {
			return fmt.Errorf("unrecognized feature gate: %s", k)
		}
		set[k] = b
	}
	for name, spec := range gateSpecs {
		if all, ok := set["AllAlpha"]; ok && spec.alpha {
			g[name] = all
		}
		if all, ok := set["AllBeta"]; ok && !spec.alpha {
			g[name] = all
		}
	}
	for k, b := range set {
		if k != "AllAlpha" && k != "AllBeta" {
			g[k] = b
		}
	}
	return nil
}

func (g featureGates) String() string {
	var pairs []string
	for k, v := range g {
		pairs = append(pairs, fmt.Sprintf("%s=%t", k, v))
	}
	return strings.Join(pairs, ",")
}
