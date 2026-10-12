package main

import (
	"encoding/csv"
	"sort"
	"strconv"
)

type statKey struct{ split, resource, format, bin string }
type byteStats struct {
	count, keys, values int64
	min, max            int
}

func inventory(w *csv.Writer, rs []record) error {
	stats := make(map[statKey]byteStats)
	for _, r := range rs {
		split := "test80"
		if r.train {
			split = "train20"
		}
		for _, s := range []string{"all", split} {
			for _, res := range []string{"*", r.resource} {
				for _, form := range []string{"*", r.format} {
					for _, bin := range []string{"*", r.bin} {
						k := statKey{s, res, form, bin}
						v := stats[k]
						if v.count == 0 || len(r.Value) < v.min {
							v.min = len(r.Value)
						}
						v.max = max(v.max, len(r.Value))
						v.count++
						v.keys += int64(len(r.Key))
						v.values += int64(len(r.Value))
						stats[k] = v
					}
				}
			}
		}
	}
	keys := make([]statKey, 0, len(stats))
	for k := range stats {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		if a.split != b.split {
			return a.split < b.split
		}
		if a.resource != b.resource {
			return a.resource < b.resource
		}
		if a.format != b.format {
			return a.format < b.format
		}
		return a.bin < b.bin
	})
	for _, k := range keys {
		s := stats[k]
		if err := w.Write([]string{k.split, k.resource, k.format, k.bin, num(s.count), num(s.keys), num(s.values), num(s.keys + s.values), strconv.Itoa(s.min), strconv.Itoa(s.max)}); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}
