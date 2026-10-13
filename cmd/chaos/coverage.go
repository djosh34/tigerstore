package main

import (
	"errors"
	"fmt"
	"reflect"
	"unicode/utf8"

	"github.com/djosh34/tigerstore/history"
)

func utf8Request(v reflect.Value) error {
	//nolint:exhaustive // Only these kinds can hold strings; the others pass.
	switch v.Kind() {
	case reflect.Pointer:
		if !v.IsNil() {
			return utf8Request(v.Elem())
		}
	case reflect.Struct:
		for _, f := range v.Fields() {
			if err := utf8Request(f); err != nil {
				return err
			}
		}
	case reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			if err := utf8Request(v.Index(i)); err != nil {
				return err
			}
		}
	case reflect.String:
		if !utf8.ValidString(v.String()) {
			return errors.New("non-UTF8 bytes unsupported by JSON history")
		}
	}
	return nil
}

type coverageReport struct {
	Ops                                                                                int `json:"ops"`
	Put, TxnSuccess, TxnFailure, Delete, MultiDelete, Range, Ambiguous, DefiniteErrors int
	WatchPuts, WatchDeletes, WatchPrevKV                                               int
	PerClientSuccess                                                                   map[int]int   `json:"per_client_success"`
	LastSuccessNS                                                                      map[int]int64 `json:"last_success_ns"`
}

func coverage(h *history.History) (coverageReport, error) {
	c := coverageReport{Ops: len(h.Ops), PerClientSuccess: map[int]int{}, LastSuccessNS: map[int]int64{}}
	for _, op := range h.Ops {
		if op.Resp == nil && op.Err == nil {
			return c, errors.New("malformed op has neither response nor error")
		}
		if op.Err != nil {
			if op.Err.Ambiguous {
				c.Ambiguous++
			} else {
				c.DefiniteErrors++
			}
			continue
		}
		c.PerClientSuccess[op.Client]++
		c.LastSuccessNS[op.Client] = max(c.LastSuccessNS[op.Client], op.Return)
		switch {
		case op.Resp.Put != nil:
			c.Put++
		case op.Resp.Txn != nil:
			if op.Resp.Txn.Succeeded {
				c.TxnSuccess++
			} else {
				c.TxnFailure++
			}
		case op.Resp.Delete != nil:
			c.Delete++
			if op.Resp.Delete.Deleted > 1 {
				c.MultiDelete++
			}
		case op.Resp.Range != nil:
			c.Range++
		}
	}
	var errs []error
	for i, w := range h.Watches {
		progress := false
		for _, r := range w.Responses {
			if r.Progress && r.Revision >= h.Target && h.Target > 0 {
				progress = true
			}
			if r.Canceled {
				errs = append(errs, fmt.Errorf("watch %d canceled in a no-compaction campaign", i))
			}
			for _, e := range r.Events {
				switch e.Type {
				case history.EventPut:
					c.WatchPuts++
				case history.EventDelete:
					c.WatchDeletes++
				}
				if e.PrevKV != nil {
					c.WatchPrevKV++
				}
			}
		}
		if !progress {
			errs = append(errs, fmt.Errorf("watch %d has no progress through target", i))
		}
	}
	if c.Put == 0 || c.TxnSuccess == 0 || c.TxnFailure == 0 || c.Delete == 0 || c.MultiDelete == 0 || c.Range == 0 || c.WatchPuts == 0 || c.WatchDeletes == 0 || c.WatchPrevKV == 0 {
		errs = append(errs, errors.New("coverage floor: require successful Put, both CAS branches, Delete, multi-key Delete, Range and watch puts/deletes/previous values"))
	}
	for i := range 3 {
		if c.PerClientSuccess[i] == 0 {
			errs = append(errs, fmt.Errorf("client %d has no successful operation", i))
		}
	}
	if len(h.Watches) != 3 {
		errs = append(errs, errors.New("coverage floor: require three member-pinned logical watches"))
	}
	return c, errors.Join(errs...)
}
