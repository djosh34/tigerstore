//go:build sim || simio

package wl

import "time"

type turnWatch struct{}

func newTurnWatch() *turnWatch             { return &turnWatch{} }
func (*turnWatch) begin(time.Time)         {}
func (*turnWatch) end()                    {}
func (*turnWatch) report(time.Time) string { return "" }
