//go:build !faultfs

package main

import (
	"errors"
	"os"
)

func serve(_ *os.File, _ string) error {
	return errors.New("control requires a -tags=faultfs build")
}
