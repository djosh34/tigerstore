package main

import (
	"bytes"
	"fmt"
	"os"

	"golang.org/x/net/http2"
)

var debugFrames = os.Getenv("LG2DEBUG") != ""

// fault injection to prove the checks fire: enc, drop, rev
var fault = os.Getenv("LG2FAULT")

func dumpFrames(b []byte) {
	if bytes.HasPrefix(b, []byte(http2.ClientPreface)) {
		b = b[len(http2.ClientPreface):]
	}
	fr := http2.NewFramer(nil, bytes.NewReader(b))
	for {
		f, err := fr.ReadFrame()
		if err != nil {
			fmt.Fprintln(os.Stderr, "dump end:", err)
			return
		}
		fmt.Fprintln(os.Stderr, "send:", f.Header())
	}
}
