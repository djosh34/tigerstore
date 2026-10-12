// Question: can a controller arm and witness real-file faults in another Go process?
// This is a throwaway worker, not tigerstore or a durability model.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
)

func main() {
	data := flag.String("data", "", "owned data file")
	socket := flag.String("control", "", "test-only Unix control socket")
	flag.Parse()
	if err := run(*data, *socket); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(path, socket string) error {
	if path == "" {
		return errors.New("--data is required")
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	if socket != "" {
		return serve(f, socket)
	}
	_, err = f.WriteAt([]byte("healthy\n"), 0)
	if err != nil {
		return err
	}
	return f.Sync()
}
