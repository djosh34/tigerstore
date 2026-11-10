// Command k8strace builds a replay trace from a cmd/etcdrec log, and shows
// what a trace holds.
//
//	k8strace convert -shape <name> -about <text> -start <ns> -end <ns> -out <file.trace.gz> <log.jsonl.gz>
//	k8strace show <file.trace.gz>...
//
// convert prints the trace's summary as Markdown. -start and -end are the
// window, in ns since the Unix epoch, as k8stest/record.sh writes them.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/djosh34/tigerstore/internal/k8strace"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "k8strace:", err)
		os.Exit(1)
	}
}

var errUsage = errors.New("usage: k8strace convert [flags] <log>, or k8strace show <trace> [<trace>]")

func run(args []string) error {
	if len(args) == 0 {
		return errUsage
	}
	switch args[0] {
	case "convert":
		return convert(args[1:])
	case "show":
		for _, path := range args[1:] {
			t, err := k8strace.Read(path)
			if err != nil {
				return err
			}
			if _, err := fmt.Fprintln(os.Stdout, k8strace.Markdown(t, nil)); err != nil {
				return err
			}
		}
		return nil
	}
	return errUsage
}

func convert(args []string) error {
	fs := flag.NewFlagSet("convert", flag.ContinueOnError)
	shape := fs.String("shape", "", "the cluster shape's name")
	about := fs.String("about", "", "how the recording was made")
	start := fs.Int64("start", 0, "the window's start, ns since the Unix epoch")
	end := fs.Int64("end", 0, "the window's end")
	out := fs.String("out", "", "the trace to write")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 || *out == "" || *end <= *start {
		return errUsage
	}
	f, err := os.Open(fs.Arg(0))
	if err != nil {
		return err
	}
	t, dropped, err := k8strace.Convert(f, k8strace.Window{Start: *start, End: *end})
	if err := errors.Join(err, f.Close()); err != nil {
		return err
	}
	t.Shape, t.About = *shape, *about
	if err := t.Write(*out); err != nil {
		return err
	}
	_, err = fmt.Fprint(os.Stdout, k8strace.Markdown(t, dropped))
	return err
}
