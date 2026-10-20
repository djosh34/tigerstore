package gosimruntime

import (
 "flag"
 "fmt"
 "os"
 "path/filepath"
 "strconv"
)

// Native flag bridge: gosim normally parses flags before translated flag init.
var tigerSeed333 = flag.Uint64("sim.seed", 1, "tigerstore simulator seed (experiment #333)")
var tigerSeeds333 = flag.Int("sim.seeds", 1, "tigerstore simulator seed count (experiment #333)")
var tigerOut333 = flag.String("r333.output", "", "host diagnostic output directory")
var tigerRecording333 int
var port333Reached = map[string]bool{}

// Diagnostic marker only; no scheduler/time/randomness or result is fed back.
func Port333Reached(name string) {
 if !port333Reached[name] { port333Reached[name] = true; fmt.Fprintln(os.Stderr, "R333_REACHED", name) }
}

func Tigerstore333Options() (string, string) {
 return strconv.FormatUint(*tigerSeed333, 10), strconv.Itoa(*tigerSeeds333)
}

// Controlled diagnostic escape: copies immutable serialized results to host files.
// No host time/randomness/results are fed back to the simulated program.
func RecordTigerstore333(trace, history string) {
 if *tigerOut333 == "" { return }
 tigerRecording333++
 dir := filepath.Join(*tigerOut333, fmt.Sprintf("run-%03d", tigerRecording333))
 if err := os.MkdirAll(dir, 0755); err != nil { panic(err) }
 for _, f := range []struct{name, data string}{{"trace.txt",trace},{"history.json",history}} {
  if err := os.WriteFile(filepath.Join(dir,f.name), []byte(f.data), 0644); err != nil { panic(err) }
 }
}
