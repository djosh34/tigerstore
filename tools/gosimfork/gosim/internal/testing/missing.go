package testing

import "github.com/jellevandenhooff/gosim/gosimruntime"

func AllocsPerRun(runs int, f func()) (avg float64) {
	gosimruntime.Unimplemented("testing.AllocsPerRun")
	panic("unreachable")
}

func CoverMode() {
	gosimruntime.Unimplemented("testing.CoverMode")
}

func Coverage() float64 {
	gosimruntime.Unimplemented("testing.Coverage")
	panic("unreachable")
}

func Init() {
	gosimruntime.Unimplemented("testing.Init")
}

func Main(matchString func(pat, str string) (bool, error), tests []InternalTest, benchmarks []InternalBenchmark, examples []InternalExample) {
	gosimruntime.Unimplemented("testing.Main")
}

func RegisterCover(c Cover) {
	gosimruntime.Unimplemented("testing.RegisterCover")
}

func RunBenchmarks(matchString func(pat, str string) (bool, error), benchmarks []InternalBenchmark) {
	gosimruntime.Unimplemented("testing.RunBenchmarks")
}

func RunExamples(matchString func(pat, str string) (bool, error), examples []InternalExample) (ok bool) {
	gosimruntime.Unimplemented("testing.RunExamples")
	panic("unreachable")
}

func RunTests(matchString func(pat, str string) (bool, error), tests []InternalTest) (ok bool) {
	gosimruntime.Unimplemented("testing.RunTests")
	panic("unreachable")
}

func Testing() bool {
	gosimruntime.Unimplemented("testing.Testing")
	panic("unreachable")
}

func Verbose() bool {
	gosimruntime.Unimplemented("testing.Verbose")
	panic("unreachable")
}

type Cover struct {
	Mode            string
	Counters        map[string][]uint32
	Blocks          map[string][]CoverBlock
	CoveredPackages string
}

type CoverBlock struct {
	Line0 uint32 // Line number for block start.
	Col0  uint16 // Column number for block start.
	Line1 uint32 // Line number for block end.
	Col1  uint16 // Column number for block end.
	Stmts uint16 // Number of statements included in this block.
}

type InternalBenchmark struct {
	Name string
	F    func(b *B)
}

type InternalExample struct {
	Name      string
	F         func()
	Output    string
	Unordered bool
}

type InternalFuzzTarget struct {
	Name string
	Fn   func(f *F)
}

type InternalTest struct {
	Name string
	F    func(*T)
}

// F is a fuzz target's argument. gosim does not run fuzz targets, so tests
// that only compile with them still translate.
type F struct {
	common
}

func (f *F) Add(args ...any) {
	gosimruntime.Unimplemented("testing.F.Add")
}

func (f *F) Fuzz(ff any) {
	gosimruntime.Unimplemented("testing.F.Fuzz")
}

type PB struct{}

// TODO: missing methods
