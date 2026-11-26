package check

// An Option changes how Witnessed or Linearizable runs.
type Option func(*options)

type options struct {
	steps     int64
	stepsUsed *int64
	stop      func() bool
	// slow makes the witness copy the whole store for every try and
	// answer, as the tests that compare it with the fast one need.
	slow bool
}

// MaxSteps fails a check, as its timeout does, once it took n steps:
// a witness step indexes up to 256 history items, charges up to 1,024
// ordinary inner items or 16,384 capacity items, encodes 2,048 diagnostic
// JSON bytes, writes two large-map entries, places an op or tries another
// order; a Porcupine step calls the model. Inside a simulation,
// whose clock stands still while the check computes, the timeout never
// comes, and this is the bound that holds.
func MaxSteps(n int64) Option {
	return func(o *options) { o.steps = n }
}

// StepsUsed stores Witnessed's weighted budget count in dst when it
// returns, on success or failure (zero before a check starts). It does
// not change the verdict or error message. A nil dst is ignored;
// Linearizable does not use this option.
func StepsUsed(dst *int64) Option {
	return func(o *options) { o.stepsUsed = dst }
}

// StopWhen fails a check, as its timeout does, once stop reports true.
// It is polled every 1,024 accounting ticks, independently of the budget
// weights: at most 65,536 inner work items. Inside a simulation it is how
// a limit of real time stops a check whose clock stands still.
func StopWhen(stop func() bool) Option {
	return func(o *options) { o.stop = stop }
}

func optionsOf(opts []Option) options {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	return o
}
