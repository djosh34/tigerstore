package check

// An Option changes how Witnessed or Linearizable runs.
type Option func(*options)

type options struct {
	steps   int64
	stats   *WitnessStats
	variant string
}

// MaxSteps fails a check, as its timeout does, once its search took n
// steps: a witness step places an op or tries another order, a Porcupine
// step calls the model. Inside a simulation, whose clock stands still
// while the check computes, the timeout never comes, and this is the
// bound that holds.
func MaxSteps(n int64) Option {
	return func(o *options) { o.steps = n }
}

// WitnessMetrics collects experimental revision-search counters (#441).
func WitnessMetrics(s *WitnessStats) Option {
	return func(o *options) { o.stats = s }
}

// RevisionSearch selects baseline, checkpoints, indexed, both, prefilter,
// fast, optional, scoped, tuned, retired, or best, for #441.
func RevisionSearch(variant string) Option {
	return func(o *options) { o.variant = variant }
}

func optionsOf(opts []Option) options {
	o := options{variant: "best"}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}
