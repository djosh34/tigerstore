//go:build planted

package planted

// Active is the name of the planted bug that is switched on. Tests set it
// before a run.
var Active string

// On reports whether the named bug is switched on.
func On(name string) bool { return name == Active }
