//go:build !planted

package planted

// On reports whether the named bug is switched on; never without the
// planted build tag.
func On(string) bool { return false }
