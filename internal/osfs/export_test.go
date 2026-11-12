package osfs

import "time"

// SetDropWait makes DropCache call wait between its tries, until the
// returned function puts time.Sleep back.
func SetDropWait(wait func(time.Duration)) (restore func()) {
	dropWait = wait
	return func() { dropWait = time.Sleep }
}
