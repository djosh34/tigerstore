//go:build !race

package coro

// Upcallcoro is a non-race instrumented version of Upcallcoro in upcallcoro_race.go.
type Upcallcoro struct {
	coro Coro
}

func (u *Upcallcoro) Start(f func()) {
	u.coro.Start(f)
}

func (u *Upcallcoro) Next() {
	u.coro.Next()
}

func (u *Upcallcoro) Upcall(f func()) {
	f()
}

func (u *Upcallcoro) Yield() {
	u.coro.Yield()
}

func (u *Upcallcoro) Finish() {
	u.coro.Finish()
}
