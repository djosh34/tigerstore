//go:build !sim

package race_test

import (
	"sync"
	"testing"
	"unsafe"

	"github.com/jellevandenhooff/gosim/internal/race"
)

//go:norace
func helper1(shared *int, wg *sync.WaitGroup) {
	defer wg.Done()
	*shared = 0
}

func TestNoRaceAnnotateOutside(t *testing.T) {
	var shared int
	var wg sync.WaitGroup
	wg.Add(2)
	go helper1(&shared, &wg)
	go helper1(&shared, &wg)
	wg.Wait()
}

//go:norace
func helper2(shared *int, wg *sync.WaitGroup) {
	go func() {
		defer wg.Done()
		*shared = 0
	}()
}

func TestRaceAnnotateGoInside(t *testing.T) {
	var shared int
	var wg sync.WaitGroup
	wg.Add(2)
	go helper2(&shared, &wg)
	go helper2(&shared, &wg)
	wg.Wait()
}

//go:norace
func helper3(shared *int, wg *sync.WaitGroup) {
	defer wg.Done()
	func() {
		*shared = 0
	}()
}

func TestNoRaceAnnotateInsideInline(t *testing.T) {
	var shared int
	var wg sync.WaitGroup
	wg.Add(2)
	go helper3(&shared, &wg)
	go helper3(&shared, &wg)
	wg.Wait()
}

//go:noinline
func wrapper(f func()) {
	// prevent inlining to ensure norace does not apply
	f()
}

//go:norace
func helper4(shared *int, wg *sync.WaitGroup) {
	defer wg.Done()
	wrapper(func() {
		*shared = 0
	})
}

func TestRaceAnnotateInsideWrapper(t *testing.T) {
	var shared int
	var wg sync.WaitGroup
	wg.Add(2)
	go helper4(&shared, &wg)
	go helper4(&shared, &wg)
	wg.Wait()
}

func wrappernewracectx(f func()) {
	done := make(chan struct{})
	race.Release(unsafe.Pointer(&done))

	race.Disable()
	go func() {
		f()
		race.Acquire(unsafe.Pointer(&done))
		close(done)
	}()
	race.Enable()
	<-done
}

func TestRaceWrappernewracectx(t *testing.T) {
	x := 0
	wrappernewracectx(func() {
		x = 1
	})
	_ = x
}

func TestNoRaceWrappernewracectx(t *testing.T) {
	x := 0
	wrappernewracectx(func() {
	})
	_ = x
}

//go:norace
func helper5(x, y int) int {
	var z int
	wrappernewracectx(func() {
		z = x + y
	})
	return z
}

func TestNoRaceWrappernewracectxReturn(t *testing.T) {
	if helper5(3, 2) != 5 {
		t.Error("bad")
	}
}

func helper6(x, y int) {
	wrappernewracectx(func() {
		if x+y != 5 {
			panic("help")
		}
	})
}

func TestNoRaceWrappernewracectxArgs(t *testing.T) {
	helper6(3, 2)
}

func helper7(x, y int) int {
	var z int
	wrappernewracectx(func() {
		z = x + y
	})
	return z
}

func TestRaceWrappernewracectxReturn(t *testing.T) {
	helper7(3, 2)
}
