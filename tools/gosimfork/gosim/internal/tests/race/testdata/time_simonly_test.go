//go:build sim

package race_test

import (
	"testing"
	"time"
)

func TestNoRaceTicker1(t *testing.T) {
	// little song and dance to make sure we can read timer.C without racing
	timer := time.NewTicker(10 * time.Millisecond)
	timer.Stop()

	select {
	case <-timer.C:
	default:
	}

	select {
	case <-timer.C:
		t.Error("bad")
	default:
	}

	var x int
	_ = x

	done := make(chan struct{}, 1)
	go func() {
		<-timer.C
		x = 2
		done <- struct{}{}
	}()

	x = 1
	timer.Reset(time.Second)

	<-done
}

/*
// TODO: Fix
func TestRaceTimer3(t *testing.T) {
	// little song and dance to make sure we can read timer.C without racing
	t1 := time.NewTimer(10 * time.Millisecond)
	<-t1.C

	t2 := time.NewTimer(10 * time.Millisecond)
	<-t2.C

	var x int
	_ = x
	var y int
	_ = y

	done := make(chan struct{}, 2)
	go func() {
		<-t1.C
		x = 2
		done <- struct{}{}
	}()

	go func() {
		<-t2.C
		y = 2
		done <- struct{}{}
	}()

	// t1 will fire later but is still racing
	t1.Reset(10 * time.Millisecond)
	x = 1
	y = 1
	t2.Reset(5 * time.Millisecond)

	<-done
	<-done
}
*/
