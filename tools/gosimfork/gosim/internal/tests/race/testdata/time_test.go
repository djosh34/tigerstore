package race_test

import (
	"testing"
	"time"
)

func TestNoRaceTimer1(t *testing.T) {
	// little song and dance to make sure we can read timer.C without racing
	timer := time.NewTimer(10 * time.Millisecond)
	<-timer.C

	var x int
	_ = x

	done := make(chan struct{}, 1)
	go func() {
		<-timer.C
		x = 1
		done <- struct{}{}
	}()

	x = 2
	timer.Reset(10 * time.Millisecond)
	<-done
}

func TestRaceTimer1(t *testing.T) {
	// little song and dance to make sure we can read timer.C without racing
	timer := time.NewTimer(10 * time.Millisecond)
	<-timer.C

	var x int
	_ = x

	done := make(chan struct{}, 1)
	go func() {
		<-timer.C
		x = 1
		done <- struct{}{}
	}()

	timer.Reset(10 * time.Millisecond)
	x = 2
	<-done
}

func TestNoRaceTimer2(t *testing.T) {
	var x int
	_ = x

	done := make(chan struct{}, 1)

	x = 1
	time.AfterFunc(10*time.Millisecond, func() {
		x = 2
		done <- struct{}{}
	})

	<-done
}

func TestRaceTimer2(t *testing.T) {
	var x int
	_ = x

	done := make(chan struct{}, 1)

	time.AfterFunc(10*time.Millisecond, func() {
		x = 2
		done <- struct{}{}
	})
	x = 1

	<-done
}

func TestRaceTicker1(t *testing.T) {
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

	timer.Reset(time.Second)
	x = 1

	<-done
}

func TestNoRaceTimer3(t *testing.T) {
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
	x = 1
	y = 1
	t1.Reset(10 * time.Millisecond)
	t2.Reset(5 * time.Millisecond)

	<-done
	<-done
}
