package race_test

import (
	"math/rand"
	"testing"

	"github.com/jellevandenhooff/gosim"
)

func TestRaceSimple(t *testing.T) {
	var x int
	_ = x

	done := make(chan struct{})
	go func() {
		x = 1
		done <- struct{}{}
	}()

	go func() {
		x = 20
		done <- struct{}{}
	}()

	<-done
	<-done
}

func TestNoRaceSimple(t *testing.T) {
	var x int
	_ = x

	wait := make(chan struct{})
	done := make(chan struct{})
	go func() {
		x = 1
		wait <- struct{}{}
		done <- struct{}{}
	}()

	go func() {
		<-wait
		x = 2
		done <- struct{}{}
	}()

	<-done
	<-done
}

func TestNoRaceRand(t *testing.T) {
	done := make(chan struct{})
	go func() {
		rand.Float64()
		done <- struct{}{}
	}()

	go func() {
		rand.Float64()
		done <- struct{}{}
	}()

	<-done
	<-done
}

func TestNoRaceLotsOfGo(t *testing.T) {
	done := make(chan struct{}, 100)

	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 10; j++ {
				go func() {
					done <- struct{}{}
				}()
			}
		}()
	}

	for i := 0; i < 100; i++ {
		<-done
	}
}

// XXX: example from go race detector blog with timer and reset
// XXX: logging should not cause edges
// XXX: filesystem works and does not cause edges (maybe?)

func TestNoRaceIsDetgo(t *testing.T) {
	gosim.IsSim()
}
