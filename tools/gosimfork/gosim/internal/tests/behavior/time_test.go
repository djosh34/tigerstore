//go:build sim

package behavior_test

import (
	"log"
	"log/slog"
	"testing"
	"time"
)

// TODO: test these against real behavior with some slack built in for time
// TODO: test monotonic time with different machines/universes

func TestTimeBasic(t *testing.T) {
	a := time.Now()

	time.Sleep(10 * time.Second)

	b := time.Now()
	if b.Sub(a) < 10*time.Second {
		t.Error(a, b)
	}

	time.Sleep(10 * time.Second)

	c := time.Now()
	if c.Sub(b) < 10*time.Second {
		t.Error(a, b)
	}

	log.Println(a, b, c)
}

func TestTimerPast(t *testing.T) {
	a := time.Now()
	timer := time.NewTimer(-10 * time.Second)
	<-timer.C
	b := time.Now()
	if !a.Equal(b) {
		t.Error(a, b)
	}
}

func TestTimerAfter(t *testing.T) {
	a := time.Now()

	<-time.After(10 * time.Second)

	b := time.Now()
	if b.Sub(a) != 10*time.Second {
		t.Error(a, b)
	}

	<-time.After(10 * time.Second)

	c := time.Now()
	if c.Sub(b) != 10*time.Second {
		t.Error(a, b)
	}
}

func TestTimerValue(t *testing.T) {
	a := time.Now()
	ret := <-time.After(10 * time.Second)
	b := time.Now()
	if b.Sub(a) != 10*time.Second {
		t.Error(a, b)
	}
	if !ret.Equal(b) {
		t.Error(ret, b)
	}
}

func TestTimerValueSleep(t *testing.T) {
	a := time.Now()
	ch := time.After(10 * time.Second)
	time.Sleep(20 * time.Second)
	ret := <-ch
	b := time.Now()
	if b.Sub(a) != 20*time.Second {
		t.Error(a, b)
	}
	if ret.Sub(a) != 10*time.Second {
		t.Error(ret, a)
	}
}

func TestTimerResetOk(t *testing.T) {
	a := time.Now()

	timer := time.NewTimer(10 * time.Second)
	if !timer.Reset(20 * time.Second) {
		t.Error("bad reset")
	}
	<-timer.C

	b := time.Now()
	if b.Sub(a) != 20*time.Second {
		t.Error(a, b)
	}
}

func TestTimerStopOk(t *testing.T) {
	a := time.Now()

	timer := time.NewTimer(10 * time.Second)

	if timer.Stop() {
		timer.Reset(20 * time.Second)
	} else {
		t.Error("bad")
	}
	<-timer.C

	b := time.Now()
	if b.Sub(a) != 20*time.Second {
		t.Error(a, b)
	}
}

func TestTimerStopNotOk(t *testing.T) {
	a := time.Now()

	timer := time.NewTimer(10 * time.Second)

	time.Sleep(20 * time.Second)

	if !timer.Stop() {
		<-timer.C
		timer.Reset(20 * time.Second)
	} else {
		t.Error("bad")
	}

	<-timer.C

	b := time.Now()
	if b.Sub(a) != 40*time.Second {
		t.Error(a, b)
	}
}

func TestTimerAfterFunc(t *testing.T) {
	a := time.Now()

	done := make(chan struct{}, 0)
	time.AfterFunc(10*time.Second, func() {
		done <- struct{}{}
	})

	<-done
	b := time.Now()
	if b.Sub(a) != 10*time.Second {
		t.Error(a, b)
	}
}

func TestTimerAfterFuncReset(t *testing.T) {
	a := time.Now()

	done := make(chan struct{}, 0)
	timer := time.AfterFunc(10*time.Second, func() {
		done <- struct{}{}
	})

	<-done
	b := time.Now()
	if b.Sub(a) != 10*time.Second {
		t.Error(a, b)
	}

	timer.Reset(10 * time.Second)
	timer.Reset(5 * time.Second)

	<-done
	c := time.Now()
	if c.Sub(b) != 5*time.Second {
		t.Error(b, c)
	}
}

func TestTimeSameStart(t *testing.T) {
	time.Sleep(10 * time.Second)

	// TODO: need a test for determinism here?
	slog.Info("after", "now", time.Now().String())
}

func TestTimeTickerStop(t *testing.T) {
	start := time.Now()

	ticker := time.NewTicker(5 * time.Second)

	v := <-ticker.C
	if now := time.Now(); now.Sub(start) != 5*time.Second {
		t.Error(start, now)
	}
	if v.Sub(start) != 5*time.Second {
		t.Error(start, v)
	}

	v = <-ticker.C
	if now := time.Now(); now.Sub(start) != 10*time.Second {
		t.Error(start, now)
	}
	if v.Sub(start) != 10*time.Second {
		t.Error(start, v)
	}

	ticker.Stop()
	time.Sleep(10 * time.Second)
	select {
	case v := <-ticker.C:
		t.Error(v, start)
	default:
	}
}

func TestTimeTickerReset(t *testing.T) {
	start := time.Now()

	ticker := time.NewTicker(5 * time.Second)

	v := <-ticker.C
	if now := time.Now(); now.Sub(start) != 5*time.Second {
		t.Error(start, now)
	}
	if v.Sub(start) != 5*time.Second {
		t.Error(start, v)
	}

	v = <-ticker.C
	if now := time.Now(); now.Sub(start) != 10*time.Second {
		t.Error(start, now)
	}
	if v.Sub(start) != 10*time.Second {
		t.Error(start, v)
	}

	time.Sleep(2 * time.Second)
	ticker.Reset(6 * time.Second)

	v = <-ticker.C
	if now := time.Now(); now.Sub(start) != 18*time.Second {
		t.Error(start, now)
	}
	if v.Sub(start) != 18*time.Second {
		t.Error(start, v)
	}

	v = <-ticker.C
	if now := time.Now(); now.Sub(start) != 24*time.Second {
		t.Error(start, now)
	}
	if v.Sub(start) != 24*time.Second {
		t.Error(start, v)
	}

	time.Sleep(2 * time.Second)
	ticker.Reset(2 * time.Second)

	v = <-ticker.C
	if now := time.Now(); now.Sub(start) != 28*time.Second {
		t.Error(start, now)
	}
	if v.Sub(start) != 28*time.Second {
		t.Error(start, v)
	}

	time.Sleep(7 * time.Second)
	ticker.Reset(5 * time.Second)

	v = <-ticker.C
	if now := time.Now(); now.Sub(start) != 35*time.Second {
		t.Error(start, now)
	}
	if v.Sub(start) != 30*time.Second {
		t.Error(start, v)
	}

	v = <-ticker.C
	if now := time.Now(); now.Sub(start) != 40*time.Second {
		t.Error(start, now)
	}
	if v.Sub(start) != 40*time.Second {
		t.Error(start, v)
	}
}

func TestTimeTickerStopReset(t *testing.T) {
	start := time.Now()

	ticker := time.NewTicker(5 * time.Second)

	v := <-ticker.C
	if now := time.Now(); now.Sub(start) != 5*time.Second {
		t.Error(start, now)
	}
	if v.Sub(start) != 5*time.Second {
		t.Error(start, v)
	}

	v = <-ticker.C
	if now := time.Now(); now.Sub(start) != 10*time.Second {
		t.Error(start, now)
	}
	if v.Sub(start) != 10*time.Second {
		t.Error(start, v)
	}

	ticker.Stop()
	time.Sleep(10 * time.Second)
	select {
	case v := <-ticker.C:
		t.Error(v, start)
	default:
	}

	ticker.Reset(3 * time.Second)
	v = <-ticker.C
	if now := time.Now(); now.Sub(start) != 23*time.Second {
		t.Error(start, now)
	}
	if v.Sub(start) != 23*time.Second {
		t.Error(start, v)
	}

	v = <-ticker.C
	if now := time.Now(); now.Sub(start) != 26*time.Second {
		t.Error(start, now)
	}
	if v.Sub(start) != 26*time.Second {
		t.Error(start, v)
	}
}

func TestTimeTickerNonBlocking(t *testing.T) {
	start := time.Now()

	ticker := time.NewTicker(5 * time.Second)

	v := <-ticker.C
	if now := time.Now(); now.Sub(start) != 5*time.Second {
		t.Error(start, now)
	}
	if v.Sub(start) != 5*time.Second {
		t.Error(start, v)
	}

	v = <-ticker.C
	if now := time.Now(); now.Sub(start) != 10*time.Second {
		t.Error(start, now)
	}
	if v.Sub(start) != 10*time.Second {
		t.Error(start, v)
	}

	time.Sleep(8 * time.Second)
	v = <-ticker.C
	if now := time.Now(); now.Sub(start) != 18*time.Second {
		t.Error(start, now)
	}
	if v.Sub(start) != 15*time.Second {
		t.Error(start, v)
	}

	v = <-ticker.C
	if now := time.Now(); now.Sub(start) != 20*time.Second {
		t.Error(start, now)
	}
	if v.Sub(start) != 20*time.Second {
		t.Error(start, v)
	}

	time.Sleep(31 * time.Second)
	v = <-ticker.C
	if now := time.Now(); now.Sub(start) != 51*time.Second {
		t.Error(start, now)
	}
	if v.Sub(start) != 25*time.Second {
		t.Error(start, v)
	}

	v = <-ticker.C
	if now := time.Now(); now.Sub(start) != 55*time.Second {
		t.Error(start, now)
	}
	if v.Sub(start) != 55*time.Second {
		t.Error(start, v)
	}
}
