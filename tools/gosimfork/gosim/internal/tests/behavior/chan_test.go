package behavior_test

import (
	"fmt"
	"log"
	"sync"
	"testing"
	"time"
)

func TestChanSendRecvClose(t *testing.T) {
	ch := make(chan int, 10)

	for i := 0; i < 5; i++ {
		ch <- i
	}
	close(ch)

	for i := 0; i < 5; i++ {
		v, ok := <-ch
		if !ok || v != i {
			t.Error(v, ok)
		}
	}
	_, ok := <-ch
	if ok {
		t.Error(ok)
	}
}

func TestChanSelectRecvNilInterface(t *testing.T) {
	ch := make(chan error, 1)
	other := make(chan struct{})

	ch <- nil

	select {
	case err := <-ch:
		if err != nil {
			t.Fatal(err)
		}
	case <-other:
	}
}

func TestGoZeroChanSendRecvClose(t *testing.T) {
	ch := make(chan int)

	sent0 := false
	sent1 := false

	go func() {
		ch <- 0
		sent0 = true
		ch <- 1
		sent1 = true
		close(ch)
	}()

	// XXX: want to spin here...
	if sent0 {
		t.Error("should not have sent 0")
	}
	if v, ok := <-ch; !ok || v != 0 {
		t.Error(v)
	}
	if sent1 {
		t.Error("should not have sent 1")
	}
	if v, ok := <-ch; !ok || v != 1 {
		t.Error(v)
	}
	if _, ok := <-ch; ok {
		t.Error(ok)
	}
}

func TestGoChanBlock(t *testing.T) {
	sent := make([]bool, 20)
	sentCh := make([]chan struct{}, 20)
	for i := 0; i < 20; i++ {
		sentCh[i] = make(chan struct{})
	}

	ch := make(chan int, 10)

	go func() {
		for i := 0; i < 20; i++ {
			ch <- i
			sent[i] = true
			close(sentCh[i])
		}
	}()

	<-sentCh[9]
	// XXX: time.Sleep here, spin??? have introspection API that says other goroutine is blocked???
	if sent[10] {
		t.Error("sent 10")
	}

	v, ok := <-ch
	if !ok || v != 0 {
		t.Error(v)
	}

	<-sentCh[10]
	// XXX: time.Sleep here, spin???
	if sent[11] {
		t.Error("sent 10")
	}

	/*
		// XXX: without this test fails because we have blocked goroutine that
		// will never finish. handle somehow?
		for i := 1; i < 20; i++ {
			v, ok := <-ch
			if !ok || v != i {
				t.Error(v)
			}
		}
	*/
}

func TestGoChanDeterministic(t *testing.T) {
	ch := make(chan int, 10)

	for i := 0; i < 5; i++ {
		i := i
		go func() {
			for j := 0; j < 5; j++ {
				ch <- i
			}
		}()
	}

	result := ""
	for i := 0; i < 5*5; i++ {
		v, ok := <-ch
		if !ok {
			t.Error(ok)
		}
		result += fmt.Sprintf("%d", v)
	}

	// XXX: assert this string is the same on every run
	log.Print(result)
	// return s
}

func TestGoSelectBasicRecv(t *testing.T) {
	ch1 := make(chan int, 10)
	ch2 := make(chan int, 10)

	go func() {
		for i := 0; i < 5; i++ {
			ch1 <- 0
		}
	}()
	go func() {
		for i := 0; i < 5; i++ {
			ch2 <- 1
		}
	}()

	result := ""

	for i := 0; i < 10; i++ {
		select {
		case v, ok := <-ch1:
			if !ok {
				t.Error(ok)
			}
			result += fmt.Sprint(v)

		case v, ok := <-ch2:
			if !ok {
				t.Error(ok)
			}
			result += fmt.Sprint(v)
		}
	}

	// XXX: assert this string is the same on every run
	log.Print(result)
	// return s
}

func TestGoSelectZeroRecv(t *testing.T) {
	ch1 := make(chan int)
	ch2 := make(chan int)

	go func() {
		for i := 0; i < 5; i++ {
			ch1 <- 0
		}
	}()
	go func() {
		for i := 0; i < 5; i++ {
			ch2 <- 1
		}
	}()

	result := ""

	for i := 0; i < 10; i++ {
		select {
		case v, ok := <-ch1:
			if !ok {
				t.Error(ok)
			}
			result += fmt.Sprint(v)

		case v, ok := <-ch2:
			if !ok {
				t.Error(ok)
			}
			result += fmt.Sprint(v)
		}
	}

	// XXX: assert this string is the same on every run
	log.Print(result)
	// return s
}

func TestChanSendSelf(t *testing.T) {
	ch := make(chan int)

	done := time.After(1 * time.Second)

	select {
	case <-ch:
		t.Error("help")
	case ch <- 10:
		t.Error("help")
	case <-done:
	}
}

func TestChanMultipleZeroSelect(t *testing.T) {
	a := make(chan int)
	b := make(chan int)

	var mu sync.Mutex
	count := 0

	go func() {
		a <- 0
		mu.Lock()
		count++
		mu.Unlock()
	}()

	go func() {
		b <- 0
		mu.Lock()
		count++
		mu.Unlock()
	}()

	select {
	case <-a:
	case <-b:
	}

	time.Sleep(10 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if count != 1 {
		t.Error("help", count)
	}
}

func TestChanRange(t *testing.T) {
	ch := make(chan int)

	go func() {
		for i := 0; i < 5; i++ {
			ch <- i
		}
		close(ch)
	}()

	j := 0
	for i := range ch {
		if i != j {
			t.Error("bad")
		}
		j++
	}
	if j != 5 {
		t.Error("bad")
	}
}

func TestChanLenCap(t *testing.T) {
	ch := make(chan int)
	if cap(ch) != 0 {
		t.Error("bad")
	}

	ch = make(chan int, 5)
	if cap(ch) != 5 {
		t.Error("bad")
	}

	if len(ch) != 0 {
		t.Error("bad")
	}

	for i := 1; i <= 5; i++ {
		ch <- i
		if len(ch) != i {
			t.Error("bad")
		}
	}

	for i := 1; i <= 5; i++ {
		j := <-ch
		if i != j {
			t.Error("bad")
		}
		if len(ch) != 5-i {
			t.Error("bad")
		}
	}
}

func TestChanNilChanSend(t *testing.T) {
	var ch chan struct{}

	go func() {
		ch <- struct{}{}
		panic("should've blocked")
	}()

	time.Sleep(10 * time.Millisecond)
}

func TestChanNilChanSendSelectOne(t *testing.T) {
	var ch chan struct{}

	go func() {
		select {
		case ch <- struct{}{}:
		}
		panic("should've blocked")
	}()

	time.Sleep(10 * time.Millisecond)
}

func TestChanNilChanSendSelectDefault(t *testing.T) {
	var ch chan struct{}

	select {
	case ch <- struct{}{}:
		panic("should've blocked")
	default:
	}
}

func TestChanNilChanSendSelectMore(t *testing.T) {
	var ch chan struct{}
	chOk := make(chan struct{}, 1)

	select {
	case ch <- struct{}{}:
		panic("should've blocked")
	case chOk <- struct{}{}:
	default:
	}
}

func TestChanNilChanRecv(t *testing.T) {
	var ch chan struct{}

	go func() {
		<-ch
		panic("should've blocked")
	}()

	time.Sleep(10 * time.Millisecond)
}

func TestChanNilChanRecvOk(t *testing.T) {
	var ch chan struct{}

	go func() {
		_, ok := <-ch
		_ = ok
		panic("should've blocked")
	}()

	time.Sleep(10 * time.Millisecond)
}

func TestChanNilChanRecvSelectOne(t *testing.T) {
	var ch chan struct{}

	go func() {
		select {
		case <-ch:
		}
		panic("should've blocked")
	}()

	time.Sleep(10 * time.Millisecond)
}

func TestChanNilChanRecvSelectDefault(t *testing.T) {
	var ch chan struct{}

	select {
	case <-ch:
		panic("should've blocked")
	default:
	}
}

func TestChanNilChanRecvSelectMore(t *testing.T) {
	var ch chan struct{}
	chOk := make(chan struct{}, 1)
	chOk <- struct{}{}

	select {
	case <-ch:
		panic("should've blocked")
	case <-chOk:
	default:
	}
}

// XXX: what if you do something crazy like send and recv on the same channel in a select?

// XXX: what if you have a select on a zero-length channel that has a writer ready to go

// XXX: test Select prefers non-default?

// XXX: test Select randomness?
