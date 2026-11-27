package wl

import (
	"testing"
	"time"
	"unsafe"
)

func awaitFetchResult(t *testing.T, l *loop) {
	t.Helper()
	select {
	case result := <-l.fetchWorker.results:
		l.fetchWorker.results <- result
	case <-time.After(5 * time.Second):
		t.Fatal("the single fetch worker did not finish")
	}
}

func TestFetchWorkerPersistsAndKeepsPostedWorkActive(t *testing.T) {
	l := &loop{hoFD: -1}
	var completed int
	makeTask := func() *fetchTask {
		return &fetchTask{work: func() error { return nil }, done: func(err error) error {
			completed++
			return err
		}}
	}
	a := makeTask()
	if !l.queueFetchTask(a) {
		t.Fatal("the first task was not admitted")
	}
	worker := l.fetchWorker
	defer worker.stop()
	awaitFetchResult(t, l)
	if l.queueFetchTask(makeTask()) || !worker.busy || completed != 0 || a.work != nil {
		t.Fatal("a posted completion released its worker slot or retained its work closure")
	}
	if err := l.runFetchCompletion(); err != nil || completed != 1 || a.done != nil {
		t.Fatalf("the first acknowledgement did not transfer ownership: %v", err)
	}
	if !l.queueFetchTask(makeTask()) || l.fetchWorker != worker {
		t.Fatal("the next piece started another goroutine instead of reusing the worker")
	}
	awaitFetchResult(t, l)
	if err := l.runFetchCompletion(); err != nil || completed != 2 || worker.busy {
		t.Fatalf("the next task was not acknowledged: %v", err)
	}
	worker.stop()
	if l.queueFetchTask(makeTask()) {
		t.Fatal("a stopped worker accepted more source I/O")
	}
}

func TestFetchWorkerStopsWithAnUnacknowledgedResult(t *testing.T) {
	l := &loop{hoFD: -1}
	book := newFetchWorkBook(83 << 20)
	lease := book.reserve(fetchCursorWork - fetchCoreWork)
	if !lease.start() {
		t.Fatal("no worker reservation")
	}
	if !l.queueFetchTask(&fetchTask{
		work: func() error { return nil },
		done: func(err error) error {
			if !lease.acknowledge() || !lease.release() {
				t.Error("shutdown acknowledgement did not return its own reservation")
			}
			return err
		},
	}) {
		t.Fatal("no task")
	}
	l.fetchWorker.stop()
	if book.used != fetchCursorWork || !l.fetchWorker.busy {
		t.Fatal("stopping the goroutine released an unacknowledged completion")
	}
	if err := l.runFetchCompletion(); err != nil || book.used != fetchCoreWork {
		t.Fatalf("shutdown could not finish the pending acknowledgement: %v", err)
	}
}

func TestFetchWorkerControlFitsTheCommonReservation(t *testing.T) {
	if unsafe.Sizeof(fetchWorker{})+unsafe.Sizeof(fetchTask{})+unsafe.Sizeof(fetchCompletion{})+4*1024 > fetchCoreWork-(64<<10) {
		t.Fatal("worker channels/control no longer fit the common 16-KiB control allowance")
	}
}
