package behavior_test

import (
	"fmt"
	"log"
	"sync"
	"testing"
)

func TestGoMutexWaitGroup(t *testing.T) {
	var mu sync.Mutex
	var wg sync.WaitGroup
	var order string

	for i := 0; i < 5; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				mu.Lock()
				order += fmt.Sprint(i)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	// TODO: assert this string is the same on every run
	log.Print(order)
}
