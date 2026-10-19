package env

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// StorageGate is used only by the experiment's Go build overlay. Its
// control files live outside the faulty disks. One selected storage call
// waits here, after reporting its stage, until the controller releases it
// or kills the process. No fsync or storage result is changed by a wait.
func StorageGate(stage string) error {
	dir := os.Getenv("CHAOS_STORAGE_GATE")
	if dir == "" {
		return nil
	}
	gateMu.Lock()
	b, err := os.ReadFile(filepath.Join(dir, "arm"))
	if errors.Is(err, os.ErrNotExist) {
		gateMu.Unlock()
		return nil
	}
	if err != nil {
		gateMu.Unlock()
		return err
	}
	if strings.TrimSpace(string(b)) != stage {
		gateMu.Unlock()
		return nil
	}
	err = os.Remove(filepath.Join(dir, "arm"))
	if err == nil {
		err = os.WriteFile(filepath.Join(dir, "hit"), []byte(stage), 0600)
	}
	gateMu.Unlock()
	if err != nil {
		return err
	}
	for deadline := time.Now().Add(90 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		b, err := os.ReadFile(filepath.Join(dir, "release"))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if err := os.Remove(filepath.Join(dir, "release")); err != nil {
			return err
		}
		switch strings.TrimSpace(string(b)) {
		case "eio":
			return syscall.EIO
		case "enospc":
			return syscall.ENOSPC
		default:
			return nil
		}
	}
	return fmt.Errorf("storage gate %s was not released within 90s", stage)
}

var gateMu sync.Mutex
