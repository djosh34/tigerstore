//go:build !planted

package wl

import (
	"testing"
	"unsafe"
)

func TestWatchDeliveryPlantHasNoProductionStorage(t *testing.T) {
	if unsafe.Sizeof(watchDeliveryPlant{}) != 0 || unsafe.Sizeof(watcher{}) != 144 || unsafe.Offsetof(watcher{}.liveBytes) != 128 || unsafe.Offsetof(watcher{}.big) != 136 {
		t.Fatal("planted delivery state grew or moved production watcher storage")
	}
}
