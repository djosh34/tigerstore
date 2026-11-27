//go:build !planted

package wl

// No state, allocations or delivery change in production. Kept away from
// watcher's final field so an empty struct does not grow its layout.
type watchDeliveryPlant struct{}

func (*watchDeliveryPlant) capture(*node, *client, *watches, int32, int) {}
func (*watchDeliveryPlant) reset()                                       {}
func (*watchDeliveryPlant) retire(int64)                                 {}
func (*watchDeliveryPlant) each(*node, *store, *watcher, int, int, func(int, bigEv) bool) (bool, error) {
	return false, nil
}
