package go123

import (
 "unsafe"
 "github.com/jellevandenhooff/gosim/internal/testing"
)

// EXPERIMENT ONLY: one machine-wide bubble, using gosim's already virtual clock.
// NOT Go synctest semantics: no per-goroutine membership, nested-bubble rejection,
// bubble lifetime join, durable-block detection, acquire/release liveness, isolated
// clocks, foreign-bubble detection, or special testing.T cleanup/context rules.
// Wait remains the generated missing-hook panic. Run may return with goroutines
// alive. This adapter only exposes deeper hooks; it is not a replay correctness port.
var synctest333Active bool
var synctest333Associations = map[unsafe.Pointer]bool{}

func InternalSynctest_Run(f func()) {
 old := synctest333Active
 synctest333Active = true
 defer func() { synctest333Active = old }()
 f()
}
func InternalSynctest_inBubble(_ any, f func()) { InternalSynctest_Run(f) }
func InternalSynctest_associate(p unsafe.Pointer) int {
 if !synctest333Active { return 0 }
 synctest333Associations[p] = true
 return 1 // internal/synctest.CurrentBubble
}
func InternalSynctest_isAssociated(p unsafe.Pointer) bool { return synctest333Active && synctest333Associations[p] }
func InternalSynctest_disassociate(p unsafe.Pointer) { delete(synctest333Associations, p) }
func InternalSynctest_acquire() any { if synctest333Active { return 1 }; return nil }
func InternalSynctest_release(any) {} // no bubble liveness counter
func TestingSynctest_testingSynctestTest(t *testing.T, f func(*testing.T)) bool {
 f(t)
 return !t.Failed()
}
