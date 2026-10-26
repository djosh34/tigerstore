package reflect

import (
	"unsafe"

	"github.com/jellevandenhooff/gosim/gosimruntime"
)

// link names used by the reflect2 package, used indirectly by etcd. expose them
// to let the package compile. make all of them panic to prevent usage.

func unsafe_New(rtype unsafe.Pointer) unsafe.Pointer {
	gosimruntime.Unimplemented("reflect.unsafe_New")
	panic("unreachable")
}

func typedmemmove(rtype unsafe.Pointer, dst, src unsafe.Pointer) {
	gosimruntime.Unimplemented("reflect.typedmemmove")
}

func unsafe_NewArray(rtype unsafe.Pointer, length int) unsafe.Pointer {
	gosimruntime.Unimplemented("reflect.unsafe_NewArray")
	panic("unreachable")
}

type noSliceHeader struct {
	Data unsafe.Pointer
	Len  int
	Cap  int
}

func typedslicecopy(elemType unsafe.Pointer, dst, src noSliceHeader) int {
	gosimruntime.Unimplemented("reflect.typedslicecopy")
	panic("unreachable")
}

func mapassign(rtype unsafe.Pointer, m unsafe.Pointer, key, val unsafe.Pointer) {
	gosimruntime.Unimplemented("reflect.mapassign")
}

func mapaccess(rtype unsafe.Pointer, m unsafe.Pointer, key unsafe.Pointer) (val unsafe.Pointer) {
	gosimruntime.Unimplemented("reflect.mapaccess")
	panic("unreachable")
}

type noHiter struct {
	key   unsafe.Pointer // Must be in first position.  Write nil to indicate iteration end (see cmd/internal/gc/range.go).
	value unsafe.Pointer // Must be in second position (see cmd/internal/gc/range.go).
	// rest fields are ignored
}

func mapiterinit(rtype unsafe.Pointer, m unsafe.Pointer) *noHiter {
	gosimruntime.Unimplemented("reflect.mapiterinit")
	panic("unreachable")
}

func mapiternext(it *noHiter) {
	gosimruntime.Unimplemented("reflect.mapiternext")
}

func ifaceE2I(rtype unsafe.Pointer, src interface{}, dst unsafe.Pointer) {
	gosimruntime.Unimplemented("reflect.ifaceE2I")
}

func makemap(rtype unsafe.Pointer, cap int) (m unsafe.Pointer) {
	gosimruntime.Unimplemented("reflect.makemap")
	panic("unreachable")
}
