package common

import (
	"encoding/binary"
	"unsafe"
)

type defaultInterface struct {
	typ  *struct{}
	word unsafe.Pointer
}

func GetUnsafeInterfacePointer(any interface{}) unsafe.Pointer {
	return unsafe.Pointer(uintptr((*defaultInterface)(unsafe.Pointer(&any)).word))
}

func GetUnsafeInterfaceUintptr(any interface{}) unsafe.Pointer {
	return (*defaultInterface)(unsafe.Pointer(&any)).word
}

func EncodedFromUInt64(i uint64) []byte {
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, i)
	return out
}

func DecodedToUInt64(bs []byte) uint64 {
	return binary.BigEndian.Uint64(bs)
}

func SetUint64(row any, id uint64) {
	rowPointer := GetUnsafeInterfacePointer(row)
	*(*uint64)(unsafe.Pointer(rowPointer)) = id
}

func GetUint64(row any) uint64 {
	rowPointer := GetUnsafeInterfacePointer(row)
	return *(*uint64)(unsafe.Pointer(rowPointer))
}
