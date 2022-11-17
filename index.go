package borm

import (
	"unsafe"
)

type IndexType string

const (
	UNIQUE IndexType = "unique"
	NORMAL IndexType = "normal"
	UNION  IndexType = "union"
)

func getIndex(inputIndex string) (IndexType, error) {
	switch IndexType(inputIndex) {
	case UNIQUE:
		return UNIQUE, nil
	case NORMAL, UNION:
		return NORMAL, nil
	}
	return "", ErrIdxNotSupport
}

func checkIsUnion(inputIndex string) bool {
	return UNION == IndexType(inputIndex)
}

type tag struct {
	offset    uintptr
	fieldType FieldType
	indexType IndexType
	fieldName string
}

func (tag *tag) GetPointerVal(p unsafe.Pointer) interface{} {
	var val interface{}
	switch tag.fieldType {
	case String:
		val = *(*string)(p)
	case Int:
		val = *(*int)(p)
	case Int8:
		val = *(*int8)(p)
	case Int16:
		val = *(*int16)(p)
	case Int32:
		val = *(*int32)(p)
	case Int64:
		val = *(*int64)(p)
	case Uint:
		val = *(*uint)(p)
	case Uint8:
		val = *(*uint8)(p)
	case Uint16:
		val = *(*uint16)(p)
	case Uint32:
		val = *(*uint32)(p)
	case Uint64:
		val = *(*uint64)(p)
	case Float32:
		val = *(*float32)(p)
	case Float64:
		val = *(*float64)(p)
	case Complex64:
		val = *(*complex64)(p)
	case Complex128:
		val = *(*complex128)(p)
	case Byte:
		val = *(*byte)(p)
	case Rune:
		val = *(*rune)(p)
	}
	return val
}

func (tag *tag) CheckIsNormal() bool {
	return tag.indexType == NORMAL
}

func (tag *tag) CheckIsUnique() bool {
	return tag.indexType == UNIQUE
}

type FieldType string

const (
	String     FieldType = `string`
	Int        FieldType = `int`
	Int8       FieldType = `int8`
	Int16      FieldType = `int16`
	Int32      FieldType = `int32`
	Int64      FieldType = `int64`
	Uint       FieldType = `uint`
	Uint8      FieldType = `uint8`
	Uint16     FieldType = `uint16`
	Uint32     FieldType = `uint32`
	Uint64     FieldType = `uint64`
	Float32    FieldType = `float32`
	Float64    FieldType = `float64`
	Complex64  FieldType = `complex64`
	Complex128 FieldType = `complex128`
	Byte       FieldType = `byte`
	Rune       FieldType = `rune`
)

func GetTag(fieldName string, dest interface{}, offset uintptr, indexType IndexType) (*tag, error) {
	if dest == nil {
		return nil, ErrIdxNotSupport
	}
	tag := &tag{}
	switch dest.(type) {
	case string:
		tag = getStringTag(offset, indexType)
	case int:
		tag = getIntTag(offset, indexType)
	case int8:
		tag = getInt8Tag(offset, indexType)
	case int16:
		tag = getInt16Tag(offset, indexType)
	case int32:
		tag = getInt32Tag(offset, indexType)
	case int64:
		tag = getInt64Tag(offset, indexType)
	case uint:
		tag = getUintTag(offset, indexType)
	case uint8:
		tag = getUint8Tag(offset, indexType)
	case uint16:
		tag = getUint16Tag(offset, indexType)
	case uint32:
		tag = getUint32Tag(offset, indexType)
	case uint64:
		tag = getUint64Tag(offset, indexType)
	case float32:
		tag = getFloat32Tag(offset, indexType)
	case float64:
		tag = getFloat64Tag(offset, indexType)
	case complex64:
		tag = getComplex64Tag(offset, indexType)
	case complex128:
		tag = getComplex128Tag(offset, indexType)
	default:
		return nil, ErrIdxNotSupport
	}
	tag.fieldName = fieldName
	return tag, nil
}

func getStringTag(offset uintptr, indexType IndexType) *tag {
	return &tag{
		offset:    offset,
		fieldType: String,
		indexType: indexType,
	}
}
func getIntTag(offset uintptr, indexType IndexType) *tag {
	return &tag{
		offset:    offset,
		fieldType: Int,
		indexType: indexType,
	}
}
func getInt8Tag(offset uintptr, indexType IndexType) *tag {
	return &tag{
		offset:    offset,
		fieldType: Int8,
		indexType: indexType,
	}
}
func getInt16Tag(offset uintptr, indexType IndexType) *tag {
	return &tag{
		offset:    offset,
		fieldType: Int16,
		indexType: indexType,
	}
}
func getInt32Tag(offset uintptr, indexType IndexType) *tag {
	return &tag{
		offset:    offset,
		fieldType: Int32,
		indexType: indexType,
	}
}
func getInt64Tag(offset uintptr, indexType IndexType) *tag {
	return &tag{
		offset:    offset,
		fieldType: Int64,
		indexType: indexType,
	}
}
func getUintTag(offset uintptr, indexType IndexType) *tag {
	return &tag{
		offset:    offset,
		fieldType: Uint,
		indexType: indexType,
	}
}
func getUint8Tag(offset uintptr, indexType IndexType) *tag {
	return &tag{
		offset:    offset,
		fieldType: Uint8,
		indexType: indexType,
	}
}
func getUint16Tag(offset uintptr, indexType IndexType) *tag {
	return &tag{
		offset:    offset,
		fieldType: Uint16,
		indexType: indexType,
	}
}
func getUint32Tag(offset uintptr, indexType IndexType) *tag {
	return &tag{
		offset:    offset,
		fieldType: Uint32,
		indexType: indexType,
	}
}
func getUint64Tag(offset uintptr, indexType IndexType) *tag {
	return &tag{
		offset:    offset,
		fieldType: Uint64,
		indexType: indexType,
	}
}
func getFloat32Tag(offset uintptr, indexType IndexType) *tag {
	return &tag{
		offset:    offset,
		fieldType: Float32,
		indexType: indexType,
	}
}
func getFloat64Tag(offset uintptr, indexType IndexType) *tag {
	return &tag{
		offset:    offset,
		fieldType: Float64,
		indexType: indexType,
	}
}
func getComplex64Tag(offset uintptr, indexType IndexType) *tag {
	return &tag{
		offset:    offset,
		fieldType: Complex64,
		indexType: indexType,
	}
}
func getComplex128Tag(offset uintptr, indexType IndexType) *tag {
	return &tag{
		offset:    offset,
		fieldType: Complex128,
		indexType: indexType,
	}
}
func getByteTag(offset uintptr, indexType IndexType) *tag {
	return &tag{
		offset:    offset,
		fieldType: Byte,
		indexType: indexType,
	}
}
func getRuneTag(offset uintptr, indexType IndexType) *tag {
	return &tag{
		offset:    offset,
		fieldType: Rune,
		indexType: indexType,
	}
}
