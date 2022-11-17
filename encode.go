package borm

import (
	"fmt"
)

const (
	segment byte = ':'
)

func encodeSeqKey(tableName string) []byte {
	return []byte(fmt.Sprintf("t:seq:%v", tableName))
}

func encodePKey(id uint32, pk_no uint64) []byte {
	return []byte(fmt.Sprintf("t:%v:%v", id, pk_no))
}

func encodeTablePrefixKey(id uint32) []byte {
	return []byte(fmt.Sprintf("t:%v", id))
}

func encodeUqIndexKeyPrefix(id, fieldIdx uint32) []byte {
	return []byte(fmt.Sprintf("u:%v:%v", id, fieldIdx))
}
func encodeNormalIndexPrefix(id, fieldIdx uint32) []byte {
	return []byte(fmt.Sprintf("i:%v:%v", id, fieldIdx))
}

func encodeUnionIndexPrefix(id uint32) []byte {
	return []byte(fmt.Sprintf("n:%v", id))
}

func encodeUqIndexKey(id uint32, fieldIdx uint32, val any) []byte {
	return []byte(fmt.Sprintf("u:%v:%v:%v", id, fieldIdx, val))
}

func encodeNormalIndexKey(id uint32, fieldIdx uint32, val any, pk_no uint64) []byte {
	return []byte(fmt.Sprintf("i:%v:%v:%v:%v", id, fieldIdx, val, pk_no))
}

func encodeUnionIndexKey(id uint32, indexContent string) []byte {
	return []byte(fmt.Sprintf("n:%v:%v", id, indexContent))
}

func encodeNormalIndexKeyPrefix(id uint32, fieldIdx uint32, val any) []byte {
	return []byte(fmt.Sprintf("i:%v:%v:%v", id, fieldIdx, val))
}
