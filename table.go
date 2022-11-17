package borm

import (
	"reflect"
	"sync"

	badger "github.com/dgraph-io/badger/v3"
)

type IRow interface {
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
	GetTableName() string
	Clone() any
}

type TableManager struct {
	tables    sync.Map
	tableSeqs sync.Map
	indexTags sync.Map
	unionTags sync.Map
}

func newTableManager() *TableManager {
	t := &TableManager{}
	t.tables = sync.Map{}
	t.indexTags = sync.Map{}
	t.unionTags = sync.Map{}
	t.tableSeqs = sync.Map{}
	return t
}

func (t *TableManager) GetTableId(tableName string) (uint32, error) {
	v, ok := t.tables.Load(tableName)
	if !ok {
		return 0, ErrTableNotFound
	}
	return v.(uint32), nil
}

func (t *TableManager) CreateTable(tp IRow, seq *badger.Sequence) error {
	tableName := tp.GetTableName()
	if _, err := t.GetTableId(tableName); err == nil {
		return ErrTableRepeat
	}
	value := reflect.ValueOf(tp)
	tapMap := map[uint32]*tag{}
	unionIndexSlice := []uint32{}
	//init index
	for i := 0; i < value.Elem().NumField(); i++ {
		//check first field must be pk field
		if i == 0 {
			if value.Elem().Type().Field(i).Name != "Id" {
				return ErrRowIdIllegal
			}
			if value.Elem().Type().Field(i).Type != reflect.TypeOf(uint64(0)) {
				return ErrRowIdIllegal
			}
		}
		tagStr := value.Elem().Type().Field(i).Tag.Get("idx")
		if tagStr == "" || tagStr == "-" {
			continue
		}
		idxType, err := getIndex(tagStr)
		if err != nil {
			return err
		}
		offset := value.Elem().Type().Field(i).Offset

		tag, err := GetTag(value.Elem().Type().Field(i).Name, value.Elem().Field(i).Interface(), offset, idxType)
		if err != nil {
			return err
		}
		tapMap[uint32(i)] = tag

		if checkIsUnion(tagStr) {
			unionIndexSlice = append(unionIndexSlice, uint32(i))
		}
	}
	tableId := uint32(0)
	t.tables.Range(func(key, value any) bool {
		tableId++
		return true
	})
	t.tables.Store(tableName, tableId)
	t.indexTags.Store(tableId, tapMap)
	t.unionTags.Store(tableId, unionIndexSlice)
	seq.Next()
	t.tableSeqs.Store(tableId, seq)
	return nil
}

func (t *TableManager) GetIndexTag(tableId uint32, fieldName string) (*tag, error) {
	tags := t.GetIndexTags(tableId)
	for _, tag := range tags {
		if tag.fieldName == fieldName {
			return tag, nil
		}
	}
	return nil, ErrIdxNotSupport
}

func (t *TableManager) GetIndexTags(tableId uint32) map[uint32]*tag {
	v, ok := t.indexTags.Load(tableId)
	if !ok {
		return map[uint32]*tag{}
	}

	return v.(map[uint32]*tag)
}

func (t *TableManager) GetUnionTags(tableId uint32) []uint32 {
	v, ok := t.unionTags.Load(tableId)
	if !ok {
		return []uint32{}
	}
	return v.([]uint32)
}

func (t *TableManager) GetNormalIdx(tableId uint32, fieldName string) (uint32, error) {
	tags := t.GetIndexTags(tableId)
	for idx, tag := range tags {
		if tag.fieldName == fieldName {
			if !tag.CheckIsNormal() {
				return 0, ErrIdxNotSupport
			}
			return idx, nil
		}
	}
	return 0, ErrIdxNotSupport
}

func (t *TableManager) GetUniqueIdx(tableId uint32, fieldName string) (uint32, error) {
	tags := t.GetIndexTags(tableId)
	for idx, tag := range tags {
		if tag.fieldName == fieldName {
			if !tag.CheckIsUnique() {
				return 0, ErrIdxNotSupport
			}
			return idx, nil
		}
	}
	return 0, ErrIdxNotSupport
}

func (t *TableManager) GetUnionTagsByFieldConditions(tableId uint32, conditionsMap map[string]any) map[uint32]any {
	v, ok := t.indexTags.Load(tableId)
	if !ok {
		return nil
	}
	resultMap := map[uint32]any{}
	for id, tag := range v.(map[uint32]*tag) {
		if v, ok := conditionsMap[tag.fieldName]; ok {
			resultMap[id] = v
		}
	}
	return resultMap

}

func (t *TableManager) Next(tableId uint32) (uint64, error) {
	v, ok := t.tableSeqs.Load(tableId)
	if !ok {
		return 0, ErrTableNotFound
	}
	return v.(*badger.Sequence).Next()
}
