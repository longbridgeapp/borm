package borm

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"unsafe"

	"github.com/longbridgeapp/borm/common"

	badger "github.com/dgraph-io/badger/v3"
)

type BormDb struct {
	db           *badger.DB
	tableManager *TableManager
}

type TableDetails struct {
	TotalCount      uint64
	UnionIndexCount uint64
	NormalIndex     map[string]uint64
	UniqueIndex     map[string]uint64
}

func New(opts ...Option) (*BormDb, error) {
	optConfig := newOptions(opts...)

	badgerConfig := badger.DefaultOptions("")
	badgerConfig = badgerConfig.WithInMemory(true)
	badgerConfig = badgerConfig.WithMemTableSize(optConfig.MemTableSize)
	switch optConfig.Logger.GetLogLevel() {
	case DEBUG:
		badgerConfig = badgerConfig.WithLoggingLevel(badger.DEBUG)
	case INFO:
		badgerConfig = badgerConfig.WithLoggingLevel(badger.INFO)
	case WARNING:
		badgerConfig = badgerConfig.WithLoggingLevel(badger.WARNING)
	case ERROR:
		badgerConfig = badgerConfig.WithLoggingLevel(badger.ERROR)
	default:
		badgerConfig = badgerConfig.WithLoggingLevel(badger.INFO)
	}
	db, err := badger.Open(badgerConfig)
	if err != nil {
		return nil, err
	}
	return &BormDb{
		db:           db,
		tableManager: newTableManager(),
	}, nil
}

func (bormDb *BormDb) Begin(update bool) *badger.Txn {
	return bormDb.db.NewTransaction(update)
}

func (bormDb *BormDb) Commit(tx *badger.Txn) error {
	defer tx.Discard()
	return tx.Commit()
}

func (bormDb *BormDb) Discard(tx *badger.Txn) {
	tx.Discard()
}

//CreateTable
func (bormDb *BormDb) CreateTable(row IRow) error {
	return bormDb.tableManager.CreateTable(row, bormDb.db)
}

//Single Insert
func (bormDb *BormDb) Insert(row IRow) error {
	err := bormDb.db.Update(func(txn *badger.Txn) error {
		return bormDb.TxInsert(txn, row)
	})
	if err == badger.ErrConflict {
		return bormDb.Insert(row)
	}
	return err
}

func (bormDb *BormDb) TxInsert(txn *badger.Txn, row IRow) error {
	tableName := row.GetTableName()
	id, err := bormDb.tableManager.GetTableId(tableName)
	if err != nil {
		return err
	}

	next, err := bormDb.tableManager.Next(id)
	if err != nil {
		return err
	}
	common.SetUint64(row, next)
	bs, err := row.Marshal()
	if err != nil {
		return err
	}

	err = txn.Set(encodePKey(id, next), bs)
	if err != nil {
		return err
	}
	return bormDb.createIndex(id, row, txn, next)
}

//BatchInsert
func (bormDb *BormDb) BatchInsert(rows []IRow) error {
	err := bormDb.db.Update(func(txn *badger.Txn) error {
		// return bormDb.TxBatchInsert(txn, rows)
		for i := 0; i < len(rows); i++ {
			err := bormDb.TxInsert(txn, rows[i])
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err == badger.ErrConflict {
		return bormDb.BatchInsert(rows)
	}
	return err
}

func (bormDb *BormDb) TxBatchInsert(txn *badger.Txn, rows []IRow) error {
	for i := 0; i < len(rows); i++ {
		err := bormDb.TxInsert(txn, rows[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (bormDb *BormDb) Delete(row IRow) error {
	err := bormDb.db.Update(func(txn *badger.Txn) error {
		return bormDb.TxDelete(txn, row)
	})
	if err == badger.ErrConflict {
		return bormDb.Delete(row)
	}
	return err
}

func (bormDb *BormDb) TxDelete(tx *badger.Txn, row IRow) error {
	rowId := common.GetUint64(row)
	if rowId == 0 {
		return ErrRowIdIllegal
	}
	tableName := row.GetTableName()
	tableId, err := bormDb.tableManager.GetTableId(tableName)
	if err != nil {
		return err
	}
	pk := encodePKey(tableId, rowId)
	item, err := tx.Get(pk)
	if err != nil {
		return err
	}
	err = item.Value(func(val []byte) error {
		return row.Unmarshal(val)
	})
	if err != nil {
		return err
	}
	err = tx.Delete(pk)
	if err != nil {
		return err
	}
	return bormDb.deleteIndex(tableId, row, tx)
}

//Update
func (bormDb *BormDb) Update(oldRow, newRow IRow) error {
	err := bormDb.db.Update(func(txn *badger.Txn) error {
		return bormDb.TxUpdate(txn, oldRow, newRow)
	})
	if err == badger.ErrConflict {
		bormDb.db.Opts().Logger.Warningf("Txn Update conflict,%v\n", newRow)
		return bormDb.Update(oldRow, newRow)
	}
	return err
}

func (bormDb *BormDb) TxUpdate(tx *badger.Txn, oldRow, newRow IRow) error {
	err := bormDb.TxDelete(tx, oldRow)
	if err != nil {
		return err
	}
	return bormDb.TxInsert(tx, newRow)
}

//Truncate table, not support tx
func (bormDb *BormDb) Truncate(row IRow) error {
	tableName := row.GetTableName()
	id, err := bormDb.tableManager.GetTableId(tableName)
	if err != nil {
		return err
	}
	prefixes := [][]byte{}
	prefixes = append(prefixes, encodeTablePrefixKey(id))
	indexTags := bormDb.tableManager.GetIndexTags(id)
	for fieldIdx, tag := range indexTags {
		if tag.CheckIsUnique() {
			prefixes = append(prefixes, encodeUqIndexKeyPrefix(id, fieldIdx))
		} else if tag.CheckIsNormal() {
			prefixes = append(prefixes, encodeNormalIndexPrefix(id, fieldIdx))
		}
	}
	if len(bormDb.tableManager.GetUnionTags(id)) > 0 {
		prefixes = append(prefixes, encodeUnionIndexPrefix(id))
	}

	for i := 0; i < len(prefixes); i++ {
		bormDb.db.DropPrefix(prefixes[i])
	}
	return nil
}

//Close
func (bormDb *BormDb) Close() error {
	return bormDb.db.Close()
}

func (bormDb *BormDb) View(fn func(txn *badger.Txn) error) error {
	return bormDb.db.View(fn)
}

func (bormDb *BormDb) TxQueryWithNormalIndex(txn *badger.Txn, row IRow, idx uint32, val any) ([]uint64, error) {
	tableId, err := bormDb.tableManager.GetTableId(row.GetTableName())
	if err != nil {
		return nil, err
	}
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()
	prefix := encodeNormalIndexKeyPrefix(tableId, idx, val)
	ids := []uint64{}
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		itemKey := it.Item().Key()
		lastIndex := bytes.LastIndexByte(itemKey, ':')
		id, err := strconv.ParseUint(string(itemKey[lastIndex+1:]), 10, 64)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func (bormDb *BormDb) TxQueryWithUniqueIndex(txn *badger.Txn, row IRow, idx uint32, val any) (uint64, error) {
	tableId, err := bormDb.tableManager.GetTableId(row.GetTableName())
	if err != nil {
		return 0, err
	}
	item, err := txn.Get(encodeUqIndexKey(tableId, idx, val))
	if err != nil {
		return 0, err
	}
	id := uint64(0)
	err = item.Value(func(val []byte) error {
		id = common.DecodedToUInt64(val)
		return nil
	})
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (bormDb *BormDb) TxQueryWithUnionIndex(txn *badger.Txn, row IRow, idxConditionsMap map[uint32]any) (uint64, error) {
	tableId, err := bormDb.tableManager.GetTableId(row.GetTableName())
	if err != nil {
		return 0, err
	}

	keys := make([]int, 0)

	for k := range idxConditionsMap {
		keys = append(keys, int(k))
	}
	sort.Ints(keys)
	indexContent := ""
	for i, key := range keys {
		if i == len(keys)-1 {
			indexContent += fmt.Sprintf("%v:%v", key, idxConditionsMap[uint32(key)])
		} else {
			indexContent += fmt.Sprintf("%v:%v:", key, idxConditionsMap[uint32(key)])
		}
	}
	item, err := txn.Get(encodeUnionIndexKey(tableId, indexContent))
	if err != nil {
		return 0, err
	}
	id := uint64(0)
	err = item.Value(func(val []byte) error {
		id = common.DecodedToUInt64(val)
		return nil
	})
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (bormDb *BormDb) TxQueryWithUnionIndexWithFieldMap(txn *badger.Txn, row IRow, conditionsMap map[string]any) (uint64, error) {
	tableId, err := bormDb.tableManager.GetTableId(row.GetTableName())
	if err != nil {
		return 0, err
	}

	idxConditionsMap := bormDb.tableManager.GetUnionTagsByFieldConditions(tableId, conditionsMap)

	keys := make([]int, 0)

	for k := range idxConditionsMap {
		keys = append(keys, int(k))
	}
	sort.Ints(keys)
	indexContent := ""
	for i, key := range keys {
		if i == len(keys)-1 {
			indexContent += fmt.Sprintf("%v:%v", key, idxConditionsMap[uint32(key)])
		} else {
			indexContent += fmt.Sprintf("%v:%v:", key, idxConditionsMap[uint32(key)])
		}
	}
	item, err := txn.Get(encodeUnionIndexKey(tableId, indexContent))
	if err != nil {
		return 0, err
	}
	id := uint64(0)
	err = item.Value(func(val []byte) error {
		id = common.DecodedToUInt64(val)
		return nil
	})
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (bormDb *BormDb) TxQueryWithPk(txn *badger.Txn, row IRow, ids []uint64, f func(IRow) error) error {
	tableId, err := bormDb.tableManager.GetTableId(row.GetTableName())
	if err != nil {
		return err
	}
	for i := 0; i < len(ids); i++ {
		key := encodePKey(tableId, ids[i])
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		err = item.Value(func(val []byte) error {
			tp := row.Clone().(IRow)
			err = tp.Unmarshal(val)
			if err != nil {
				return err
			}
			return f(tp)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (bormDb *BormDb) TxForeach(txn *badger.Txn, row IRow, f func(IRow) error) error {
	id, err := bormDb.tableManager.GetTableId(row.GetTableName())
	if err != nil {
		return err
	}
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()
	prefix := encodeTablePrefixKey(id)
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		err := it.Item().Value(func(v []byte) error {
			tp := row.Clone().(IRow)
			err = tp.Unmarshal(v)
			if err != nil {
				return err
			}
			return f(tp)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (bormDb *BormDb) Foreach(row IRow, f func(IRow) error) error {
	err := bormDb.db.View(func(txn *badger.Txn) error {
		return bormDb.TxForeach(txn, row, f)
	})
	if err == badger.ErrConflict {
		bormDb.db.Opts().Logger.Warningf("Foreach Txn read conflict,%v\n", row)
		return bormDb.Foreach(row, f)
	}
	return err
}

func (bormDb *BormDb) Count(row IRow) (count uint64, err error) {
	err = bormDb.db.View(func(txn *badger.Txn) error {
		count, err = bormDb.TxCount(txn, row)
		return err
	})
	if err == badger.ErrConflict {
		bormDb.db.Opts().Logger.Warningf("Count Txn read conflict,%v\n", row)
		return bormDb.Count(row)
	}
	return
}

func (bormDb *BormDb) TxCount(txn *badger.Txn, row IRow) (uint64, error) {
	id, err := bormDb.tableManager.GetTableId(row.GetTableName())
	if err != nil {
		return 0, err
	}
	prefix := encodeTablePrefixKey(id)
	count := bormDb.countWithPrefix(txn, prefix)
	return count, nil
}

func (bormDb *BormDb) GetFieldValWithFieldName(item IRow, FieldName string) (any, error) {
	tableName := item.GetTableName()
	tableId, err := bormDb.tableManager.GetTableId(tableName)
	if err != nil {
		return nil, err
	}
	tag, err := bormDb.tableManager.GetIndexTag(tableId, FieldName)
	if err != nil {
		return nil, err
	}
	ptr0 := common.GetUnsafeInterfaceUintptr(item)
	val := tag.GetPointerVal(unsafe.Pointer(uintptr(ptr0) + tag.offset))
	return val, nil
}

func (bormDb *BormDb) GetFieldValWithFieldIndex(item IRow, fieldIdx uint32) (any, error) {
	tableName := item.GetTableName()
	tableId, err := bormDb.tableManager.GetTableId(tableName)
	if err != nil {
		return nil, err
	}
	tagMap := bormDb.tableManager.GetIndexTags(tableId)
	tag := tagMap[fieldIdx]
	ptr0 := common.GetUnsafeInterfaceUintptr(item)
	val := tag.GetPointerVal(unsafe.Pointer(uintptr(ptr0) + tag.offset))
	return val, nil
}

//Dump
//dump table all row data, that this is not in order
func (bormDb *BormDb) Dump(tp IRow) ([]IRow, error) {
	results := []IRow{}
	bormDb.Foreach(tp, func(row IRow) error {
		results = append(results, row)
		return nil
	})
	return results, nil
}

//Snoop
//output all table row data count, index count
func (bormDb *BormDb) Snoop(tp IRow) (*TableDetails, error) {
	tableName := tp.GetTableName()
	id, err := bormDb.tableManager.GetTableId(tableName)
	if err != nil {
		return nil, err
	}
	tableDetails := &TableDetails{
		UniqueIndex: map[string]uint64{},
		NormalIndex: map[string]uint64{},
	}
	err = bormDb.View(func(txn *badger.Txn) error {

		totalRows, err := bormDb.TxCount(txn, tp)
		if err != nil {
			return err
		}
		tableDetails.TotalCount = totalRows

		indexTags := bormDb.tableManager.GetIndexTags(id)
		for fieldIdx, tag := range indexTags {
			if tag.CheckIsUnique() {
				count := bormDb.countWithPrefix(txn, encodeUqIndexKeyPrefix(id, fieldIdx))
				tableDetails.UniqueIndex[tag.fieldName] = count
				continue
			}
			if tag.CheckIsNormal() {
				count := bormDb.countWithPrefix(txn, encodeNormalIndexPrefix(id, fieldIdx))
				tableDetails.NormalIndex[tag.fieldName] = count
				continue
			}
		}
		if len(bormDb.tableManager.GetUnionTags(id)) > 0 {
			count := bormDb.countWithPrefix(txn, encodeUnionIndexPrefix(id))
			tableDetails.UnionIndexCount = count
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return tableDetails, nil
}

func (bormDb *BormDb) countWithPrefix(txn *badger.Txn, prefix []byte) uint64 {
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()
	count := uint64(0)
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		count++
	}
	return count
}

func (bormDb *BormDb) createUnionIndex(tableId uint32, item IRow, txn *badger.Txn, next uint64, unionTags []uint32, indexTags map[uint32]*tag) error {
	if len(unionTags) == 0 {
		return nil
	}
	ptr0 := common.GetUnsafeInterfaceUintptr(item)
	indexContent := ""
	for _, fieldIdx := range unionTags {
		tag, ok := indexTags[fieldIdx]
		if !ok {
			bormDb.db.Opts().Logger.Warningf("Union tag not found in indexTags,%v,%v\n", unionTags, indexTags)
			return ErrIdxNotSupport
		}
		val := tag.GetPointerVal(unsafe.Pointer(uintptr(ptr0) + tag.offset))
		indexContent += fmt.Sprintf("%v:%v", fieldIdx, val)
	}
	key := encodeUnionIndexKey(tableId, indexContent)
	if _, err := txn.Get(key); err == nil {
		return ErrIdxUniqueConflict
	}
	return txn.Set(key, common.EncodedFromUInt64(next))
}

func (bormDb *BormDb) createIndex(tableId uint32, item IRow, txn *badger.Txn, next uint64) error {
	indexTags := bormDb.tableManager.GetIndexTags(tableId)
	//not found index setup in table
	if len(indexTags) == 0 {
		return nil
	}
	ptr0 := common.GetUnsafeInterfaceUintptr(item)
	for fieldIdx, tag := range indexTags {
		val := tag.GetPointerVal(unsafe.Pointer(uintptr(ptr0) + tag.offset))
		if tag.CheckIsUnique() {
			key := encodeUqIndexKey(tableId, fieldIdx, val)
			if _, err := txn.Get(key); err == nil {
				return ErrIdxUniqueConflict
			}
			err := txn.Set(key, common.EncodedFromUInt64(next))
			if err != nil {
				return err
			}
		} else if tag.CheckIsNormal() {
			key := encodeNormalIndexKey(tableId, fieldIdx, val, next)
			err := txn.Set(key, nil)
			if err != nil {
				return err
			}
		}
	}
	//not found union index setup in table
	unionTags := bormDb.tableManager.GetUnionTags(tableId)
	if len(unionTags) == 0 {
		return nil
	}
	indexContent := ""
	for i, fieldIdx := range unionTags {
		tag, ok := indexTags[fieldIdx]
		if !ok {
			bormDb.db.Opts().Logger.Warningf("Union tag not found in indexTags,%v,%v\n", unionTags, indexTags)
			return ErrIdxNotSupport
		}
		val := tag.GetPointerVal(unsafe.Pointer(uintptr(ptr0) + tag.offset))
		if i == len(unionTags)-1 {
			indexContent += fmt.Sprintf("%v:%v", fieldIdx, val)
		} else {
			indexContent += fmt.Sprintf("%v:%v:", fieldIdx, val)
		}
	}
	key := encodeUnionIndexKey(tableId, indexContent)
	if _, err := txn.Get(key); err == nil {
		return ErrIdxUniqueConflict
	}
	return txn.Set(key, common.EncodedFromUInt64(next))
}

func (bormDb *BormDb) deleteIndex(tableId uint32, item IRow, txn *badger.Txn) error {
	indexTags := bormDb.tableManager.GetIndexTags(tableId)
	if len(indexTags) == 0 {
		return nil
	}
	ptr0 := common.GetUnsafeInterfaceUintptr(item)
	for i, tag := range indexTags {
		val := tag.GetPointerVal(unsafe.Pointer(uintptr(ptr0) + tag.offset))
		if tag.CheckIsUnique() {
			key := encodeUqIndexKey(tableId, i, val)
			if err := txn.Delete(key); err != nil {
				return err
			}
		} else if tag.CheckIsNormal() {
			key := encodeNormalIndexKey(tableId, i, val, common.GetUint64(item))
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
	}
	unionTags := bormDb.tableManager.GetUnionTags(tableId)
	if len(unionTags) == 0 {
		return nil
	}
	indexContent := ""
	for i, fieldIdx := range unionTags {
		tag, ok := indexTags[fieldIdx]
		if !ok {
			bormDb.db.Opts().Logger.Warningf("Union tag not found in indexTags,%v,%v\n", unionTags, indexTags)
			return ErrIdxNotSupport
		}
		val := tag.GetPointerVal(unsafe.Pointer(uintptr(ptr0) + tag.offset))
		if i == len(unionTags)-1 {
			indexContent += fmt.Sprintf("%v:%v", fieldIdx, val)
		} else {
			indexContent += fmt.Sprintf("%v:%v:", fieldIdx, val)
		}
	}
	key := encodeUnionIndexKey(tableId, indexContent)
	return txn.Delete(key)
}
