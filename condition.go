package borm

import (
	"borm/common"
	"fmt"
	"math/big"
	"sort"
	"time"

	badger "github.com/dgraph-io/badger/v3"
	"github.com/elliotchance/orderedmap/v2"
)

type ICompoundConditions[T IRow] interface {
	Eq(fieldName string, val any) ICompoundConditions[T]
	In(fieldNames []string, values [][]any) ICompoundConditions[T]
	SortBy(reversed bool, sortKey ...string) ICompoundConditions[T]
	Limit(offset, limit int) ICompoundConditions[T]
	exec(txn *badger.Txn, db *BormDb) ([]T, error)
}

type inFilterCondition struct {
	fieldNames []string
	values     [][]any
}
type BaseCompoundCondition[T IRow] struct {
	fieldValueMap      *orderedmap.OrderedMap[string, any]
	inFilterConditions []inFilterCondition
	row                IRow

	sortKey   []string
	reverse   bool
	offset    int
	limit     int
	validated bool
}

func DefaultBaseCompoundCondition[T IRow](row IRow) *BaseCompoundCondition[T] {
	return &BaseCompoundCondition[T]{
		row:                row,
		validated:          true,
		fieldValueMap:      orderedmap.NewOrderedMap[string, any](),
		inFilterConditions: []inFilterCondition{},
	}
}

func (c *BaseCompoundCondition[T]) getRow() IRow {
	return c.row
}

func (c *BaseCompoundCondition[T]) CheckValidate() error {
	if !c.validated {
		return ErrQueryInvalid
	}
	return nil
}

func (c *BaseCompoundCondition[T]) queryInRowIds(txn *badger.Txn, db *BormDb, tableId uint32) ([]uint64, error) {
	arrays := [][]uint64{}
	for _, inFilterCondition := range c.inFilterConditions {
		fieldValues := []fieldKeyValue{}
		for _, values := range inFilterCondition.values {
			if len(values) != len(inFilterCondition.fieldNames) {
				return nil, ErrQueryInvalid
			}
			for j, value := range values {
				fieldValues = append(fieldValues, fieldKeyValue{
					fieldName: inFilterCondition.fieldNames[j],
					val:       value,
				})
			}
			ids, err := c.subQueryV2(txn, db, tableId, fieldValues)
			if err != nil {
				return nil, err
			}
			arrays = append(arrays, ids)
		}
	}
	return common.ArrayAggregate(arrays...), nil
}

func (c *BaseCompoundCondition[T]) queryEqRowIds(txn *badger.Txn, db *BormDb, tableId uint32) ([]uint64, error) {
	fieldValues := make([]fieldKeyValue, c.fieldValueMap.Len())

	for i, key := range c.fieldValueMap.Keys() {
		element := c.fieldValueMap.GetElement(key)
		fieldValues[i] = fieldKeyValue{
			fieldName: element.Key,
			val:       element.Value,
		}
	}
	return c.subQueryV2(txn, db, tableId, fieldValues)
}

type fieldKeyValue struct {
	fieldName string
	val       any
}

func (c *BaseCompoundCondition[T]) subQueryV2(txn *badger.Txn, db *BormDb, tableId uint32, fieldValues []fieldKeyValue) ([]uint64, error) {
	uniqueIdxMap := map[uint32]any{}
	unionIdxMap := map[uint32]any{}
	normalIdxMap := orderedmap.NewOrderedMap[uint32, any]()

	for _, fieldValue := range fieldValues {
		idx, err := db.tableManager.GetUniqueIdx(tableId, fieldValue.fieldName)
		if err != nil {
			if err == ErrIdxNotSupport {
				idx, err = db.tableManager.GetNormalIdx(tableId, fieldValue.fieldName)
				if err != nil {
					return nil, err
				}
				normalIdxMap.Set(idx, fieldValue.val)
			} else {
				return nil, err
			}
		} else {
			uniqueIdxMap[idx] = fieldValue.val
		}
	}
	//first match unionIdxMap
	unionTags := db.tableManager.GetUnionTags(tableId)
	for _, idx := range unionTags {
		val, ok := normalIdxMap.Get(idx)
		if ok {
			unionIdxMap[idx] = val
		}
	}
	//if match union index, delete from normalIdxMap
	//else set unionIdxMap empty
	if len(unionIdxMap) == len(unionTags) {
		for _, idx := range unionTags {
			normalIdxMap.Delete(idx)
		}
	} else {
		unionIdxMap = nil
	}
	arrays := [][]uint64{}
	if len(unionIdxMap) > 0 {
		id, err := db.TxQueryWithUnionIndex(txn, c.row, unionIdxMap)
		if err != nil {
			if err == ErrKeyNotFound {
				return []uint64{}, nil
			}
		}
		arrays = append(arrays, []uint64{id})
	}

	for idx, val := range uniqueIdxMap {
		id, err := db.TxQueryWithUniqueIndex(txn, c.row, idx, val)
		if err != nil {
			if err == ErrKeyNotFound {
				return []uint64{}, nil
			}
		}
		arrays = append(arrays, []uint64{id})
	}
	if normalIdxMap.Len() > 0 {
		element := normalIdxMap.Front()
		ids, err := db.TxQueryWithNormalIndex(txn, c.row, element.Key, element.Value)
		if err != nil {
			if err == ErrKeyNotFound {
				return []uint64{}, nil
			}
		}
		for i := 0; i < len(ids); {
			if c.leftPrefixIndexMatch(txn, db, ids[i], normalIdxMap) {
				i++
			} else {
				ids = append(ids[:i], ids[i+1:]...)
			}
		}
		//left normal index match data
		arrays = append(arrays, ids)
	}
	return common.ArrayIntersection(arrays...), nil
}

func (c *BaseCompoundCondition[T]) queryRowIds(txn *badger.Txn, db *BormDb) ([]uint64, error) {
	err := c.CheckValidate()
	if err != nil {
		return nil, err
	}
	tableName := c.row.GetTableName()
	tableId, err := db.tableManager.GetTableId(tableName)
	if err != nil {
		return nil, err
	}
	intersection := [][]uint64{}
	if c.fieldValueMap.Len() > 0 {
		eqIds, err := c.queryEqRowIds(txn, db, tableId)
		if err != nil {
			return nil, err
		}
		intersection = append(intersection, eqIds)
	}
	if len(c.inFilterConditions) > 0 {
		inIds, err := c.queryInRowIds(txn, db, tableId)
		if err != nil {
			return nil, err
		}
		intersection = append(intersection, inIds)
	}
	queryResults := common.ArrayIntersection(intersection...)
	return queryResults, nil
}

func (c *BaseCompoundCondition[T]) exec(txn *badger.Txn, db *BormDb) ([]T, error) {

	start := time.Now()

	ids, err := c.queryRowIds(txn, db)
	if err != nil {
		return nil, err
	}
	//pre pk id sort
	if c.reverse {
		sort.Slice(ids, func(i, j int) bool { return ids[i] > ids[j] })
	} else {
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	}
	db.db.Opts().Logger.Infof("[%v][rows:%v]", time.Since(start), len(ids))
	results := []T{}
	err = db.TxQueryWithPk(txn, c.row, ids, func(row IRow) error {
		results = append(results, row.(T))
		return nil
	})
	if err != nil {
		return nil, err
	}
	results, err = c.sort(db, results)
	if err != nil {
		return nil, err
	}
	startIndex, endIndex := c.getStartAndEndRange(len(results))
	return results[startIndex:endIndex], nil
}

func (c *BaseCompoundCondition[T]) getStartAndEndRange(len int) (startIndex, endIndex int) {
	if c.offset > len {
		return 0, 0
	}
	startIndex = c.offset
	endIndex = len
	limitIndex := c.limit + startIndex
	if c.limit > 0 && limitIndex <= len {
		endIndex = limitIndex
	}
	return startIndex, endIndex
}

func (c *BaseCompoundCondition[T]) leftPrefixIndexMatch(txn *badger.Txn, db *BormDb, id uint64, normalIdxMap *orderedmap.OrderedMap[uint32, any]) bool {
	b := false
	err := db.TxQueryWithPk(txn, c.row, []uint64{id}, func(row IRow) error {
		for _, key := range normalIdxMap.Keys() {
			element := normalIdxMap.GetElement(key)

			expectedVal, err := db.GetFieldValWithFieldIndex(row, element.Key)
			if err != nil {
				return err
			}
			cmp, err := c.compare(element.Value, expectedVal)
			if err != nil {
				return err
			}
			if cmp != 0 {
				b = false
				return nil
			}
		}
		b = true
		return nil
	})
	if err != nil {
		b = false
	}
	return b

}

func (c *BaseCompoundCondition[T]) sort(db *BormDb, items []T) ([]T, error) {
	if len(c.sortKey) == 0 {
		return items, nil
	}
	sort.Slice(items, func(i, j int) bool {
		for _, sortKey := range c.sortKey {
			left := items[i]
			right := items[j]
			leftVal, err := db.GetFieldValWithFieldName(left, sortKey)
			if err != nil {
				return false
			}
			rightVal, err := db.GetFieldValWithFieldName(right, sortKey)
			if err != nil {
				return false
			}
			if c.reverse {
				leftVal, rightVal = rightVal, leftVal
			}
			cmp, err := c.compare(leftVal, rightVal)
			if err != nil {
				return false
			}
			if cmp == -1 {
				return true
			} else if cmp == 0 {
				continue
			}
			return false
		}
		return false
	})
	return items, nil
}

func (c *BaseCompoundCondition[T]) compare(value, other interface{}) (int, error) {
	switch value.(type) {
	case time.Time:
		tother, ok := other.(time.Time)
		if !ok {
			return 0, ErrTypeNotBeSort
		}

		if value.(time.Time).Equal(tother) {
			return 0, nil
		}

		if value.(time.Time).Before(tother) {
			return -1, nil
		}
		return 1, nil
	case big.Float:
		o, ok := other.(big.Float)
		if !ok {
			return 0, ErrTypeNotBeSort
		}

		v := value.(big.Float)

		return v.Cmp(&o), nil
	case big.Int:
		o, ok := other.(big.Int)
		if !ok {
			return 0, ErrTypeNotBeSort
		}

		v := value.(big.Int)

		return v.Cmp(&o), nil
	case big.Rat:
		o, ok := other.(big.Rat)
		if !ok {
			return 0, ErrTypeNotBeSort
		}

		v := value.(big.Rat)

		return v.Cmp(&o), nil
	case int:
		tother, ok := other.(int)
		if !ok {
			return 0, ErrTypeNotBeSort
		}

		if value.(int) == tother {
			return 0, nil
		}

		if value.(int) < tother {
			return -1, nil
		}
		return 1, nil
	case int8:
		tother, ok := other.(int8)
		if !ok {
			return 0, ErrTypeNotBeSort
		}

		if value.(int8) == tother {
			return 0, nil
		}

		if value.(int8) < tother {
			return -1, nil
		}
		return 1, nil

	case int16:
		tother, ok := other.(int16)
		if !ok {
			return 0, ErrTypeNotBeSort
		}

		if value.(int16) == tother {
			return 0, nil
		}

		if value.(int16) < tother {
			return -1, nil
		}
		return 1, nil
	case int32:
		tother, ok := other.(int32)
		if !ok {
			return 0, ErrTypeNotBeSort
		}

		if value.(int32) == tother {
			return 0, nil
		}

		if value.(int32) < tother {
			return -1, nil
		}
		return 1, nil

	case int64:
		tother, ok := other.(int64)
		if !ok {
			return 0, ErrTypeNotBeSort
		}

		if value.(int64) == tother {
			return 0, nil
		}

		if value.(int64) < tother {
			return -1, nil
		}
		return 1, nil
	case uint:
		tother, ok := other.(uint)
		if !ok {
			return 0, ErrTypeNotBeSort
		}

		if value.(uint) == tother {
			return 0, nil
		}

		if value.(uint) < tother {
			return -1, nil
		}
		return 1, nil
	case uint8:
		tother, ok := other.(uint8)
		if !ok {
			return 0, ErrTypeNotBeSort
		}

		if value.(uint8) == tother {
			return 0, nil
		}

		if value.(uint8) < tother {
			return -1, nil
		}
		return 1, nil

	case uint16:
		tother, ok := other.(uint16)
		if !ok {
			return 0, ErrTypeNotBeSort
		}

		if value.(uint16) == tother {
			return 0, nil
		}

		if value.(uint16) < tother {
			return -1, nil
		}
		return 1, nil
	case uint32:
		tother, ok := other.(uint32)
		if !ok {
			return 0, ErrTypeNotBeSort
		}

		if value.(uint32) == tother {
			return 0, nil
		}

		if value.(uint32) < tother {
			return -1, nil
		}
		return 1, nil

	case uint64:
		tother, ok := other.(uint64)
		if !ok {
			return 0, ErrTypeNotBeSort
		}

		if value.(uint64) == tother {
			return 0, nil
		}

		if value.(uint64) < tother {
			return -1, nil
		}
		return 1, nil
	case float32:
		tother, ok := other.(float32)
		if !ok {
			return 0, ErrTypeNotBeSort
		}

		if value.(float32) == tother {
			return 0, nil
		}

		if value.(float32) < tother {
			return -1, nil
		}
		return 1, nil
	case float64:
		tother, ok := other.(float64)
		if !ok {
			return 0, ErrTypeNotBeSort
		}

		if value.(float64) == tother {
			return 0, nil
		}

		if value.(float64) < tother {
			return -1, nil
		}
		return 1, nil
	case string:
		tother, ok := other.(string)
		if !ok {
			return 0, ErrTypeNotBeSort
		}

		if value.(string) == tother {
			return 0, nil
		}

		if value.(string) < tother {
			return -1, nil
		}
		return 1, nil
	default:
		valS := fmt.Sprintf("%s", value)
		otherS := fmt.Sprintf("%s", other)
		if valS == otherS {
			return 0, nil
		}

		if valS < otherS {
			return -1, nil
		}

		return 1, nil
	}
}

type AndCompoundCondition[T IRow] struct {
	*BaseCompoundCondition[T]
}

//OrCompoundCondition
//TODO waiting to implemented
type OrCompoundCondition[T IRow] struct {
	*BaseCompoundCondition[T]
}

//Eq like where user_id=568;
func (condition *AndCompoundCondition[T]) Eq(fieldName string, value any) ICompoundConditions[T] {
	_, ok := condition.fieldValueMap.Get(fieldName)
	if ok {
		condition.validated = false
	} else {
		condition.fieldValueMap.Set(fieldName, value)
	}
	return condition
}

//In like where (user_id,type) in ((568,6),(569,6),(600,8));
func (condition *AndCompoundCondition[T]) In(fieldNames []string, values [][]any) ICompoundConditions[T] {
	condition.inFilterConditions = append(condition.inFilterConditions, inFilterCondition{
		fieldNames: fieldNames,
		values:     values,
	})
	return condition
}

func (condition *AndCompoundCondition[T]) SortBy(reversed bool, sortKey ...string) ICompoundConditions[T] {
	condition.reverse = reversed
	condition.sortKey = sortKey
	return condition
}

func (condition *AndCompoundCondition[T]) Limit(offset, limit int) ICompoundConditions[T] {
	condition.offset = offset
	condition.limit = limit
	return condition
}
