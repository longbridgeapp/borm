package borm

import (
	badger "github.com/dgraph-io/badger/v3"
)

func WithAnd[T IRow](t T) ICompoundConditions[T] {
	condition := &AndCompoundCondition[T]{
		BaseCompoundCondition: DefaultBaseCompoundCondition[T](t),
	}
	return condition
}

func Find[T IRow](db *BormDb, condition ICompoundConditions[T]) ([]T, error) {
	var (
		results []T
		err     error
	)
	err = db.View(func(txn *badger.Txn) error {
		results, err = TxFind(txn, db, condition)
		return err
	})
	if err != nil {
		return nil, err
	}
	return results, nil
}

func TxFind[T IRow](txn *badger.Txn, db *BormDb, condition ICompoundConditions[T]) ([]T, error) {
	return condition.query(txn, db)
}

func First[T IRow](db *BormDb, condition ICompoundConditions[T]) (T, error) {
	var (
		t T
	)
	err := db.View(func(txn *badger.Txn) error {
		result, err := TxFirst(txn, db, condition)
		if err != nil {
			return err
		}
		t = result
		return nil
	})
	return t, err
}

func TxFirst[T IRow](txn *badger.Txn, db *BormDb, condition ICompoundConditions[T]) (T, error) {
	var (
		t T
	)
	condition = condition.Limit(0, 1)
	results, err := condition.query(txn, db)
	if err != nil {
		return t, err
	}
	if len(results) == 0 {
		return t, ErrKeyNotFound
	}
	t = results[0]
	return t, nil
}

func Last[T IRow](db *BormDb, condition ICompoundConditions[T]) (T, error) {
	var (
		t T
	)
	err := db.View(func(txn *badger.Txn) error {
		result, err := TxLast(txn, db, condition)
		if err != nil {
			return err
		}
		t = result
		return nil
	})
	return t, err
}

func TxLast[T IRow](txn *badger.Txn, db *BormDb, condition ICompoundConditions[T]) (T, error) {
	var (
		t T
	)
	condition = condition.SortBy(true).Limit(0, 1)
	results, err := condition.query(txn, db)
	if err != nil {
		return t, err
	}
	if len(results) == 0 {
		return t, ErrKeyNotFound
	}
	t = results[0]
	return t, nil
}

func Count[T IRow](db *BormDb, condition ICompoundConditions[T]) (int, error) {
	var (
		count int
		err   error
	)
	err = db.View(func(txn *badger.Txn) error {
		count, err = TxCount(txn, db, condition)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func TxCount[T IRow](txn *badger.Txn, db *BormDb, condition ICompoundConditions[T]) (int, error) {
	return condition.count(txn, db)
}
