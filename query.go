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
	return condition.exec(txn, db)
}
