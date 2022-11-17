package borm

import (
	"github.com/dgraph-io/badger/v3"
	"github.com/pkg/errors"
)

var (
	ErrValueLogSize          = badger.ErrValueLogSize
	ErrKeyNotFound           = badger.ErrKeyNotFound
	ErrTxnTooBig             = badger.ErrTxnTooBig
	ErrConflict              = badger.ErrConflict
	ErrReadOnlyTxn           = badger.ErrReadOnlyTxn
	ErrDiscardedTxn          = badger.ErrDiscardedTxn
	ErrEmptyKey              = badger.ErrEmptyKey
	ErrInvalidKey            = badger.ErrInvalidKey
	ErrBannedKey             = badger.ErrBannedKey
	ErrThresholdZero         = badger.ErrThresholdZero
	ErrNoRewrite             = badger.ErrNoRewrite
	ErrRejected              = badger.ErrRejected
	ErrInvalidRequest        = badger.ErrInvalidRequest
	ErrManagedTxn            = badger.ErrManagedTxn
	ErrNamespaceMode         = badger.ErrNamespaceMode
	ErrInvalidDump           = badger.ErrInvalidDump
	ErrZeroBandwidth         = badger.ErrZeroBandwidth
	ErrWindowsNotSupported   = badger.ErrWindowsNotSupported
	ErrPlan9NotSupported     = badger.ErrPlan9NotSupported
	ErrTruncateNeeded        = badger.ErrTruncateNeeded
	ErrBlockedWrites         = badger.ErrBlockedWrites
	ErrNilCallback           = badger.ErrNilCallback
	ErrEncryptionKeyMismatch = badger.ErrEncryptionKeyMismatch
	ErrInvalidDataKeyID      = badger.ErrInvalidDataKeyID
	ErrInvalidEncryptionKey  = badger.ErrInvalidEncryptionKey
	ErrGCInMemoryMode        = badger.ErrGCInMemoryMode
	ErrDBClosed              = badger.ErrDBClosed
)

var (
	ErrTableRepeat       = errors.New("Table already exists")
	ErrTableNotFound     = errors.New("Table not found")
	ErrIdxNotSupport     = errors.New("Index type not support")
	ErrIdxUniqueConflict = errors.New("Unique index conflict")
	ErrBatchInsertError  = errors.New("Number of inserts must be greater than 0")
	ErrRowIdIllegal      = errors.New("The row id must be set")
	ErrQueryInvalid      = errors.New("The query is invalid")
	ErrTypeNotBeSort     = errors.New("The sort key type error")
)
