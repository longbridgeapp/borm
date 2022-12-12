package borm

import (
	"fmt"
	"sync"
	"testing"

	"github.com/longbridgeapp/borm/pb"
	"github.com/stretchr/testify/require"
)

func TestCount(t *testing.T) {
	t.Run("Count", func(t *testing.T) {
		runNewBorm(t, func(t *testing.T, db *BormDb) {
			err := db.CreateTable(&pb.AccountInfo{})
			require.NoError(t, err)

			for i := 0; i < 100; i++ {
				accountInfo := &pb.AccountInfo{
					Aaid:           uint64(10000 + i),
					AccountChannel: "lb",
					CashBooks:      make(map[string]*pb.Detail),
					StockBooks:     make(map[string]*pb.Detail),
					AccountProperties: &pb.AccountProperties{
						MainCurrency: "HKD",
						MaxFinance:   fmt.Sprint(i + 100000),
					},
				}
				err = db.Insert(accountInfo)
				require.NoError(t, err)
			}

			count, err := Count(db, WithAnd(&pb.AccountInfo{}).Eq("AccountChannel", "lb"))
			require.NoError(t, err)
			require.Equal(t, count, 100)

			count, err = Count(db, WithAnd(&pb.AccountInfo{}).Eq("AccountChannel", "lb").Limit(20, 50))
			require.NoError(t, err)
			require.Equal(t, count, 50)

			count, err = Count(db, WithAnd(&pb.AccountInfo{}).Eq("AccountChannel", "lb").Limit(0, 1))
			require.NoError(t, err)
			require.Equal(t, count, 1)

			count, err = Count(db, WithAnd(&pb.AccountInfo{}).Eq("AccountChannel", "longport_test"))
			require.NoError(t, err)
			require.Equal(t, count, 0)

			count, err = Count(db, WithAnd(&pb.AccountInfo{}).Eq("AccountChannel", "lb").Eq("Aaid", uint64(10005)))
			require.NoError(t, err)
			require.Equal(t, count, 1)

			count, err = Count(db, WithAnd(&pb.AccountInfo{}).Eq("AccountChannel", "lb").In([]string{"Aaid"}, [][]any{
				{uint64(10005)},
				{uint64(10006)},
				{uint64(10008)},
				{uint64(10009)},
				{uint64(1009)},
			}))
			require.NoError(t, err)
			require.Equal(t, count, 4)

			count, err = Count(db, WithAnd(&pb.AccountInfo{}).In([]string{"Aaid"}, [][]any{
				{uint64(10005)},
				{uint64(10006)},
				{uint64(10008)},
				{uint64(10009)},
				{uint64(1009)},
			}).Eq("AccountChannel", "longport_test"))
			require.NoError(t, err)
			require.Equal(t, count, 0)

			count, err = Count(db, WithAnd(&pb.AccountInfo{}).Eq("AccountChannel", "lb").In([]string{"Aaid"}, [][]any{
				{uint64(1005)},
				{uint64(1006)},
				{uint64(1008)},
				{uint64(1009)},
				{uint64(1009)},
			}))
			require.NoError(t, err)
			require.Equal(t, count, 0)

		})
	})
}

func TestLimit(t *testing.T) {
	t.Run("limit", func(t *testing.T) {
		runNewBorm(t, func(t *testing.T, db *BormDb) {
			err := db.CreateTable(&pb.AccountInfo{})
			require.NoError(t, err)
			for i := 0; i < 10; i++ {
				accountInfo := &pb.AccountInfo{
					Aaid:           uint64(10000 + i),
					AccountChannel: "lb",
					CashBooks:      make(map[string]*pb.Detail),
					StockBooks:     make(map[string]*pb.Detail),
					AccountProperties: &pb.AccountProperties{
						MainCurrency: "HKD",
						MaxFinance:   fmt.Sprint(i + 100000),
					},
				}
				err = db.Insert(accountInfo)
				require.NoError(t, err)
			}

			results, err := Find(db, WithAnd(&pb.AccountInfo{}).Eq("AccountChannel", "lb").Limit(0, 5))
			require.NoError(t, err)
			require.Equal(t, len(results), 5)

			results, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("AccountChannel", "lb").Limit(0, 10))
			require.NoError(t, err)
			require.Equal(t, len(results), 10)

			results, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("AccountChannel", "lb").Limit(0, 1))
			require.NoError(t, err)
			require.Equal(t, len(results), 1)

			results, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("AccountChannel", "lb").Limit(0, 11))
			require.NoError(t, err)
			require.Equal(t, len(results), 10)

			results, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("AccountChannel", "lb").Limit(11, 10))
			require.NoError(t, err)
			require.Equal(t, len(results), 0)

			results, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("AccountChannel", "lb").Limit(5, 100))
			require.NoError(t, err)
			require.Equal(t, len(results), 5)

			results, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("AccountChannel", "lb"))
			require.NoError(t, err)
			require.Equal(t, len(results), 10)

		})
	})
	t.Run("sort limit", func(t *testing.T) {
		runNewBorm(t, func(t *testing.T, db *BormDb) {
			err := db.CreateTable(&pb.AccountInfo{})
			require.NoError(t, err)
			for i := 0; i < 10; i++ {
				accountInfo := &pb.AccountInfo{
					Aaid:           uint64(10000 + i),
					AccountChannel: "lb",
					CashBooks:      make(map[string]*pb.Detail),
					StockBooks:     make(map[string]*pb.Detail),
					AccountProperties: &pb.AccountProperties{
						MainCurrency: "HKD",
						MaxFinance:   fmt.Sprint(i + 100000),
					},
				}
				err = db.Insert(accountInfo)
				require.NoError(t, err)
			}

			results, err := Find(db, WithAnd(&pb.AccountInfo{}).Eq("AccountChannel", "lb").SortBy(false, "Aaid", "AccountChannel").Limit(0, 5))
			require.NoError(t, err)
			require.Equal(t, len(results), 5)

			for i := 0; i < len(results); i++ {
				require.Equal(t, results[i].Id, uint64(i+1))
			}

			results, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("AccountChannel", "lb").SortBy(true, "Aaid", "AccountChannel").Limit(0, 5))
			require.NoError(t, err)
			require.Equal(t, len(results), 5)

			for i := 0; i < len(results); i++ {
				require.Equal(t, results[i].Id, uint64(10-i))
			}

			results, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("AccountChannel", "lb").SortBy(true, "Aaid", "AccountChannel").Limit(0, 10))
			require.NoError(t, err)
			require.Equal(t, len(results), 10)
			for i := 0; i < len(results); i++ {
				require.Equal(t, results[i].Id, uint64(10-i))
			}

			results, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("AccountChannel", "longport_test").SortBy(true, "Aaid", "AccountChannel").Limit(0, 10))
			require.NoError(t, err)
			require.Equal(t, len(results), 0)

			results, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("AccountChannel", "lb").Limit(3, 10).SortBy(true, "Aaid", "AccountChannel"))
			require.NoError(t, err)
			require.Equal(t, len(results), 7)

			for i := 0; i < len(results); i++ {
				require.Equal(t, results[i].Id, uint64(7-i))
			}
		})
	})
}

func TestFirstOrLast(t *testing.T) {
	t.Run("First", func(t *testing.T) {
		runNewBorm(t, func(t *testing.T, db *BormDb) {
			err := db.CreateTable(&pb.AccountInfo{})
			require.NoError(t, err)

			for i := 0; i < 10; i++ {
				accountInfo := &pb.AccountInfo{
					Aaid:           uint64(10000 + i),
					AccountChannel: "lb",
					CashBooks:      make(map[string]*pb.Detail),
					StockBooks:     make(map[string]*pb.Detail),
					AccountProperties: &pb.AccountProperties{
						MainCurrency: "HKD",
						MaxFinance:   fmt.Sprint(i + 100000),
					},
				}
				err = db.Insert(accountInfo)
				require.NoError(t, err)
			}

			result, err := Find(db, WithAnd(&pb.AccountInfo{}).Eq("Aaid", uint64(10005)).Eq("AccountChannel", "lb").Limit(0, 1))
			require.NoError(t, err)
			require.Equal(t, result[0].Id, uint64(6))
			require.Equal(t, result[0].AccountProperties.MaxFinance, "100005")

			result, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("AccountChannel", "lb").Limit(0, 1))
			require.NoError(t, err)
			require.Equal(t, result[0].Id, uint64(1))
			require.Equal(t, result[0].AccountProperties.MaxFinance, "100000")

			result, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("AccountChannel", "lb_sg").Limit(0, 1))
			require.NoError(t, err)
			require.Equal(t, len(result), 0)

			result, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("Aaid", uint64(10005)).Eq("AccountChannel", "lb_sg").Limit(0, 1))
			require.NoError(t, err)
			require.Equal(t, len(result), 0)

			result, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("Aaid", uint64(10005)).Eq("account_properties", "test").Limit(0, 1))
			require.ErrorIs(t, err, ErrIdxNotSupport)

			result, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("Aaid", uint64(10005)).Eq("Aaid", uint64(10006)).Limit(0, 1))
			require.ErrorIs(t, err, ErrQueryInvalid)

			result, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("Aaid", uint64(10005)).Limit(0, 1))
			require.NoError(t, err)
			require.Equal(t, result[0].Id, uint64(6))
			require.Equal(t, result[0].AccountProperties.MaxFinance, "100005")

			result, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("Aaid", uint64(10005)).Eq("Aaid", uint64(10005)).Limit(0, 1))
			require.ErrorIs(t, err, ErrQueryInvalid)

			result, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("Aaid", uint64(10005)).Eq("AccountChannel", "lb").Limit(0, 1))
			require.NoError(t, err)
			require.Equal(t, result[0].Id, uint64(6))
			require.Equal(t, result[0].AccountProperties.MaxFinance, "100005")

			result, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("Aaid", uint64(10005)).Eq("AccountChannel", "lb_sg").Limit(0, 1))
			require.NoError(t, err)
			require.Equal(t, len(result), 0)
		})
		runNewBorm(t, func(t *testing.T, db *BormDb) {
			err := db.CreateTable(&pb.AccountInfo{})
			require.NoError(t, err)

			for i := 0; i < 10; i++ {
				accountInfo := &pb.AccountInfo{
					Aaid:           uint64(10000 + i),
					AccountChannel: "lb",
					CashBooks:      make(map[string]*pb.Detail),
					StockBooks:     make(map[string]*pb.Detail),
					AccountProperties: &pb.AccountProperties{
						MainCurrency: "HKD",
						MaxFinance:   fmt.Sprint(i + 100000),
					},
				}
				err = db.Insert(accountInfo)
				require.NoError(t, err)
			}
			result, err := First(db, WithAnd(&pb.AccountInfo{}).Eq("Aaid", uint64(10005)).Eq("AccountChannel", "lb"))
			require.NoError(t, err)
			require.Equal(t, result.Id, uint64(6))
			require.Equal(t, result.AccountProperties.MaxFinance, "100005")

			result, err = First(db, WithAnd(&pb.AccountInfo{}).Eq("AccountChannel", "lb"))
			require.NoError(t, err)
			require.Equal(t, result.Id, uint64(1))
			require.Equal(t, result.AccountProperties.MaxFinance, "100000")

			result, err = First(db, WithAnd(&pb.AccountInfo{}).Eq("AccountChannel", "lb_sg"))
			require.ErrorIs(t, err, ErrKeyNotFound)
			require.Equal(t, result == nil, true)

			result, err = First(db, WithAnd(&pb.AccountInfo{}).Eq("Aaid", uint64(10005)).Eq("AccountChannel", "lb_sg"))
			require.ErrorIs(t, err, ErrKeyNotFound)
			require.Equal(t, result == nil, true)

			result, err = First(db, WithAnd(&pb.AccountInfo{}).Eq("Aaid", uint64(10005)).Eq("account_properties", "test"))
			require.ErrorIs(t, err, ErrIdxNotSupport)

			result, err = First(db, WithAnd(&pb.AccountInfo{}).Eq("Aaid", uint64(10005)).Eq("Aaid", uint64(10006)))
			require.ErrorIs(t, err, ErrQueryInvalid)

			result, err = First(db, WithAnd(&pb.AccountInfo{}).Eq("Aaid", uint64(10005)))
			require.NoError(t, err)
			require.Equal(t, result.Id, uint64(6))
			require.Equal(t, result.AccountProperties.MaxFinance, "100005")

			result, err = First(db, WithAnd(&pb.AccountInfo{}).Eq("Aaid", uint64(10005)).Eq("Aaid", uint64(10005)))
			require.ErrorIs(t, err, ErrQueryInvalid)

			result, err = First(db, WithAnd(&pb.AccountInfo{}).Eq("Aaid", uint64(10005)).Eq("AccountChannel", "lb"))
			require.NoError(t, err)
			require.Equal(t, result.Id, uint64(6))
			require.Equal(t, result.AccountProperties.MaxFinance, "100005")

			result, err = First(db, WithAnd(&pb.AccountInfo{}).Eq("Aaid", uint64(10005)).Eq("AccountChannel", "lb_sg"))
			require.ErrorIs(t, err, ErrKeyNotFound)
			require.Equal(t, result == nil, true)
		})
	})

	t.Run("Last", func(t *testing.T) {
		runNewBorm(t, func(t *testing.T, db *BormDb) {
			err := db.CreateTable(&pb.AccountInfo{})
			require.NoError(t, err)

			for i := 0; i < 10; i++ {
				accountInfo := &pb.AccountInfo{
					Aaid:           uint64(10000 + i),
					AccountChannel: "lb",
					CashBooks:      make(map[string]*pb.Detail),
					StockBooks:     make(map[string]*pb.Detail),
					AccountProperties: &pb.AccountProperties{
						MainCurrency: "HKD",
						MaxFinance:   fmt.Sprint(i + 100000),
					},
				}
				err = db.Insert(accountInfo)
				require.NoError(t, err)
			}

			result, err := Find(db, WithAnd(&pb.AccountInfo{}).Eq("Aaid", uint64(10005)).Eq("AccountChannel", "lb").SortBy(true).Limit(0, 1))
			require.NoError(t, err)
			require.Equal(t, result[0].Id, uint64(6))
			require.Equal(t, result[0].AccountProperties.MaxFinance, "100005")

			result, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("AccountChannel", "lb").SortBy(true).Limit(0, 1))
			require.NoError(t, err)
			require.Equal(t, result[0].Id, uint64(10))
			require.Equal(t, result[0].AccountProperties.MaxFinance, "100009")

			result, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("AccountChannel", "lb_sg").SortBy(true).Limit(0, 1))
			require.NoError(t, err)
			require.Equal(t, len(result), 0)

			result, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("Aaid", uint64(10005)).Eq("AccountChannel", "lb_sg").SortBy(true).Limit(0, 1))
			require.NoError(t, err)
			require.Equal(t, len(result), 0)

			result, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("Aaid", uint64(10005)).Eq("account_properties", "test").SortBy(true).Limit(0, 1))
			require.ErrorIs(t, err, ErrIdxNotSupport)

			result, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("Aaid", uint64(10005)).Eq("Aaid", uint64(10006)).SortBy(true).Limit(0, 1))
			require.ErrorIs(t, err, ErrQueryInvalid)

			result, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("Aaid", uint64(10005)).SortBy(true).Limit(0, 1))
			require.NoError(t, err)
			require.Equal(t, result[0].Id, uint64(6))
			require.Equal(t, result[0].AccountProperties.MaxFinance, "100005")

			result, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("Aaid", uint64(10005)).Eq("Aaid", uint64(10005)).SortBy(true).Limit(0, 1))
			require.ErrorIs(t, err, ErrQueryInvalid)

			result, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("Aaid", uint64(10005)).Eq("AccountChannel", "lb").SortBy(true).Limit(0, 1))
			require.NoError(t, err)
			require.Equal(t, result[0].Id, uint64(6))
			require.Equal(t, result[0].AccountProperties.MaxFinance, "100005")

			result, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("Aaid", uint64(10005)).Eq("AccountChannel", "lb_sg").SortBy(true).Limit(0, 1))
			require.NoError(t, err)
			require.Equal(t, len(result), 0)
		})
		runNewBorm(t, func(t *testing.T, db *BormDb) {
			err := db.CreateTable(&pb.AccountInfo{})
			require.NoError(t, err)

			for i := 0; i < 10; i++ {
				accountInfo := &pb.AccountInfo{
					Aaid:           uint64(10000 + i),
					AccountChannel: "lb",
					CashBooks:      make(map[string]*pb.Detail),
					StockBooks:     make(map[string]*pb.Detail),
					AccountProperties: &pb.AccountProperties{
						MainCurrency: "HKD",
						MaxFinance:   fmt.Sprint(i + 100000),
					},
				}
				err = db.Insert(accountInfo)
				require.NoError(t, err)
			}

			result, err := Last(db, WithAnd(&pb.AccountInfo{}).Eq("Aaid", uint64(10005)).Eq("AccountChannel", "lb"))
			require.NoError(t, err)
			require.Equal(t, result.Id, uint64(6))
			require.Equal(t, result.AccountProperties.MaxFinance, "100005")

			result, err = Last(db, WithAnd(&pb.AccountInfo{}).Eq("AccountChannel", "lb"))
			require.NoError(t, err)
			require.Equal(t, result.Id, uint64(10))
			require.Equal(t, result.AccountProperties.MaxFinance, "100009")

			result, err = Last(db, WithAnd(&pb.AccountInfo{}).Eq("AccountChannel", "lb_sg"))
			require.ErrorIs(t, err, ErrKeyNotFound)
			require.Equal(t, result == nil, true)

			result, err = Last(db, WithAnd(&pb.AccountInfo{}).Eq("Aaid", uint64(10005)).Eq("AccountChannel", "lb_sg"))
			require.ErrorIs(t, err, ErrKeyNotFound)
			require.Equal(t, result == nil, true)

			result, err = Last(db, WithAnd(&pb.AccountInfo{}).Eq("Aaid", uint64(10005)).Eq("account_properties", "test"))
			require.ErrorIs(t, err, ErrIdxNotSupport)
			require.Equal(t, result == nil, true)

			result, err = Last(db, WithAnd(&pb.AccountInfo{}).Eq("Aaid", uint64(10005)).Eq("Aaid", uint64(10006)))
			require.ErrorIs(t, err, ErrQueryInvalid)
			require.Equal(t, result == nil, true)

			result, err = Last(db, WithAnd(&pb.AccountInfo{}).Eq("Aaid", uint64(10005)))
			require.NoError(t, err)
			require.Equal(t, result.Id, uint64(6))
			require.Equal(t, result.AccountProperties.MaxFinance, "100005")

			result, err = Last(db, WithAnd(&pb.AccountInfo{}).Eq("Aaid", uint64(10005)).Eq("Aaid", uint64(10005)))
			require.ErrorIs(t, err, ErrQueryInvalid)
			require.Equal(t, result == nil, true)

			result, err = Last(db, WithAnd(&pb.AccountInfo{}).Eq("Aaid", uint64(10005)).Eq("AccountChannel", "lb"))
			require.NoError(t, err)
			require.Equal(t, result.Id, uint64(6))
			require.Equal(t, result.AccountProperties.MaxFinance, "100005")

			result, err = Last(db, WithAnd(&pb.AccountInfo{}).Eq("Aaid", uint64(10005)).Eq("AccountChannel", "lb_sg"))
			require.ErrorIs(t, err, ErrKeyNotFound)
			require.Equal(t, result == nil, true)
		})
	})
}

func TestConcurrentFind(t *testing.T) {
	t.Run("ConcurrentFind", func(t *testing.T) {
		runNewBorm(t, func(t *testing.T, db *BormDb) {
			err := db.CreateTable(&pb.Person{})
			require.NoError(t, err)

			wait := sync.WaitGroup{}
			wait.Add(1)

			go func() {
				defer wait.Done()
				txn := db.Begin(true)
				defer db.Commit(txn)
				for i := 0; i < 1000; i++ {
					err = db.TxInsert(txn, &pb.Person{
						Name:     "jacky",
						Phone:    fmt.Sprintf("+86%v", i),
						Age:      20,
						BirthDay: 19901111,
						Gender:   pb.Gender_men,
					})
					require.NoError(t, err)
				}
			}()

			for {
				results, err := Find(db, WithAnd(&pb.Person{}).Eq("Name", "jacky"))
				require.NoError(t, err)
				if len(results) == 1000 {
					break
				}
				require.Equal(t, len(results), 0)

			}
			wait.Wait()

		})

		runNewBorm(t, func(t *testing.T, db *BormDb) {
			err := db.CreateTable(&pb.Person{})
			require.NoError(t, err)

			wait := sync.WaitGroup{}
			wait.Add(1)

			go func() {
				defer wait.Done()
				for i := 0; i < 1000; i++ {
					err = db.Insert(&pb.Person{
						Name:     "jacky",
						Phone:    fmt.Sprintf("+86%v", i),
						Age:      20,
						BirthDay: 19901111,
						Gender:   pb.Gender_men,
					})
					require.NoError(t, err)
				}
			}()
			for {
				results, err := Find(db, WithAnd(&pb.Person{}).Eq("Name", "jacky"))
				require.NoError(t, err)
				if len(results) == 1000 {
					break
				}
				require.Less(t, len(results), 1000)
			}
			wait.Wait()
		})

		runNewBorm(t, func(t *testing.T, db *BormDb) {
			err := db.CreateTable(&pb.Person{})
			require.NoError(t, err)

			txReadOnly := db.Begin(false)
			defer db.Discard(txReadOnly)

			sw := false
			go func() {
				defer func() {
					sw = true
				}()
				tx := db.Begin(true)
				defer db.Discard(tx)
				for i := 0; i < 10000; i++ {
					err = db.TxInsert(tx, &pb.Person{
						Name:     "jacky",
						Phone:    fmt.Sprintf("+86%v", i),
						Age:      20,
						BirthDay: 19901111,
						Gender:   pb.Gender_men,
					})
					require.NoError(t, err)
				}
			}()
			for {
				count, err := db.TxCount(txReadOnly, &pb.Person{})
				require.NoError(t, err)
				require.Equal(t, count, uint64(0))
				if sw {
					break
				}
			}
		})
	})
}

func TestFind(t *testing.T) {
	t.Run("normal index check", func(t *testing.T) {
		runNewBorm(t, func(t *testing.T, db *BormDb) {
			err := db.CreateTable(&pb.AccountInfo{})
			require.NoError(t, err)

			for i := 0; i < 10; i++ {
				accountInfo := &pb.AccountInfo{
					Aaid:           uint64(10000 + i),
					AccountChannel: "lb",
					CashBooks:      make(map[string]*pb.Detail),
					StockBooks:     make(map[string]*pb.Detail),
					AccountProperties: &pb.AccountProperties{
						MainCurrency: "HKD",
						MaxFinance:   fmt.Sprint(i + 100000),
					},
				}
				err = db.Insert(accountInfo)
				require.NoError(t, err)
			}

			results, err := Find(db, WithAnd(&pb.AccountInfo{}).Eq("Aaid", uint64(10005)).Eq("AccountChannel", "lb"))
			require.NoError(t, err)
			require.Equal(t, len(results), 1)
			require.Equal(t, results[0].AccountProperties.MaxFinance, "100005")

			results, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("AccountChannel", "lb"))
			require.NoError(t, err)
			require.Equal(t, len(results), 10)

			results, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("AccountChannel", "lb_sg"))
			require.NoError(t, err)
			require.Equal(t, len(results), 0)

			results, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("Aaid", uint64(10005)).Eq("AccountChannel", "lb_sg"))
			require.NoError(t, err)
			require.Equal(t, len(results), 0)

			results, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("Aaid", uint64(10005)).Eq("account_properties", "test"))
			require.ErrorIs(t, err, ErrIdxNotSupport)
			require.Equal(t, len(results), 0)

			results, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("Aaid", uint64(10005)).Eq("Aaid", uint64(10006)))
			require.ErrorIs(t, err, ErrQueryInvalid)
			require.Equal(t, len(results), 0)

			results, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("Aaid", uint64(10005)))
			require.NoError(t, err)
			require.Equal(t, len(results), 1)

			results, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("Aaid", uint64(10005)).Eq("Aaid", uint64(10005)))
			require.ErrorIs(t, err, ErrQueryInvalid)
			require.Equal(t, len(results), 0)

			results, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("Aaid", uint64(10005)).Eq("AccountChannel", "lb"))
			require.NoError(t, err)
			require.Equal(t, len(results), 1)
			require.Equal(t, results[0].AccountProperties.MaxFinance, "100005")

			results, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("Aaid", uint64(10005)).Eq("AccountChannel", "lb_sg"))
			require.NoError(t, err)
			require.Equal(t, len(results), 0)
		})
	})

	t.Run("union index check", func(t *testing.T) {
		runNewBorm(t, func(t *testing.T, db *BormDb) {
			err := db.CreateTable(&pb.AccountInfo{})
			require.NoError(t, err)
			for i := 0; i < 1000; i++ {
				accountInfo := &pb.AccountInfo{
					Aaid:           uint64(10000 + i),
					AccountChannel: "lb",
					CashBooks:      make(map[string]*pb.Detail),
					StockBooks:     make(map[string]*pb.Detail),
					AccountProperties: &pb.AccountProperties{
						MainCurrency: "HKD",
						MaxFinance:   fmt.Sprint(i + 100000),
					},
				}
				err = db.Insert(accountInfo)
				require.NoError(t, err)
			}
			for i := 0; i < 1000; i++ {
				results, err := Find(db, WithAnd(&pb.AccountInfo{}).Eq("Aaid", uint64(uint64(10000+i))).Eq("AccountChannel", "lb"))
				require.NoError(t, err)
				require.Equal(t, len(results), 1)
				require.Equal(t, results[0].AccountProperties.MaxFinance, fmt.Sprint(i+100000))
			}
			results, err := Find(db, WithAnd(&pb.AccountInfo{}).Eq("Aaid", uint64(10005)).Eq("AccountChannel", "lb_sg"))
			require.NoError(t, err)
			require.Equal(t, len(results), 0)

			results, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("Aaid", uint64(10005)))
			require.NoError(t, err)
			require.Equal(t, len(results), 1)

			results, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("AccountChannel", "lb"))
			require.NoError(t, err)
			require.Equal(t, len(results), 1000)

			results, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("AccountChannel", "lb").Eq("Aaid", uint64(10005)))
			require.NoError(t, err)
			require.Equal(t, len(results), 1)

			results, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("Aaid", uint64(10005)).Eq("AccountChannel", "lb"))
			require.NoError(t, err)
			require.Equal(t, len(results), 1)

			results, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("Aaid", 10005).Eq("Aaid", uint64(10005)))
			require.ErrorIs(t, err, ErrQueryInvalid)
			require.Nil(t, results)

			results, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("AccountChannel", "lb").Eq("Aaid", uint64(10005)).Eq("Aaid", uint64(10006)))
			require.ErrorIs(t, err, ErrQueryInvalid)
			require.Nil(t, results)
		})
	})

	t.Run("unique index check", func(t *testing.T) {
		runNewBorm(t, func(t *testing.T, db *BormDb) {
			err := db.CreateTable(&pb.Person{})
			require.NoError(t, err)

			for i := 0; i < 10; i++ {
				person := &pb.Person{
					Name:  "jacky",
					Phone: fmt.Sprintf("+86%v", i),
					Age:   30,
				}
				err := db.Insert(person)
				require.NoError(t, err)
			}
			for i := 0; i < 20; i++ {
				results, err := Find(db, WithAnd(&pb.Person{}).Eq("Phone", fmt.Sprintf("+86%v", i)))
				require.NoError(t, err)
				if i > 9 {
					require.Equal(t, len(results), 0)
				} else {
					require.Equal(t, len(results), 1)
				}
			}

			results, err := Find(db, WithAnd(&pb.Person{}).Eq("Name", "jacky"))
			require.NoError(t, err)
			require.Equal(t, len(results), 10)

			results, err = Find(db, WithAnd(&pb.Person{}).Eq("Phone", "+865").Eq("Name", "jacky"))
			require.NoError(t, err)
			require.Equal(t, len(results), 1)

			results, err = Find(db, WithAnd(&pb.Person{}).Eq("Phone", "+865").Eq("Phone", "866"))
			require.ErrorIs(t, err, ErrQueryInvalid)
			require.Equal(t, len(results), 0)

		})
	})

}

func TestIn(t *testing.T) {
	t.Run("normal index check", func(t *testing.T) {
		runNewBorm(t, func(t *testing.T, db *BormDb) {
			err := db.CreateTable(&pb.AccountInfo{})
			require.NoError(t, err)

			for i := 0; i < 10; i++ {
				accountInfo := &pb.AccountInfo{
					Aaid:           uint64(10000 + i),
					AccountChannel: "lb",
					CashBooks:      make(map[string]*pb.Detail),
					StockBooks:     make(map[string]*pb.Detail),
					AccountProperties: &pb.AccountProperties{
						MainCurrency: "HKD",
						MaxFinance:   fmt.Sprint(i + 100000),
					},
				}
				err = db.Insert(accountInfo)
				require.NoError(t, err)
			}
			ss := [][]any{}
			ss = append(ss, []any{"lb", 10005}, []any{"lb", 10006})
			results, err := Find(db, WithAnd(&pb.AccountInfo{}).In([]string{"AccountChannel", "Aaid"}, ss).Limit(0, 5))
			require.NoError(t, err)
			require.Equal(t, len(results), 2)

			results, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("AccountChannel", "lb").In([]string{"AccountChannel", "Aaid"}, ss).Limit(0, 5))
			require.NoError(t, err)
			require.Equal(t, len(results), 2)

			results, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("AccountChannel", "longport_test").In([]string{"AccountChannel", "Aaid"}, ss).Limit(0, 5))
			require.NoError(t, err)
			require.Equal(t, len(results), 0)

			ss = [][]any{}
			ss = append(ss, []any{"longport_test", 10005}, []any{"lb", 10006})
			results, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("AccountChannel", "lb").In([]string{"AccountChannel", "Aaid"}, ss).Limit(0, 5))
			require.NoError(t, err)
			require.Equal(t, len(results), 1)

			ss = [][]any{}
			ss = append(ss, []any{"lb", 10015}, []any{"lb", 10006})
			results, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("AccountChannel", "lb").In([]string{"AccountChannel", "Aaid"}, ss).Limit(0, 5))
			require.NoError(t, err)
			require.Equal(t, len(results), 1)

			ss = [][]any{}
			ss = append(ss, []any{"lb", 10015}, []any{"longport_test", 10006})
			results, err = Find(db, WithAnd(&pb.AccountInfo{}).Eq("AccountChannel", "lb").In([]string{"AccountChannel", "Aaid"}, ss).Limit(0, 5))
			require.NoError(t, err)
			require.Equal(t, len(results), 0)

			ss = [][]any{}
			ss = append(ss, []any{"lb", 10015}, []any{"longport_test", 10006})
			results, err = Find(db, WithAnd(&pb.AccountInfo{}).In([]string{"AccountChannel", "Aaid"}, ss).Limit(0, 5))
			require.NoError(t, err)
			require.Equal(t, len(results), 0)

			ss = [][]any{}
			ss = append(ss, []any{"lb"}, []any{"longport_test"})
			results, err = Find(db, WithAnd(&pb.AccountInfo{}).In([]string{"AccountChannel", "Aaid"}, ss).Limit(0, 5))
			require.ErrorIs(t, err, ErrQueryInvalid)
			require.Equal(t, len(results), 0)

			ss = [][]any{}

			for i := 0; i < 100; i++ {
				ss = append(ss, []any{"lb", uint64(10000 + i)}, []any{"lb", uint64(10000 + i)})
			}

			results, err = Find(db, WithAnd(&pb.AccountInfo{}).In([]string{"AccountChannel", "Aaid"}, ss).SortBy(true, "Aaid").Limit(0, 100))
			require.NoError(t, err)
			require.Equal(t, len(results), 10)
			for i := 0; i < 10; i++ {
				require.Equal(t, results[i].Aaid, uint64(10009-i))
			}

			results, err = Find(db, WithAnd(&pb.AccountInfo{}).In([]string{"AccountChannel", "Aaid"}, ss).SortBy(false, "Aaid").Limit(0, 100))
			require.NoError(t, err)
			require.Equal(t, len(results), 10)
			for i := 0; i < 10; i++ {
				require.Equal(t, results[i].Aaid, uint64(10000+i))
			}
		})
	})
}
