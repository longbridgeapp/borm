package borm

import (
	"fmt"
	"sync"
	"testing"

	"github.com/longbridgeapp/borm/pb"

	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/require"
)

func runNewBorm(t *testing.T, test func(*testing.T, *BormDb)) {
	db, err := New()
	require.NoError(t, err)
	test(t, db)
	require.NoError(t, db.Close())
}

func TestCreateTable(t *testing.T) {
	t.Run("CreateTable", func(t *testing.T) {
		runNewBorm(t, func(t *testing.T, db *BormDb) {
			err := db.CreateTable(&pb.Person{})
			require.NoError(t, err)
			err = db.CreateTable(&pb.Person{})
			require.ErrorIs(t, err, ErrTableRepeat)

			err = db.CreateTable(&pb.Order{})
			require.NoError(t, err)
		})
	})
	t.Run("pk id check", func(t *testing.T) {
		runNewBorm(t, func(t *testing.T, db *BormDb) {
			err := db.CreateTable(&pb.IllegalPerson_1{})
			require.ErrorIs(t, err, ErrRowIdIllegal)
			err = db.CreateTable(&pb.IllegalPerson_2{})
			require.ErrorIs(t, err, ErrRowIdIllegal)
		})
	})
}

func TestManageTable(t *testing.T) {
	t.Run("Snoop", func(t *testing.T) {
		runNewBorm(t, func(t *testing.T, db *BormDb) {
			err := db.CreateTable(&pb.AccountInfo{})
			require.NoError(t, err)

			detail, err := db.Snoop(&pb.AccountInfo{})
			require.NoError(t, err)
			require.Equal(t, detail.TotalCount, uint64(0))
			require.Equal(t, detail.TotalCount, uint64(0))
			require.Equal(t, len(detail.NormalIndex), 2)
			require.Equal(t, len(detail.UniqueIndex), 0)
			require.Equal(t, detail.NormalIndex["AccountChannel"], uint64(0))
			require.Equal(t, detail.NormalIndex["Aaid"], uint64(0))

			items := []IRow{}
			for i := 0; i < 1000; i++ {
				accountInfo := &pb.AccountInfo{
					Aaid:           uint64(10000 + i),
					AccountChannel: "lb",
					CashBooks:      make(map[string]*pb.Detail),
					StockBooks:     make(map[string]*pb.Detail),
					AccountProperties: &pb.AccountProperties{
						MainCurrency: "HKD",
						MaxFinance:   "10000",
					},
				}
				items = append(items, accountInfo)
			}
			err = db.BatchInsert(items)
			require.NoError(t, err)
			detail, err = db.Snoop(&pb.AccountInfo{})
			require.NoError(t, err)
			require.Equal(t, detail.TotalCount, uint64(1000))
			require.Equal(t, detail.TotalCount, uint64(1000))
			require.Equal(t, len(detail.NormalIndex), 2)
			require.Equal(t, detail.NormalIndex["AccountChannel"], uint64(1000))
			require.Equal(t, detail.NormalIndex["Aaid"], uint64(1000))
			require.Equal(t, len(detail.NormalIndex), 2)

			err = db.Truncate(&pb.AccountInfo{})
			require.NoError(t, err)

			detail, err = db.Snoop(&pb.AccountInfo{})
			require.NoError(t, err)
			require.Equal(t, detail.TotalCount, uint64(0))
			require.Equal(t, detail.TotalCount, uint64(0))
			require.Equal(t, len(detail.NormalIndex), 2)
			require.Equal(t, len(detail.UniqueIndex), 0)
			require.Equal(t, detail.NormalIndex["AccountChannel"], uint64(0))
			require.Equal(t, detail.NormalIndex["Aaid"], uint64(0))
		})
	})

	t.Run("Dump", func(t *testing.T) {
		runNewBorm(t, func(t *testing.T, db *BormDb) {
			err := db.CreateTable(&pb.AccountInfo{})
			require.NoError(t, err)

			detail, err := db.Snoop(&pb.AccountInfo{})
			require.NoError(t, err)
			require.Equal(t, detail.TotalCount, uint64(0))
			require.Equal(t, detail.TotalCount, uint64(0))
			require.Equal(t, len(detail.NormalIndex), 2)
			require.Equal(t, len(detail.UniqueIndex), 0)
			require.Equal(t, detail.NormalIndex["AccountChannel"], uint64(0))
			require.Equal(t, detail.NormalIndex["Aaid"], uint64(0))

			items := []IRow{}
			for i := 0; i < 1000; i++ {
				accountInfo := &pb.AccountInfo{
					Aaid:           uint64(10000 + i),
					AccountChannel: "lb",
					CashBooks:      make(map[string]*pb.Detail),
					StockBooks:     make(map[string]*pb.Detail),
					AccountProperties: &pb.AccountProperties{
						MainCurrency: "HKD",
						MaxFinance:   "10000",
					},
				}
				items = append(items, accountInfo)
			}
			err = db.BatchInsert(items)
			require.NoError(t, err)

			results, err := db.Dump(&pb.AccountInfo{})
			require.NoError(t, err)
			require.Equal(t, len(results), 1000)
		})
	})
}

func TestUnionIndex(t *testing.T) {
	t.Run("union index check", func(t *testing.T) {
		runNewBorm(t, func(t *testing.T, db *BormDb) {
			err := db.CreateTable(&pb.AccountInfo{})
			require.NoError(t, err)
			txn := db.Begin(true)
			for i := 0; i < 10; i++ {
				accountInfo := &pb.AccountInfo{
					Aaid:           uint64(10000 + i),
					AccountChannel: "lb",
					CashBooks:      make(map[string]*pb.Detail),
					StockBooks:     make(map[string]*pb.Detail),
					AccountProperties: &pb.AccountProperties{
						MainCurrency: "HKD",
						MaxFinance:   "10000",
					},
				}
				err = db.TxInsert(txn, accountInfo)
				require.NoError(t, err)
			}

			for i := 0; i < 10; i++ {
				accountInfo := &pb.AccountInfo{
					Aaid:           uint64(10000 + i),
					AccountChannel: "lb",
					CashBooks:      make(map[string]*pb.Detail),
					StockBooks:     make(map[string]*pb.Detail),
					AccountProperties: &pb.AccountProperties{
						MainCurrency: "HKD",
						MaxFinance:   "10000",
					},
				}
				err = db.TxInsert(txn, accountInfo)
				require.ErrorIs(t, err, ErrIdxUniqueConflict)
			}
			for i := 0; i < 10; i++ {
				id, err := db.TxQueryWithUnionIndexWithFieldMap(txn, &pb.AccountInfo{}, map[string]any{"Aaid": uint64(10000 + i), "AccountChannel": "lb"})
				require.NoError(t, err)
				require.Equal(t, id, uint64(i+1))

				err = db.TxDelete(txn, id, &pb.AccountInfo{})
				require.NoError(t, err)
			}

			for i := 0; i < 10; i++ {
				_, err := db.TxQueryWithUnionIndexWithFieldMap(txn, &pb.AccountInfo{}, map[string]any{"Aaid": uint64(10000 + i), "AccountChannel": "lb"})
				require.ErrorIs(t, err, badger.ErrKeyNotFound)
			}

			defer txn.Commit()
		})
	})
}

func TestNormalIndex(t *testing.T) {
	t.Run("normal index check", func(t *testing.T) {
		runNewBorm(t, func(t *testing.T, db *BormDb) {
			err := db.CreateTable(&pb.Person{})
			require.NoError(t, err)
			txn := db.Begin(true)
			defer db.Discard(txn)

			for i := 0; i < 10; i++ {
				err := db.TxInsert(txn, &pb.Person{
					Name:     "jacky",
					Phone:    fmt.Sprintf("+86%d", i),
					Age:      10 + uint32(i),
					BirthDay: 19901111,
					Gender:   pb.Gender_men,
				})
				require.NoError(t, err)
			}

			for i := 0; i < 10; i++ {
				err := db.TxInsert(txn, &pb.Person{
					Name:     "frank",
					Phone:    fmt.Sprintf("+85%d", i),
					Age:      10 + uint32(i),
					BirthDay: 19901111,
					Gender:   pb.Gender_men,
				})
				require.NoError(t, err)
			}

			results, err := db.TxQueryWithNormalIndex(txn, &pb.Person{}, 3, 15)
			require.NoError(t, err)
			require.Equal(t, len(results), 2)

			results, err = db.TxQueryWithNormalIndex(txn, &pb.Person{}, 1, "jacky")
			require.NoError(t, err)
			require.Equal(t, len(results), 10)

			results, err = db.TxQueryWithNormalIndex(txn, &pb.Person{}, 1, "frank")
			require.NoError(t, err)
			require.Equal(t, len(results), 10)

			results, err = db.TxQueryWithNormalIndex(txn, &pb.Person{}, 7, "+865")
			require.NoError(t, err)
			require.Equal(t, len(results), 0)
		})
	})
	t.Run("max size query", func(t *testing.T) {
		runNewBorm(t, func(t *testing.T, db *BormDb) {
			err := db.CreateTable(&pb.Person{})
			require.NoError(t, err)
			txn := db.Begin(true)
			defer db.Discard(txn)

			for i := 0; i < 10000; i++ {
				err := db.TxInsert(txn, &pb.Person{
					Name:     "jacky",
					Phone:    fmt.Sprintf("+86%d", i),
					Age:      10 + uint32(i),
					BirthDay: 19901111,
					Gender:   pb.Gender_men,
				})
				require.NoError(t, err)
			}
			results, err := db.TxQueryWithNormalIndex(txn, &pb.Person{}, 1, "jacky")
			require.NoError(t, err)
			require.Equal(t, len(results), 10000)
		})
	})
}

func TestUniqueIndex(t *testing.T) {
	t.Run("unique index check", func(t *testing.T) {
		runNewBorm(t, func(t *testing.T, db *BormDb) {
			err := db.CreateTable(&pb.Person{})
			require.NoError(t, err)
			txn := db.Begin(true)
			defer db.Discard(txn)

			for i := 0; i < 10; i++ {
				err := db.TxInsert(txn, &pb.Person{
					Name:     "jacky",
					Phone:    fmt.Sprintf("+86%d", i),
					Age:      10 + uint32(i),
					BirthDay: 19901111,
					Gender:   pb.Gender_men,
				})
				require.NoError(t, err)
			}

			for i := 0; i < 10; i++ {
				err := db.TxInsert(txn, &pb.Person{
					Name:     "frank",
					Phone:    fmt.Sprintf("+85%d", i),
					Age:      10 + uint32(i),
					BirthDay: 19901111,
					Gender:   pb.Gender_men,
				})
				require.NoError(t, err)
			}

			id, err := db.TxQueryWithUniqueIndex(txn, &pb.Person{}, 3, 15)
			require.ErrorIs(t, err, ErrKeyNotFound)
			require.Equal(t, id, uint64(0))

			id, err = db.TxQueryWithUniqueIndex(txn, &pb.Person{}, 2, "+865")
			require.NoError(t, err)
			require.Equal(t, id, uint64(6))

			id, err = db.TxQueryWithUniqueIndex(txn, &pb.Person{}, 2, "+855")
			require.NoError(t, err)
			require.Equal(t, id, uint64(16))

			id, err = db.TxQueryWithUniqueIndex(txn, &pb.Person{}, 2, "+875")
			require.ErrorIs(t, err, ErrKeyNotFound)

			results, err := db.TxQueryWithNormalIndex(txn, &pb.Person{}, 3, 15)
			require.NoError(t, err)
			require.Equal(t, len(results), 2)

			results, err = db.TxQueryWithNormalIndex(txn, &pb.Person{}, 1, "frank")
			require.NoError(t, err)
			require.Equal(t, len(results), 10)

		})
	})
}

func TestConcurrent(t *testing.T) {
	t.Run("insert no conflict", func(t *testing.T) {
		runNewBorm(t, func(t *testing.T, db *BormDb) {
			err := db.CreateTable(&pb.Person{})
			require.NoError(t, err)
			wait := sync.WaitGroup{}
			wait.Add(2)
			go func() {
				for i := 0; i < 10; i++ {
					err := db.Insert(&pb.Person{
						Name:     "jacky",
						Phone:    fmt.Sprintf("+%d", i),
						Age:      uint32(i),
						BirthDay: 19901111,
						Gender:   pb.Gender_men,
					})
					require.NoError(t, err)
				}
				defer wait.Done()
			}()
			go func() {
				for i := 10; i < 20; i++ {
					err := db.Insert(&pb.Person{
						Name:     "jacky",
						Phone:    fmt.Sprintf("+%d", i),
						Age:      uint32(i),
						BirthDay: 19901111,
						Gender:   pb.Gender_men,
					})
					require.NoError(t, err)
				}
				defer wait.Done()
			}()
			wait.Wait()
			i := 0
			err = db.Foreach(&pb.Person{}, func(item IRow) error {
				i++
				return nil
			})
			require.Equal(t, i, 20)
		})
	})
	t.Run("insert uq conflict", func(t *testing.T) {
		runNewBorm(t, func(t *testing.T, db *BormDb) {
			err := db.CreateTable(&pb.Person{})
			require.NoError(t, err)
			wait := sync.WaitGroup{}
			wait.Add(3)
			go func() {
				defer wait.Done()
				for i := 0; i < 10; i++ {
					err := db.Insert(&pb.Person{
						Name:     "a_jacky",
						Phone:    fmt.Sprintf("+%d", i),
						Age:      uint32(i),
						BirthDay: 19901111,
						Gender:   pb.Gender_men,
					})
					fmt.Println(err)
				}
			}()
			go func() {
				defer wait.Done()
				for i := 0; i < 10; i++ {
					err := db.Insert(&pb.Person{
						Name:     "b_jacky",
						Phone:    fmt.Sprintf("+%d", i),
						Age:      uint32(i),
						BirthDay: 19921111,
						Gender:   pb.Gender_men,
					})
					fmt.Println(err)

				}

			}()

			go func() {
				defer wait.Done()
				for i := 0; i < 10; i++ {
					err := db.Insert(&pb.Person{
						Name:     "c_jacky",
						Phone:    fmt.Sprintf("+%d", i),
						Age:      uint32(i),
						BirthDay: 19921111,
						Gender:   pb.Gender_men,
					})
					fmt.Println(err)

				}
			}()
			wait.Wait()
			detail, err := db.Snoop(&pb.Person{})
			require.NoError(t, err)
			require.Equal(t, detail.TotalCount, uint64(10))

			require.Equal(t, len(detail.NormalIndex), 2)
			require.Equal(t, len(detail.UniqueIndex), 1)
			require.Equal(t, detail.NormalIndex["Name"], uint64(10))
			require.Equal(t, detail.NormalIndex["Age"], uint64(10))
			require.Equal(t, detail.UniqueIndex["Phone"], uint64(10))

			rows, err := db.Dump(&pb.Person{})
			require.NoError(t, err)
			require.Equal(t, len(rows), 10)
		})
	})

	t.Run("update uq conflict", func(t *testing.T) {
		runNewBorm(t, func(t *testing.T, db *BormDb) {
			err := db.CreateTable(&pb.Person{})
			require.NoError(t, err)
			for i := 0; i < 10; i++ {
				db.Insert(&pb.Person{
					Name:     "jacky",
					Phone:    fmt.Sprintf("+%d", i),
					Age:      uint32(i),
					BirthDay: 19901111,
					Gender:   pb.Gender_men,
				})
			}
			//fmt.Println(db.Dump(&pb.Person{}))
			wait := sync.WaitGroup{}
			wait.Add(2)
			go func() {
				for i := 0; i < 10; i++ {
					err = db.Update(uint64(i+1), &pb.Person{
						Name:     "a_jacky",
						Phone:    fmt.Sprintf("+%d", i),
						Age:      uint32(i),
						BirthDay: 19901111,
						Gender:   pb.Gender_men,
					})
					require.NoError(t, err)
				}
				defer wait.Done()
			}()
			go func() {
				for i := 0; i < 10; i++ {
					err = db.Update(uint64(i+1), &pb.Person{
						Name:     "b_jacky",
						Phone:    fmt.Sprintf("+%d", i),
						Age:      uint32(i),
						BirthDay: 19901111,
						Gender:   pb.Gender_men,
					})
					require.NoError(t, err)
				}
				defer wait.Done()
			}()

			wait.Wait()
			//fmt.Println(db.Dump(&pb.Person{}))
			detail, err := db.Snoop(&pb.Person{})
			require.NoError(t, err)
			require.Equal(t, detail.TotalCount, uint64(10))

			require.Equal(t, len(detail.NormalIndex), 2)
			require.Equal(t, len(detail.UniqueIndex), 1)
			require.Equal(t, detail.NormalIndex["Name"], uint64(10))
			require.Equal(t, detail.NormalIndex["Age"], uint64(10))
			require.Equal(t, detail.UniqueIndex["Phone"], uint64(10))

			rows, err := db.Dump(&pb.Person{})
			require.NoError(t, err)
			require.Equal(t, len(rows), 10)
		})
	})
}

func TestConcurrentQuery(t *testing.T) {
	t.Run("conflict", func(t *testing.T) {
		runNewBorm(t, func(t *testing.T, db *BormDb) {
			err := db.CreateTable(&pb.Person{})
			require.NoError(t, err)
			wait := sync.WaitGroup{}
			wait.Add(2)
			go func() {
				for i := 0; i < 100; i++ {
					db.Insert(&pb.Person{
						Name:     "jacky",
						Phone:    fmt.Sprintf("+%d", i),
						Age:      uint32(i),
						BirthDay: 19901111,
						Gender:   pb.Gender_men,
					})
				}
				defer wait.Done()
			}()

			go func() {
				for {
					results, err := db.Dump(&pb.Person{})
					require.NoError(t, err)
					if len(results) == 100 {
						result, err := Last(db, WithAnd(&pb.Person{}).Eq("Name", "jacky"))
						require.NoError(t, err)
						require.Equal(t, result.Age, uint32(99))
						break
					}
				}
				defer wait.Done()
			}()
			wait.Wait()
		})
	})
}

func TestConcurrentUpdate(t *testing.T) {
	t.Run("ConcurrentUpdate", func(t *testing.T) {
		runNewBorm(t, func(t *testing.T, db *BormDb) {
			err := db.CreateTable(&pb.Person{})
			require.NoError(t, err)

			err = db.Insert(&pb.Person{
				Name:     "jacky",
				Phone:    "13575468007",
				Age:      20,
				BirthDay: 19901111,
				Gender:   pb.Gender_men,
			})
			require.NoError(t, err)

			wait := sync.WaitGroup{}
			wait.Add(2)

			go func() {
				defer wait.Done()
				for i := 0; i < 10; i++ {
					tx := db.Begin(true)
					results := []*pb.Person{}
					err := db.TxForeach(tx, &pb.Person{}, func(item IRow) error {
						results = append(results, item.(*pb.Person))
						return nil
					})
					require.NoError(t, err)

					err = db.TxUpdate(tx, results[0].Id, &pb.Person{
						Name:     "jacky",
						Phone:    "13575468007",
						Age:      results[0].Age + 1,
						BirthDay: 19901111,
						Gender:   pb.Gender_men,
					})
					tx.Commit()
					require.NoError(t, err)
					if err != nil {
						fmt.Println(err)
					}
				}
			}()

			go func() {
				defer wait.Done()
				for i := 0; i < 10; i++ {
					tx := db.Begin(true)
					results := []*pb.Person{}
					err := db.TxForeach(tx, &pb.Person{}, func(item IRow) error {
						results = append(results, item.(*pb.Person))
						return nil
					})
					require.NoError(t, err)

					err = db.TxUpdate(tx, results[0].Id, &pb.Person{
						Name:     "jacky",
						Phone:    "13575468007",
						Age:      results[0].Age + 1,
						BirthDay: 19901111,
						Gender:   pb.Gender_men,
					})
					tx.Commit()
					require.NoError(t, err)
					if err != nil {
						fmt.Println(err)
					}

				}
			}()
			wait.Wait()

			results := []*pb.Person{}
			err = db.Foreach(&pb.Person{}, func(item IRow) error {
				results = append(results, item.(*pb.Person))
				return nil
			})
			require.LessOrEqual(t, results[0].Age, uint32(40))
		})
	})
}



func TestBatchInsert(t *testing.T) {
	t.Run("BatchInsert", func(t *testing.T) {
		runNewBorm(t, func(t *testing.T, db *BormDb) {
			err := db.CreateTable(&pb.Person{})
			require.NoError(t, err)
			items := []IRow{}
			items = append(items, &pb.Person{
				Name:  "jacky",
				Phone: "13575468007",
				Age:   30,
			}, &pb.Person{
				Name:  "jim",
				Phone: "15088434234",
				Age:   29,
			})
			err = db.BatchInsert(items)
			require.NoError(t, err)

			items[0] = &pb.Person{
				Name:  "frank",
				Phone: "15088542234",
				Age:   30,
			}
			err = db.BatchInsert(items)
			require.Equal(t, err, ErrIdxUniqueConflict)
			err = db.BatchInsert(items[:1])
			require.NoError(t, err)
			results := []string{}
			err = db.Foreach(&pb.Person{}, func(item IRow) error {
				results = append(results, item.(*pb.Person).Name)
				return nil
			})
			require.NoError(t, err)
			require.EqualValues(t, results, []string{"jacky", "jim", "frank"})
		})
	})
}

func TestTruncate(t *testing.T) {
	runNewBorm(t, func(t *testing.T, db *BormDb) {
		err := db.CreateTable(&pb.Person{})
		require.NoError(t, err)
		items := []IRow{}
		items = append(items, &pb.Person{
			Name:  "jacky",
			Phone: "13575468007",
			Age:   30,
		}, &pb.Person{
			Name:  "jim",
			Phone: "15088434234",
			Age:   29,
		})
		err = db.BatchInsert(items)
		require.NoError(t, err)

		items[0] = &pb.Person{
			Name:  "frank",
			Phone: "15088542234",
			Age:   30,
		}

		err = db.BatchInsert(items)
		require.Equal(t, err, ErrIdxUniqueConflict)

		err = db.BatchInsert(items[:1])
		require.NoError(t, err)

		tableInfo, err := db.Snoop(&pb.Person{})
		require.NoError(t, err)
		require.Equal(t, tableInfo.TotalCount, uint64(3))
		require.Equal(t, tableInfo.UnionIndexCount, uint64(0))
		require.Equal(t, tableInfo.NormalIndex["Name"], uint64(3))
		require.Equal(t, tableInfo.NormalIndex["Age"], uint64(3))
		require.Equal(t, tableInfo.UniqueIndex["Phone"], uint64(3))

		results := []string{}
		err = db.Foreach(&pb.Person{}, func(item IRow) error {
			results = append(results, item.(*pb.Person).Name)
			return nil
		})
		require.NoError(t, err)
		require.EqualValues(t, results, []string{"jacky", "jim", "frank"})

		err = db.Truncate(&pb.Person{})
		require.NoError(t, err)

		tableInfo, err = db.Snoop(&pb.Person{})
		require.NoError(t, err)
		require.Equal(t, tableInfo.TotalCount, uint64(0))
		require.Equal(t, tableInfo.UnionIndexCount, uint64(0))
		require.Equal(t, tableInfo.NormalIndex["Name"], uint64(0))
		require.Equal(t, tableInfo.NormalIndex["Age"], uint64(0))
		require.Equal(t, tableInfo.UniqueIndex["Phone"], uint64(0))
	})
}

func TestDelete(t *testing.T) {
	runNewBorm(t, func(t *testing.T, db *BormDb) {
		err := db.CreateTable(&pb.Person{})
		require.NoError(t, err)
		items := []IRow{}
		items = append(items, &pb.Person{
			Name:  "jacky",
			Phone: "13575468007",
			Age:   30,
		}, &pb.Person{
			Name:  "jim",
			Phone: "15088434234",
			Age:   29,
		})
		err = db.BatchInsert(items)
		require.NoError(t, err)

		items[0] = &pb.Person{
			Name:  "frank",
			Phone: "15088542234",
			Age:   30,
		}

		err = db.BatchInsert(items)
		require.Equal(t, err, ErrIdxUniqueConflict)

		err = db.BatchInsert(items[:1])
		require.NoError(t, err)

		results := []*pb.Person{}
		err = db.Foreach(&pb.Person{}, func(item IRow) error {
			results = append(results, item.(*pb.Person))
			return nil
		})
		require.NoError(t, err)
		require.EqualValues(t, len(results), 3)

		for i := 0; i < len(results); i++ {
			err = db.Delete(results[i].Id, &pb.Person{})
			require.NoError(t, err)
		}

		err = db.Foreach(&pb.Person{}, func(item IRow) error {
			require.Fail(t, "must not found data")
			return nil
		})
		require.NoError(t, err)
	})
}
func TestUpdate(t *testing.T) {
	runNewBorm(t, func(t *testing.T, db *BormDb) {
		err := db.CreateTable(&pb.Person{})
		require.NoError(t, err)

		items := []IRow{}
		items = append(items, &pb.Person{
			Name:  "jacky",
			Phone: "13575468007",
			Age:   30,
		}, &pb.Person{
			Name:  "jim",
			Phone: "15088434234",
			Age:   29,
		})
		err = db.BatchInsert(items)
		require.NoError(t, err)

		results := []*pb.Person{}
		err = db.Foreach(&pb.Person{}, func(item IRow) error {
			results = append(results, item.(*pb.Person))
			return nil
		})
		require.NoError(t, err)
		require.EqualValues(t, len(results), 2)

		require.EqualValues(t, &pb.Person{
			Id:    results[0].Id,
			Name:  "jacky",
			Phone: "13575468007",
			Age:   30,
		}, results[0])

		require.EqualValues(t, &pb.Person{
			Id:    results[1].Id,
			Name:  "jim",
			Phone: "15088434234",
			Age:   29,
		}, results[1])

		err = db.Update(results[0].Id, &pb.Person{
			Name:  "jacky",
			Phone: "13575468007",
			Age:   35,
		})
		require.NoError(t, err)
		err = db.Update(results[1].Id, &pb.Person{
			Name:  "jim",
			Phone: "15088434234",
			Age:   34,
		})
		require.NoError(t, err)

		results = []*pb.Person{}
		err = db.Foreach(&pb.Person{}, func(item IRow) error {
			results = append(results, item.(*pb.Person))
			return nil
		})
		require.NoError(t, err)
		require.EqualValues(t, len(results), 2)

		require.EqualValues(t, &pb.Person{
			Id:    results[0].Id,
			Name:  "jacky",
			Phone: "13575468007",
			Age:   35,
		}, results[0])

		require.EqualValues(t, &pb.Person{
			Id:    results[1].Id,
			Name:  "jim",
			Phone: "15088434234",
			Age:   34,
		}, results[1])

		err = db.Update(results[0].Id, &pb.Person{
			Name:  "jacky",
			Phone: "15088434234",
			Age:   35,
		})
		require.ErrorIs(t, err, ErrIdxUniqueConflict)

		err = db.Update(results[1].Id, &pb.Person{
			Name:  "jim",
			Phone: "15088434235",
			Age:   34,
		})
		require.NoError(t, err)

		err = db.Update(results[0].Id, &pb.Person{
			Name:  "jacky",
			Phone: "15088434234",
			Age:   35,
		})
		require.NoError(t, err)

		results = []*pb.Person{}
		err = db.Foreach(&pb.Person{}, func(item IRow) error {
			results = append(results, item.(*pb.Person))
			return nil
		})
		require.NoError(t, err)
		require.EqualValues(t, len(results), 2)

		require.EqualValues(t, &pb.Person{
			Id:    results[0].Id,
			Name:  "jacky",
			Phone: "15088434234",
			Age:   35,
		}, results[0])

		require.EqualValues(t, &pb.Person{
			Id:    results[1].Id,
			Name:  "jim",
			Phone: "15088434235",
			Age:   34,
		}, results[1])
	})
	runNewBorm(t, func(t *testing.T, db *BormDb) {
		err := db.CreateTable(&pb.Person{})
		require.NoError(t, err)

		for i := 0; i < 10; i++ {
			db.Insert(&pb.Person{
				Name:     "jacky",
				Phone:    fmt.Sprintf("+%d", i),
				Age:      uint32(i),
				BirthDay: 19901111,
				Gender:   pb.Gender_men,
			})
		}

		results, err := Find(db, WithAnd(&pb.Person{}).Eq("Name", "jacky"))
		require.NoError(t, err)

		for i := 0; i < len(results); i++ {
			results[i].BirthDay = 19921016
			err := db.Update(results[i].Id, results[i])
			require.NoError(t, err)
		}

		results, err = Find(db, WithAnd(&pb.Person{}).Eq("Name", "jacky"))
		require.NoError(t, err)
		for i := 0; i < len(results); i++ {
			require.Equal(t, results[i].BirthDay, uint32(19921016))
		}
	})

	runNewBorm(t, func(t *testing.T, db *BormDb) {
		err := db.CreateTable(&pb.Person{})
		require.NoError(t, err)

		for i := 0; i < 10; i++ {
			db.Insert(&pb.Person{
				Name:     "jacky",
				Phone:    fmt.Sprintf("+%d", i),
				Age:      uint32(i),
				BirthDay: 19901111,
				Gender:   pb.Gender_men,
			})
		}

		tx := db.Begin(true)

		err = db.TxUpdate(tx, 1, &pb.Person{
			Name:     "jacky",
			Phone:    "+0",
			Age:      31,
			BirthDay: 19901111,
			Gender:   pb.Gender_men,
		})
		require.NoError(t, err)
		err = db.TxUpdate(tx, 2, &pb.Person{
			Name:     "jacky",
			Phone:    "+1",
			Age:      32,
			BirthDay: 19901111,
			Gender:   pb.Gender_men,
		})
		require.NoError(t, err)
		err = db.TxUpdate(tx, 3, &pb.Person{
			Name:     "jacky",
			Phone:    "+0",
			Age:      33,
			BirthDay: 19901111,
			Gender:   pb.Gender_men,
		})
		require.ErrorIs(t, err, ErrIdxUniqueConflict)

		if err != nil {
			tx.Discard()
		}
		results, err := Find(db, WithAnd(&pb.Person{}).Eq("Name", "jacky"))
		require.NoError(t, err)
		for i := 0; i < 10; i++ {
			require.Equal(t, results[i].Age, uint32(i))

		}
	})
}

func TestDbCount(t *testing.T) {
	t.Run("normal index check", func(t *testing.T) {
		runNewBorm(t, func(t *testing.T, db *BormDb) {
			err := db.CreateTable(&pb.AccountInfo{})
			require.NoError(t, err)

			for i := 0; i < 10000; i++ {
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
			err = db.CreateTable(&pb.Person{})
			require.NoError(t, err)
			for i := 0; i < 5000; i++ {
				err := db.Insert(&pb.Person{
					Name:     "jacky",
					Phone:    fmt.Sprintf("+86%d", i),
					Age:      10 + uint32(i),
					BirthDay: 19901111,
					Gender:   pb.Gender_men,
				})
				require.NoError(t, err)
			}
			count, err := db.Count(&pb.AccountInfo{})
			require.NoError(t, err)
			require.Equal(t, uint64(10000), uint64(count))

			count, err = db.Count(&pb.Person{})
			require.NoError(t, err)
			require.Equal(t, uint64(5000), uint64(count))
		})
	})
}


// type T struct {
// 	a uint64
// 	b uint64
// }

// func TestUnsafe(t *testing.T) {
// 	t1 := &T{
// 		a: 1,
// 		b: 2,
// 	}
// 	value := reflect.ValueOf(t1)
// 	ptr0 := common.GetUnsafeInterfaceUintptr(t1)

// 	runtime.SetFinalizer(t1, func(obj any) {
// 		fmt.Println("finalizer T", obj)
// 	})
// 	offset_a := value.Elem().Type().Field(0).Offset
// 	offset_b := value.Elem().Type().Field(1).Offset

// 	val_a := *(*uint64)(unsafe.Pointer(uintptr(ptr0) + offset_a))
// 	val_b := *(*uint64)(unsafe.Pointer(uintptr(ptr0) + offset_b))
// 	fmt.Println("before", val_a, val_b)

// 	//doSomeAllocation()
// 	for i := 0; i < 10000; i++ {
// 		time.Sleep(time.Second)
// 		val_a := *(*uint64)(unsafe.Pointer(uintptr(ptr0) + offset_a))
// 		val_b := *(*uint64)(unsafe.Pointer(uintptr(ptr0) + offset_b))
// 		fmt.Println("after", val_a, val_b)
// 	}

// }

// func doSomeAllocation() {
// 	var a *int

// 	// memory increase to force the GC
// 	for i := 0; i < 10000000; i++ {
// 		i := 1
// 		a = &i
// 	}

// 	_ = a
// }
