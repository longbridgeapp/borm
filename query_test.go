package borm

import (
	"fmt"
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
