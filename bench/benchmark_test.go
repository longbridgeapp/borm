package bench

import (
	"fmt"
	"testing"

	"github.com/longbridgeapp/borm"
	"github.com/longbridgeapp/borm/common"
	"github.com/longbridgeapp/borm/pb"
)

func BenchmarkInsert(b *testing.B) {
	b.StopTimer()
	db, err := borm.New()
	if err != nil {
		b.Fatal(err)
	}
	err = db.CreateTable(&pb.Account{})
	if err != nil {
		b.Fatal(err)
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		err := db.Insert(&pb.Account{
			AccountNo:      fmt.Sprintf("20000%d", i),
			PhoneNumber:    fmt.Sprintf("+861357546%d", i),
			Identification: fmt.Sprintf("3306833242343%v", i),
			Gender:         0,
			Age:            22,
			Address:        "西湖区公园里 01 号",
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDelete(b *testing.B) {
	b.StopTimer()
	db, err := borm.New()
	if err != nil {
		b.Fatal(err)
	}
	err = db.CreateTable(&pb.Account{})
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 100000; i++ {
		err := db.Insert(&pb.Account{
			AccountNo:      fmt.Sprintf("20000%d", i),
			PhoneNumber:    fmt.Sprintf("+861357546%d", i),
			Identification: fmt.Sprintf("3306833242343%v", i),
			Gender:         0,
			Age:            22,
			Address:        "西湖区公园里 01 号",
		})
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		err := db.Delete(uint64(i)+1, &pb.Account{})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNormalQuery(b *testing.B) {
	b.StopTimer()
	db, err := borm.New()
	if err != nil {
		b.Fatal(err)
	}
	err = db.CreateTable(&pb.Account{})
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 100000; i++ {
		err := db.Insert(&pb.Account{
			AccountNo:      fmt.Sprintf("20000%d", i),
			PhoneNumber:    fmt.Sprintf("+861357546%d", i),
			Identification: fmt.Sprintf("3306833242343%v", i),
			Gender:         0,
			Age:            22,
			Address:        "西湖区公园里 01 号",
		})
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, err := borm.Find(db, borm.WithAnd(&pb.Account{}).Eq("AccountNo", fmt.Sprintf("20000%d", i)))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUniqueQuery(b *testing.B) {
	b.StopTimer()
	db, err := borm.New()
	if err != nil {
		b.Fatal(err)
	}
	err = db.CreateTable(&pb.Account{})
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 100000; i++ {
		err := db.Insert(&pb.Account{
			AccountNo:      fmt.Sprintf("20000%d", i),
			PhoneNumber:    fmt.Sprintf("+861357546%d", i),
			Identification: fmt.Sprintf("3306833242343%v", i),
			Gender:         0,
			Age:            22,
			Address:        "西湖区公园里 01 号",
		})
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, err := borm.Find(db, borm.WithAnd(&pb.Account{}).Eq("Identification", fmt.Sprintf("3306833242343%v", i)))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUnionQuery(b *testing.B) {
	b.StopTimer()
	db, err := borm.New()
	if err != nil {
		b.Fatal(err)
	}
	err = db.CreateTable(&pb.Account{})
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 100000; i++ {
		err := db.Insert(&pb.Account{
			AccountNo:      fmt.Sprintf("20000%d", i),
			PhoneNumber:    fmt.Sprintf("+861357546%d", i),
			Identification: fmt.Sprintf("3306833242343%v", i),
			Gender:         0,
			Age:            22,
			Address:        "西湖区公园里 01 号",
		})
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, err := borm.Find(db, borm.WithAnd(&pb.Account{}).Eq("AccountNo", fmt.Sprintf("20000%d", i)).Eq("PhoneNumber", fmt.Sprintf("+861357546%d", i)))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJointQuery(b *testing.B) {
	b.StopTimer()
	db, err := borm.New()
	if err != nil {
		b.Fatal(err)
	}
	err = db.CreateTable(&pb.Account{})
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 100000; i++ {
		err := db.Insert(&pb.Account{
			AccountNo:      fmt.Sprintf("20000%d", i),
			PhoneNumber:    fmt.Sprintf("+861357546%d", i),
			Identification: fmt.Sprintf("3306833242343%v", i),
			Gender:         0,
			Age:            22,
			Address:        "西湖区公园里 01 号",
		})
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, err := borm.Find(db, borm.WithAnd(&pb.Account{}).Eq("AccountNo", fmt.Sprintf("20000%d", i)).Eq("PhoneNumber", fmt.Sprintf("+861357546%d", i)).Eq("Identification", fmt.Sprintf("3306833242343%v", i)))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetSetRowID(b *testing.B) {
	b.StopTimer()
	person := &pb.Person{
		Name:     "jacky",
		Phone:    fmt.Sprintf("+861357546%d", 1),
		Age:      uint32(30),
		BirthDay: 19901111,
		Gender:   pb.Gender_men,
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		common.SetUint64(person, uint64(i))
		id := common.GetUint64(person)
		if id != uint64(i) {
			b.Fatal()
		}
	}
}

func prePrepare(b *testing.B, db *borm.BormDb) error {
	err := db.CreateTable(&pb.AccountInfo{})
	if err != nil {
		b.Fatal(err)
	}
	err = db.CreateTable(&pb.Order{})
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 100000; i++ {
		accountInfo := &pb.AccountInfo{
			AccountChannel: "longport_test",
			Aaid:           uint64(i),
			CashBooks:      make(map[string]*pb.Detail),
			StockBooks:     make(map[string]*pb.Detail),
			AccountProperties: &pb.AccountProperties{
				MaxFinance:     "100000",
				MainCurrency:   "HKD",
				MaxTradeCredit: "1000000",
			},
		}
		err = db.Insert(accountInfo)
		if err != nil {
			b.Fatal(err)
		}
	}
	return nil
}

func BenchmarkSimulatedCreateNewOrder(b *testing.B) {
	b.StopTimer()
	db, err := borm.New()
	if err != nil {
		b.Fatal(err)
	}
	err = prePrepare(b, db)
	if err != nil {
		b.Fatal(err)
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		txn := db.Begin(true)
		defer txn.Commit()
		order := &pb.Order{
			AccountChannel: "longport_test",
			Aaid:           uint64(i),
			OrderId:        fmt.Sprintln(i),
			OrgId:          fmt.Sprintln(i),
			CounterId:      "ST/HK/700",
			Currency:       "HKD",
			Market:         "HK",
			EntrustType:    0,
			EntrustStatus:  0,
			Side:           0,
			EntrustAmount:  "100000",
			EntrustQty:     "200",
		}
		err = db.TxInsert(txn, order)
		if err != nil {
			b.Fatal(err)
		}
	}
}

//BenchmarkQueryIn-12         1101           1117096 ns/op          389150 B/op       8881 allocs/op
//BenchmarkQueryIn-12         1240            976675 ns/op          360781 B/op       7804 allocs/op
//BenchmarkQueryIn-12         1250            904871 ns/op          288364 B/op       6104 allocs/op
func BenchmarkQueryIn(b *testing.B) {
	b.StopTimer()
	db, err := borm.New(borm.WithLoggingLevel(borm.WARNING))
	if err != nil {
		b.Fatal(err)
	}
	err = db.CreateTable(&pb.OrderPot{})
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 100000; i++ {
		liveness := 0
		aaid := 1111
		if int32(i%1000) == 0 {
			liveness = 1
			aaid = 2222
		}
		orderPot := &pb.OrderPot{
			AccountChannel: "lb",
			Aaid:           uint64(aaid),
			OrderId:        int64(i),
			OrgId:          int64(i),
			CounterId:      "700",
			Market:         "HK",
			EntrustType:    1,
			EntrustStatus:  0,
			EntrustAmount:  "1000",
			EntrustQty:     "100",
			Currency:       "HKD",
			Liveness:       int32(liveness),
			IsAttached:     0,
		}
		err = db.Insert(orderPot)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StartTimer()

	ss := [][]any{}
	ss = append(ss, []any{uint64(2222), "lb", int32(1), int32(0)}, []any{uint64(3333), "lb", int32(1), int32(0)})

	for i := 0; i < b.N; i++ {
		results, err := borm.Find(db, borm.WithAnd(&pb.OrderPot{}).In([]string{"Aaid", "AccountChannel", "Liveness", "IsAttached"}, ss).SortBy(true, "OrderId").Limit(0, 100))
		if err != nil {
			b.Fatal(err)
		}
		if len(results) != 100 {
			b.Fail()
		}
	}
}

func BenchmarkQueryEq(b *testing.B) {
	b.StopTimer()
	db, err := borm.New(borm.WithLoggingLevel(borm.WARNING))
	if err != nil {
		b.Fatal(err)
	}
	err = db.CreateTable(&pb.OrderPot{})
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 100000; i++ {
		liveness := 0
		aaid := 1111
		if int32(i%1000) == 0 {
			liveness = 1
			aaid = 2222
		}
		orderPot := &pb.OrderPot{
			AccountChannel: "lb",
			Aaid:           uint64(aaid),
			OrderId:        int64(i),
			OrgId:          int64(i),
			CounterId:      "700",
			Market:         "HK",
			EntrustType:    1,
			EntrustStatus:  0,
			EntrustAmount:  "1000",
			EntrustQty:     "100",
			Currency:       "HKD",
			Liveness:       int32(liveness),
			IsAttached:     0,
		}
		err = db.Insert(orderPot)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		results, err := borm.Find(db, borm.WithAnd(&pb.OrderPot{}).Eq("Aaid", uint64(2222)).Eq("AccountChannel", "lb").Eq("Liveness", int32(1)).Eq("IsAttached", int32(0)).SortBy(true, "OrderId").Limit(0, 100))
		if err != nil {
			b.Fatal(err)
		}
		if len(results) != 100 {
			b.Fatal(len(results))
		}
	}
}
