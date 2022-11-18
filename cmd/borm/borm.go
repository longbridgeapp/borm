package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/longbridgeapp/borm"
	"github.com/longbridgeapp/borm/pb"
)

var (
	n int
)

func init() {
	flag.IntVar(&n, "n", 0, "n")
}

func main() {
	flag.Parse()
	db, err := borm.New()
	if err != nil {
		log.Fatal(err)
	}
	err = db.CreateTable(&pb.Order{})
	if err != nil {
		log.Fatal(err)
	}
	err = db.CreateTable(&pb.AccountInfo{})
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < n; i++ {
		tx := db.Begin(true)
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
		err = db.TxInsert(tx, accountInfo)
		if err != nil {
			log.Fatal(err)
		}
		order := &pb.Order{
			AccountChannel: accountInfo.AccountChannel,
			Aaid:           accountInfo.Aaid,
			OrderId:        fmt.Sprintf("id_%v", i),
			OrgId:          fmt.Sprintf("id_%v", i),
			CounterId:      "ST/HK/700",
			Currency:       "HKD",
			Market:         "HK",
			EntrustType:    0,
			EntrustStatus:  0,
			Side:           1,
			EntrustAmount:  "1000",
			EntrustQty:     "10",
		}
		err = db.TxInsert(tx, order)
		if err != nil {
			log.Fatal(err)
		}
		err = db.Commit(tx)
		if err != nil {
			log.Fatal(err)
		}
	}
	log.Println("data all ready")
	now := time.Now()
	for i := 0; i < 10; i++ {
		err = OrderFilled(db, uint64(10000+i), "lb", fmt.Sprintf("id_%v", i))
		if err != nil {
			log.Fatal(err)
		}
	}
	log.Println(time.Since(now))

	for {
		total, _ := db.Count(&pb.AccountInfo{})
		log.Println("total: ", total)
		time.Sleep(time.Second * 10)
	}
}

func OrderFilled(db *borm.BormDb, aaid uint64, accountChannel string, orderId string) error {
	tx := db.Begin(true)

	err := func() error {
		orders, err := borm.TxFind(tx, db, borm.WithAnd(&pb.Order{}).Eq("AccountChannel", accountChannel).Eq("Aaid", aaid).Eq("OrderId", orderId))
		if err != nil {
			return err
		}
		newOrder := &pb.Order{
			AccountChannel: orders[0].AccountChannel,
			Aaid:           orders[0].Aaid,
			OrderId:        orders[0].OrderId,
			OrgId:          orders[0].OrgId,
			CounterId:      orders[0].CounterId,
			Currency:       orders[0].Currency,
			Market:         orders[0].Market,
			EntrustType:    orders[0].EntrustType,
			EntrustStatus:  1,
			Side:           orders[0].Side,
			EntrustAmount:  "0",
			EntrustQty:     "0",
		}
		accountInfos, err := borm.TxFind(tx, db, borm.WithAnd(&pb.AccountInfo{}).Eq("AccountChannel", "lb").Eq("Aaid", aaid))
		if err != nil {
			return err
		}
		newAccount := &pb.AccountInfo{
			AccountChannel:    accountInfos[0].AccountChannel,
			Aaid:              accountInfos[0].Aaid,
			AccountProperties: accountInfos[0].AccountProperties,
		}
		newAccount.CashBooks = make(map[string]*pb.Detail)
		newAccount.CashBooks["HKD"] = &pb.Detail{}
		newAccount.CashBooks["HKD"].OutStanding = "-1000"
		newAccount.StockBooks = make(map[string]*pb.Detail)
		newAccount.StockBooks["ST/HK/700"] = &pb.Detail{}
		newAccount.StockBooks["ST/HK/700"].OutStanding = "10"

		err = db.TxUpdate(tx, orders[0], newOrder)
		if err != nil {
			return err
		}

		err = db.TxUpdate(tx, accountInfos[0], newAccount)
		if err != nil {
			return err
		}

		return nil
	}()
	if err != nil {
		db.Discard(tx)
	} else {
		return db.Commit(tx)
	}
	return nil
}
