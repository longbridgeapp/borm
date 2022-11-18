package main

import (
	"log"

	"github.com/longbridgeapp/borm"
	"github.com/longbridgeapp/borm/cmd/example/definition"
)

func main() {
	db, err := borm.New()
	if err != nil {
		log.Fatal(err)
	}
	err = db.CreateTable(&definition.Account{})
	if err != nil {
		log.Fatal(err)
	}
	info, _ := db.Snoop(&definition.Account{})
	log.Printf("table snoop:%+v", info)
}

func uqIndexQuery(db *borm.BormDb) {
	account, err := borm.First(db, borm.WithAnd(&definition.Account{}).Eq("IdentityId", "330683199212122018"))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("account info:%+v", account)
}

func normalIndexQuery(db *borm.BormDb) {
	accounts, err := borm.Find(db, borm.WithAnd(&definition.Account{}).Eq("Name", "jacky").Eq("Age", uint32(30)))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("accounts info:%+v", accounts)
}

func unionIndexQuery(db *borm.BormDb) {
	accounts, err := borm.Find(db, borm.WithAnd(&definition.Account{}).Eq("PhoneNumber", "+8613575468007").Eq("Country", "China"))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("accounts info:%+v", accounts)
}
