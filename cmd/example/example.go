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

func insert(db *borm.BormDb) {
	account := &definition.Account{
		Name:        "jacky",
		IdentityId:  "330683199212122018",
		PhoneNumber: "+8613575468007",
		Country:     "China",
		Age:         30,
		Gender:      definition.Gender_men,
	}
	db.Insert(account)
}

func delete(db *borm.BormDb) {
	db.Delete(1, &definition.Account{})
}

func update(db *borm.BormDb) {
	account := &definition.Account{
		Name:        "jacky",
		IdentityId:  "330683199212122018",
		PhoneNumber: "+8613575468007",
		Country:     "China",
		Age:         32,
		Gender:      definition.Gender_men,
	}

	db.Update(1, account)
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

func inQuery(db *borm.BormDb) {
	ss := [][]any{}
	ss = append(ss, []any{"jack"}, []any{"rose"})
	//select * from account where Name in('jack','rose')
	accounts, err := borm.Find(db, borm.WithAnd(&definition.Account{}).In([]string{"Name"}, ss))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("accounts info:%+v", accounts)

	ss = [][]any{}
	ss = append(ss, []any{"jack", "US"}, []any{"rose", "UK"})
	//select * from account where Name in(('jack','US'),('rose','UK'))
	accounts, err = borm.Find(db, borm.WithAnd(&definition.Account{}).In([]string{"Name", "Country"}, ss))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("accounts info:%+v", accounts)
}

func multipleConditionQuery(db *borm.BormDb) {
	ss := [][]any{}
	ss = append(ss, []any{30}, []any{31}, []any{32}, []any{33}, []any{34})
	//select * from account where Age in(30,31,32,33,34) and Country='China' order by Age limit 100
	accounts, err := borm.Find(db, borm.WithAnd(&definition.Account{}).In([]string{"Age"}, ss).Eq("Country", "China").SortBy(true, "Age").Limit(0, 100))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("accounts info:%+v", accounts)
}

func transaction(db *borm.BormDb) {
	//transaction begin
	tx := db.Begin(true)

	//query record
	account, err := borm.TxFirst(tx, db, borm.WithAnd(&definition.Account{}).Eq("IdentityId", "330683199212122018"))
	if err != nil {
		tx.Discard()
		return
	}
	account.Age++
	//update record
	err = db.TxUpdate(tx, account.Id, account)
	if err != nil {
		tx.Discard()
		return
	}
	//transaction commit
	tx.Commit()
}
