# Quickstart
borm is high performance nosql engine, You can use it to build high-performance trading systems.

Requires Go 1.18 or newer.

## Usage
### Simple initialization

```protobuf
syntax = "proto3";
package definition;
import "github.com/gogo/protobuf/gogoproto/gogo.proto";

option (gogoproto.unmarshaler_all) = true;
option (gogoproto.sizer_all) = true;
option (gogoproto.marshaler_all) = true;

enum Gender{
  men = 0;
  women = 1;
}
message Account{
  uint64 id=1;
  string name = 2[(gogoproto.moretags) = "idx:\"normal\""];
  string identity_id=3[(gogoproto.moretags) = "idx:\"unique\""];
  string phone_number=4[(gogoproto.moretags) = "idx:\"union\""];
  string country=5[(gogoproto.moretags) = "idx:\"union\""];
  uint32 age=6[(gogoproto.moretags) = "idx:\"normal\""];
  Gender gender=7;
}
```

```go
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
	log.Printf("table snoop: %+v", info)
    //table snoop: &{TotalCount:0 UnionIndexCount:0 NormalIndex:map[Age:0 Country:0 Name:0 PhoneNumber:0] UniqueIndex:map[IdentityId:0]}
}
```

### Custom initialization
#### Index Based Query
```go
//select * from account where IdentityId='330683199212122018' limit 1
borm.First(db, borm.WithAnd(&definition.Account{}).Eq("IdentityId", "330683199212122018"))
```
```go
 //select * from account where Name='jacky' and Age=30
 borm.Find(db, borm.WithAnd(&definition.Account{}).Eq("Name", "jacky").Eq("Age", uint32(30)))
```

```go
ss := [][]any{}
ss = append(ss, []any{"jack"}, []any{"rose"})
//select * from account where Name in('jack','rose')
accounts, err := borm.Find(db, borm.WithAnd(&definition.Account{}).In([string{"Name"}, ss))
```
```go
ss = [][]any{}
ss = append(ss, []any{"jack", "US"}, []any{"rose", "UK"})
//select * from account where (Name,Country) in(('jack','US'),('rose','UK'))
accounts, err = borm.Find(db, borm.WithAnd(&definition.Account{}).In([]string{"Name", "Country"}, ss))
```
```go
ss := [][]any{}
ss = append(ss, []any{30}, []any{31}, []any{32}, []any{33}, []any{34})
//select * from account where Age in(30,31,32,33,34) and Country='China' order by Age limit 100
accounts, err := borm.Find(db, borm.WithAnd(&definition.Account{}).In([]string{"Age"}, ss).Eq("Country","China").SortBy(true, "Age").Limit(0, 100))
```


#### Insert Record
```go
func insert(db *borm.BormDb) {
	account := &definition.Account{
		Name:        "jacky",
		IdentityId:  "330683199212122018",
		PhoneNumber: "+8613575468007",
		Country:     "China",
		Age:         30,
		Gender:      definition.Gender_men,
	}
	//insert into account values ('jacky','330683199212122018','+8613575468007','China',30,0)
	db.Insert(account)
}
```
#### Delete Record
```go
func delete(db *borm.BormDb) {
	//delete from account where id=1
	db.Delete(1, &definition.Account{})
}
```
#### Update Record
```go
func update(db *borm.BormDb) {
	account := &definition.Account{
		Name:        "jacky",
		IdentityId:  "330683199212122018",
		PhoneNumber: "+8613575468007",
		Country:     "China",
		Age:         32,
		Gender:      definition.Gender_men,
	}
	//update account set Name='jacky',IdentityId='330683199212122018',PhoneNumber='+8613575468007',Country='China',Age=32,Gender=0 where id=1
	db.Update(1, account)
}
```