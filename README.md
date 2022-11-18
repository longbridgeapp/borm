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
//unique index query
//select * from account where IdentityId='330683199212122018' limit 1
func indexQuery(db *borm.BormDb) {
	account, err := borm.First(db, borm.WithAnd(&definition.Account{}).Eq("IdentityId", "330683199212122018"))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("account info:%+v", account)
}

//normal index query
//select * from account where Name='jacky' and Age=30
func normalIndexQuery(db *borm.BormDb) {
	accounts, err := borm.Find(db, borm.WithAnd(&definition.Account{}).Eq("Name", "jacky").Eq("Age", uint32(30)))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("accounts info:%+v", accounts)
}
//union index query
//select * from account where PhoneNumber='+8613575468007' and Country='China'
func unionIndexQuery(db *borm.BormDb) {
	accounts, err := borm.Find(db, borm.WithAnd(&definition.Account{}).Eq("PhoneNumber", "+8613575468007").Eq("Country", "China"))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("accounts info:%+v", accounts)
}

```