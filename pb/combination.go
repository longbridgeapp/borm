package pb

func (*Person) GetTableName() string {
	return "Person"
}

func (person *Person) Clone() any {
	return &Person{}
}

func (person *Person) DeepCp() *Person {
	return &Person{}
}

func (*Order) GetTableName() string {
	return "Order"
}

func (*Order) Clone() any {
	return &Order{}
}

func (*IllegalPerson_1) GetTableName() string {
	return "IllegalPerson_1"
}

func (*IllegalPerson_1) Clone() any {
	return &IllegalPerson_1{}
}

func (*IllegalPerson_2) GetTableName() string {
	return "IllegalPerson_2"
}

func (*IllegalPerson_2) Clone() any {
	return &IllegalPerson_1{}
}

func (*AccountInfo) GetTableName() string {
	return "AccountInfo"
}

func (*AccountInfo) Clone() any {
	return &AccountInfo{}
}

func (*Account) GetTableName() string {
	return "Account"
}

func (*Account) Clone() any {
	return &Account{}
}

func (*OrderPot) GetTableName() string {
	return "OrderPot"
}

func (*OrderPot) Clone() any {
	return &OrderPot{}
}
