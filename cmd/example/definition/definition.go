package definition

func (*Account) GetTableName() string {
	return "Account"
}

func (*Account) Clone() any {
	return &Account{}
}
