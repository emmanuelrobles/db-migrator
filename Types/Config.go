package config

type Configurations struct {
	FromDb       DBConfig
	ToDb     DBConnection
	PrimaryKey DBPrimaryKey
}

type DBConfig struct {
	Connection   DBConnection
	QueryOptions DBQueryOptions
}

type DBQueryOptions struct {
	Sort DBSortOptions
	Where DBWhereOptions
	Top uint16
}

type DBSortOptions struct {
	SortDir string
	SortBy string
}

type DBWhereOptions struct {
	Filter string
}

type DBPrimaryKey struct {
	Name string
	Processing PrimaryKeyProcessing
}

type DBConnection struct {
	Server string
	Port uint16
	User string
	Password string
	DataBase string
	Table string
}

type PrimaryKeyProcessing string

const (
	AUTOGEN = "Autogen"
	DELETE = "Delete"
)