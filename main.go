package main

import (
	"database/sql"
	c "dbMigrator/Types"
	"fmt"
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/spf13/viper"
	"log"
	"strconv"
	"strings"
)

func main() {
	log.Println("Starting to read config")

	// Loads the config
	getConfig := func(name string, extension string, path string) c.Configurations {
		viper.SetConfigName(name)
		viper.SetConfigType(extension)
		viper.AddConfigPath(path)

		var configuration c.Configurations

		if err := viper.ReadInConfig(); err != nil {
			log.Printf("Error reading config file, %s\n", err)
		}


		if err := viper.Unmarshal(&configuration) ;err != nil {
			log.Printf("Unable to decode into struct, %v\n", err)
		}
		return configuration
	}

	configuration := getConfig("config", "yaml", ".")

	log.Println("config was read")

	// Build the VS given a config
	buildCS := func (connection c.DBConnection) string{
		return fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s;",connection.Server,connection.User,connection.Password,connection.Port,connection.DataBase)
	}


	var fromDb *sqlx.DB
	fromDb, err := sqlx.Connect("sqlserver", buildCS(configuration.FromDb.Connection))
	if err != nil {
		panic(err.Error())
	}

	var toDb *sqlx.DB
	toDb, err = sqlx.Connect("sqlserver", buildCS(configuration.ToDb))
	if err != nil {
		panic(err.Error())
	}

	data := GetElements(configuration.FromDb, fromDb)

	s := GetLastElementID(toDb)(configuration.ToDb.Table)(configuration.PrimaryKey.Name)
	ProcessPrimaryKey(configuration.PrimaryKey,data,s)
	_, err = InertElements(data, toDb, configuration.ToDb.Table,configuration.PrimaryKey)
	if err != nil {
		panic(err)
	}

}

func GetElements(config c.DBConfig, db *sqlx.DB) []map[string] interface{}{

	query := fmt.Sprintf("SELECT TOP %v * FROM %s %s Order By %s %s",
		config.QueryOptions.Top,
		config.Connection.Table,
		config.QueryOptions.Where.Filter,
		config.QueryOptions.Sort.SortBy,
		config.QueryOptions.Sort.SortDir)

	scan := func (rows *sqlx.Rows) ([]map[string]interface{}, error) {
		defer rows.Close()

		sanitize := func(element interface{}) interface{} {
			switch element.(type) {
			case []byte:
				var err error
				element, err = strconv.ParseFloat(string(element.([]byte)), 64)
				if err != nil {
					panic(err.Error())
				}
			}
			return element
		}

		colTypes, err := rows.ColumnTypes()
		if err != nil {
			return nil, err
		}
		numColumns := len(colTypes)

		values := make([]interface{}, numColumns)
		for i := range values {
			values[i] = new(interface{})
		}

		var results []map[string]interface{}
		for rows.Next() {
			if err := rows.Scan(values...); err != nil {
				return nil, err
			}


			dest := make(map[string]interface{}, numColumns)
			for i, column := range colTypes {
				dest[column.Name()] = sanitize(*(values[i].(*interface{})))
			}
			results = append(results, dest)
		}

		if err := rows.Err(); err != nil {
			return nil, err
		}
		return results, nil
	}
	
	rows,err := db.Queryx(query)
	if err != nil {
		panic(err)
	}
	selectScan, err := scan(rows)
	if err != nil {
		panic(err)
	}
	return selectScan
}

func InertElements(elements []map[string]interface{}, db *sqlx.DB, tableName string, primaryKey c.DBPrimaryKey) (sql.Result, error) {
	getElementsMetadata := func (data map[string]interface{}) (string,string) {
		insert := make([]string, 0, len(data))
		insertVal := make([]string, 0, len(data))
		for k := range data {
			insert = append(insert, fmt.Sprintf("%s",k))
			insertVal = append(insertVal, fmt.Sprintf("%s%s",":",k))
		}
		return strings.Join(insert,","), strings.Join(insertVal,",")
	}

	insert,insertVal := getElementsMetadata(elements[0])
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",tableName,insert,insertVal)

	return db.NamedExec(query, elements)
}

func GetLastElementID (db *sqlx.DB) func(tableName string) func(primaryKey string) func() int {
	return func(tableName string) func(primaryKey string) func() int {
		return func(primaryKey string) func() int {
			query := fmt.Sprintf("SELECT TOP 1 %s FROM %s Order By %s DESC",
				primaryKey,
				tableName,
				primaryKey)

			var id int
			err := db.Get(&id, query)
			if err != nil {
				panic(err)
			}

			function := func(id int) func() int {
				return func() int {
					id = id+1
					return id
				}
			}
			return function(id)
		}
	}
}

func ProcessPrimaryKey(config c.DBPrimaryKey,element []map[string]interface{}, idGen func() int) {
	switch config.Processing {
	case c.AUTOGEN:
		for _, e := range element {
			e[config.Name] = idGen()
		}
	case c.DELETE:
		for _, e := range element {
			delete(e,config.Name)
		}
	}
}