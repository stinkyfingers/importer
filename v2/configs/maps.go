package configs

import (
	"github.com/curt-labs/polkImporter/helpers/database"
	_ "github.com/go-sql-driver/mysql"

	"database/sql"
	"strconv"
)

var (
	configAttributeTypeMapStmt = "select AcesTypeID, ID from ConfigAttributeType where AcesTypeID is not null and AcesTypeID > 0"
	configAttributeMapStmt     = "select ID, ConfigAttributeTypeID, vcdbID from ConfigAttribute where vcdbID > 0 and vcdbID is not null"
)

func getConfigAttributeMap() (map[string]int, error) {
	var err error
	aMap := make(map[string]int)
	db, err := sql.Open("mysql", database.ConnectionString())
	if err != nil {
		return aMap, err
	}
	defer db.Close()

	stmt, err := db.Prepare(configAttributeMapStmt)
	if err != nil {
		return aMap, err
	}
	defer stmt.Close()
	res, err := stmt.Query()
	var i, c, v int

	for res.Next() {
		err = res.Scan(&i, &c, &v)
		if err != nil {
			return aMap, err
		}
		aMap[strconv.Itoa(c)+":"+strconv.Itoa(v)] = i
	}
	return aMap, err
}

func getConfigAttriguteTypeMap() (map[int]int, error) {
	var err error
	aMap := make(map[int]int)
	db, err := sql.Open("mysql", database.ConnectionString())
	if err != nil {
		return aMap, err
	}
	defer db.Close()

	stmt, err := db.Prepare(configAttributeTypeMapStmt)
	if err != nil {
		return aMap, err
	}
	defer stmt.Close()
	res, err := stmt.Query()
	var a, i int

	for res.Next() {
		err = res.Scan(&a, &i)
		if err != nil {
			return aMap, err
		}
		aMap[a] = i
	}
	return aMap, err
}
