package importer

import (
	"database/sql"
	"github.com/curt-labs/polkImporter/helpers/database"
	_ "github.com/go-sql-driver/mysql"
	// "log"
)

var (
	acesTypeToCurtType = `select ID, AcesTypeID from ConfigAttributeType where AcesTypeID > 0`
)

func AcesTypeToCurtType() (map[int]int, error) {
	var err error
	theMap := make(map[int]int)
	db, err := sql.Open("mysql", database.ConnectionString())
	if err != nil {
		return theMap, err
	}
	defer db.Close()

	stmt, err := db.Prepare(acesTypeToCurtType)
	if err != nil {
		return theMap, err
	}
	defer stmt.Close()
	res, err := stmt.Query()
	if err != nil {
		return theMap, err
	}
	var aaia, curt int
	for res.Next() {
		err = res.Scan(&aaia, &curt)
		if err != nil {
			return theMap, err
		}
		theMap[curt] = aaia
	}
	return theMap, err
}
