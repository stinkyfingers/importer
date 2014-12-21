package v2

import (
	"database/sql"
	"github.com/curt-labs/polkImporter/helpers/database"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"strconv"
)

func DiffBaseVehicles(dbCollection string) error {
	bvs, err := MongoToBase(dbCollection)
	if err != nil {
		return err
	}
	log.Print("Total baseVehicles to check: ", len(bvs))

	bases := BvgArray(bvs)

	_, err = AuditBaseVehicles(bases, dbCollection)
	return err
}

func DiffSubmodels(dbCollection string) error {
	sbs, err := MongoToSubmodel(dbCollection)
	if err != nil {
		return err
	}
	log.Print("Total submodels to check: ", len(sbs))

	subs := SmgArray(sbs)

	_, err = AuditSubmodels(subs, dbCollection)
	if err != nil {
		return err
	}
	return err
}

func DiffConfigs(dbCollection string) error {
	craws, err := MongoToConfig(dbCollection)
	if err != nil {
		return err
	}
	log.Print("Total config vehicles to check: ", len(craws))

	cons := CgArray(craws)

	err = AuditConfigs(cons)
	if err != nil {
		return err
	}
	return err
}

//run this before RunDiff() if you're having trouble with max_connections (may want to run after to reset max_connections too).
func setMaxConnections(num int) error {
	var err error
	db, err := sql.Open("mysql", database.ConnectionString())
	if err != nil {
		return err
	}
	defer db.Close()

	stmt, err := db.Prepare("set global max_connections = " + strconv.Itoa(num))
	if err != nil {
		return err
	}
	defer stmt.Close()
	_, err = stmt.Exec()
	if err != nil {
		return err
	}
	return err
}
