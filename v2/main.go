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
	log.Print("Total individual records to check (using baseVehicleDiff): ", len(bvs))

	bases := BvgArray(bvs)
	log.Print("Number of Base Vehicles to audit: ", len(bases)) //2743  db.aries.distinct("baseVehicleId").length

	baseIds, doneIds, err := AuditBaseVehicles(bases, dbCollection)
	log.Print("Rows/vehicles to do: ", baseIds, "  rows/vehicles done: ", doneIds)
	if err != nil {
		return err
	}

	return err
}

func DiffSubmodels(dbCollection string) error {
	sbs, err := MongoToSubmodel(dbCollection)
	if err != nil {
		return err
	}
	log.Print("Total individual records to check (using submodelDiff): ", len(sbs))

	subs := SmgArray(sbs)
	log.Print("Number of Submodels to audit: ", len(subs))

	subIds, doneIds, err := AuditSubmodels(subs, dbCollection)
	log.Print("Rows/vehicles to do: ", subIds, "  rows/vehicles done: ", doneIds)
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
	log.Print("Total individual records to check (using configDiff): ", len(craws))

	cons := CgArray(craws)
	log.Print("Number of Vehicles' Configs to audit: ", len(cons))

	err = NewAuditConfigs(cons)
	if err != nil {
		return err
	}

	// err = AuditConfigs(cons)
	// if err != nil {
	// 	return err
	// }
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
