package importer

import (
	"github.com/curt-labs/polkImporter/helpers/database"
	_ "github.com/go-sql-driver/mysql"

	"database/sql"
	"log"
)

func Run(filename string, headerLines int, dbCollection string) error {
	log.Print("Running")
	var err error
	err = CaptureCsv(filename, headerLines, dbCollection)
	if err != nil {
		return err
	}
	return err
}

func RunAfterCsvMongoed(dbCollection string) error {
	var err error

	//will use a little sql
	err = increaseConnections(800)
	if err != nil {
		returnConnections()
		return err
	}

	bvs, err := MongoToBase(dbCollection)
	if err != nil {
		returnConnections()
		return err
	}
	log.Print("Total baseVehicles to check: ", len(bvs))

	bases := BvgArray(bvs)

	baseIds, err := AuditBaseVehicles(bases)
	log.Print("Number of groups of base models to pass into submodels: ", len(baseIds))

	sbs, err := MongoToSubmodel(baseIds, dbCollection)
	if err != nil {
		returnConnections()
		return err
	}
	log.Print("Total submodels to check: ", len(sbs))

	subs := SmgArray(sbs)
	subIds, err := AuditSubmodels(subs)
	if err != nil {
		returnConnections()
		return err
	}
	log.Print("Number of groups of submodels to pass into configurations: ", len(subIds))

	configVehicles, err := MongoToConfig(subIds, dbCollection)
	log.Print("Total vehicles to check: ", len(configVehicles))

	cons := CgArray(configVehicles)
	log.Print("Number of vehicles (grouped by VehicleID) to audit the configurations of: ", len(cons))

	err = AuditConfigs(cons)
	if err != nil {
		returnConnections()
		return err
	}
	err = returnConnections()
	return err
}

//alter num_connections
func increaseConnections(num int) error {
	var err error
	db, err := sql.Open("mysql", database.ConnectionString())
	defer db.Close()
	if err != nil {
		return err
	}

	stmt, err := db.Prepare("set global max_connections = 800")
	defer stmt.Close()
	if err != nil {
		return err
	}

	_, err = stmt.Exec()
	if err != nil {
		return err
	}
	return err
}

func returnConnections() error {
	var err error
	db, err := sql.Open("mysql", database.ConnectionString())
	defer db.Close()
	if err != nil {
		return err
	}

	stmt, err := db.Prepare("set global max_connections = 151")
	defer stmt.Close()
	if err != nil {
		return err
	}

	_, err = stmt.Exec()
	if err != nil {
		return err
	}
	return err
}
