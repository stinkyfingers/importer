package v2

import (
	"database/sql"
	"github.com/curt-labs/polkImporter/helpers/database"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"strconv"
)

//run once - captures csv, turns into a mongoDB collection
func ImportCsv(filename string, headerLines int, dbCollection string) error {
	log.Print("Running")
	var err error
	err = CaptureCsv(filename, headerLines, dbCollection)
	if err != nil {
		return err
	}
	return err
}

//run repeatedly - entering new vehicles and vehicleparts from the generated queries will reduce subsequent output from this function
func RunDiff(dbCollection string, auditConfigs bool) error {
	bvs, err := MongoToBase(dbCollection)
	if err != nil {
		return err
	}
	log.Print("Total baseVehicles to check: ", len(bvs))

	bases := BvgArray(bvs)

	_, err = AuditBaseVehicles(bases, dbCollection)
	// log.Print("Number of groups of base models to pass into submodels: ", len(baseIds))

	// sbs, err := MongoToSubmodel(baseIds, dbCollection)
	// if err != nil {
	// 	return err
	// }
	// log.Print("Total submodels to check: ", len(sbs))

	// subs := SmgArray(sbs)
	// subIds, err := AuditSubmodels(subs)
	// if err != nil {
	// 	return err
	// }
	// log.Print("Number of groups of submodels to pass into configurations: ", len(subIds))

	// configVehicles, err := MongoToConfig(subIds, dbCollection)
	// log.Print("Total vehicles to check: ", len(configVehicles))

	// //TODO - stop here and "processMissingConfigs"
	// cons := CgArray(configVehicles)
	// log.Print("Number of vehicles (grouped by VehicleID) to audit the configurations of: ", len(cons))
	// if auditConfigs == true {
	// 	err = AuditConfigs(cons)
	// 	if err != nil {
	// 		return err
	// 	}
	// }
	return err
}

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

// //you may run repeatedly, but will probably only need one pass. RunDiff() doesn't handle baseV's and subs missing from the Basevehicle and submodel tables well.
// //This function generates queries to insert them, or dump them in 'unknown' files.
// func GetQueriesForNewBaseVehiclesAndSubmodels(dbCollection string) error {
// 	err := QueriesToInsertBaseVehiclesInBaseVehicleTable(dbCollection)
// 	if err != nil {
// 		return err
// 	}

// 	err = QueriesToInsertSubmodelsInSubmodelTable(dbCollection)
// 	if err != nil {
// 		return err
// 	}
// 	return err
// }

// func GetQueriesToInsertConfigs(dbCollection string) error {
// 	err := QueriesToInsertMissingConfigs(dbCollection)
// 	if err != nil {
// 		return err
// 	}
// 	return err
// }

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

// func RunBaseVehiclesOnly(dbCollection string) ([]int, error) {
// 	var baseIds []int
// 	bvs, err := MongoToBase(dbCollection)
// 	if err != nil {
// 		return baseIds, err
// 	}
// 	log.Print("Total baseVehicles to check: ", len(bvs))

// 	bases := BvgArray(bvs)

// 	baseIds, err = AuditBaseVehicles(bases)
// 	log.Print("Number of groups of base models to pass into submodels: ", len(baseIds))

// 	err = QueriesToInsertBaseVehiclesInBaseVehicleTable(dbCollection)
// 	if err != nil {
// 		return baseIds, err
// 	}
// 	log.Print(baseIds)
// 	return baseIds, err
// }
