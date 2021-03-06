package v2

import (
	"database/sql"
	"github.com/curt-labs/polkImporter/helpers/database"
	"github.com/curt-labs/polkImporter/v2/configs"
	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/mgo.v2"
	"log"
	"strconv"
)

func Run() error {
	var err error
	file := "/Users/macuser/Desktop/Polk/Aries_Offroad_Coverage_US_201410.csv"

	dbCollection := "aries"
	submodelCollection := "ariesSubs"
	configCollection := "ariesConfigs"
	err = CaptureCsv(file, 1, dbCollection)
	if err != nil {
		return err
	}
	err = DiffBaseVehicles(dbCollection)
	if err != nil {
		return err
	}
	err = CaptureCsv("exports/VehiclesToDiffBySubmodel.csv", 0, submodelCollection)
	if err != nil {
		return err
	}
	err = DiffSubmodels(submodelCollection)
	if err != nil {
		return err
	}
	err = CaptureCsv("exports/VehiclesToDiffByConfig.csv", 0, configCollection)
	if err != nil {
		return err
	}
	//break into 10,000 line segments while running config differentiator
	seg := 10000
	collectionLength, err := GetCollectionLength(dbCollection)
	if err != nil {
		return err
	}
	for y := 0; y <= collectionLength; y += seg {
		err = DiffConfigsRedux(configCollection, seg, y) //limit,skip -10000 limit seems to work well
		if err != nil {
			return err
		}
	}
	return err
}

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

func DiffConfigsRedux(dbCollection string, limit, skip int) error {
	craws, err := configs.MongoToConfigurationsBatch(dbCollection, limit, skip)
	if err != nil {
		return err
	}
	log.Print("Total individual records to check (using configDiff): ", len(craws))

	cons := configs.ConfigArray(craws)
	log.Print("Number of Vehicles' Configs to audit: ", len(cons))

	err = configs.ReduceConfigs(cons)
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

func GetCollectionLength(dbCollection string) (int, error) {
	var length int
	session, err := mgo.Dial(database.MongoConnectionString().Addrs[0])
	if err != nil {
		return length, err
	}
	defer session.Close()
	collection := session.DB("importer").C(dbCollection)
	length, err = collection.Count()
	return length, err
}
