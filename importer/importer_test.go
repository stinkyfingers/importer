package importer

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestImporter(t *testing.T) {

	Convey("Test Polk Diff", t, func() {
		var err error
		// // Import and mongo-ize csv

		// file := "/Users/macuser/Desktop/Polk/AriesTestData.csv"

		// file := "/Users/macuser/Desktop/Polk/CurtTestData.csv"

		// file := "/Users/macuser/Desktop/Polk/AriesLongTestData.csv"

		// file := "/Users/macuser/Desktop/Polk/Aries_Offroad_Coverage_US_201410.csv"
		dbCollection := "aries"
		t.Log(dbCollection)

		// file := "/Users/macuser/Desktop/Polk/Trailer_Hitches_Coverage_US_201410.csv"

		// err = ImportCsv(file, 1, dbCollection)
		// So(err, ShouldBeNil)

		// // Process data from Mongo

		// err = setMaxConnections(800)
		// So(err, ShouldBeNil)
		// err = RunDiff(dbCollection, false) //false - no audit configs; no DB writes
		// So(err, ShouldBeNil)
		// err = setMaxConnections(151)
		// So(err, ShouldBeNil)

		// //make BaseVehicle tabele inserts from "baseVehiclessNeededInBaseVehiclesTable"
		// err := GetQueriesForNewBaseVehiclesAndSubmodels(dbCollection)
		// So(err, ShouldBeNil)

		// // make BaseVehicle tabele inserts from "baseVehiclessNeededInBaseVehiclesTable"
		// err = QueriesToInsertMissingConfigs(dbCollection)
		// So(err, ShouldBeNil)

		err = getVehiclesByBase(dbCollection, "exports/BaseVehiclesNeededInBaseVehicleTable.csv")
		So(err, ShouldBeNil)

		err = getVehiclesBySubmodel(dbCollection, "exports/SubmodelsNeededInSubmodelTable.txt")
		So(err, ShouldBeNil)

	})

}
