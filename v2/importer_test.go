package v2

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

		// file := "/Users/macuser/Desktop/Polk/Trailer_Hitches_Coverage_US_201410.csv"
		dbCollection := "aries"
		submodelCollection := "ariesSubs"
		configCollection := "ariesConfigs"
		t.Log(dbCollection, submodelCollection, configCollection)

		// err = ImportCsv(file, 1, dbCollection)
		// So(err, ShouldBeNil)

		// err = CaptureCsv("exports/VehiclesToDiffBySubmodel.csv", 0, submodelCollection)
		// So(err, ShouldBeNil)
		// err = CaptureCsv("exports/VehiclesToDiffByConfig.csv", 0, configCollection)
		// So(err, ShouldBeNil)

		// err = CaptureCsv("exports/AriesConfigTest.csv", 1, "ariesConfigTest")
		// So(err, ShouldBeNil)

		// // err = setMaxConnections(1200)
		// So(err, ShouldBeNil)
		// err = DiffBaseVehicles(dbCollection) //false - no audit configs; no DB writes
		// So(err, ShouldBeNil)

		// err = DiffSubmodels(submodelCollection)
		// So(err, ShouldBeNil)

		// err = DiffConfigsRedux("ariesConfigs", 10000, 0) //usually ariesConfigs - limit,skip
		// So(err, ShouldBeNil)
		// time.Sleep(time.Second * 30)
		// t.Log(0)
		err = DiffConfigsRedux("ariesConfigs", 10000, 100000) //usually ariesConfigs - limit,skip
		So(err, ShouldBeNil)

		// err = setMaxConnections(151)
		// So(err, ShouldBeNil)

		// err = RemoveDuplicates("exports/VehiclePartJoins.txt")
		// So(err, ShouldBeNil)
		// err = RemoveDuplicates("exports/MissingPartNumbers_Base.csv")
		// So(err, ShouldBeNil)
		// err = RemoveDuplicates("exports/MissingPartNumbers_Submodel.csv")
		// So(err, ShouldBeNil)
		// err = RemoveDuplicates("exports/VehiclesToDiffBySubmodel.csv")
		// So(err, ShouldBeNil)
		// err = RemoveDuplicates("exports/VehiclesToDiffByConfig.csv")
		// So(err, ShouldBeNil)
		// m, _ := getSubmodelBaseToVehicleMap()
		// t.Log(m["1452:6641"])

		// err = CreateTestData()
		// So(err, ShouldBeNil)

	})

}
