package importer

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestImporter(t *testing.T) {

	Convey("Test Polk Diff", t, func() {
		// file := "/Users/macuser/Desktop/Polk/AriesTestData.csv"
		// file := "/Users/macuser/Desktop/Polk/CurtTestData.csv"
		// file := "/Users/macuser/Desktop/Polk/AriesLongTestData.csv"
		// file := "/Users/macuser/Desktop/Polk/Aries_Offroad_Coverage_US_201410.csv"
		// file := "/Users/macuser/Desktop/Polk/Trailer_Hitches_Coverage_US_201410.csv"
		// err := Run(file, 1, true, false)
		// So(err, ShouldBeNil)

		// bvs, err := MongoToBase()
		// So(err, ShouldBeNil)
		// So(bvs, ShouldNotBeNil)

		// bases := BvgArray(bvs)
		// So(bases, ShouldNotBeNil)

		// AuditBaseVehicles(bases)

		RunAfterCsvMongoed()

	})

}
