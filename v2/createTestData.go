package v2

import (
	"github.com/curt-labs/polkImporter/helpers/database"
	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"log"
)

func CreateTestData() error {
	var testData []CsvVehicle

	session, err := mgo.Dial(database.MongoConnectionString().Addrs[0])
	if err != nil {
		return err
	}
	defer session.Close()
	collection := session.DB("importer").C("aries")

	//write to csv raw vehicles
	err = collection.Find(bson.M{"baseVehicleId": "125149", "submodelId": "430"}).All(&testData)
	if err != nil {
		return err
	}
	log.Print(testData)
	//write back to mongo

	collection2 := session.DB("importer").C("testData")
	for _, row := range testData {
		err = collection2.Insert(&row)
		if err != nil {
			return err
		}

	}
	return err

}
