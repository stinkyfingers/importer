package importer

import (
	"encoding/csv"
	"github.com/curt-labs/polkImporter/helpers/database"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"log"
	"os"
	"strconv"
)

type SimpleVehicle struct {
	BaseID       int    `bson:"baseVehicleId,omitempty"`
	SubmodelID   int    `bson:"submodelId,omitempty"`
	AAIAMakeID   int    `bson:"makeId,omitempty"`
	AAIAModelID  int    `bson:"modelId,omitempty"`
	AAIAYearID   int    `bson:"yearId,omitempty"`
	SubmodelName string `bson:"submodel,omitempty"`
}

// insert into BaseVehicle (AAIABaseVehicleID, YearID, MakeID, ModelID) values(?,?,(select * from vcdb_Make where AAIAMakeID = ?),(select * from vcdb_Model where AAIAModelID = ?))

func getVehiclesByBase(dbCollection string, filename string) error {
	var sv SimpleVehicle
	bvbq, err := os.Create("exports/BaseVehicleInBaseVehicleTableQueries.txt")
	if err != nil {
		return err
	}
	h := []byte(" insert into BaseVehicle (AAIABaseVehicleID, YearID, MakeID, ModelID) values \n")
	off := int64(0)
	n, err := bvbq.WriteAt(h, off)
	if err != nil {
		return err
	}
	off += int64(n)

	session, err := mgo.Dial(database.MongoConnectionString().Addrs[0])
	if err != nil {
		return err
	}
	defer session.Close()
	collection := session.DB("importer").C(dbCollection)

	csvfile, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer csvfile.Close()
	reader := csv.NewReader(csvfile)
	reader.FieldsPerRecord = -1 //flexible number of fields

	lines, err := reader.ReadAll()
	if err != nil {
		return err
	}

	lines = lines[1:] //axe header
	var id int
	for _, line := range lines {
		id, err = strconv.Atoi(line[0])
		err = collection.Find(bson.M{"baseVehicleId": id}).One(&sv)
		if err != nil {
			return err
		}
		log.Print(sv)
		h = []byte("(" + strconv.Itoa(sv.BaseID) + "," + strconv.Itoa(sv.AAIAYearID) + ",(select ID from vcdb_Make where AAIAMakeID = " + strconv.Itoa(sv.AAIAMakeID) + "),(select ID from vcdb_Model where AAIAModelID = " + strconv.Itoa(sv.AAIAModelID) + ")),\n")
		n, err := bvbq.WriteAt(h, off)
		if err != nil {
			return err
		}
		off += int64(n)

	}

	return err
}

func getVehiclesBySubmodel(dbCollection string, filename string) error {
	var sv SimpleVehicle
	bvbq, err := os.Create("exports/SubmodelInSubmodelTableQueries.txt")
	if err != nil {
		return err
	}
	h := []byte(" insert into Submodel (AAIASubmodelID, Name) values \n")
	off := int64(0)
	n, err := bvbq.WriteAt(h, off)
	if err != nil {
		return err
	}
	off += int64(n)

	session, err := mgo.Dial(database.MongoConnectionString().Addrs[0])
	if err != nil {
		return err
	}
	defer session.Close()
	collection := session.DB("importer").C(dbCollection)

	csvfile, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer csvfile.Close()
	reader := csv.NewReader(csvfile)
	reader.FieldsPerRecord = -1 //flexible number of fields

	lines, err := reader.ReadAll()
	if err != nil {
		return err
	}

	lines = lines[1:] //axe header
	var id int
	for _, line := range lines {
		id, err = strconv.Atoi(line[0])
		err = collection.Find(bson.M{"submodelId": id}).One(&sv)
		if err != nil {
			return err
		}
		log.Print(sv)
		h = []byte("(" + strconv.Itoa(sv.SubmodelID) + "," + sv.SubmodelName + "),\n")
		n, err := bvbq.WriteAt(h, off)
		if err != nil {
			return err
		}
		off += int64(n)

	}

	return err
}
