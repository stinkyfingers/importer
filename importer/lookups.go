package importer

import (
	"database/sql"
	"encoding/csv"
	"github.com/curt-labs/polkImporter/helpers/database"
	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"os"
	"strconv"
)

var (
	vehicleMakeMap  = `select ID, AAIAMakeID from vcdb_Make`
	vehicleModelMap = `select ID, AAIAModelID from vcdb_Model`
)

func QueriesToInsertSubmodelsInSubmodelTable(dbCollection string) error {
	//mongo
	session, err := mgo.Dial(database.MongoConnectionString().Addrs[0])
	if err != nil {
		return err
	}
	defer session.Close()
	collection := session.DB("importer").C(dbCollection)

	//file write setup
	subs, err := os.Create("exports/SubmodelInserts.txt")
	if err != nil {
		return err
	}
	off := int64(0)
	h := []byte("insert into Submodel (AAIASubmodelID, SubmodelName) values \n")
	n, err := subs.WriteAt(h, off)
	off += int64(n)

	//csv
	csvfile, err := os.Open("exports/SubmodelsNeededInSubmodelTable.csv")
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

	var c CsvVehicle

	for _, line := range lines {
		SubmodelID, err := strconv.Atoi(line[0])
		err = collection.Find(bson.M{"submodelID": SubmodelID}).One(&c)
		if err != nil {
			return err
		}
		b := []byte("(" + strconv.Itoa(SubmodelID) + "," + c.SubModel + "),\n")
		n, err := subs.WriteAt(b, off)
		if err != nil {
			return err
		}
		off += int64(n)
	}
	return err
}

func QueriesToInsertBaseVehiclesInBaseVehicleTable(dbCollection string) error {
	var err error
	//maps
	makeMap, err := GetVehicleMakeMap()
	if err != nil {
		return err
	}
	modelMap, err := GetVehicleModelMap()
	if err != nil {
		return err
	}

	//mongo
	session, err := mgo.Dial(database.MongoConnectionString().Addrs[0])
	if err != nil {
		return err
	}
	defer session.Close()
	collection := session.DB("importer").C(dbCollection)

	//file write setup
	bases, err := os.Create("exports/BaseVehicleInserts.txt")
	if err != nil {
		return err
	}
	off := int64(0)
	h := []byte("insert into BaseVehicle (AAIABaseVehicleID, YearID, MakeID, ModelID) values \n")
	n, err := bases.WriteAt(h, off)
	off += int64(n)

	unknown, err := os.Create("exports/UnknownBaseVehicles.txt")
	if err != nil {
		return err
	}
	unknownOff := int64(0)
	h = []byte("AAIABaseVehicleID,PartNumber \n")
	m, err := bases.WriteAt(h, unknownOff)
	unknownOff += int64(m)

	//csv
	csvfile, err := os.Open("exports/BaseVehiclesNeededInBaseVehicleTable.csv")
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
	var c CsvVehicle

	for _, line := range lines {

		BaseVehicleID, err := strconv.Atoi(line[0])

		err = collection.Find(bson.M{"baseVehicleId": BaseVehicleID}).One(&c)

		var curtMakeID, curtModelID int
		var ok bool
		if curtMakeID, ok = makeMap[c.MakeID]; !ok {
			b := []byte(strconv.Itoa(BaseVehicleID) + "," + c.PartNumber + "\n")
			n, err := unknown.WriteAt(b, unknownOff)
			if err != nil {
				return err
			}
			unknownOff += int64(n)
			continue
		}
		if curtModelID, ok = modelMap[c.ModelID]; !ok {
			b := []byte(strconv.Itoa(BaseVehicleID) + "," + c.PartNumber + "\n")
			n, err := unknown.WriteAt(b, unknownOff)
			if err != nil {
				return err
			}
			unknownOff += int64(n)
			continue
		}

		sql := []byte(" (" + strconv.Itoa(c.BaseVehicleID) + "," + strconv.Itoa(c.YearID) + "," + strconv.Itoa(curtMakeID) + "," + strconv.Itoa(curtModelID) + "),\n")
		n, err := bases.WriteAt(sql, off)
		if err != nil {
			return err
		}
		off += int64(n)
	}
	return err
}

func GetVehicleMakeMap() (map[int]int, error) {
	var err error
	makeMap := make(map[int]int)
	db, err := sql.Open("mysql", database.ConnectionString())
	if err != nil {
		return makeMap, err
	}
	defer db.Close()

	stmt, err := db.Prepare(vehicleMakeMap)
	if err != nil {
		return makeMap, err
	}
	defer stmt.Close()
	res, err := stmt.Query()
	if err != nil {
		return makeMap, err
	}
	var id, aaiaId int
	for res.Next() {
		err = res.Scan(&id, &aaiaId)
		if err != nil {
			return makeMap, err
		}
		makeMap[aaiaId] = id
	}
	return makeMap, err
}

func GetVehicleModelMap() (map[int]int, error) {
	var err error
	modelMap := make(map[int]int)
	db, err := sql.Open("mysql", database.ConnectionString())
	if err != nil {
		return modelMap, err
	}
	defer db.Close()

	stmt, err := db.Prepare(vehicleModelMap)
	if err != nil {
		return modelMap, err
	}
	defer stmt.Close()
	res, err := stmt.Query()
	if err != nil {
		return modelMap, err
	}
	var id, aaiaId int
	for res.Next() {
		err = res.Scan(&id, &aaiaId)
		if err != nil {
			return modelMap, err
		}
		modelMap[aaiaId] = id
	}
	return modelMap, err
}
