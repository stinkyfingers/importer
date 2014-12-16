package importer

import (
	"database/sql"
	"encoding/csv"
	"github.com/curt-labs/polkImporter/helpers/database"
	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"log"
	"os"
	"strconv"
)

var (
	vehicleMakeMap       = `select ID, AAIAMakeID from vcdb_Make`
	vehicleModelMap      = `select ID, AAIAModelID from vcdb_Model`
	configAttributeTypes = `select ID, AcesTypeID  from ConfigAttributeType where AcesTypeID > 0`
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
	csvfile, err := os.Create("exports/SubmodelsNeededInSubmodelTable.csv")
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

func GetQueriesToInsertMissingConfigs(dbCollection string) error {
	//mongo
	session, err := mgo.Dial(database.MongoConnectionString().Addrs[0])
	if err != nil {
		return err
	}
	defer session.Close()
	// collection := session.DB("importer").C(dbCollection)

	//csv
	csvfile, err := os.Open("exports/MissingConfigs.csv")
	if err != nil {
		return err
	}
	defer csvfile.Close()

	reader := csv.NewReader(csvfile)
	// reader.FieldsPerRecord = -1 //flexible number of fields

	lines, err := reader.ReadAll()
	if err != nil {
		return err
	}

	lines = lines[1:] //axe header

	var aaiaConfigID, aaiaConfigTypeID int
	for _, line := range lines {
		aaiaConfigID, err = strconv.Atoi(line[0])
		if err != nil {
			return err
		}
		aaiaConfigTypeID, err = strconv.Atoi(line[1])
		if err != nil {
			return err
		}

		var table, idField, valueField string
		switch {
		case aaiaConfigTypeID == 1:
			table = "WheelBase"
			idField = table + "ID"
			valueField = table
		case aaiaConfigTypeID == 2:
			table = "BodyType"
			idField = table + "ID"
			valueField = table + "Name"
		case aaiaConfigTypeID == 3:
			table = "DriveType"
			idField = table + "ID"
			valueField = table + "Name"
		case aaiaConfigTypeID == 4:
			table = "BodyNumDoors"
			idField = table + "ID"
			valueField = table
		case aaiaConfigTypeID == 5:
			table = "BedLength"
			idField = table + "ID"
			valueField = table + ""
		case aaiaConfigTypeID == 6:
			table = "FuelType"
			idField = table + "ID"
			valueField = table + "Name"
		case aaiaConfigTypeID == 7:
			table = "EngineBase"
			idField = table + "ID"
			valueField = "Liter"
		case aaiaConfigTypeID == 8:
			table = "Aspiration"
			idField = table + "ID"
			valueField = table + "Name"
		case aaiaConfigTypeID == 9:
			table = "BedType"
			idField = table + "ID"
			valueField = table + "Name"
		case aaiaConfigTypeID == 10:
			table = "BrakeABS"
			idField = table + "ID"
			valueField = table + "Name"
		case aaiaConfigTypeID == 11:
			table = "BrakeSystem"
			idField = table + "ID"
			valueField = table + "Name"
		case aaiaConfigTypeID == 12:
			table = "CylinderHeadType"
			idField = table + "ID"
			valueField = table + "Name"
		case aaiaConfigTypeID == 13:
			table = "EngineDesignation"
			idField = table + "ID"
			valueField = table + "Name"
		case aaiaConfigTypeID == 14:
			table = "Mfr"
			idField = table + "ID"
			valueField = table + "Name"
		case aaiaConfigTypeID == 15:
			table = "EngineVersion"
			idField = table + "ID"
			valueField = table + ""
		case aaiaConfigTypeID == 16:
			table = "EngineVin"
			idField = table + "ID"
			valueField = table + "Name"
		case aaiaConfigTypeID == 17:
			table = "BrakeType"
			idField = table + "ID"
			valueField = table + "Name"
		case aaiaConfigTypeID == 18:
			table = "SpringType"
			idField = table + "ID"
			valueField = table + "Name"
		case aaiaConfigTypeID == 19:
			table = "FuelDeliverySubType"
			idField = table + "ID"
			valueField = table + "Name"
		case aaiaConfigTypeID == 20:
			table = "FuelDeliveryType"
			idField = table + "ID"
			valueField = table + "Name"
		case aaiaConfigTypeID == 21:
			table = "FuelSystemControlType"
			idField = table + "ID"
			valueField = table + "Name"
		case aaiaConfigTypeID == 22:
			table = "FuelSystemDesign"
			idField = table + "ID"
			valueField = table + "Name"
		case aaiaConfigTypeID == 23:
			table = "IgnitionSystemType"
		case aaiaConfigTypeID == 24:
			table = "MfrBodyCode"
			idField = table + "ID"
			valueField = table + "Name"
		case aaiaConfigTypeID == 25:
			table = "PowerOutput"
			idField = table + "ID"
			valueField = "HorsePower"
		case aaiaConfigTypeID == 26:
			table = "BrakeType"
			idField = table + "ID"
			valueField = table + "Name"
		case aaiaConfigTypeID == 27:
			table = "SpringType"
			idField = table + "ID"
			valueField = table + "Name"
		case aaiaConfigTypeID == 29:
			table = "SteeringSystem"
			idField = table + "ID"
			valueField = table + "Name"
		case aaiaConfigTypeID == 30:
			table = "SteeringType"
			idField = table + "ID"
			valueField = table + "Name"
		case aaiaConfigTypeID == 31:
			table = "Transmission"
			idField = table + "ID"
			valueField = table + "ElecControlledID"
		// case aaiaConfigTypeID == 34:
		// 	table = "Transmission"
		// 	idField = table + "ID"
		// 	valueField = table + "Name"
		// case aaiaConfigTypeID == 35:
		// 	table = "TransmissionBase"
		// 	idField = table + "ID"
		// 	valueField = table + "Name"
		case aaiaConfigTypeID == 36:
			table = "TransmissionControlType"
			idField = table + "ID"
			valueField = table + "Name"
		case aaiaConfigTypeID == 37:
			table = "TransmissionManufacturerCode"
			idField = table + "ID"
			valueField = table + ""
		case aaiaConfigTypeID == 38:
			table = "TransmissionNumSpeeds"
			idField = table + "ID"
			valueField = table + "Name"
		case aaiaConfigTypeID == 38:
			table = "TransmissionNumSpeeds"
			idField = table + "ID"
			valueField = table + ""
		case aaiaConfigTypeID == 38:
			table = "TransmissionType"
			idField = table + "ID"
			valueField = table + "Name"
		case aaiaConfigTypeID == 38:
			table = "Valves"
			idField = table + "ID"
			valueField = table + "Name"

		}
		valueName, err := GetAcesConfigValueName(idField, valueField, table, aaiaConfigID)
		if err != nil {
			return err
		}
		log.Print(valueName)
		// err = collection.Find(bson.M{"baseVehicleId": BaseVehicleID}).One(&configValue)
		//TODO finish generating inserts
	}
	return err
}

func GetConfigTypeMap() (map[int]int, error) {
	var err error
	ctMap := make(map[int]int)
	db, err := sql.Open("mysql", database.ConnectionString())
	if err != nil {
		return ctMap, err
	}
	defer db.Close()

	stmt, err := db.Prepare(configAttributeTypes)
	if err != nil {
		return ctMap, err
	}
	defer stmt.Close()
	var curtID, acesID int
	res, err := stmt.Query()
	if err != nil {
		return ctMap, err
	}
	for res.Next() {
		err = res.Scan(&curtID, &acesID)
		if err != nil {
			return ctMap, err
		}
		ctMap[acesID] = curtID
	}
	return ctMap, err
}

func GetAcesConfigValueName(idField, valueField, table string, id int) (string, error) {
	var valueName string
	sqlStmt := "select " + valueField + " from " + table + " where " + idField + " = " + strconv.Itoa(id)
	db, err := sql.Open("mysql", database.ConnectionString())
	if err != nil {
		return valueName, err
	}
	defer db.Close()

	stmt, err := db.Prepare(sqlStmt)
	if err != nil {
		return valueName, err
	}
	defer stmt.Close()

	err = stmt.QueryRow().Scan(&valueName)
	if err != nil {
		return valueName, err
	}
	return valueName, err
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
