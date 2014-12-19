package v2

import (
	"github.com/curt-labs/polkImporter/helpers/database"
	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"database/sql"
	// "errors"
	"log"
	// "os"
	"reflect"
	"strconv"
	"sync"
)

type SubmodelRaw struct {
	ID         int    `bson:"submodelId,omitempty"`
	BaseID     int    `bson:"baseVehicleId,omitempty"`
	VehicleID  int    `bson:"vehicleId,omitempty"`
	PartNumber string `bson:"partNumber,omitempty"`
}

type SubmodelGroup struct {
	SubID    int `bson:"submodelId,omitempty"`
	BaseID   int `bson:"baseVehicleId,omitempty"`
	Vehicles []Vehicle
}

var (
	getVehicleIdFromAAIASubmodel = `select v.ID from Submodel as s
		join vcdb_Vehicle as v on v.SubmodelID = s.ID
		join BaseVehicle as b on b.ID = v.BaseVehicleID
		where b.AAIABaseVehicleID = ?
		and s.AAIASubmodelID = ?
		and (v.ConfigID = 0 or v.ConfigID is null)`
	arrayOfAAIASubmodelIDs          = `select AAIASubmodelID from Submodel`
	insertSubmodelIntoSubmodelTable = "insert into Submodel (AAIASubmodelID, SubmodelName) values (?,?)"
	insertSubmodelInVehicleTable    = "insert into vcdb_Vehicle(BaseVehicleID, SubmodelID, RegionID) values(?,?,0)"
)

var initSubMaps sync.Once
var subMap map[int]int
var submodelBaseToVehicleMap map[string]int

func submodelMap() {
	var err error
	// existingOldPartNumbersArray, _ = GetArrayOfOldPartNumbersForWhichThereExistsACurtPartID()
	// existingBaseIdArray, _ = GetArrayOfAAIABaseVehicleIDsForWhichThereExistsACurtBaseID()
	missingPartNumbers, err = createMissingPartNumbers()
	if err != nil {
		log.Print("err creating missingPartNumbers ", err)
	}
	vehiclePartJoins, err = createVehiclePartJoins()
	if err != nil {
		log.Print("err creating vehiclePartJoins ", err)
	}

	partMap, err = getPartMap()
	if err != nil {
		log.Print(err)
	}
	baseMap, err = getBaseMap()
	if err != nil {
		log.Print(err)
	}

	vehiclePartMap, err = getVehiclePartMap()
	if err != nil {
		log.Print(err)
	}
	subMap, err = getSubMap()
	if err != nil {
		log.Print(err)
	}
	submodelBaseToVehicleMap, err = getSubmodelBaseToVehicleMap()
	if err != nil {
		log.Print(err)
	}
}

//take int array (baseModelID) and return array of SubmodelRaw objects
func MongoToSubmodel(dbCollection string) ([]SubmodelRaw, error) {
	var err error
	var sbs []SubmodelRaw
	session, err := mgo.Dial(database.MongoConnectionString().Addrs[0])
	if err != nil {
		return sbs, err
	}
	defer session.Close()
	collection := session.DB("importer").C(dbCollection)
	err = collection.Find(nil).All(&sbs)

	return sbs, err
}

func SmgArray(sbs []SubmodelRaw) []SubmodelGroup {
	var subs []SubmodelGroup
	for _, row := range sbs {
		addS := true
		for i, sub := range subs {
			if sub.SubID == row.ID {
				//don't add base
				addS = false

				addV := true
				for j, veh := range subs[i].Vehicles {
					if veh.ID == row.VehicleID {
						addV = false

						addP := true
						for _, part := range subs[i].Vehicles[j].PartNumbers {
							if part == row.PartNumber {
								addP = false
							}
						}
						if addP == true {
							subs[i].Vehicles[j].PartNumbers = append(subs[i].Vehicles[j].PartNumbers, row.PartNumber)
						}
					}
				}
				if addV == true {
					var v Vehicle
					v.ID = row.VehicleID
					v.PartNumbers = append(v.PartNumbers, row.PartNumber)
					subs[i].Vehicles = append(subs[i].Vehicles, v)
				}
			}
		}
		if addS == true {
			var v Vehicle
			var sg SubmodelGroup
			v.ID = row.VehicleID
			v.PartNumbers = append(v.PartNumbers, row.PartNumber)
			sg.SubID = row.ID
			sg.BaseID = row.BaseID
			sg.Vehicles = append(sg.Vehicles, v)
			subs = append(subs, sg)
		}
	}
	return subs
}

func AuditSubmodels(submodels []SubmodelGroup, dbCollection string) ([]int, error) {
	var subIds []int
	var err error

	var subTally, configTally int

	for _, submodel := range submodels {
		allSame := true
		for i := 0; i < len(submodel.Vehicles); i++ {
			if i > 0 {
				allSame = reflect.DeepEqual(submodel.Vehicles[i].PartNumbers, submodel.Vehicles[i-1].PartNumbers)
				// log.Print(allSame)
				if allSame == true {
					subTally++
				} else {
					configTally++
					break
				}
			}
		}
		if allSame == true {
			//check and add part(s) to submodel vehicle
			//TODO verify that this works
			for _, vehicle := range submodel.Vehicles {
				for _, part := range vehicle.PartNumbers {
					// log.Print(submodel)
					_, err = CheckSubmodelAndParts(submodel.SubID, submodel.BaseID, part, dbCollection)
					if err != nil {
						return subIds, err
					}

				}
			}

		} else {
			//add submodel to config group - will search for configs by subId
			subIds = append(subIds, submodel.SubID)
		}
	}

	log.Print("subs to add: ", subTally, "   configs to pass on: ", configTally)
	//write missing vehicles
	err = WriteMissingVehiclesToCsv("submodelId", "VehiclesToDiffByConfig", dbCollection, subIds)
	if err != nil {
		log.Print(err)
		return subIds, err
	}
	return subIds, err
}

func CheckSubmodelAndParts(aaiaSubmodelId int, aaiaBaseId int, partNumber string, dbCollection string) (int, error) {
	var vehicleId int
	var partId int
	var baseId int
	var subId int
	var ok bool
	var err error
	initSubMaps.Do(submodelMap)

	//check part
	if partId, ok = partMap[partNumber]; !ok {
		//missing part - write to csv
		b := []byte(partNumber + "\n")
		n, err := missingPartNumbers.WriteAt(b, missingPartNumbersOffset)
		if err != nil {
			return vehicleId, err
		}
		missingPartNumbersOffset += int64(n)
		return vehicleId, err
	} else {
		partId = partMap[partNumber]
	}

	//check BV
	if baseId, ok = baseMap[aaiaBaseId]; !ok {
		baseId, err = InsertBaseVehicleIntoBaseVehicleTable(aaiaBaseId, dbCollection)
		if err != nil {
			return vehicleId, err
		}
	} else {
		baseId = baseMap[aaiaBaseId]
	}
	//check sub
	if subId, ok = subMap[aaiaSubmodelId]; !ok {
		subId, err = InsertSubmodelIntoSubmodelTable(aaiaSubmodelId, dbCollection)
		if err != nil {
			return vehicleId, err
		}
	} else {
		subId = subMap[aaiaSubmodelId]
	}

	//check v
	vehicleId, err = CheckVehiclesForSubmodel(subId, baseId)
	if err != nil {
		return vehicleId, err
	}
	// log.Print("vehicle ID ", vehicleId, " part id ", partId)

	//check vehicle part join
	err = CheckVehiclePartJoin(vehicleId, partId, false)
	if err != nil {
		return vehicleId, err
	}
	return vehicleId, err
}

func InsertSubmodelIntoSubmodelTable(aaiaSubmodelId int, dbCollection string) (int, error) {
	var err error
	var sv SimpleVehicle
	var subId int
	//get deeets from mongo
	session, err := mgo.Dial(database.MongoConnectionString().Addrs[0])
	if err != nil {
		return subId, err
	}
	defer session.Close()
	collection := session.DB("importer").C(dbCollection)
	err = collection.Find(bson.M{"submodelId": aaiaSubmodelId}).One(&sv)
	if err != nil {
		return subId, err
	}

	db, err := sql.Open("mysql", database.ConnectionString())
	if err != nil {
		return subId, err
	}
	defer db.Close()

	stmt, err := db.Prepare(insertSubmodelIntoSubmodelTable)
	if err != nil {
		return subId, err
	}
	defer stmt.Close()
	res, err := stmt.Exec(aaiaSubmodelId, sv.SubmodelName)
	if err != nil {
		return subId, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return subId, err
	}
	subId = int(id)
	return subId, err
}

func CheckVehiclesForSubmodel(subId int, baseId int) (int, error) {
	var vehicleId int
	var subBase string
	var err error
	var ok bool

	subBase = strconv.Itoa(subId) + ":" + strconv.Itoa(baseId)

	if vehicleId, ok = submodelBaseToVehicleMap[subBase]; !ok {
		db, err := sql.Open("mysql", database.ConnectionString())
		if err != nil {
			return vehicleId, err
		}
		defer db.Close()

		stmt, err := db.Prepare(insertSubmodelInVehicleTable)
		if err != nil {
			return vehicleId, err
		}
		defer stmt.Close()
		res, err := stmt.Exec(baseId, subId)
		if err != nil {
			return vehicleId, err
		}
		id, err := res.LastInsertId()
		if err != nil {
			return vehicleId, err
		}
		vehicleId = int(id)

	} else {
		return vehicleId, err
	}
	return vehicleId, err
}
