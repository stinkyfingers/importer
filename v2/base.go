package v2

import (
	"github.com/curt-labs/polkImporter/helpers/database"
	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"database/sql"
	// "errors"
	"log"
	"os"
	"reflect"
	"strconv"
	"sync"
)

type BaseVehicleRaw struct {
	ID         int    `bson:"baseVehicleId,omitempty"`
	VehicleID  int    `bson:"vehicleId,omitempty"`
	PartNumber string `bson:"partNumber,omitempty"`
}

type BaseVehicleGroup struct {
	BaseID   int `bson:"baseVehicleId,omitempty"`
	Vehicles []Vehicle
}

type Vehicle struct {
	ID          int
	PartNumbers []string
}

var (
	getVehicleIdFromAAIABase = `select v.ID from BaseVehicle as b 
		join vcdb_Vehicle as v on v.BaseVehicleID = b.ID
		where b.AAIABaseVehicleID = ?
		and (v.SubmodelID = 0 or v.SubmodelID is null)
		and (v.ConfigID = 0 or v.ConfigID is null)`
	getVehiclePart = `select vp.ID from vcdb_VehiclePart as vp 
		join Part as p on p.partID = vp.PartNumber
		where p.oldPartNumber = ?
		and vp.VehicleID = ?`
	arrayOfAAIABaseIDs                    = `select AAIABaseVehicleID from BaseVehicle`
	arrayOfOldPartNumbers                 = `select oldPartNumber from Part where oldPartNumber is not null`
	insertBaseVehicleIntoBaseVehicleTable = `insert into BaseVehicle (AAIABaseVehicleID, YearID, MakeID, ModelID) values (?,?,?,?)`
	insertMake                            = "insert into vcdb_Make (AAIAMakeID, MakeName) values (?,?)"
	insertModel                           = "insert into vcdb_Model(AAIAModelID, ModelName, VehicleTypeID) values(?,?,?)"
	insertBaseVehicleInVehicleTable       = "insert into vcdb_Vehicle(BaseVehicleID, RegionID) values(?,0)"
	insertVehiclePartJoin                 = "insert into vcdb_VehiclePart(VehicleID, PartNumber) values(?,?)"
	checkVehiclePart                      = "select PartNumber from vcdb_VehiclePart where VehicleID = ? and PartNumber = ?"
)

var initMaps sync.Once
var missingPartNumbersOffset int64 = 0
var missingPartNumbers *os.File

var vehiclePartJoins *os.File
var partMap map[string]int
var baseMap map[int]int
var modelMap map[int]int
var makeMap map[int]int
var baseToVehicleMap map[int]int
var vehiclePartArray []string

// var vehiclePartMap map[int]int
var vehiclePartJoinsOffset int64 = 0

func initMap() {
	var err error

	missingPartNumbers, err = createMissingPartNumbers("MissingPartNumbers_Base")
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
	makeMap, err = getMakeMap()
	if err != nil {
		log.Print(err)
	}
	modelMap, err = getModelMap()
	if err != nil {
		log.Print(err)
	}
	baseToVehicleMap, err = getBaseVehicleToVehicleMap()
	if err != nil {
		log.Print(err)
	}

	vehiclePartArray, err = getVehiclePartArray()
	if err != nil {
		log.Print(err)
	}
}

//For all mongodb entries, returns BaseVehicleRaws
func MongoToBase(dbCollection string) ([]BaseVehicleRaw, error) {
	var err error
	var bvs []BaseVehicleRaw
	session, err := mgo.Dial(database.MongoConnectionString().Addrs[0])
	if err != nil {
		return bvs, err
	}
	defer session.Close()
	collection := session.DB("importer").C(dbCollection)
	err = collection.Find(nil).All(&bvs)
	return bvs, err
}

//creates array of BaseVehicleGroups, which contain arrays of vehicles, which contain arrays of partNumbers (oldPartNumbers)
func BvgArray(bvs []BaseVehicleRaw) []BaseVehicleGroup {
	var bases []BaseVehicleGroup

	for _, row := range bvs {
		addB := true
		for i, base := range bases {
			if base.BaseID == row.ID {
				//don't add base
				addB = false

				addV := true
				for j, veh := range bases[i].Vehicles {
					if veh.ID == row.VehicleID {
						addV = false

						addP := true
						for _, part := range bases[i].Vehicles[j].PartNumbers {
							if part == row.PartNumber {
								addP = false
							}
						}
						if addP == true {
							bases[i].Vehicles[j].PartNumbers = append(bases[i].Vehicles[j].PartNumbers, row.PartNumber)
						}
					}
				}
				if addV == true {
					var v Vehicle
					v.ID = row.VehicleID
					v.PartNumbers = append(v.PartNumbers, row.PartNumber)
					bases[i].Vehicles = append(bases[i].Vehicles, v)
				}
			}
		}
		if addB == true {
			var v Vehicle
			var bg BaseVehicleGroup
			v.ID = row.VehicleID
			v.PartNumbers = append(v.PartNumbers, row.PartNumber)
			bg.BaseID = row.ID
			bg.Vehicles = append(bg.Vehicles, v)
			bases = append(bases, bg)
		}
	}
	return bases
}

func AuditBaseVehicles(bases []BaseVehicleGroup, dbCollection string) (int, int, error) {
	var baseIds, doneIds []int
	var todoCount, doneCount int
	var err error
	initMaps.Do(initMap)

	//run diff
	for _, base := range bases {
		allSame := true
		for i := 0; i < len(base.Vehicles); i++ {
			if i > 0 {
				allSame = reflect.DeepEqual(base.Vehicles[i].PartNumbers, base.Vehicles[i-1].PartNumbers)
			}
		}
		if allSame == true {
			//check and add part(s) to base vehicle
			//TODO verify that this works
			for _, vehicle := range base.Vehicles {
				for _, part := range vehicle.PartNumbers {
					_, err := CheckBaseVehicleAndParts(base.BaseID, part, dbCollection)
					if err != nil {
						return todoCount, doneCount, err
					}
				}
			}
			doneIds = append(doneIds, base.BaseID)
		} else {
			//add base to submodel group - will search for submodels by baseId
			baseIds = append(baseIds, base.BaseID)

		}
	}
	//write to file for processing by sub
	err = WriteMissingVehiclesToCsv("baseVehicleId", "VehiclesToDiffBySubmodel", dbCollection, baseIds)
	if err != nil {
		return todoCount, doneCount, err
	}

	//get counts for logs
	todoCount, err = getVehicleCount(baseIds, "baseVehicleId", dbCollection)
	if err != nil {
		return todoCount, doneCount, err
	}
	doneCount, err = getVehicleCount(doneIds, "baseVehicleId", dbCollection)
	if err != nil {
		return todoCount, doneCount, err
	}

	//send csv of to-do's to mongo
	err = CaptureCsv("exports/VehiclesToDiffBySubmodel.csv", 0, "ariesSubs")
	if err != nil {
		return todoCount, doneCount, err
	}

	return todoCount, doneCount, err
}

//returns Curt vcdb_VehicleID and err
func CheckBaseVehicleAndParts(aaiaBaseId int, partNumber string, dbCollection string) (int, error) {
	var vehicleId int
	var partId int
	var baseId int
	var ok bool
	var err error

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
		baseMap[aaiaBaseId] = baseId
	} else {
		baseId = baseMap[aaiaBaseId]
	}

	//check v
	vehicleId, err = CheckVehiclesForBaseVehicle(baseId)
	if err != nil {
		return vehicleId, err
	}
	// log.Print("vehicle ID ", vehicleId, " part id ", partId)

	//check vehicle part join
	// log.Print("VID ", vehicleId, "   partID", partId)
	err = CheckVehiclePartJoin(vehicleId, partId, true)
	if err != nil {
		return vehicleId, err
	}
	return vehicleId, err
}

func InsertBaseVehicleIntoBaseVehicleTable(aaiaBaseId int, dbCollection string) (int, error) {
	var err error
	var sv SimpleVehicle
	var baseId int
	//get deeets from mongo
	session, err := mgo.Dial(database.MongoConnectionString().Addrs[0])
	if err != nil {
		return baseId, err
	}
	defer session.Close()
	collection := session.DB("importer").C(dbCollection)
	err = collection.Find(bson.M{"baseVehicleId": aaiaBaseId}).One(&sv)
	if err != nil {
		return baseId, err
	}

	//check make
	makeId, err := CheckMake(sv.AAIAMakeID, sv.MakeName)
	if err != nil {
		return baseId, err
	}

	//check model
	modelId, err := CheckModel(sv.AAIAModelID, sv.ModelName, sv.VehicleTypeID)
	if err != nil {
		return baseId, err
	}

	db, err := sql.Open("mysql", database.ConnectionString())
	if err != nil {
		return baseId, err
	}
	defer db.Close()

	stmt, err := db.Prepare(insertBaseVehicleIntoBaseVehicleTable)
	if err != nil {
		return baseId, err
	}
	defer stmt.Close()
	res, err := stmt.Exec(aaiaBaseId, sv.AAIAYearID, makeId, modelId)
	if err != nil {
		return baseId, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return baseId, err
	}
	baseId = int(id)
	return baseId, err
}

func CheckVehiclePartJoin(vehicleId, partId int, doInserts bool) error {
	var err error

	vp := strconv.Itoa(vehicleId) + ":" + strconv.Itoa(partId)
	for _, vpart := range vehiclePartArray {
		if vp == vpart {
			// log.Print("FOUND VP MATCH", vp)
			return nil
		}
	}
	// log.Print("Missing VP Match ", vehicleId, ":", partId)

	if doInserts == false {
		b := []byte("(" + strconv.Itoa(vehicleId) + "," + strconv.Itoa(partId) + "),\n")
		n, err := vehiclePartJoins.WriteAt(b, vehiclePartJoinsOffset)
		if err != nil {
			return err
		}
		vehiclePartJoinsOffset += int64(n)
	}
	if doInserts == true {
		db, err := sql.Open("mysql", database.ConnectionString())
		if err != nil {
			return err
		}
		defer db.Close()

		stmt, err := db.Prepare(insertVehiclePartJoin)
		if err != nil {
			return err
		}
		defer stmt.Close()

		_, err = stmt.Exec(vehicleId, partId)
		if err != nil {
			log.Print("ER ", vehicleId)
			return err
		}
		vehiclePartArray = append(vehiclePartArray, vp)
	}

	return err
}

func CheckVehiclesForBaseVehicle(baseId int) (int, error) {
	var vehicleId int
	var err error
	var ok bool

	if vehicleId, ok = baseToVehicleMap[baseId]; !ok {
		db, err := sql.Open("mysql", database.ConnectionString())
		if err != nil {
			return vehicleId, err
		}
		defer db.Close()

		stmt, err := db.Prepare(insertBaseVehicleInVehicleTable)
		if err != nil {
			return vehicleId, err
		}
		defer stmt.Close()
		res, err := stmt.Exec(baseId)
		if err != nil {
			return vehicleId, err
		}
		id, err := res.LastInsertId()
		if err != nil {
			return vehicleId, err
		}
		vehicleId = int(id)
		baseToVehicleMap[baseId] = vehicleId
	} else {
		return vehicleId, err
	}
	return vehicleId, err
}

func CheckMake(aaiaMakeId int, makeName string) (int, error) {
	// var makeId int
	var err error
	if makeId, ok := makeMap[aaiaMakeId]; !ok {
		db, err := sql.Open("mysql", database.ConnectionString())
		if err != nil {
			return makeId, err
		}
		defer db.Close()

		stmt, err := db.Prepare(insertMake)
		if err != nil {
			return makeId, err
		}
		defer stmt.Close()
		res, err := stmt.Exec(aaiaMakeId, makeName)
		if err != nil {
			return makeId, err
		}
		id, err := res.LastInsertId()
		if err != nil {
			return makeId, err
		}
		makeId = int(id)
		makeMap[aaiaMakeId] = makeId
		return makeId, err
	} else {
		return makeId, err
	}
}

func CheckModel(aaiaModelId int, modelName string, vehicleTypeId int) (int, error) {
	// var modelId int
	var err error
	if modelId, ok := modelMap[aaiaModelId]; !ok {
		db, err := sql.Open("mysql", database.ConnectionString())
		if err != nil {
			return modelId, err
		}
		defer db.Close()

		stmt, err := db.Prepare(insertModel)
		if err != nil {
			return modelId, err
		}
		defer stmt.Close()
		res, err := stmt.Exec(aaiaModelId, modelName, vehicleTypeId)
		if err != nil {
			return modelId, err
		}
		id, err := res.LastInsertId()
		if err != nil {
			return modelId, err
		}
		modelId = int(id)
		modelMap[aaiaModelId] = modelId
		return modelId, err
	} else {
		return modelId, err
	}
}

func createMissingPartNumbers(title string) (*os.File, error) {
	missingPartNumbers, err := os.Create("exports/" + title + ".csv")
	if err != nil {
		return missingPartNumbers, err
	}
	h := []byte("Missing Part Numbers \n")
	n, err := missingPartNumbers.WriteAt(h, missingPartNumbersOffset)
	missingPartNumbersOffset += int64(n)

	return missingPartNumbers, err
}
func createVehiclePartJoins() (*os.File, error) {
	vehiclePartJoins, err := os.Create("exports/VehiclePartJoins.txt")
	if err != nil {
		return vehiclePartJoins, err
	}
	h := []byte("insert into vcdb_VehiclePart(VehicleID, PartNumber) values \n")
	n, err := vehiclePartJoins.WriteAt(h, vehiclePartJoinsOffset)
	vehiclePartJoinsOffset += int64(n)

	return vehiclePartJoins, err
}

func WriteMissingVehiclesToCsv(lookupField string, fileExportName string, dbCollection string, objectArray []int) error {
	var crs []CsvVehicle
	//get deeets from mongo
	session, err := mgo.Dial(database.MongoConnectionString().Addrs[0])
	if err != nil {
		return err
	}
	defer session.Close()
	collection := session.DB("importer").C(dbCollection)

	err = collection.Find(bson.M{lookupField: bson.M{"$in": objectArray}}).All(&crs)
	if err != nil {
		return err
	}

	csvVehiclesToPassOn, err := os.Create("exports/" + fileExportName + ".csv")
	if err != nil {
		return err
	}
	csvVoff := int64(0)
	for _, cg := range crs {
		b := []byte(cg.Make + "," + cg.Model + "," + cg.SubModel + "," + cg.Year + "," + strconv.Itoa(int(cg.GVW)) + "," + strconv.Itoa(cg.VehicleID) + "," + strconv.Itoa(cg.BaseVehicleID) + "," + strconv.Itoa(cg.YearID) + "," + strconv.Itoa(cg.MakeID) + "," + strconv.Itoa(cg.ModelID) + "," + strconv.Itoa(cg.SubmodelID) + "," + strconv.Itoa(int(cg.VehicleTypeID)) + "," + strconv.Itoa(int(cg.FuelTypeID)) + "," + strconv.Itoa(int(cg.FuelDeliveryID)) + "," + strconv.Itoa(int(cg.AcesLiter)) + "," + strconv.Itoa(int(cg.AcesCC)) + "," + strconv.Itoa(int(cg.AcesCID)) + "," + strconv.Itoa(int(cg.AcesCyl)) + "," + cg.AcesBlockType + "," + strconv.Itoa(int(cg.AspirationID)) + "," + strconv.Itoa(int(cg.DriveTypeID)) + "," + strconv.Itoa(int(cg.BodyTypeID)) + "," + strconv.Itoa(int(cg.BodyNumDoorsID)) + "," + strconv.Itoa(int(cg.EngineVinID)) + "," + strconv.Itoa(int(cg.RegionID)) + "," + strconv.Itoa(int(cg.PowerOutputID)) + "," + strconv.Itoa(int(cg.FuelDelConfigID)) + "," + strconv.Itoa(int(cg.BodyStyleConfigID)) + "," + strconv.Itoa(int(cg.ValvesID)) + "," + strconv.Itoa(int(cg.CylHeadTypeID)) + "," + cg.BlockType + "," + strconv.Itoa(int(cg.EngineBaseID)) + "," + strconv.Itoa(int(cg.EngineConfigID)) + "," + cg.PCDBPartTerminologyName + "," + string(cg.Position) + "," + cg.PartNumber + "," + cg.PartDesc + "," + strconv.Itoa(cg.VehicleCount) + "," + strconv.Itoa(cg.DistributedPartOpportunity) + "," + strconv.Itoa(cg.MaximumPartOpportunity) + "\n")
		n, err := csvVehiclesToPassOn.WriteAt(b, csvVoff)
		if err != nil {
			return err
		}
		csvVoff += int64(n)
	}
	return err
}

func getVehicleCount(ids []int, lookupField string, dbCollection string) (int, error) {
	var count int
	session, err := mgo.Dial(database.MongoConnectionString().Addrs[0])
	if err != nil {
		return count, err
	}
	defer session.Close()
	collection := session.DB("importer").C(dbCollection)

	count, err = collection.Find(bson.M{lookupField: bson.M{"$in": ids}}).Count()
	if err != nil {
		return count, err
	}
	return count, err
}
