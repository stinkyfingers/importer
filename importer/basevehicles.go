package importer

import (
	"github.com/curt-labs/polkImporter/helpers/database"
	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/mgo.v2"
	// "gopkg.in/mgo.v2/bson"

	"database/sql"
	"errors"
	"log"
	"os"
	"reflect"
	"strconv"
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
	arrayOfAAIABaseIDs    = `select AAIABaseVehicleID from BaseVehicle`
	arrayOfOldPartNumbers = `select oldPartNumber from Part where oldPartNumber is not null`
)

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

func AuditBaseVehicles(bases []BaseVehicleGroup) ([]int, error) {
	var baseIds []int
	//get reference arrays
	existingBaseIdArray, err := GetArrayOfAAIABaseVehicleIDsForWhichThereExistsACurtBaseID()
	if err != nil {
		return baseIds, err
	}

	existingOldPartNumbersArray, err := GetArrayOfOldPartNumbersForWhichThereExistsACurtPartID()
	if err != nil {
		return baseIds, err
	}

	//create files to write
	baseNeed, err := os.Create("exports/BaseVehiclesNeeded.txt")
	if err != nil {
		return baseIds, err
	}
	baseOffset := int64(0)
	h := []byte("insert into vcdb_Vehicle (BaseVehicleID,AppID,RegionID) values \n")
	n, err := baseNeed.WriteAt(h, baseOffset)
	baseOffset += int64(n)

	partNeed, err := os.Create("exports/BaseVehicle_PartsNeeded.txt")
	if err != nil {
		return baseIds, err
	}
	partOffset := int64(0)
	h = []byte("insert into vcdb_VehiclePart(VehicleID, PartNumber) values \n")
	n, err = partNeed.WriteAt(h, partOffset)
	partOffset += int64(n)

	baseInBaseTableNeed, err := os.Create("exports/BaseVehiclesNeededInBaseVehicleTable.csv")
	if err != nil {
		return baseIds, err
	}
	baseTableOffset := int64(0)
	h = []byte("AAIABaseVehicleID,\n")
	n, err = baseInBaseTableNeed.WriteAt(h, baseTableOffset)
	baseTableOffset += int64(n)

	partInPartTableNeed, err := os.Create("exports/PartsNeededInPartTable.csv")
	if err != nil {
		return baseIds, err
	}
	partTableOffset := int64(0)
	h = []byte("PartNumber,\n")
	n, err = partInPartTableNeed.WriteAt(h, partTableOffset)
	partTableOffset += int64(n)

	//run diff
	var baseTally, subTally int
	for _, base := range bases {
		allSame := true
		for i := 0; i < len(base.Vehicles); i++ {
			if i > 0 {
				allSame = reflect.DeepEqual(base.Vehicles[i].PartNumbers, base.Vehicles[i-1].PartNumbers)
				log.Print(allSame)
				if allSame == true {
					baseTally++
				} else {
					subTally++
					break
				}
				// break
			}
		}
		if allSame == true {
			//check and add part(s) to base vehicle
			//TODO verify that this works
			for i, vehicle := range base.Vehicles {
				for j, part := range vehicle.PartNumbers {
					vehicleID, err := CheckBaseVehicleAndParts(base.BaseID, part, existingBaseIdArray, existingOldPartNumbersArray)
					if err != nil && i == 0 && j == 0 { //avoid multiple entries
						if err.Error() == "needbase" {
							log.Print("need a base vehicle ", base.BaseID)
							sql := " ((select b.ID from BaseVehicle as b where b.AAIABaseVehicleID = " + strconv.Itoa(base.BaseID) + "),0,0),\n"
							n, err := baseNeed.WriteAt([]byte(sql), baseOffset)
							if err != nil {
								return baseIds, err
							}
							baseOffset += int64(n)
							//enter ugly nested query in Vehicle_Part too
							sqlVehPart := []byte("((select ID from vcdb_Vehicle where BaseVehicleID = (select b.ID from BaseVehicle as b where b.AAIABaseVehicleID = " + strconv.Itoa(base.BaseID) + ") and SubmodelID = 0 and (ConfigID is null or ConfigID = 0)) , (select partID from Part where oldPartNumber = '" + part + "')),\n")
							m, err := partNeed.WriteAt([]byte(sqlVehPart), partOffset)
							if err != nil {
								return baseIds, err
							}
							partOffset += int64(m)
						}
						if err.Error() == "needpart" {
							log.Print("Need a part ", part, " for vehicleID ", vehicleID)
							sql := "(" + strconv.Itoa(vehicleID) + ", (select partID from Part where oldPartNumber = '" + part + "')),\n"
							n, err := partNeed.WriteAt([]byte(sql), partOffset)
							if err != nil {
								return baseIds, err
							}
							partOffset += int64(n)
						}
						if err.Error() == "nobasevehicleinbasetable" {
							b := []byte(strconv.Itoa(base.BaseID) + "\n")
							n, err := baseInBaseTableNeed.WriteAt(b, baseTableOffset)
							if err != nil {
								return baseIds, err
							}
							baseTableOffset += int64(n)
						}
						if err.Error() == "nooldpartinparttable" {
							b := []byte("'" + part + "',\n")
							n, err := partInPartTableNeed.WriteAt(b, partTableOffset)
							if err != nil {
								return baseIds, err
							}
							partTableOffset += int64(n)
						}
					}
				}
			}

		} else {
			//add base to submodel group - will search for submodels by baseId
			baseIds = append(baseIds, base.BaseID)
		}
	}
	//remove dupes from file
	err = RemoveDuplicates("exports/BaseVehiclesNeeded.txt")
	err = RemoveDuplicates("exports/BaseVehicle_PartsNeeded.txt")
	err = RemoveDuplicates("exports/BaseVehiclesNeededInBaseVehicleTable.csv")
	err = RemoveDuplicates("exports/PartsNeededInPartTable.csv")
	log.Print("base: ", baseTally, "    sub: ", subTally)

	return baseIds, err
}

//returns Curt vcdb_VehicleID and err
func CheckBaseVehicleAndParts(aaiaBaseId int, partNumber string, existingBaseIdArray []int, existingOldPartNumbersArray []string) (int, error) {
	db, err := sql.Open("mysql", database.ConnectionString())
	if err != nil {
		return 0, err
	}
	defer db.Close()

	//check base vehicle existence
	stmt, err := db.Prepare(getVehicleIdFromAAIABase)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	var vehicleID int
	err = stmt.QueryRow(aaiaBaseId).Scan(&vehicleID)
	if err != nil {
		if err == sql.ErrNoRows {
			for _, x := range existingBaseIdArray {
				if x == aaiaBaseId {
					err = errors.New("needbase")
					return 0, err
				}
			}
			//there is literally no curt Basevehicle for this AAIABaseVehicleID - needs insert in basevehicle table
			err = errors.New("nobasevehicleinbasetable")
			return 0, err
		}
		return 0, err
	}

	//check partnum
	stmt, err = db.Prepare(getVehiclePart)
	var partID int
	err = stmt.QueryRow(partNumber, &vehicleID).Scan(&partID)
	if err != nil {
		if err == sql.ErrNoRows {
			for _, y := range existingOldPartNumbersArray {
				if y == partNumber {
					err = errors.New("needpart")
					return vehicleID, err
				}
			}
			err = errors.New("nooldpartinparttable")
			return 0, err
		}
		return vehicleID, err
	}
	defer stmt.Close()
	return vehicleID, err
}

func GetArrayOfAAIABaseVehicleIDsForWhichThereExistsACurtBaseID() ([]int, error) {
	var err error
	var id int
	var arrayIDs []int
	db, err := sql.Open("mysql", database.ConnectionString())
	if err != nil {
		return arrayIDs, err
	}
	defer db.Close()

	stmt, err := db.Prepare(arrayOfAAIABaseIDs)
	if err != nil {
		return arrayIDs, err
	}
	defer stmt.Close()
	res, err := stmt.Query()
	if err != nil {
		return arrayIDs, err
	}
	for res.Next() {
		err = res.Scan(&id)
		if err != nil {
			return arrayIDs, err
		}
		arrayIDs = append(arrayIDs, id)
	}
	return arrayIDs, err
}

func GetArrayOfOldPartNumbersForWhichThereExistsACurtPartID() ([]string, error) {
	var err error
	var id string
	var arrayIDs []string
	db, err := sql.Open("mysql", database.ConnectionString())
	if err != nil {
		return arrayIDs, err
	}
	defer db.Close()

	stmt, err := db.Prepare(arrayOfOldPartNumbers)
	if err != nil {
		return arrayIDs, err
	}
	defer stmt.Close()
	res, err := stmt.Query()
	if err != nil {
		return arrayIDs, err
	}
	for res.Next() {
		err = res.Scan(&id)
		if err != nil {
			return arrayIDs, err
		}
		arrayIDs = append(arrayIDs, id)
	}
	return arrayIDs, err
}
