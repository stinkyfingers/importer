package importer

import (
	"github.com/curt-labs/polkImporter/helpers/database"
	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"database/sql"
	"errors"
	"log"
	"os"
	"reflect"
	"strconv"
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
		and (v.ConfigID = 0 or v.Config is null)`
	arrayOfAAIASubmodelIDs = `select AAIASubmodelID from Submodel`
)

//take int array (baseModelID) and return array of SubmodelRaw objects
func MongoToSubmodel(baseIds []int, dbCollection string) ([]SubmodelRaw, error) {
	var err error
	var sbs []SubmodelRaw
	session, err := mgo.Dial(database.MongoConnectionString().Addrs[0])
	if err != nil {
		return sbs, err
	}
	defer session.Close()
	collection := session.DB("importer").C(dbCollection)
	err = collection.Find(bson.M{"baseVehicleId": bson.M{"$in": baseIds}}).All(&sbs)

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

func AuditSubmodels(submodels []SubmodelGroup) ([]int, error) {
	var subIds []int
	existingSubIdArray, err := GetArrayOfAAIASubmodelIDsForWhichThereExistsACurtSubmodelID()
	if err != nil {
		return subIds, err
	}
	existingBaseIdArray, err := GetArrayOfAAIABaseVehicleIDsForWhichThereExistsACurtBaseID()
	if err != nil {
		return subIds, err
	}
	existingOldPartNumbersArray, err := GetArrayOfOldPartNumbersForWhichThereExistsACurtPartID()
	if err != nil {
		return subIds, err
	}

	subNeed, err := os.Create("exports/SubmodelsNeeded.txt")
	if err != nil {
		return subIds, err
	}
	subOffset := int64(0)
	h := []byte("insert into vcdb_Vehicle (BaseVehicleID,SubmodelID, AppID, RegionID) values \n")
	n, err := subNeed.WriteAt(h, subOffset)
	subOffset += int64(n)

	partNeed, err := os.Create("exports/Submodel_PartsNeeded.txt")
	if err != nil {
		return subIds, err
	}
	partOffset := int64(0)
	h = []byte("insert into vcdb_VehiclePart(VehicleID, PartNumber) values \n")
	n, err = partNeed.WriteAt(h, partOffset)
	partOffset += int64(n)

	subInSubTableNeed, err := os.Create("exports/SubmodelsNeededInSubmodelTable.csv")
	if err != nil {
		return subIds, err
	}
	subTableOffset := int64(0)

	baseInBaseTableNeed, err := os.Create("exports/BaseVehiclesNeededInBaseVehicleTable2.csv")
	if err != nil {
		return subIds, err
	}
	baseTableOffset := int64(0)
	h = []byte("AAIABaseVehicleID,\n")
	n, err = baseInBaseTableNeed.WriteAt(h, baseTableOffset)
	baseTableOffset += int64(n)

	partInPartTableNeed, err := os.Create("exports/PartsNeededInPartTable2.csv")
	if err != nil {
		return subIds, err
	}
	partTableOffset := int64(0)
	h = []byte("PartNumber,\n")
	n, err = partInPartTableNeed.WriteAt(h, partTableOffset)
	partTableOffset += int64(n)

	var subTally, configTally int

	for _, submodel := range submodels {
		allSame := true
		for i := 0; i < len(submodel.Vehicles); i++ {
			if i > 0 {
				allSame = reflect.DeepEqual(submodel.Vehicles[i].PartNumbers, submodel.Vehicles[i-1].PartNumbers)
				log.Print(allSame)
				if allSame == true {
					subTally++
				} else {
					configTally++
					break
				}
				// break

			}
		}
		if allSame == true {
			log.Print("YOU ARE HERE __________")
			//check and add part(s) to submodel vehicle
			//TODO verify that this works
			for i, vehicle := range submodel.Vehicles {
				for j, part := range vehicle.PartNumbers {
					log.Print(submodel)
					vehicleID, err := CheckSubmodelAndParts(submodel.SubID, submodel.BaseID, part, existingSubIdArray, existingBaseIdArray, existingOldPartNumbersArray)
					if err != nil && i == 0 && j == 0 { //avoid multiple entries
						if err.Error() == "needsub" {
							log.Print("need a submodel vehicle ", submodel.SubID)
							sql := "((select ID from BaseVehicle where AAIABaseVehicleID = " + strconv.Itoa(submodel.BaseID) + "),(select s.ID from Submodel as s where s.AAIASubmodelID = " + strconv.Itoa(submodel.SubID) + " limit 1),0,0),\n"
							n, err := subNeed.WriteAt([]byte(sql), subOffset)
							if err != nil {
								return subIds, err
							}
							subOffset += int64(n)
							//enter ugly nested query in Vehicle_Part too
							sqlVehPart := []byte("((select ID from vcdb_Vehicle where BaseVehicleID =(select b.ID from BaseVehicle as b where b.AAIABaseVehicleID = " + strconv.Itoa(submodel.BaseID) + " limit 1) and SubmodelID = (select s.ID from Submodel as s where s.AAIASubmodelID = " + strconv.Itoa(submodel.SubID) + " limit 1) and (ConfigID = 0 or ConfigID is null) limit 1) , (select partID from Part where oldPartNumber = '" + part + "')),\n")
							m, err := partNeed.WriteAt([]byte(sqlVehPart), partOffset)
							if err != nil {
								return subIds, err
							}
							partOffset += int64(m)
						}
						if err.Error() == "needpart" {
							log.Print("Need a part ", part, " for vehicleID ", vehicleID)
							sql := "(" + strconv.Itoa(vehicleID) + ", (select partID from Part where oldPartNumber = '" + part + "')),\n"
							n, err := partNeed.WriteAt([]byte(sql), partOffset)
							if err != nil {
								return subIds, err
							}
							partOffset += int64(n)
						}
						if err.Error() == "needsubmodelinsubmodeltable" {
							log.Print("need submodel in submodel table")
							b := []byte(strconv.Itoa(submodel.SubID) + "\n")
							n, err := subInSubTableNeed.WriteAt(b, subTableOffset)
							if err != nil {
								return subIds, err
							}
							subTableOffset += int64(n)
						}
						if err.Error() == "needbasevehiclesinbasevehicletable" {
							log.Print("need base", submodel.BaseID)
							b := []byte(strconv.Itoa(submodel.BaseID) + "\n")
							n, err := baseInBaseTableNeed.WriteAt(b, baseTableOffset)
							if err != nil {
								return subIds, err
							}
							baseTableOffset += int64(n)
						}
						if err.Error() == "nooldpartinparttable" {
							b := []byte("'" + part + "',\n")
							n, err := partInPartTableNeed.WriteAt(b, partTableOffset)
							if err != nil {
								return subIds, err
							}
							partTableOffset += int64(n)
						}
					}
				}
			}

		} else {
			//add submodel to config group - will search for configs by subId
			log.Print("appending to subIDS")
			subIds = append(subIds, submodel.SubID)
		}
	}
	//remove dupes from file
	err = RemoveDuplicates("exports/SubmodelsNeeded.txt")
	err = RemoveDuplicates("exports/Submodel_PartsNeeded.txt")
	err = RemoveDuplicates("exports/SubmodelsNeededInSubmodelTable.csv")
	err = RemoveDuplicates("exports/BaseVehiclesNeededInBaseVehicleTable2.csv")
	log.Print("subs to add: ", subTally, "   configs to pass on: ", configTally)
	return subIds, err
}

//returns Curt vcdb_VehicleID and err
func CheckSubmodelAndParts(aaiaSubmodelId, aaiaBaseVehicleId int, partNumber string, existingSubmodelIdArray, existingBaseIdArray []int, existingOldPartNumbersArray []string) (int, error) {
	db, err := sql.Open("mysql", database.ConnectionString())
	if err != nil {
		return 0, err
	}
	defer db.Close()

	//check base vehicle existence
	stmt, err := db.Prepare(getVehicleIdFromAAIASubmodel)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	var vehicleID int
	err = stmt.QueryRow(aaiaBaseVehicleId, aaiaSubmodelId).Scan(&vehicleID)
	if err != nil {
		if err == sql.ErrNoRows {
			for _, y := range existingBaseIdArray {
				if y == aaiaBaseVehicleId {
					for _, x := range existingSubmodelIdArray {
						if x == aaiaSubmodelId {
							err = errors.New("needsub")
							return 0, err
						}
					}
					err = errors.New("needsubmodelinsubmodeltable")
					return 0, err
				}
			}
			err = errors.New("needbasevehiclesinbasevehicletable")
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
	return vehicleID, err
}

func GetArrayOfAAIASubmodelIDsForWhichThereExistsACurtSubmodelID() ([]int, error) {
	var err error
	var id int
	var arrayIDs []int
	db, err := sql.Open("mysql", database.ConnectionString())
	if err != nil {
		return arrayIDs, err
	}
	defer db.Close()

	stmt, err := db.Prepare(arrayOfAAIASubmodelIDs)
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
