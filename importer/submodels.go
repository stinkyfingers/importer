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
		and v.ConfigID = 0`
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
			sg.Vehicles = append(sg.Vehicles, v)
			subs = append(subs, sg)
		}
	}
	return subs
}

func AuditSubmodels(submodels []SubmodelGroup) ([]int, error) {
	var subIds []int

	subNeed, err := os.Create("SubmodelsNeeded")
	if err != nil {
		return subIds, err
	}
	subOffset := int64(0)

	partNeed, err := os.Create("Submodel_PartsNeeded")
	if err != nil {
		return subIds, err
	}
	partOffset := int64(0)

	for _, submodel := range submodels {
		allSame := true
		for i := 0; i < len(submodel.Vehicles); i++ {
			if i > 0 {
				allSame = reflect.DeepEqual(submodel.Vehicles[i].PartNumbers, submodel.Vehicles[i-1].PartNumbers)
				log.Print(allSame)
				break
			}
		}
		if allSame == true {
			//check and add part(s) to submodel vehicle
			//TODO verify that this works
			for i, vehicle := range submodel.Vehicles {
				for j, part := range vehicle.PartNumbers {
					vehicleID, err := CheckSubmodelAndParts(submodel.SubID, submodel.BaseID, part)
					if err != nil && i == 0 && j == 0 { //avoid multiple entries
						if err.Error() == "needbase" {
							log.Print("need a submodel vehicle ", submodel.SubID)
							sql := "insert into vcdb_Vehicle (BaseVehicleID) values (select b.ID from BaseVehicle as b where b.AAIABaseVehicleID = " + strconv.Itoa(submodel.SubID) + ")\n"
							n, err := subNeed.WriteAt([]byte(sql), subOffset)
							if err != nil {
								return subIds, err
							}
							subOffset += int64(n)
						}
						if err.Error() == "needpart" {
							log.Print("Need a part ", part, " for vehicleID ", vehicleID)
							sql := "insert into vcdb_VehiclePart(VehicleID, PartNumber) values(" + strconv.Itoa(vehicleID) + ", (select partID from Part where oldPartNumber = " + part + "))\n"
							n, err := partNeed.WriteAt([]byte(sql), partOffset)
							if err != nil {
								return subIds, err
							}
							partOffset += int64(n)
						}
					}
				}
			}

		} else {
			//add base to submodel group - will search for submodels by baseId
			subIds = append(subIds, submodel.SubID)
		}
	}
	return subIds, err
}

//returns Curt vcdb_VehicleID and err
func CheckSubmodelAndParts(aaiaSubmodelId, aaiaBaseVehicleId int, partNumber string) (int, error) {
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
			err = errors.New("needbase")
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
			err = errors.New("needpart")
			return vehicleID, err
		}
		return vehicleID, err
	}
	return vehicleID, err
}
