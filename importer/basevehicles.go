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
		and v.SubmodelID = 0
		and v.ConfigID = 0`
	getVehiclePart = `select vp.ID from vcdb_VehiclePart as vp 
		join Part as p on p.partID = vp.PartNumber
		where p.oldPartNumber = ?
		and vp.VehicleID = ?`
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

	baseNeed, err := os.Create("BaseVehiclesNeeded")
	if err != nil {
		return baseIds, err
	}
	baseOffset := int64(0)

	partNeed, err := os.Create("BaseVehicle_PartsNeeded")
	if err != nil {
		return baseIds, err
	}
	partOffset := int64(0)

	for _, base := range bases {
		allSame := true
		for i := 0; i < len(base.Vehicles); i++ {
			if i > 0 {
				allSame = reflect.DeepEqual(base.Vehicles[i].PartNumbers, base.Vehicles[i-1].PartNumbers)
				log.Print(allSame)
				break
			}
		}
		if allSame == true {
			//check and add part(s) to base vehicle
			//TODO verify that this works
			for i, vehicle := range base.Vehicles {
				for j, part := range vehicle.PartNumbers {
					vehicleID, err := CheckBaseVehicleAndParts(base.BaseID, part)
					if err != nil && i == 0 && j == 0 { //avoid multiple entries
						if err.Error() == "needbase" {
							log.Print("need a base vehicle ", base.BaseID)
							sql := "insert into vcdb_Vehicle (BaseVehicleID) values (select b.ID from BaseVehicle as b where b.AAIABaseVehicleID = " + strconv.Itoa(base.BaseID) + ")\n"
							n, err := baseNeed.WriteAt([]byte(sql), baseOffset)
							if err != nil {
								return baseIds, err
							}
							baseOffset += int64(n)
						}
						if err.Error() == "needpart" {
							log.Print("Need a part ", part, " for vehicleID ", vehicleID)
							sql := "insert into vcdb_VehiclePart(VehicleID, PartNumber) values(" + strconv.Itoa(vehicleID) + ", (select partID from Part where oldPartNumber = " + part + "))\n"
							n, err := partNeed.WriteAt([]byte(sql), partOffset)
							if err != nil {
								return baseIds, err
							}
							partOffset += int64(n)
						}
					}
				}
			}

		} else {
			//add base to submodel group - will search for submodels by baseId
			baseIds = append(baseIds, base.BaseID)
		}
	}
	return baseIds, err
}

//returns Curt vcdb_VehicleID and err
func CheckBaseVehicleAndParts(aaiaBaseId int, partNumber string) (int, error) {
	db, err := sql.Open("mysql", database.ConnectionString())
	defer db.Close()
	if err != nil {
		return 0, err
	}

	//check base vehicle existence
	stmt, err := db.Prepare(getVehicleIdFromAAIABase)
	defer stmt.Close()
	if err != nil {
		return 0, err
	}

	var vehicleID int
	err = stmt.QueryRow(aaiaBaseId).Scan(&vehicleID)
	if err != nil {
		if err == sql.ErrNoRows {
			err = errors.New("needbase")
			return 0, err
		}
		return 0, err
	}

	//check partnum
	stmt, err = db.Prepare(getVehiclePart)
	defer stmt.Close()
	if err != nil {
		return 0, err
	}
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
