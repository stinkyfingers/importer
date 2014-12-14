package importer

import (
	"github.com/curt-labs/polkImporter/helpers/database"
	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/mgo.v2"
	// "gopkg.in/mgo.v2/bson"

	"log"
	// "os"
	"reflect"
	// "runtime"
	// "strconv"
	// "strings"
)

type BaseVehicleRaw struct {
	ID         int    `bson:"baseVehicleId,omitempty"`
	VehicleID  int    `bson:"vehicleId,omitempty"`
	PartNumber string `bson:"partNumber,omitempty"`
}

type SubmodelRaw struct {
	ID         int    `bson:"submodelId,omitempty"`
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

func Run(filename string, headerLines int, useOldPartNumbers bool, insertMissingData bool) error {
	log.Print("Running")
	var err error
	err = CaptureCsv(filename, headerLines)
	if err != nil {
		return err
	}
	return err
}

func RunAfterMongo() error {
	bvs, err := MongoToBase()
	if err != nil {
		return err
	}
	bases := BvgArray(bvs)
	baseIds := AuditBaseVehicles(bases)
	sbs, err := MongoToSubmodel(baseIds)
	log.Print(len(sbs))
	return err
}

//For all mongodb entries, returns BaseVehicleRaws
func MongoToBase() ([]BaseVehicleRaw, error) {
	var err error
	var bvs []BaseVehicleRaw
	session, err := mgo.Dial(database.MongoConnectionString().Addrs[0])
	if err != nil {
		return bvs, err
	}
	defer session.Close()
	collection := session.DB("importer").C("ariesTest")
	err = collection.Find(nil).All(&bvs)
	return bvs, err
}

func MongoToSubmodel(baseIds []int) ([]SubmodelRaw, error) {
	var err error
	var sbs []SubmodelRaw
	session, err := mgo.Dial(database.MongoConnectionString().Addrs[0])
	if err != nil {
		return sbs, err
	}
	defer session.Close()
	collection := session.DB("importer").C("ariesTest")
	err = collection.Find(baseIds).All(&sbs) //TODO - select WHERE

	return sbs, err
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

func AuditBaseVehicles(bases []BaseVehicleGroup) []int {
	var baseIds []int
	for _, base := range bases {
		allSame := true
		for i := 0; i < len(base.Vehicles); i++ {
			if i > 0 {
				allSame = reflect.DeepEqual(base.Vehicles[i].PartNumbers, base.Vehicles[i-1].PartNumbers)
				log.Print(allSame)
			}
		}
		if allSame == true {
			// log.Print(base)
			//check and add part(s) to base vehicle
		} else {
			//add base to submodel group - will search for submodels by baseId
			baseIds = append(baseIds, base.BaseID)

		}
	}
	return baseIds
}
