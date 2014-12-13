package importer

import (
	"github.com/curt-labs/polkImporter/helpers/database"
	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/mgo.v2"
	// "gopkg.in/mgo.v2/bson"

	"log"
	// "os"
	// "reflect"
	// "runtime"
	// "strconv"
	// "strings"
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

type VehicleRaw struct {
	BaseID     int
	VehicleID  int
	PartNumber string
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

func BetterBase(bvs []BaseVehicleRaw) {
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
	for _, bbb := range bases {
		log.Print(bbb)
	}
	log.Print(len(bases))

}

func MakeBaseVehicles(bvs []BaseVehicleRaw) {
	var bases []BaseVehicleGroup

	for _, row := range bvs {
		var v Vehicle
		var b BaseVehicleGroup
		addB := true
		for _, base := range bases {
			if base.BaseID == row.ID {
				addB = false

				addV := true
				for _, veh := range base.Vehicles {
					if veh.ID == row.VehicleID {
						addV = false
						addP := true
						for _, p := range veh.PartNumbers {
							if p == row.PartNumber {
								addP = false
							}
						}
						if addP == true {
							veh.PartNumbers = append(veh.PartNumbers, row.PartNumber)
							log.Print("append part", row.PartNumber, "to v ", veh.ID)
						}
					}
				}
				if addV == true {
					var tempV Vehicle
					tempV.PartNumbers = append(tempV.PartNumbers, row.PartNumber)
					tempV.ID = row.VehicleID
					log.Print("TEMP", tempV)
					base.Vehicles = append(base.Vehicles, tempV)

				}
			}

		}
		if addB == true {
			v.ID = row.VehicleID
			v.PartNumbers = append(v.PartNumbers, row.PartNumber)
			b.Vehicles = append(b.Vehicles, v)
			b.BaseID = row.ID
			bases = append(bases, b)
			log.Print("ROW", row)
		}

	}
	for _, bb := range bases {
		log.Print(bb)
	}

}

func MakeBaseMap(bvs []BaseVehicleRaw) {
	// vmap := make(map[int][]string)
	bmap := make(map[int][]map[int][]string)

	for _, row := range bvs {
		tempmap := make(map[int][]string)
		tempmap[row.VehicleID] = append(tempmap[row.VehicleID], row.PartNumber)
		bmap[row.ID] = append(bmap[row.ID], tempmap)

	}
	// log.Print((bmap))
	for i, row := range bmap {
		log.Print(i, row, "\n")
	}
}
