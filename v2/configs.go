package v2

import (
	"github.com/curt-labs/polkImporter/helpers/database"
	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/mgo.v2"
	// "gopkg.in/mgo.v2/bson"

	"database/sql"
	"errors"
	"log"
	"os"
	// "reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type ConfigVehicleRaw struct {
	SubmodelID        int     `bson:"submodelId,omitempty"`
	BaseID            int     `bson:"baseVehicleId,omitempty"`
	VehicleID         int     `bson:"vehicleId,omitempty"`
	VehicleTypeID     uint8   `bson:"vehicleTypeId,omitempty"`
	FuelTypeID        uint8   `bson:"fuelTypeId,omitempty"`       // 6 FuelType
	FuelDeliveryID    uint8   `bson:"fuelDeliveryId,omitempty"`   //20 FuelDeliveryType
	AcesLiter         float64 `bson:"acesLiter,omitempty"`        //EngineBase.Liter
	AcesCC            float64 `bson:"acesCc,omitempty"`           //EngineBase.CC
	AcesCID           uint16  `bson:"acesCid,omitempty"`          //EngineBase.CID
	AcesCyl           uint8   `bson:"acesCyl,omitempty"`          //EngineBase.Cylinders
	AcesBlockType     string  `bson:"acesBlockType,omitempty"`    //EngineBase.BlockType
	AspirationID      uint8   `bson:"aspirationId,omitempty"`     // 8 Aspiration
	DriveTypeID       uint8   `bson:"driveId,omitempty"`          // 3 DriveType
	BodyTypeID        uint8   `bson:"bodyTypeId,omitempty"`       // 2 BodyType
	BodyNumDoorsID    uint8   `bson:"bodyNumDoors,omitempty"`     // 4 BodyNumDoors
	EngineVinID       uint8   `bson:"engineVin,omitempty"`        // 16 EngineVIN
	RegionID          uint8   `bson:"regionId,omitempty"`         //Region
	PowerOutputID     uint16  `bson:"powerOutputId,omitempty"`    // 25 PowerOutput
	FuelDelConfigID   uint8   `bson:"fuelDelConfigId,omitempty"`  //FuelDeliveryConfig
	BodyStyleConfigID uint8   `bson:"bodyStyeConfigId,omitempty"` //BodyStyleConfig
	ValvesID          uint8   `bson:"valvesId,omitempty"`         // 40 Valves
	CylHeadTypeID     uint8   `bson:"cylHeadTypeId,omitempty"`    // 12 CylinderHeadType
	BlockType         string  `bson:"blockType,omitempty"`        //EngineBase.BlockType
	EngineBaseID      uint16  `bson:"engineBaseId,omitempty"`     // 7 EngineBase
	EngineConfigID    uint16  `bson:"engineConfigId,omitempty"`   //EngineConfig
	PartNumber        string  `bson:"partNumber,omitempty"`
}

type ConfigVehicleGroup struct {
	VehicleID      int `bson:"vehicleId,omitempty"`
	SubID          int `bson:"submodelId,omitempty"`
	BaseID         int `bson:"baseVehicleId,omitempty"`
	DiffConfigs    []int
	ConfigVehicles []ConfigVehicleRaw
}

var (
	configMapStmt = `select ca.ConfigAttributeTypeID, cat.AcesTypeID, ca.vcdbID, ca.ID 
			from CurtDev.ConfigAttribute as ca 
			join CurtDev.ConfigAttributeType as cat on cat.ID = ca.ConfigAttributeTypeID`

	checkVehicleJoin = `select v.ID, vca.VehicleConfigID from vcdb_Vehicle as v 
		join BaseVehicle as b on b.ID = v.BaseVehicleID
		join Submodel as s on s.ID = v.SubmodelID
		join VehicleConfigAttribute as vca on vca.VehicleConfigID = v.ConfigID
		where b.AAIABaseVehicleID = ?
		and s.AAIASubmodelID = ?
		and vca.AttributeID = ?`
	vehicleJoinMapStmt = `select v.ID, vca.VehicleConfigID, b.AAIABaseVehicleID, s.AAIASubmodelID, vca.AttributeID from vcdb_Vehicle as v 
		join BaseVehicle as b on b.ID = v.BaseVehicleID
		join Submodel as s on s.ID = v.SubmodelID
		join VehicleConfigAttribute as vca on vca.VehicleConfigID = v.ConfigID`
	insertVehiclePartStmt    = `insert into vcdb_VehiclePart (VehicleID, PartNumber) values (?, (select partID from Part where oldPartNumber = ?))`
	insertVehicleConfigStmt  = `insert into VehicleConfig (AAIAVehicleConfigID) values (0)`
	insertCurtConfigTypeStmt = `insert into ConfigAttributeType(name, AcesTypeID, sort) values (?,?,?)`
	insertCurtConfigStmt     = `insert into ConfigAttribute(ConfigAttributeTypeID, parentID, vcdbID, value) values(?,0,?,?)`
	getCurtConfigValueIdStmt = `select ID from ConfigAttribute where ConfigAttributeTypeID = ? and vcdbID = ? `
)

var (
	ConfigTypesOffset      int64 = 0
	ConfigOffset           int64 = 0
	VehicleConfigOffset    int64 = 0
	configPartNeededOffset int64 = 0
)

var acesTypeCurtTypeMap map[int]int
var configMap map[string]string
var configAttributeTypeMap map[int]int
var configAttributeMap map[string]int
var initConfigMaps sync.Once

func initConfigMap() {
	var err error
	configMap, _ = GetConfigMap()
	configAttributeTypeMap, err = getConfigAttriguteTypeMap()
	if err != nil {
		log.Print(err)
	}
	configAttributeMap, err = getConfigAttributeMap()
	if err != nil {
		log.Print(err)
	}
	partMap, err = getPartMap()
	if err != nil {
		log.Print(err)
	}
	missingPartNumbers, err = createMissingPartNumbers("MissingPartNumbers_Configs")
	if err != nil {
		log.Print("err creating missingPartNumbers ", err)
	}
}

//For all mongodb entries, returns BaseVehicleRaws
func MongoToConfig(dbCollection string) ([]ConfigVehicleRaw, error) {
	var err error
	var cgs []ConfigVehicleRaw

	session, err := mgo.Dial(database.MongoConnectionString().Addrs[0])
	if err != nil {
		return cgs, err
	}
	defer session.Close()
	collection := session.DB("importer").C(dbCollection)

	//write to csv raw vehicles
	err = collection.Find(nil).All(&cgs)

	return cgs, err
}

func CgArray(cgs []ConfigVehicleRaw) []ConfigVehicleGroup {
	var configVehicles []ConfigVehicleGroup
	for _, row := range cgs {
		addC := true
		for i, _ := range configVehicles {
			if configVehicles[i].VehicleID == row.VehicleID {
				addC = false
				configVehicles[i].ConfigVehicles = append(configVehicles[i].ConfigVehicles, row)
			}
		}
		if addC == true {
			var cvg ConfigVehicleGroup
			cvg.BaseID = row.BaseID
			cvg.SubID = row.SubmodelID
			cvg.VehicleID = row.VehicleID
			cvg.ConfigVehicles = append(cvg.ConfigVehicles, row)
			configVehicles = append(configVehicles, cvg)
		}
	}
	return configVehicles
}

//New Additive method - for each cvg
//loop through configVehicles
//for each field
//create array of field valeus (unique - remove duplciate)
//if len(arrayUniquerFieldValues)> 1{
//create cvgArray [len(arrayUniqueFieldValues)]cvgVehicles
//for each arrayUniqueFieldValues{

// }
// }

var newCvgs, newCvgs3, newCvgsDriveType, newCvgsBodyNumDoors, newCvgsEngineVin, newCvgsBodyType, newCvgsAcesLiter, newCvgsAcesCC, newCvgsAcesCid, newCvgsAcesBlock, newCvgsPower, newCvgsFuelDelConfig, newCvgsBodyStyle, newCvgValves, newCvgsCylHeadType, newCvgsEngineBase, newCvgsEngineConfig []ConfigVehicleGroup

func Reduce2(cvg ConfigVehicleGroup) error {
	var err error

	//loop through fields
	var ftype []int
	for _, c := range cvg.ConfigVehicles {
		//fuel type
		ftype = append(ftype, int(c.FuelTypeID))
	}
	ftype = removeDuplicatesFromIntArray(ftype)

	//FUEL TYPE
	if len(ftype) > 1 {
		mmm := make(map[uint8][]ConfigVehicleRaw)

		for _, c := range cvg.ConfigVehicles {
			// log.Print(c, mmm)
			mmm[c.FuelTypeID] = append(mmm[c.FuelTypeID], c)
		}
		// log.Print(mmm)
		for _, m := range mmm {
			var tempCvg ConfigVehicleGroup
			tempCvg.BaseID = cvg.BaseID
			tempCvg.SubID = cvg.SubID
			tempCvg.VehicleID = cvg.VehicleID
			for _, mm := range m {
				tempCvg.ConfigVehicles = append(tempCvg.ConfigVehicles, mm)
			}
			tempCvg.DiffConfigs = append(tempCvg.DiffConfigs, 6) // FUEL TYPE
			newCvgs = append(newCvgs, tempCvg)
		}
	} else {
		newCvgs = append(newCvgs, cvg)
	}

	//DO IT LIKE 15 more times....then process the configs

	return err
}

func Reduce3() error {
	var err error

	for _, cvg := range newCvgs {
		//loop through fields
		var ftype []int
		for _, c := range cvg.ConfigVehicles {
			//fuel type
			ftype = append(ftype, int(c.FuelDeliveryID))
		}
		ftype = removeDuplicatesFromIntArray(ftype)

		//FUEL TYPE
		if len(ftype) > 1 {
			mmm := make(map[uint8][]ConfigVehicleRaw)

			for _, c := range cvg.ConfigVehicles {
				// log.Print(c, mmm)
				mmm[c.FuelDeliveryID] = append(mmm[c.FuelDeliveryID], c)
			}
			// log.Print(mmm)
			for _, m := range mmm {
				var tempCvg ConfigVehicleGroup
				tempCvg.BaseID = cvg.BaseID
				tempCvg.SubID = cvg.SubID
				tempCvg.VehicleID = cvg.VehicleID
				for _, mm := range m {
					tempCvg.ConfigVehicles = append(tempCvg.ConfigVehicles, mm)
				}
				tempCvg.DiffConfigs = cvg.DiffConfigs                 //previous diffCOnfigs
				tempCvg.DiffConfigs = append(tempCvg.DiffConfigs, 20) // FUEL DELIVERY

				newCvgs3 = append(newCvgs3, tempCvg)
			}
		} else {
			newCvgs3 = append(newCvgs3, cvg)
		}
	}

	return err
}

func ReduceDriveType() error {
	var err error

	for _, cvg := range newCvgs3 {
		//loop through fields
		var ftype []int
		for _, c := range cvg.ConfigVehicles {
			//fuel type
			ftype = append(ftype, int(c.DriveTypeID))
		}
		ftype = removeDuplicatesFromIntArray(ftype)

		//FUEL TYPE
		if len(ftype) > 1 {
			mmm := make(map[uint8][]ConfigVehicleRaw)

			for _, c := range cvg.ConfigVehicles {
				// log.Print(c, mmm)
				mmm[c.DriveTypeID] = append(mmm[c.DriveTypeID], c)
			}
			// log.Print(mmm)
			for _, m := range mmm {
				var tempCvg ConfigVehicleGroup
				tempCvg.BaseID = cvg.BaseID
				tempCvg.SubID = cvg.SubID
				tempCvg.VehicleID = cvg.VehicleID
				for _, mm := range m {
					tempCvg.ConfigVehicles = append(tempCvg.ConfigVehicles, mm)
				}
				tempCvg.DiffConfigs = cvg.DiffConfigs                //previous diffCOnfigs
				tempCvg.DiffConfigs = append(tempCvg.DiffConfigs, 3) // DRIVE TYPE

				newCvgsDriveType = append(newCvgsDriveType, tempCvg)
			}
		} else {
			newCvgsDriveType = append(newCvgsDriveType, cvg)
		}
	}
	//DO IT LIKE 15 more times....then process the configs
	// log.Print(len(newCvgsDriveType), "\n\n")
	// for _, r := range newCvgsDriveType {
	// 	log.Print(r, "\n\n")
	// }
	return err
}

func ReduceBodyNumDoors() error {
	var err error

	for _, cvg := range newCvgsDriveType {
		//loop through fields
		var ftype []int
		for _, c := range cvg.ConfigVehicles {
			//fuel type
			ftype = append(ftype, int(c.BodyNumDoorsID))
		}
		ftype = removeDuplicatesFromIntArray(ftype)

		//FUEL TYPE
		if len(ftype) > 1 {
			mmm := make(map[uint8][]ConfigVehicleRaw)

			for _, c := range cvg.ConfigVehicles {
				// log.Print(c, mmm)
				mmm[c.BodyNumDoorsID] = append(mmm[c.BodyNumDoorsID], c)
			}
			// log.Print(mmm)
			for _, m := range mmm {
				var tempCvg ConfigVehicleGroup
				tempCvg.BaseID = cvg.BaseID
				tempCvg.SubID = cvg.SubID
				tempCvg.VehicleID = cvg.VehicleID
				for _, mm := range m {
					tempCvg.ConfigVehicles = append(tempCvg.ConfigVehicles, mm)
				}
				tempCvg.DiffConfigs = cvg.DiffConfigs                //previous diffCOnfigs
				tempCvg.DiffConfigs = append(tempCvg.DiffConfigs, 4) // NUM DOORS

				newCvgsBodyNumDoors = append(newCvgsBodyNumDoors, tempCvg)
			}
		} else {
			newCvgsBodyNumDoors = append(newCvgsBodyNumDoors, cvg)
		}
	}
	// log.Print(len(newCvgsBodyNumDoors), "\n\n")
	// for _, r := range newCvgsBodyNumDoors {
	// 	log.Print(r, "\n\n")
	// }

	return err
}

func ReduceEngineVin() error {
	var err error

	for _, cvg := range newCvgsBodyNumDoors {
		//loop through fields
		var ftype []int
		for _, c := range cvg.ConfigVehicles {
			//fuel type
			ftype = append(ftype, int(c.EngineVinID))
		}
		ftype = removeDuplicatesFromIntArray(ftype)

		//FUEL TYPE
		if len(ftype) > 1 {
			mmm := make(map[uint8][]ConfigVehicleRaw)

			for _, c := range cvg.ConfigVehicles {
				// log.Print(c, mmm)
				mmm[c.EngineVinID] = append(mmm[c.EngineVinID], c)
			}
			// log.Print(mmm)
			for _, m := range mmm {
				var tempCvg ConfigVehicleGroup
				tempCvg.BaseID = cvg.BaseID
				tempCvg.SubID = cvg.SubID
				tempCvg.VehicleID = cvg.VehicleID
				for _, mm := range m {
					tempCvg.ConfigVehicles = append(tempCvg.ConfigVehicles, mm)
				}
				tempCvg.DiffConfigs = cvg.DiffConfigs //previous diffCOnfigs
				tempCvg.DiffConfigs = append(tempCvg.DiffConfigs, 16)

				newCvgsEngineVin = append(newCvgsEngineVin, tempCvg)
			}
		} else {
			newCvgsEngineVin = append(newCvgsEngineVin, cvg)
		}
	}
	// log.Print(len(newCvgsEngineVin), "\n\n")
	// for _, r := range newCvgsEngineVin {
	// 	log.Print(r, "\n\n")
	// }

	return err
}

func ReduceBodyType() error {
	var err error

	for _, cvg := range newCvgsEngineVin {
		//loop through fields
		var ftype []int
		for _, c := range cvg.ConfigVehicles {
			//fuel type
			ftype = append(ftype, int(c.BodyTypeID))
		}
		ftype = removeDuplicatesFromIntArray(ftype)

		//FUEL TYPE
		if len(ftype) > 1 {
			mmm := make(map[uint8][]ConfigVehicleRaw)

			for _, c := range cvg.ConfigVehicles {
				// log.Print(c, mmm)
				mmm[c.BodyTypeID] = append(mmm[c.BodyTypeID], c)
			}
			// log.Print(mmm)
			for _, m := range mmm {
				var tempCvg ConfigVehicleGroup
				tempCvg.BaseID = cvg.BaseID
				tempCvg.SubID = cvg.SubID
				tempCvg.VehicleID = cvg.VehicleID
				for _, mm := range m {
					tempCvg.ConfigVehicles = append(tempCvg.ConfigVehicles, mm)
				}
				tempCvg.DiffConfigs = cvg.DiffConfigs //previous diffCOnfigs
				tempCvg.DiffConfigs = append(tempCvg.DiffConfigs, 16)

				newCvgsBodyType = append(newCvgsBodyType, tempCvg)
			}
		} else {
			newCvgsBodyType = append(newCvgsBodyType, cvg)
		}
	}
	// log.Print(len(newCvgsBodyType), "\n\n")
	// for _, r := range newCvgsBodyType {
	// 	log.Print(r, "\n\n")
	// }
	return err
}

func ReduceAcesLiter() error {
	var err error

	for _, cvg := range newCvgsBodyType {
		//loop through fields
		var ftype []int
		for _, c := range cvg.ConfigVehicles {
			//fuel type
			ftype = append(ftype, int(c.AcesLiter))
		}
		ftype = removeDuplicatesFromIntArray(ftype)

		//FUEL TYPE
		if len(ftype) > 1 {
			mmm := make(map[float64][]ConfigVehicleRaw)

			for _, c := range cvg.ConfigVehicles {
				// log.Print(c, mmm)
				mmm[c.AcesLiter] = append(mmm[c.AcesLiter], c)
			}
			// log.Print(mmm)
			for _, m := range mmm {
				var tempCvg ConfigVehicleGroup
				tempCvg.BaseID = cvg.BaseID
				tempCvg.SubID = cvg.SubID
				tempCvg.VehicleID = cvg.VehicleID
				for _, mm := range m {
					tempCvg.ConfigVehicles = append(tempCvg.ConfigVehicles, mm)
				}
				tempCvg.DiffConfigs = cvg.DiffConfigs                  //previous diffCOnfigs
				tempCvg.DiffConfigs = append(tempCvg.DiffConfigs, 106) // 6 - Aces Liter - special case

				newCvgsAcesLiter = append(newCvgsAcesLiter, tempCvg)
			}
		} else {
			newCvgsAcesLiter = append(newCvgsAcesLiter, cvg)
		}
	}

	return err
}

func ReduceAcesCC() error {
	var err error

	for _, cvg := range newCvgsAcesLiter {
		//loop through fields
		var ftype []int
		for _, c := range cvg.ConfigVehicles {
			//fuel type
			ftype = append(ftype, int(c.AcesCC))
		}
		ftype = removeDuplicatesFromIntArray(ftype)

		//FUEL TYPE
		if len(ftype) > 1 {
			mmm := make(map[float64][]ConfigVehicleRaw)

			for _, c := range cvg.ConfigVehicles {
				// log.Print(c, mmm)
				mmm[c.AcesCC] = append(mmm[c.AcesCC], c)
			}
			// log.Print(mmm)
			for _, m := range mmm {
				var tempCvg ConfigVehicleGroup
				tempCvg.BaseID = cvg.BaseID
				tempCvg.SubID = cvg.SubID
				tempCvg.VehicleID = cvg.VehicleID
				for _, mm := range m {
					tempCvg.ConfigVehicles = append(tempCvg.ConfigVehicles, mm)
				}
				tempCvg.DiffConfigs = cvg.DiffConfigs                  //previous diffCOnfigs
				tempCvg.DiffConfigs = append(tempCvg.DiffConfigs, 206) // 6 - Aces Liter - special case

				newCvgsAcesCC = append(newCvgsAcesCC, tempCvg)
			}
		} else {
			newCvgsAcesCC = append(newCvgsAcesCC, cvg)
		}
	}

	return err
}

func ReduceAcesCid() error {
	var err error

	for _, cvg := range newCvgsAcesCC {
		//loop through fields
		var ftype []int
		for _, c := range cvg.ConfigVehicles {
			//fuel type
			ftype = append(ftype, int(c.AcesCID))
		}
		ftype = removeDuplicatesFromIntArray(ftype)

		//FUEL TYPE
		if len(ftype) > 1 {
			mmm := make(map[uint16][]ConfigVehicleRaw)

			for _, c := range cvg.ConfigVehicles {
				// log.Print(c, mmm)
				mmm[c.AcesCID] = append(mmm[c.AcesCID], c)
			}
			// log.Print(mmm)
			for _, m := range mmm {
				var tempCvg ConfigVehicleGroup
				tempCvg.BaseID = cvg.BaseID
				tempCvg.SubID = cvg.SubID
				tempCvg.VehicleID = cvg.VehicleID
				for _, mm := range m {
					tempCvg.ConfigVehicles = append(tempCvg.ConfigVehicles, mm)
				}
				tempCvg.DiffConfigs = cvg.DiffConfigs                  //previous diffCOnfigs
				tempCvg.DiffConfigs = append(tempCvg.DiffConfigs, 306) // 6 - Aces Liter - special case

				newCvgsAcesCid = append(newCvgsAcesCid, tempCvg)
			}
		} else {
			newCvgsAcesCid = append(newCvgsAcesCid, cvg)
		}
	}
	//DO IT LIKE 15 more times....then process the configs
	// log.Print(len(newCvgsAcesCid), "\n\n")
	// for _, r := range newCvgsAcesCid {
	// 	log.Print(r, "\n\n")
	// }

	return err
}

func ReduceAcesBlock() error {
	var err error

	for _, cvg := range newCvgsAcesCid {
		//loop through fields
		var ftype []string
		for _, c := range cvg.ConfigVehicles {
			//fuel type
			ftype = append(ftype, c.AcesBlockType)
		}
		ftype = removeDuplicatesFromStringArray(ftype)

		//FUEL TYPE
		if len(ftype) > 1 {
			mmm := make(map[string][]ConfigVehicleRaw)

			for _, c := range cvg.ConfigVehicles {
				// log.Print(c, mmm)
				mmm[c.AcesBlockType] = append(mmm[c.AcesBlockType], c)
			}
			// log.Print(mmm)
			for _, m := range mmm {
				var tempCvg ConfigVehicleGroup
				tempCvg.BaseID = cvg.BaseID
				tempCvg.SubID = cvg.SubID
				tempCvg.VehicleID = cvg.VehicleID
				for _, mm := range m {
					tempCvg.ConfigVehicles = append(tempCvg.ConfigVehicles, mm)
				}
				tempCvg.DiffConfigs = cvg.DiffConfigs                  //previous diffCOnfigs
				tempCvg.DiffConfigs = append(tempCvg.DiffConfigs, 999) // HELP

				newCvgsAcesBlock = append(newCvgsAcesBlock, tempCvg)
			}
		} else {
			newCvgsAcesBlock = append(newCvgsAcesBlock, cvg)
		}
	}

	return err
}

func ReducePowerOutput() error {
	var err error

	for _, cvg := range newCvgsAcesBlock {
		//loop through fields
		var ftype []int
		for _, c := range cvg.ConfigVehicles {
			//fuel type
			ftype = append(ftype, int(c.PowerOutputID))
		}
		ftype = removeDuplicatesFromIntArray(ftype)

		//FUEL TYPE
		if len(ftype) > 1 {
			mmm := make(map[uint16][]ConfigVehicleRaw)

			for _, c := range cvg.ConfigVehicles {
				// log.Print(c, mmm)
				mmm[c.PowerOutputID] = append(mmm[c.PowerOutputID], c)
			}
			// log.Print(mmm)
			for _, m := range mmm {
				var tempCvg ConfigVehicleGroup
				tempCvg.BaseID = cvg.BaseID
				tempCvg.SubID = cvg.SubID
				tempCvg.VehicleID = cvg.VehicleID
				for _, mm := range m {
					tempCvg.ConfigVehicles = append(tempCvg.ConfigVehicles, mm)
				}
				tempCvg.DiffConfigs = cvg.DiffConfigs                 //previous diffCOnfigs
				tempCvg.DiffConfigs = append(tempCvg.DiffConfigs, 25) // 6 - Aces Liter - special case

				newCvgsPower = append(newCvgsPower, tempCvg)
			}
		} else {
			newCvgsPower = append(newCvgsPower, cvg)
		}
	}

	return err
}

func ReduceFuelDelConfig() error {
	var err error

	for _, cvg := range newCvgsPower {
		//loop through fields
		var ftype []int
		for _, c := range cvg.ConfigVehicles {
			//fuel type
			ftype = append(ftype, int(c.FuelDelConfigID))
		}
		ftype = removeDuplicatesFromIntArray(ftype)

		//FUEL TYPE
		if len(ftype) > 1 {
			mmm := make(map[uint8][]ConfigVehicleRaw)

			for _, c := range cvg.ConfigVehicles {
				// log.Print(c, mmm)
				mmm[c.FuelDelConfigID] = append(mmm[c.FuelDelConfigID], c)
			}
			// log.Print(mmm)
			for _, m := range mmm {
				var tempCvg ConfigVehicleGroup
				tempCvg.BaseID = cvg.BaseID
				tempCvg.SubID = cvg.SubID
				tempCvg.VehicleID = cvg.VehicleID
				for _, mm := range m {
					tempCvg.ConfigVehicles = append(tempCvg.ConfigVehicles, mm)
				}
				tempCvg.DiffConfigs = cvg.DiffConfigs //previous diffCOnfigs
				tempCvg.DiffConfigs = append(tempCvg.DiffConfigs, 20)

				newCvgsFuelDelConfig = append(newCvgsFuelDelConfig, tempCvg)
			}
		} else {
			newCvgsFuelDelConfig = append(newCvgsFuelDelConfig, cvg)
		}
	}

	return err
}

func ReduceBodyStyle() error {
	var err error

	for _, cvg := range newCvgsFuelDelConfig {
		//loop through fields
		var ftype []int
		for _, c := range cvg.ConfigVehicles {
			//fuel type
			ftype = append(ftype, int(c.BodyStyleConfigID))
		}
		ftype = removeDuplicatesFromIntArray(ftype)

		//FUEL TYPE
		if len(ftype) > 1 {
			mmm := make(map[uint8][]ConfigVehicleRaw)

			for _, c := range cvg.ConfigVehicles {
				// log.Print(c, mmm)
				mmm[c.BodyStyleConfigID] = append(mmm[c.BodyStyleConfigID], c)
			}
			// log.Print(mmm)
			for _, m := range mmm {
				var tempCvg ConfigVehicleGroup
				tempCvg.BaseID = cvg.BaseID
				tempCvg.SubID = cvg.SubID
				tempCvg.VehicleID = cvg.VehicleID
				for _, mm := range m {
					tempCvg.ConfigVehicles = append(tempCvg.ConfigVehicles, mm)
				}
				tempCvg.DiffConfigs = cvg.DiffConfigs                  //previous diffCOnfigs
				tempCvg.DiffConfigs = append(tempCvg.DiffConfigs, 999) // HELP

				newCvgsBodyStyle = append(newCvgsBodyStyle, tempCvg)
			}
		} else {
			newCvgsBodyStyle = append(newCvgsBodyStyle, cvg)
		}
	}

	return err
}

func ReduceValves() error {
	var err error

	for _, cvg := range newCvgsBodyStyle {
		//loop through fields
		var ftype []int
		for _, c := range cvg.ConfigVehicles {
			//fuel type
			ftype = append(ftype, int(c.ValvesID))
		}
		ftype = removeDuplicatesFromIntArray(ftype)

		//FUEL TYPE
		if len(ftype) > 1 {
			mmm := make(map[uint8][]ConfigVehicleRaw)

			for _, c := range cvg.ConfigVehicles {
				// log.Print(c, mmm)
				mmm[c.ValvesID] = append(mmm[c.ValvesID], c)
			}
			// log.Print(mmm)
			for _, m := range mmm {
				var tempCvg ConfigVehicleGroup
				tempCvg.BaseID = cvg.BaseID
				tempCvg.SubID = cvg.SubID
				tempCvg.VehicleID = cvg.VehicleID
				for _, mm := range m {
					tempCvg.ConfigVehicles = append(tempCvg.ConfigVehicles, mm)
				}
				tempCvg.DiffConfigs = cvg.DiffConfigs                 //previous diffCOnfigs
				tempCvg.DiffConfigs = append(tempCvg.DiffConfigs, 40) // HELP

				newCvgValves = append(newCvgValves, tempCvg)
			}
		} else {
			newCvgValves = append(newCvgValves, cvg)
		}
	}

	return err
}

func ReduceCylHead() error {
	var err error

	for _, cvg := range newCvgValves {
		//loop through fields
		var ftype []int
		for _, c := range cvg.ConfigVehicles {
			//fuel type
			ftype = append(ftype, int(c.CylHeadTypeID))
		}
		ftype = removeDuplicatesFromIntArray(ftype)

		//FUEL TYPE
		if len(ftype) > 1 {
			mmm := make(map[uint8][]ConfigVehicleRaw)

			for _, c := range cvg.ConfigVehicles {
				// log.Print(c, mmm)
				mmm[c.CylHeadTypeID] = append(mmm[c.CylHeadTypeID], c)
			}
			// log.Print(mmm)
			for _, m := range mmm {
				var tempCvg ConfigVehicleGroup
				tempCvg.BaseID = cvg.BaseID
				tempCvg.SubID = cvg.SubID
				tempCvg.VehicleID = cvg.VehicleID
				for _, mm := range m {
					tempCvg.ConfigVehicles = append(tempCvg.ConfigVehicles, mm)
				}
				tempCvg.DiffConfigs = cvg.DiffConfigs                 //previous diffCOnfigs
				tempCvg.DiffConfigs = append(tempCvg.DiffConfigs, 12) // HELP

				newCvgsCylHeadType = append(newCvgsCylHeadType, tempCvg)
			}
		} else {
			newCvgsCylHeadType = append(newCvgsCylHeadType, cvg)
		}
	}

	return err
}

func ReduceEngineBase() error {
	var err error

	for _, cvg := range newCvgsCylHeadType {
		//loop through fields
		var ftype []int
		for _, c := range cvg.ConfigVehicles {
			//fuel type
			ftype = append(ftype, int(c.EngineBaseID))
		}
		ftype = removeDuplicatesFromIntArray(ftype)

		//FUEL TYPE
		if len(ftype) > 1 {
			mmm := make(map[uint16][]ConfigVehicleRaw)

			for _, c := range cvg.ConfigVehicles {
				// log.Print(c, mmm)
				mmm[c.EngineBaseID] = append(mmm[c.EngineBaseID], c)
			}
			// log.Print(mmm)
			for _, m := range mmm {
				var tempCvg ConfigVehicleGroup
				tempCvg.BaseID = cvg.BaseID
				tempCvg.SubID = cvg.SubID
				tempCvg.VehicleID = cvg.VehicleID
				for _, mm := range m {
					tempCvg.ConfigVehicles = append(tempCvg.ConfigVehicles, mm)
				}
				tempCvg.DiffConfigs = cvg.DiffConfigs                 //previous diffCOnfigs
				tempCvg.DiffConfigs = append(tempCvg.DiffConfigs, 15) // HELP

				newCvgsEngineBase = append(newCvgsEngineBase, tempCvg)
			}
		} else {
			newCvgsEngineBase = append(newCvgsEngineBase, cvg)
		}
	}

	return err
}

func ReduceEngineConfig() error {
	var err error

	for _, cvg := range newCvgsEngineBase {
		//loop through fields
		var ftype []int
		for _, c := range cvg.ConfigVehicles {
			//fuel type
			ftype = append(ftype, int(c.EngineConfigID))
		}
		ftype = removeDuplicatesFromIntArray(ftype)

		//FUEL TYPE
		if len(ftype) > 1 {
			mmm := make(map[uint16][]ConfigVehicleRaw)

			for _, c := range cvg.ConfigVehicles {
				// log.Print(c, mmm)
				mmm[c.EngineConfigID] = append(mmm[c.EngineConfigID], c)
			}
			// log.Print(mmm)
			for _, m := range mmm {
				var tempCvg ConfigVehicleGroup
				tempCvg.BaseID = cvg.BaseID
				tempCvg.SubID = cvg.SubID
				tempCvg.VehicleID = cvg.VehicleID
				for _, mm := range m {
					tempCvg.ConfigVehicles = append(tempCvg.ConfigVehicles, mm)
				}
				tempCvg.DiffConfigs = cvg.DiffConfigs                //previous diffCOnfigs
				tempCvg.DiffConfigs = append(tempCvg.DiffConfigs, 7) // HELP

				newCvgsEngineConfig = append(newCvgsEngineConfig, tempCvg)
			}
		} else {
			newCvgsEngineConfig = append(newCvgsEngineConfig, cvg)
		}
	}
	//DO IT LIKE 15 more times....then process the configs
	// log.Print(len(newCvgsEngineConfig), "\n\n")
	// for _, r := range newCvgsEngineConfig {
	// 	log.Print(r, "\n\n")
	// }
	return err
}

//NEW
// foreach VehicleID's array of configs:
// for each config compile array of unique config values
// get length of each
//split into x groups of vehicles for lowest number of array values x, that is greater than 1
//repeat for each subgroup until there is only arrays of 1 unique config value

func NewAuditConfigs(configVehicleGroups []ConfigVehicleGroup) error {
	var err error

	for _, cvg := range configVehicleGroups { //each vehicleID with config array
		err = Reduce2(cvg)
		if err != nil {
			return err
		}
	}
	err = Reduce3()
	err = ReduceDriveType()
	err = ReduceBodyNumDoors()
	err = ReduceEngineVin()
	err = ReduceBodyType()
	err = ReduceAcesLiter()

	err = ReduceAcesCC()
	err = ReduceAcesCid()
	err = ReduceAcesBlock()
	err = ReducePowerOutput()
	err = ReduceFuelDelConfig()
	err = ReduceBodyStyle()
	err = ReduceValves()
	err = ReduceCylHead()
	err = ReduceEngineBase()
	err = ReduceEngineConfig()

	return err
}

//Hopefully, Vehicles are limited to unique arrays of configs and can be created based on their DiffConfigs arrays
//This kicks it off
func ProcessReducedConfigs() error {
	var err error
	configErrorFile, err := os.Create("exports/ConfigErrorFile.txt")
	if err != nil {
		return err
	}
	off := int64(0)

	log.Print("Num of ConfigVehicles Processed: ", len(newCvgsEngineConfig), "\n\n")
	for _, r := range newCvgsEngineConfig {
		processCvg := false
		if len(r.ConfigVehicles) > 1 {
			for i, con := range r.ConfigVehicles {
				if i > 1 { //not the first - we compare to i-1
					comparedConfigs, _ := CompareConfigFields(con, r.ConfigVehicles[i-1])
					if comparedConfigs == false {
						//write this ConfigVehicleGroup to file
						b := []byte(strconv.Itoa(r.BaseID) + "," + strconv.Itoa(r.SubID) + "," + strconv.Itoa(r.VehicleID) + "\n")
						n, err := configErrorFile.WriteAt(b, off)
						if err != nil {
							return err
						}
						off += int64(n)
						continue
					} else {
						processCvg = true
						//good to process
					}
				}
			}
		} else {
			processCvg = true //only a single attribute array - also good to process

		}
		if processCvg == true {
			//begin the databasing
			err = AuditConfigsRedux(r)
		}
	}
	return err
}

//This just compares the config fields of two Configs Set for the types we actually look at
func CompareConfigFields(c1, c2 ConfigVehicleRaw) (bool, error) {
	var err error
	var match bool
	if c1.AcesCC == c2.AcesCC && c1.AcesCID == c2.AcesCID && c1.AcesLiter == c2.AcesLiter && c1.AspirationID == c2.AspirationID && c1.AcesBlockType == c2.AcesBlockType && c1.FuelTypeID == c2.FuelTypeID && c1.FuelDeliveryID == c2.FuelDeliveryID && c1.DriveTypeID == c2.DriveTypeID && c1.BodyNumDoorsID == c2.BodyNumDoorsID && c1.EngineVinID == c2.EngineVinID && c1.BodyTypeID == c2.BodyTypeID && c1.PowerOutputID == c2.PowerOutputID && c1.FuelDeliveryID == c2.FuelDeliveryID && c1.BodyStyleConfigID == c2.BodyStyleConfigID && c1.ValvesID == c2.ValvesID && c1.CylHeadTypeID == c2.CylHeadTypeID && c1.EngineBaseID == c2.EngineBaseID && c1.EngineConfigID == c2.EngineConfigID {
		match = true
	}

	return match, err
}

func removeDuplicatesFromIntArray(a []int) []int {
	var output []int
	for i, num := range a {
		var addit bool = true
		if i == 0 {
			output = append(output, num)
		}
		for _, o := range output {
			if o == num {
				addit = false
			}
		}
		if addit == true {
			output = append(output, num)
		}
	}
	return output
}

func removeDuplicatesFromStringArray(a []string) []string {
	var output []string
	for i, num := range a {
		var addit bool = true
		if i == 0 {
			output = append(output, num)
		}
		for _, o := range output {
			if o == num {
				addit = false
			}
		}
		if addit == true {
			output = append(output, num)
		}
	}
	return output
}

func AuditConfigsRedux(cvg ConfigVehicleGroup) error {
	var err error
	initConfigMaps.Do(initConfigMap)

	//for each config type
	//1. is there a Curt config type?
	//N: Create curt type from vcdb
	//2. is there a Curt config (value)?
	//N. Create a Curt Value from vcdb
	//3. Is there a Curt vehicle with these Configs?
	//N: Create curt vehicle
	//4. Is there a Curt PartID for this partnumber?
	//N: log the missing part; break
	//5. Is there a part join for the part associated with these configs?
	//N: Create vcdb_VehiclePart join

	//TODO
	for _, c := range cvg.ConfigVehicles {
		for _, diffTypeId := range cvg.DiffConfigs {
			curtConfigType := configAttributeTypeMap[diffTypeId]
			if curtConfigType == 0 {
				//need curt config type
			}

			aaiaValue, err := getAaiaConfigValueFromTypeId(c, diffTypeId)
			if err != nil {
				//need aaia value
				return err
			}
			//get curt configValueIds - or insert them
			curtConfigValueId, err := checkCurtConfigValue(diffTypeId, curtConfigType, aaiaValue)
			if err != nil {
				return err
			}
			log.Print(curtConfigValueId)

		}
	}

	// //check for curt configtype - error out
	// for _, aaiaConfigType := range cvg.DiffConfigs {
	// 	curtConfigType := configAttributeTypeMap[aaiaConfigType]
	// 	if curtConfigType == 0 {
	// 		log.Print("Missing Type: ", aaiaConfigType)
	// 		//need configType
	// 		// log.Panic("Missing type")
	// 	}
	// }

	// for _, c := range cvg.ConfigVehicles {
	// 	for _, diffTypeId := range cvg.DiffConfigs {
	// 		//get curt config type
	// 		curtConfigType := configAttributeTypeMap[diffTypeId]

	// 		//get aaiaa configValue
	// 		aaiaConValID, err := getAaiaConfigValueFromTypeId(c, diffTypeId)
	// 		if err != nil {
	// 			log.Print(err)
	// 			//create this config
	// 		}

	// 		//getCurt config Values
	// 		curtConfigValue, err := getCurtConfigValue(curtConfigType, aaiaConValID.(int)) //
	// 		if err != nil {
	// 			if err == sql.ErrNoRows {
	// 				if curtConfigValue == 0 {
	// 					log.Print("Missing Type Value, curttype: ", diffTypeId, " ", aaiaConValID, " ", curtConfigType)
	// 					//need configValue
	// 					curtConfigValue, err = insertCurtConfig(diffTypeId, aaiaConValID.(int), curtConfigType)
	// 					if err != nil {
	// 						log.Panic(err)
	// 						return err
	// 					}
	// 					log.Print("Created Value: ", curtConfigValue)
	// 				}
	// 			} else {
	// 				return err
	// 			}
	// 		}

	// 	}

	// }

	return err
}

func checkCurtConfigValue(aaiaConfigTypeId, curtConfigType int, aaiaValue string) (int, error) {
	var err error
	var curtConfigAttributeId int
	db, err := sql.Open("mysql", database.ConnectionString())
	if err != nil {
		return curtConfigAttributeId, err
	}
	defer db.Close()

	stmt, err := db.Prepare("select ID from ConfigAttribute where ConfigAttributeTypeID = ? and value = ?")
	if err != nil {
		return curtConfigAttributeId, err
	}
	defer stmt.Close()
	err = stmt.QueryRow(curtConfigType, aaiaValue).Scan(&curtConfigAttributeId)
	if err != nil {
		if err == sql.ErrNoRows {
			err = nil
			acesId, err := getAcesConfigurationID(aaiaConfigTypeId, aaiaValue)
			if err != nil {
				return curtConfigAttributeId, err
			}
			//insert this value

			stmt, err = db.Prepare("insert into ConfigAttribute (ConfigAttributeTypeID, vcdbID, value) values (?,?,?)")
			if err != nil {
				return curtConfigAttributeId, err
			}
			res, err := stmt.Exec(curtConfigType, acesId, aaiaValue)
			id, err := res.LastInsertId()
			curtConfigAttributeId = int(id)

		}
		return curtConfigAttributeId, err
	}
	return curtConfigAttributeId, err

}

//ugly stupid way to "map" these
func getAaiaConfigValueFromTypeId(cr ConfigVehicleRaw, aaiaConfigId int) (string, error) {

	switch {
	case aaiaConfigId == 2:
		return string(cr.BodyTypeID), nil
	case aaiaConfigId == 3:
		return string(cr.DriveTypeID), nil
	case aaiaConfigId == 4:
		return string(cr.BodyNumDoorsID), nil
	case aaiaConfigId == 6:
		return string(cr.FuelTypeID), nil
	case aaiaConfigId == 7:
		return string(cr.EngineConfigID), nil
	case aaiaConfigId == 8:
		return string(cr.AspirationID), nil
	case aaiaConfigId == 12:
		return string(cr.CylHeadTypeID), nil
	case aaiaConfigId == 16:
		return string(cr.EngineVinID), nil
	case aaiaConfigId == 20:
		return string(cr.FuelDeliveryID), nil
	case aaiaConfigId == 106:
		return strconv.FormatFloat(cr.AcesLiter, 'b', 2, 10), nil
	case aaiaConfigId == 206:
		return strconv.FormatFloat(cr.AcesCC, 'b', 2, 10), nil
	case aaiaConfigId == 306:
		return string(cr.AcesCID), nil
	case aaiaConfigId == 999:
		return cr.AcesBlockType, nil
	case aaiaConfigId == 25:
		return string(cr.PowerOutputID), nil
	case aaiaConfigId == 20:
		return string(cr.FuelDeliveryID), nil
	case aaiaConfigId == 999:
		return string(cr.BodyStyleConfigID), nil
	case aaiaConfigId == 40:
		return string(cr.ValvesID), nil
	case aaiaConfigId == 12:
		return string(cr.CylHeadTypeID), nil
	case aaiaConfigId == 15:
		return string(cr.EngineBaseID), nil
	case aaiaConfigId == 7:
		return string(cr.EngineConfigID), nil
	default:
		return "", errors.New("No Config")
	}
	return "", errors.New("No Config")
}

// FuelTypeID        uint8   `bson:"fuelTypeId,omitempty"`       // 6 FuelType

//OLD

//for each ConfigVehicleGroup in the array, compare config arrays
func AuditConfigs(configVehicleGroups []ConfigVehicleGroup) error {
	var err error
	initConfigMaps.Do(initConfigMap)

	for _, configVehicleGroup := range configVehicleGroups {
		log.Print("VEHICLE GROUP: ", configVehicleGroup)
		var ok bool

		configsToProcess := make(map[string][]string)

		for i, configs := range configVehicleGroup.ConfigVehicles {
			if _, ok = partMap[configs.PartNumber]; ok {
				if i > 0 { //not the first configVehicle

					if configs.FuelTypeID != configVehicleGroup.ConfigVehicles[i-1].FuelTypeID {
						// fuelType = true
						configsToProcess[configs.PartNumber] = append(configsToProcess[configs.PartNumber], strconv.Itoa(6)+","+strconv.Itoa(int(configs.FuelTypeID)))
					}
					if configs.FuelDeliveryID != configVehicleGroup.ConfigVehicles[i-1].FuelDeliveryID {
						// fuelDeliveryID = true
						configsToProcess[configs.PartNumber] = append(configsToProcess[configs.PartNumber], strconv.Itoa(20)+","+strconv.Itoa(int(configs.FuelDeliveryID)))
					}

					// if configs.AcesCC != configVehicleGroup.ConfigVehicles[i-1].AcesCC {
					// 	acesCC = true
					// 	// configsToProcess = append()
					// }
					// if configs.AcesCID != configVehicleGroup.ConfigVehicles[i-1].AcesCID {
					// 	acesCID = true
					// 	// configsToProcess = append()
					// }

					if configs.AspirationID != configVehicleGroup.ConfigVehicles[i-1].AspirationID {
						// aspirationID = true
						configsToProcess[configs.PartNumber] = append(configsToProcess[configs.PartNumber], strconv.Itoa(8)+","+strconv.Itoa(int(configs.AspirationID)))
					}
					if configs.DriveTypeID != configVehicleGroup.ConfigVehicles[i-1].DriveTypeID {
						// driveTypeID = true
						configsToProcess[configs.PartNumber] = append(configsToProcess[configs.PartNumber], strconv.Itoa(3)+","+strconv.Itoa(int(configs.DriveTypeID)))
					}
					if configs.BodyTypeID != configVehicleGroup.ConfigVehicles[i-1].BodyTypeID {
						// bodyTypeID = true
						configsToProcess[configs.PartNumber] = append(configsToProcess[configs.PartNumber], strconv.Itoa(2)+","+strconv.Itoa(int(configs.BodyTypeID)))
					}
					if configs.BodyNumDoorsID != configVehicleGroup.ConfigVehicles[i-1].BodyNumDoorsID {
						// bodyNumDoorsID = true
						configsToProcess[configs.PartNumber] = append(configsToProcess[configs.PartNumber], strconv.Itoa(4)+","+strconv.Itoa(int(configs.BodyNumDoorsID)))
					}
					if configs.EngineVinID != configVehicleGroup.ConfigVehicles[i-1].EngineVinID {
						// engineVinID = true
						configsToProcess[configs.PartNumber] = append(configsToProcess[configs.PartNumber], strconv.Itoa(16)+","+strconv.Itoa(int(configs.EngineVinID)))
					}
					// if configs.RegionID != configVehicleGroup.ConfigVehicles[i-1].RegionID {
					// 	regionID = true
					// 	// configsToProcess = append()
					// }
					if configs.PowerOutputID != configVehicleGroup.ConfigVehicles[i-1].PowerOutputID {
						// powerOutputID = true
						configsToProcess[configs.PartNumber] = append(configsToProcess[configs.PartNumber], strconv.Itoa(25)+","+strconv.Itoa(int(configs.PowerOutputID)))
					}
					if configs.FuelDelConfigID != configVehicleGroup.ConfigVehicles[i-1].FuelDelConfigID {
						// fuelDelConfigID = true
						configsToProcess[configs.PartNumber] = append(configsToProcess[configs.PartNumber], strconv.Itoa(19)+","+strconv.Itoa(int(configs.FuelDelConfigID)))
					}
					// if configs.BodyStyleConfigID != configVehicleGroup.ConfigVehicles[i-1].BodyStyleConfigID {
					// 	bodyStyleConfigID = true
					// 	configsToProcess = append()
					// }
					if configs.ValvesID != configVehicleGroup.ConfigVehicles[i-1].ValvesID {
						// valvesID = true
						configsToProcess[configs.PartNumber] = append(configsToProcess[configs.PartNumber], strconv.Itoa(40)+","+strconv.Itoa(int(configs.ValvesID)))
					}
					if configs.CylHeadTypeID != configVehicleGroup.ConfigVehicles[i-1].CylHeadTypeID {
						// cylHeadTypeID = true
						configsToProcess[configs.PartNumber] = append(configsToProcess[configs.PartNumber], strconv.Itoa(12)+","+strconv.Itoa(int(configs.CylHeadTypeID)))
					}
					// if configs.BlockType != configVehicleGroup.ConfigVehicles[i-1].BlockType {
					// 	blockType = true
					// 	configsToProcess = append()
					// }
					//TODO
					// if configs.EngineBaseID != configVehicleGroup.ConfigVehicles[i-1].EngineBaseID {
					// 	// engineBaseID = true
					// 	configsToProcess[configs.PartNumber] = append(configsToProcess[configs.PartNumber], strconv.Itoa(13)+","+strconv.Itoa(int(configs.EngineBaseID)))
					// 	configsToProcess[configs.PartNumber] = append(configsToProcess[configs.PartNumber], strconv.Itoa(14)+","+strconv.Itoa(int(configs.EngineBaseID)))
					// }
					// if configs.EngineConfigID != configVehicleGroup.ConfigVehicles[i-1].EngineConfigID {
					// 	// engineConfigID = true
					// 	configsToProcess[configs.PartNumber] = append(configsToProcess[configs.PartNumber], strconv.Itoa(13)+","+strconv.Itoa(int(configs.EngineConfigID)))
					// 	configsToProcess[configs.PartNumber] = append(configsToProcess[configs.PartNumber], strconv.Itoa(14)+","+strconv.Itoa(int(configs.EngineConfigID)))
					// 	configsToProcess[configs.PartNumber] = append(configsToProcess[configs.PartNumber], strconv.Itoa(15)+","+strconv.Itoa(int(configs.EngineConfigID)))
					// }
					// if configs.AcesCyl != configVehicleGroup.ConfigVehicles[i-1].AcesCyl || configs.AcesBlockType != configVehicleGroup.ConfigVehicles[i-1].AcesBlockType || configs.AcesLiter != configVehicleGroup.ConfigVehicles[i-1].AcesLiter {
					// 	// engineConfigID = true
					// 	configsToProcess[configs.PartNumber] = append(configsToProcess[configs.PartNumber], strconv.Itoa(7)+","+strconv.Itoa(int(configs.AcesCyl)))
					// 	configsToProcess[configs.PartNumber] = append(configsToProcess[configs.PartNumber], strconv.Itoa(7)+","+configs.AcesBlockType)
					// 	configsToProcess[configs.PartNumber] = append(configsToProcess[configs.PartNumber], strconv.Itoa(7)+","+strconv.Itoa(int(configs.AcesLiter)))
					// }

				} //end not-the-first configVehicle
			} else { //oldPartNumber not in db - write to file
				b := []byte(configs.PartNumber + "\n")
				n, err := missingPartNumbers.WriteAt(b, missingPartNumbersOffset)
				if err != nil {
					return err
				}
				missingPartNumbersOffset += int64(n)
				continue
			}

		} //end loop of vehicleGroup configs

		//remove duplicates
		for i, partsConfigs := range configsToProcess {
			sort.Strings(partsConfigs)
			var tempList []string
			tempList = append(tempList, partsConfigs[0])
			for j, con := range partsConfigs {
				if j != 0 {
					if con != partsConfigs[j-1] {
						tempList = append(tempList, con)
					}
				}
			}
			configsToProcess[i] = tempList
		}

		//process configs
		log.Print("FOR ", configVehicleGroup.VehicleID, configsToProcess)
		err = ProcessConfigs(&configVehicleGroup, configsToProcess)

	} //end of spot-checking each attribute

	// //remove duplicates
	err = RemoveDuplicates("exports/MissingPartNumbers_Configs.csv")

	log.Print("MADE IT TO THE END. ", err)
	return err
}

//TODO replace audit
// is there a vehicle with these x configs?
// Y: is there a part join? N: add part Y: continue
// N: are there these configattributes?
// Y: {create joins in vehicleconfigattribute, create vehicle, add part}
// N: Are there the configattributes' configattributetypes?
// Y: {create configattributes,create joins in vehicleconfigattribute, create vehicle, add part}
// N: --there should be every config attribute. some are combos

//New
//for each config type
//1. is there a Curt config type?
//N: Create curt type from vcdb
//2. is there a Curt config (value)?
//N. Create a Curt Value from vcdb
//3. Is there a Curt vehicle with these Configs?
//N: Create curt vehicle
//4. Is there a Curt PartID for this partnumber?
//N: log the missing part; break
//5. Is there a part join for the part associated with these configs?
//N: Create vcdb_VehiclePart join

//Processes configs for each configVehicleGroup. Takes the configVehicleGroup and a map of partnumber:[]configs to differentiate on.
func ProcessConfigs(configVehicleGroup *ConfigVehicleGroup, configsToProcess map[string][]string) error {
	//configVehicleGroup has aaiaBaseID, aaiaSubmodelID, vehicleID
	//configsToProcess map is map for the above vehicle of partNumber []aaiaConfigType:aaiaConfigValue
	var err error
	initConfigMaps.Do(initConfigMap)

	//7 is weird - engine
	for partNumber, cons := range configsToProcess {
		var configAttributeArray []int //array of config attributes associated with this part
		for _, aaiaCon := range cons {
			aaiaConTypeID, err := strconv.Atoi(strings.Split(aaiaCon, ",")[0])
			if err != nil {
				return err
			}
			aaiaConValID, err := strconv.Atoi(strings.Split(aaiaCon, ",")[1])
			if err != nil {
				return err
			}

			log.Print("AAIA Type: ", aaiaConTypeID, ", AAIA val:", aaiaConValID, " part-", partNumber)

			//#1
			curtConfigType := configAttributeTypeMap[aaiaConTypeID]
			if curtConfigType == 0 {
				log.Print("Missing Type :", aaiaConTypeID)
				//need configType
				log.Panic("Missing type")
			}

			//#2
			// cTypeAValue := strconv.Itoa(curtConfigType) + ":" + strconv.Itoa(aaiaConValID)

			//can't use map since it's repeatedly updated
			// curtConfigValue := configAttributeMap[cTypeAValue]
			curtConfigValue, err := getCurtConfigValue(curtConfigType, aaiaConValID)
			if err != nil {
				if err == sql.ErrNoRows {
					if curtConfigValue == 0 {
						log.Print("Missing Type Value, curttype: ", aaiaConTypeID, " ", aaiaConValID, " ", curtConfigType)
						//need configValue
						curtConfigValue, err = insertCurtConfig(aaiaConTypeID, aaiaConValID, curtConfigType)
						if err != nil {
							log.Panic(err)
							return err
						}
						log.Print("Created Value: ", curtConfigValue)
					}
				} else {
					return err
				}
			}

			configAttributeArray = append(configAttributeArray, curtConfigValue)

			// //#1 & #2- is there a CurtConfigType and Value
			// curtCon := configMap[aaiaCon]

			// log.Print("CURT CONFIG: ", curtCon)
			// // curtConTypeID, err := strconv.Atoi(strings.Split(curtCon, ",")[0])
			// // if err != nil {
			// // 	return err
			// // }
			// var curtConValID int
			// curtConSplitArray := strings.Split(curtCon, ",")
			// if len(curtConSplitArray) > 1 {
			// 	curtConValID, err = strconv.Atoi(curtConSplitArray[1])
			// 	if err != nil {
			// 		return err
			// 	}
			// } else {
			// 	//TODO - there are no ConfigAttiributes for this aaia atribute yet  7,13,14,15
			// }
			// configAttributeArray = append(configAttributeArray, curtConValID)

			// //find vehicleJoin

			// // err = CheckVehicleJoin(configVehicleGroup.BaseID, configVehicleGroup.SubID, curtConTypeID, partNumber)
			// // if err == sql.ErrNoRows {
			// // 	//first 'NO'
			// // 	if curtCon != "" {
			// // 		//create joins in vca, vehicle, vehiclepart
			// // 	} else {
			// // 		//create ca, vca, vehcile vehiclepart
			// // 	}

			// // }

		} //end config loop
		//#3
		err = FindVehicleWithAttributes(configVehicleGroup.BaseID, configVehicleGroup.SubID, partNumber, configAttributeArray)
		if err != nil {
			log.Print(err)
			return err
		}

	} //end Part Number loop

	return err
}

func FindVehicleWithAttributes(cBaseID int, cSubmodelID int, partNumber string, configAttributeArray []int) error {
	//build goddamn query
	//find vehicle with these attri
	sqlStmt := `select  v.ID from vcdb_Vehicle as v
		join BaseVehicle as b on b.ID = v.BaseVehicleID
		join Submodel as s on s.ID = v.SubmodelID
		join VehicleConfigAttribute as vca on vca.VehicleConfigID = v.ConfigID
		where b.AAIABaseVehicleID = ?
		and s.AAIASubmodelID = ?
		and vca.VehicleConfigID in
		(select vca.VehicleConfigID from VehicleConfigAttribute as vca
		where vca.AttributeID = ` + strconv.Itoa(configAttributeArray[0]) + `) `

	for i := 0; i < len(configAttributeArray); i++ {
		if configAttributeArray[i] != 0 {
			sqlStmt += ` and vca.VehicleConfigID in
		(select vca.VehicleConfigID from VehicleConfigAttribute as vca
		where  vca.AttributeID = ` + strconv.Itoa(configAttributeArray[i]) + `) `
		}
	}

	db, err := sql.Open("mysql", database.ConnectionString())
	if err != nil {
		return err
	}
	defer db.Close()

	stmt, err := db.Prepare(sqlStmt)
	if err != nil {
		return err
	}
	defer stmt.Close()
	var vId int
	err = stmt.QueryRow(cBaseID, cSubmodelID).Scan(&vId)
	log.Print("vId and err ", vId, err)
	if err != nil {
		log.Print("NEED VEHICLE")
		if err == sql.ErrNoRows {
			//no matching vehicle, must create
			err = CreateVehicleConfigAttributes(cBaseID, cSubmodelID, partNumber, configAttributeArray)
			if err != nil {
				log.Print(err)
				return err
			}

		}
		return err
	} else {
		log.Print("VEHICLE FOUND, CHECKING PARTS")
		//insert vehiclePart if no match
		findPartStmt := "select ID from vcdb_VehiclePart where VehicleID = ? and PartNumber = ?"
		stmt, err = db.Prepare(findPartStmt)
		if err != nil {
			return err
		}
		var successVPid int
		err = stmt.QueryRow(vId, partNumber).Scan(&successVPid)
		if err != nil {
			if err == sql.ErrNoRows {
				//insert vp
				stmt, err = db.Prepare(insertVehiclePartStmt)
				if err != nil {
					return err
				}
				_, err = stmt.Exec(vId, partNumber)
			}
			return err //actual error
		}
		log.Print("VEHICLEPART FOUND - Part ", partNumber, " exists for ", cBaseID, cSubmodelID)
		//end find and/or insert
		return err
	}

	return err
}

func getCurtConfigValue(curtConfigTypeId, aaiaConfigValueId int) (int, error) {
	var err error
	var curtConfigValueId int
	db, err := sql.Open("mysql", database.ConnectionString())
	if err != nil {
		return curtConfigValueId, err
	}
	defer db.Close()

	stmt, err := db.Prepare(getCurtConfigValueIdStmt)
	if err != nil {
		return curtConfigValueId, err
	}
	defer stmt.Close()
	err = stmt.QueryRow(curtConfigTypeId, aaiaConfigValueId).Scan(&curtConfigValueId)
	if err != nil {
		return curtConfigValueId, err
	}
	return curtConfigValueId, err
}

//insert into ConfigAttribute(ConfigAttributeTypeID, parentID, vcdbID, value) values(?,0,?,?)
func insertCurtConfig(aaiaConfigType, aaiaConfigValue, curtConfigTypeId int) (int, error) {
	var err error
	var curtConfigValueId int
	var value string

	//get config value (from aaia)
	value, err = getAcesConfigurationValueName(aaiaConfigType, aaiaConfigValue)
	if err != nil {
		return curtConfigValueId, err
	}

	db, err := sql.Open("mysql", database.ConnectionString())
	if err != nil {
		return curtConfigValueId, err
	}
	defer db.Close()

	stmt, err := db.Prepare(insertCurtConfigStmt)
	if err != nil {
		return curtConfigValueId, err
	}
	defer stmt.Close()
	res, err := stmt.Exec(curtConfigTypeId, aaiaConfigValue, value)
	if err != nil {
		return curtConfigValueId, err
	}
	id, err := res.LastInsertId()
	curtConfigValueId = int(id)
	return curtConfigValueId, err
}

func getAcesConfigurationValueName(aaiaConfigTypeID, id int) (string, error) {
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
		idField = table + "ID"
		valueField = table + "Name"
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
	case aaiaConfigTypeID == 39:
		table = "TransmissionType"
		idField = table + "ID"
		valueField = table + "Name"
	case aaiaConfigTypeID == 40:
		table = "Valves"
		idField = table + "ID"
		valueField = table + "PerEngine"
	default:
		log.Panic("Missing Curt Config Type")

	}

	var valueName string
	sqlStmt := "select " + valueField + " from " + table + " where " + idField + " = " + strconv.Itoa(id)
	log.Print("stmt ", sqlStmt)
	db, err := sql.Open("mysql", database.VcdbConnectionString())
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
		if err == sql.ErrNoRows {
			//TODO list the weirdest lacks of configs
			err = nil
		}
		return valueName, err
	}
	return valueName, err
}

func getAcesConfigurationID(aaiaConfigTypeID int, value string) (string, error) {
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
		idField = table + "ID"
		valueField = table + "Name"
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
	case aaiaConfigTypeID == 39:
		table = "TransmissionType"
		idField = table + "ID"
		valueField = table + "Name"
	case aaiaConfigTypeID == 40:
		table = "Valves"
		idField = table + "ID"
		valueField = table + "PerEngine"
	default:
		log.Panic("Missing Curt Config Type")

	}

	var valueName string
	sqlStmt := "select " + idField + "ID  from " + table + " where " + valueField + " = " + value
	log.Print("stmt ", sqlStmt)
	db, err := sql.Open("mysql", database.VcdbConnectionString())
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
		if err == sql.ErrNoRows {
			//TODO list the weirdest lacks of configs
			err = nil
		}
		return valueName, err
	}
	return valueName, err
}

func CreateVehicleConfigAttributes(cBaseID int, cSubmodelID int, partNumber string, configAttributeArray []int) error {
	db, err := sql.Open("mysql", database.ConnectionString())
	if err != nil {
		return err
	}
	defer db.Close()
	log.Print("insert vehicleConfig")
	//new vehicleConfig
	stmt, err := db.Prepare(insertVehicleConfigStmt)
	if err != nil {
		return err
	}
	res, err := stmt.Exec()
	if err != nil {
		return err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return err
	}
	vConfigId := int(id)

	log.Print("insert vehicle. baseID: ", cBaseID, "   sub: ", cSubmodelID, "   confg: ", vConfigId)
	//insert new vehicle first
	//TODO - missing base or submodel?
	vehicleInsertStmt := `insert into vcdb_Vehicle (BaseVehicleID, SubModelID, ConfigID, AppID) values ((select ID from BaseVehicle where AAIABaseVehicleID = ?), (select ID from Submodel where AAIASubmodelID = ?),?,0)`
	stmt, err = db.Prepare(vehicleInsertStmt)
	if err != nil {
		return err
	}
	res, err = stmt.Exec(cBaseID, cSubmodelID, vConfigId)
	if err != nil {
		return err
	}
	id, err = res.LastInsertId()
	if err != nil {
		return err
	}
	vId := int(id)

	//insert vehicleConfigAttribute
	//TODO - fix this
	sqlStmt := `insert into VehicleConfigAttribute (AttributeID, VehicleConfigID) values `
	for i := 1; i < len(configAttributeArray); i++ {
		// sqlStmt += sqlAddOns
		if configAttributeArray[i] != 0 {
			log.Print(configAttributeArray[i], "__", vConfigId)
			sqlStmt += `(` + strconv.Itoa(configAttributeArray[i]) + `,` + strconv.Itoa(vConfigId) + `),`
		}
	}

	sqlStmt = strings.TrimRight(sqlStmt, ",")

	log.Print("insert vehicleConfigAttributes", sqlStmt)
	stmt, err = db.Prepare(sqlStmt)
	if err != nil {
		log.Print("STMT ERR ", err)
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec()
	if err != nil {
		return err
	}
	log.Print("HERE - insert vehicle")
	err = InsertVehiclePart(vId, partNumber)
	if err != nil {
		return err
	}
	return err
}

func InsertVehiclePart(vId int, partNum string) error {
	log.Print("INsert Vehicle")
	db, err := sql.Open("mysql", database.ConnectionString())
	if err != nil {
		return err
	}
	defer db.Close()

	stmt, err := db.Prepare(insertVehiclePartStmt)
	if err != nil {
		return err
	}
	defer stmt.Close()
	_, err = stmt.Exec(vId, partNum)
	if err != nil {
		return err
	}
	return err
}

func CheckVehicleConfigMap(typeID, baseID, subID int, vehicleJoinMap map[string]string) (int, int, error) {
	var err error
	var vId, vConId int
	var baseSubAttr, vehicleVehConfig string

	strArray := []string{strconv.Itoa(baseID), strconv.Itoa(subID), strconv.Itoa(typeID)}

	baseSubAttr = strings.Join(strArray, ",")

	vehicleVehConfig = vehicleJoinMap[baseSubAttr]
	if vehicleVehConfig == "" {
		err = errors.New("novehicleconfig")
		return 0, 0, err
	}
	// var vArray []string
	vArray := strings.Split(vehicleVehConfig, ",")
	vId, err = strconv.Atoi(vArray[0])
	if err != nil {
		return 0, 0, err
	}
	vConId, err = strconv.Atoi(vArray[1])
	if err != nil {
		return 0, 0, err
	}
	return vId, vConId, nil
}

//maps acesTypeID,acesValID:typeID, valID
//Used to check for the existence of curt configAttributeType ID and configAttribute ID  using AAIA config type and attribute
func GetConfigMap() (map[string]string, error) {
	var err error
	configMap := make(map[string]string)
	db, err := sql.Open("mysql", database.ConnectionString())
	if err != nil {
		return configMap, err
	}
	defer db.Close()

	stmt, err := db.Prepare(configMapStmt)
	if err != nil {
		return configMap, err
	}
	defer stmt.Close()
	var typeID, acesTypeID, acesValID, valID *int
	var k, v string
	res, err := stmt.Query()
	for res.Next() {
		err = res.Scan(&typeID, &acesTypeID, &acesValID, &valID)
		if err != nil {
			return configMap, err
		}
		if *acesTypeID > 0 && *acesValID > 0 {
			k = strconv.Itoa(*acesTypeID) + "," + strconv.Itoa(*acesValID)
			v = strconv.Itoa(*typeID) + "," + strconv.Itoa(*valID)
			configMap[k] = v
		}

	}
	return configMap, err
}

func CreateVehicleJoinMap() (map[string]string, error) {
	joinMap := make(map[string]string)

	db, err := sql.Open("mysql", database.ConnectionString())
	if err != nil {
		return joinMap, err
	}
	defer db.Close()

	stmt, err := db.Prepare(vehicleJoinMapStmt)
	if err != nil {
		return joinMap, err
	}
	defer stmt.Close()

	res, err := stmt.Query()
	if err != nil {
		return joinMap, err
	}
	var vId, vConId, aaiaBaseId, aaiaSubId, attrId int
	var baseSubAttr, vehicleVehConfig string
	for res.Next() {
		err = res.Scan(&vId, &vConId, &aaiaBaseId, &aaiaSubId, &attrId)
		if err != nil {
			return joinMap, err
		}
		strArray := []string{strconv.Itoa(aaiaBaseId), strconv.Itoa(aaiaSubId), strconv.Itoa(attrId)}
		baseSubAttr = strings.Join(strArray, ",")

		vArray := []string{strconv.Itoa(vId), strconv.Itoa(vConId)}
		vehicleVehConfig = strings.Join(vArray, ",")

		joinMap[baseSubAttr] = vehicleVehConfig
	}
	return joinMap, err
}
