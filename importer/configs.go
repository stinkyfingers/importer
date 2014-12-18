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
)

var (
	ConfigTypesOffset      int64 = 0
	ConfigOffset           int64 = 0
	VehicleConfigOffset    int64 = 0
	configPartNeededOffset int64 = 0
)

var acesTypeCurtTypeMap map[int]int
var configMap map[string]string
var initMaps sync.Once

func initMap() {
	configMap, _ = GetConfigMap()
}

//For all mongodb entries, returns BaseVehicleRaws
func MongoToConfig(subIds []int, dbCollection string) ([]ConfigVehicleRaw, error) {
	var err error
	var cgs []ConfigVehicleRaw
	vehiclesDifferentiatedByConfig, err := os.Create("exports/VehiclesDifferentiatedByConfig.csv") //List of every vehicle that we're going to process by config, beforehand
	if err != nil {
		return cgs, err
	}
	off := int64(0)

	session, err := mgo.Dial(database.MongoConnectionString().Addrs[0])
	if err != nil {
		return cgs, err
	}
	defer session.Close()
	collection := session.DB("importer").C(dbCollection)
	err = collection.Find(bson.M{"submodelId": bson.M{"$in": subIds}}).All(&cgs)

	for _, cg := range cgs {
		b := []byte(strconv.Itoa(cg.BaseID) + "," + strconv.Itoa(cg.SubmodelID) + "," + strconv.Itoa(cg.VehicleID) + "," + strconv.Itoa(int(cg.VehicleTypeID)) + "," + strconv.Itoa(int(cg.FuelTypeID)) + "," + strconv.Itoa(int(cg.FuelDeliveryID)) + "," + strconv.Itoa(int(cg.AcesLiter)) + "," + strconv.Itoa(int(cg.AcesCC)) + "," + strconv.Itoa(int(cg.AcesCID)) + "," + strconv.Itoa(int(cg.AcesCyl)) + "," + cg.AcesBlockType + "," + strconv.Itoa(int(cg.AspirationID)) + "," + strconv.Itoa(int(cg.DriveTypeID)) + "," + strconv.Itoa(int(cg.BodyTypeID)) + "," + strconv.Itoa(int(cg.BodyNumDoorsID)) + "," + strconv.Itoa(int(cg.EngineVinID)) + "," + strconv.Itoa(int(cg.RegionID)) + "," + strconv.Itoa(int(cg.PowerOutputID)) + "," + strconv.Itoa(int(cg.FuelDelConfigID)) + "," + strconv.Itoa(int(cg.BodyStyleConfigID)) + "," + strconv.Itoa(int(cg.ValvesID)) + "," + strconv.Itoa(int(cg.CylHeadTypeID)) + "," + cg.BlockType + "," + strconv.Itoa(int(cg.EngineBaseID)) + "," + strconv.Itoa(int(cg.EngineConfigID)) + "," + cg.PartNumber + "\n")
		n, err := vehiclesDifferentiatedByConfig.WriteAt(b, off)
		if err != nil {
			return cgs, err
		}
		off += int64(n)
	}
	err = RemoveDuplicates("exports/VehiclesDifferentiatedByConfig.csv")
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

//for each ConfigVehicleGroup in the array, compare config arrays

func AuditConfigs(configVehicleGroups []ConfigVehicleGroup) error {

	// configMap, err := GetConfigMap()
	// if err != nil {
	// 	return err
	// }
	// vehicleJoinMap, err := CreateVehicleJoinMap()
	// if err != nil {
	// 	return err
	// }

	// MissingConfigTypes, _, err := createMissingConfigTypesFile()
	// MissingConfigs, _, err := createMissingConfigsFile()
	// MissingVehicleConfigs, _, err := createMissingVehicleConfigurationsFile()
	// if err != nil {
	// 	return err
	// }

	config_PartsNeeded, err := os.Create("Config_PartsNeeded.txt")
	if err != nil {
		return err
	}
	b := []byte("insert into vcdb_VehiclePart (VehicleID, PartNumber) values ")
	n, err := config_PartsNeeded.WriteAt(b, int64(0))
	if err != nil {
		return err
	}
	configPartNeededOffset += int64(n)

	for _, configVehicleGroup := range configVehicleGroups {
		log.Print("__", configVehicleGroup)
		// fuelType := false
		// fuelDeliveryID := false
		// acesLiter := false
		// acesCC := false
		// acesCID := false
		// acesCyl := false
		// acesBlockType := false
		// aspirationID := false
		// driveTypeID := false
		// bodyTypeID := false
		// bodyNumDoorsID := false
		// engineVinID := false
		// regionID := false
		// powerOutputID := false
		// fuelDelConfigID := false
		// bodyStyleConfigID := false
		// valvesID := false
		// cylHeadTypeID := false
		// blockType := false
		// engineBaseID := false
		// engineConfigID := false

		configsToProcess := make(map[string][]string)

		for i, configs := range configVehicleGroup.ConfigVehicles {
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
				if configs.EngineBaseID != configVehicleGroup.ConfigVehicles[i-1].EngineBaseID {
					// engineBaseID = true
					configsToProcess[configs.PartNumber] = append(configsToProcess[configs.PartNumber], strconv.Itoa(13)+","+strconv.Itoa(int(configs.EngineBaseID)))
					configsToProcess[configs.PartNumber] = append(configsToProcess[configs.PartNumber], strconv.Itoa(14)+","+strconv.Itoa(int(configs.EngineBaseID)))
				}
				if configs.EngineConfigID != configVehicleGroup.ConfigVehicles[i-1].EngineConfigID {
					// engineConfigID = true
					configsToProcess[configs.PartNumber] = append(configsToProcess[configs.PartNumber], strconv.Itoa(13)+","+strconv.Itoa(int(configs.EngineConfigID)))
					configsToProcess[configs.PartNumber] = append(configsToProcess[configs.PartNumber], strconv.Itoa(14)+","+strconv.Itoa(int(configs.EngineConfigID)))
					configsToProcess[configs.PartNumber] = append(configsToProcess[configs.PartNumber], strconv.Itoa(15)+","+strconv.Itoa(int(configs.EngineConfigID)))
				}
				if configs.AcesCyl != configVehicleGroup.ConfigVehicles[i-1].AcesCyl || configs.AcesBlockType != configVehicleGroup.ConfigVehicles[i-1].AcesBlockType || configs.AcesLiter != configVehicleGroup.ConfigVehicles[i-1].AcesLiter {
					// engineConfigID = true
					configsToProcess[configs.PartNumber] = append(configsToProcess[configs.PartNumber], strconv.Itoa(7)+","+strconv.Itoa(int(configs.AcesCyl)))
					configsToProcess[configs.PartNumber] = append(configsToProcess[configs.PartNumber], strconv.Itoa(7)+","+configs.AcesBlockType)
					configsToProcess[configs.PartNumber] = append(configsToProcess[configs.PartNumber], strconv.Itoa(7)+","+strconv.Itoa(int(configs.AcesLiter)))
				}

			} //end not-the-first configVehicle

		} //end loop of vehicleGroup configs
		log.Print("FOR ", configVehicleGroup.VehicleID, configsToProcess)
		err = ProcessConfigs(&configVehicleGroup, configsToProcess)

		// 	initMaps.Do(initMap)

		// 	//fueltype
		// 	if fuelType == true {
		// 		log.Print("fuelType")
		// 		//get config attribute ID
		// 		acesType := 6
		// 		// for _, c := range configVehicleGroup.ConfigVehicles {
		// 		// 	curtTypeAndVal := configMap[strconv.Itoa(acesType)+","+strconv.Itoa(int(c.FuelTypeID))]
		// 		// 	curtAttr := strings.Split(curtTypeAndVal, ",")[0]
		// 		// 	log.Print(curtAttr, " ", acesType, " ", c.FuelTypeID)
		// 		// 	//check vehicle join
		// 		// 	err = CheckVehicleJoin(c.BaseID, c.SubmodelID, curtAttr, c.PartNumber)
		// 		// }

		// 		for _, c := range configVehicleGroup.ConfigVehicles {
		// 			acesValue := int(c.FuelTypeID)
		// 			err = auditConfigs(acesType, acesValue, configMap, vehicleJoinMap, c, MissingVehicleConfigs, MissingConfigs, config_PartsNeeded)
		// 			if err != nil {
		// 				log.Print(err)
		// 				return err
		// 			}
		// 		}
		// 	}

		// 	//fueldelivery
		// 	if fuelDeliveryID == true {
		// 		log.Print("fuelDeliveryID ")
		// 		acesType := 20
		// 		for _, c := range configVehicleGroup.ConfigVehicles {
		// 			acesValue := int(c.FuelDeliveryID)
		// 			err = auditConfigs(acesType, acesValue, configMap, vehicleJoinMap, c, MissingVehicleConfigs, MissingConfigs, config_PartsNeeded)
		// 			if err != nil {
		// 				log.Print(err)
		// 				return err
		// 			}
		// 		}
		// 	}
		// 	//aspiration
		// 	if aspirationID == true {
		// 		log.Print("aspirationID")
		// 		acesType := 8
		// 		for _, c := range configVehicleGroup.ConfigVehicles {
		// 			acesValue := int(c.AspirationID)
		// 			log.Print("ASP", acesValue)
		// 			err = auditConfigs(acesType, acesValue, configMap, vehicleJoinMap, c, MissingVehicleConfigs, MissingConfigs, config_PartsNeeded)
		// 			if err != nil {
		// 				log.Print(err)
		// 				return err
		// 			}
		// 		}
		// 	}
		// 	//drive type
		// 	if driveTypeID == true {
		// 		log.Print("driveTypeID")
		// 		acesType := 3
		// 		for _, c := range configVehicleGroup.ConfigVehicles {
		// 			acesValue := int(c.DriveTypeID)
		// 			log.Print("ASP", acesValue)
		// 			err = auditConfigs(acesType, acesValue, configMap, vehicleJoinMap, c, MissingVehicleConfigs, MissingConfigs, config_PartsNeeded)
		// 			if err != nil {
		// 				log.Print(err)
		// 				return err
		// 			}
		// 		}
		// 	}
		// 	//body type
		// 	if bodyTypeID == true {
		// 		log.Print("bodyTypeID")
		// 		acesType := 2
		// 		for _, c := range configVehicleGroup.ConfigVehicles {
		// 			acesValue := int(c.BodyTypeID)
		// 			log.Print("ASP", acesValue)
		// 			err = auditConfigs(acesType, acesValue, configMap, vehicleJoinMap, c, MissingVehicleConfigs, MissingConfigs, config_PartsNeeded)
		// 			if err != nil {
		// 				log.Print(err)
		// 				return err
		// 			}
		// 		}
		// 	}
		// 	//body num doors
		// 	if bodyNumDoorsID == true {
		// 		log.Print("bodyNumDoorsID")
		// 		acesType := 4
		// 		for _, c := range configVehicleGroup.ConfigVehicles {
		// 			acesValue := int(c.BodyNumDoorsID)
		// 			err = auditConfigs(acesType, acesValue, configMap, vehicleJoinMap, c, MissingVehicleConfigs, MissingConfigs, config_PartsNeeded)
		// 			if err != nil {
		// 				log.Print(err)
		// 				return err
		// 			}
		// 		}
		// 	}
		// 	//engine vin
		// 	if engineVinID == true {
		// 		log.Print("engineVinID")
		// 		acesType := 16
		// 		for _, c := range configVehicleGroup.ConfigVehicles {
		// 			acesValue := int(c.EngineVinID)
		// 			err = auditConfigs(acesType, acesValue, configMap, vehicleJoinMap, c, MissingVehicleConfigs, MissingConfigs, config_PartsNeeded)
		// 			if err != nil {
		// 				log.Print(err)
		// 				return err
		// 			}
		// 		}
		// 	}
		// 	//power output
		// 	if powerOutputID == true {
		// 		log.Print("powerOutputID")
		// 		acesType := 25
		// 		for _, c := range configVehicleGroup.ConfigVehicles {
		// 			acesValue := int(c.PowerOutputID)
		// 			err = auditConfigs(acesType, acesValue, configMap, vehicleJoinMap, c, MissingVehicleConfigs, MissingConfigs, config_PartsNeeded)
		// 			if err != nil {
		// 				log.Print(err)
		// 				return err
		// 			}
		// 		}
		// 	}
		// 	//fuel del - TODO is this that same as subtype
		// 	if fuelDelConfigID == true {
		// 		log.Print("fuelDelConfigID")
		// 		acesType := 19
		// 		for _, c := range configVehicleGroup.ConfigVehicles {
		// 			acesValue := int(c.FuelDelConfigID)
		// 			err = auditConfigs(acesType, acesValue, configMap, vehicleJoinMap, c, MissingVehicleConfigs, MissingConfigs, config_PartsNeeded)
		// 			if err != nil {
		// 				log.Print(err)
		// 				return err
		// 			}
		// 		}
		// 	}
		// 	//valves
		// 	if valvesID == true {
		// 		log.Print("valvesID")
		// 		acesType := 40
		// 		for _, c := range configVehicleGroup.ConfigVehicles {
		// 			acesValue := int(c.ValvesID)
		// 			err = auditConfigs(acesType, acesValue, configMap, vehicleJoinMap, c, MissingVehicleConfigs, MissingConfigs, config_PartsNeeded)
		// 			if err != nil {
		// 				log.Print(err)
		// 				return err
		// 			}
		// 		}
		// 	}
		// 	//cyl head type
		// 	if cylHeadTypeID == true {
		// 		log.Print("cylHeadTypeID")
		// 		acesType := 12
		// 		for _, c := range configVehicleGroup.ConfigVehicles {
		// 			acesValue := int(c.CylHeadTypeID)
		// 			err = auditConfigs(acesType, acesValue, configMap, vehicleJoinMap, c, MissingVehicleConfigs, MissingConfigs, config_PartsNeeded)
		// 			if err != nil {
		// 				log.Print(err)
		// 				return err
		// 			}
		// 		}
		// 	}
		// 	// //engine base - TODO - is this "Engine?"
		// 	if engineBaseID == true {
		// 		log.Print("engineBaseID")
		// 		acesType := 7
		// 		for _, c := range configVehicleGroup.ConfigVehicles {
		// 			acesValue := int(c.EngineBaseID)
		// 			err = auditConfigs(acesType, acesValue, configMap, vehicleJoinMap, c, MissingVehicleConfigs, MissingConfigs, config_PartsNeeded)
		// 			if err != nil {
		// 				log.Print(err)
		// 				return err
		// 			}
		// 		}
		// 	}

		// 	if acesLiter == true {
		// 		log.Print("Liters")
		// 		acesType := 7
		// 		for _, c := range configVehicleGroup.ConfigVehicles {
		// 			acesValue := int(c.AcesLiter)
		// 			err = auditConfigs(acesType, acesValue, configMap, vehicleJoinMap, c, MissingVehicleConfigs, MissingConfigs, config_PartsNeeded)
		// 			if err != nil {
		// 				log.Print(err)
		// 				return err
		// 			}
		// 		}
		// 	}

		// 	// if acesLiter == true || acesCyl == true || acesBlockType == true {
		// 	// 	log.Print("engine base")
		// 	// 	acesType := 7
		// 	// 	for _, c := range configVehicleGroup.ConfigVehicles {
		// 	// 		acesValue := strconv.FormatFloat(c.AcesLiter, 'f', 1, 64) + " Liter" + c.AcesBlockType + "_" + strconv.Itoa(int(c.AcesCyl))

		// 	// 		err = auditConfigs(acesType, acesValue, configMap, vehicleJoinMap, c, MissingVehicleConfigs, MissingConfigs, config_PartsNeeded)
		// 	// 		if err != nil {
		// 	// 			log.Print(err)
		// 	// 			return err
		// 	// 		}
		// 	// 	}
		// 	// }

		// 	//NON - CURT->ACES CONFIGS
		// 	// if acesLiter == true {
		// 	// 	b := []byte("acesLiter\n")
		// 	// 	n, err := MissingConfigTypes.WriteAt(b, ConfigTypesOffset)
		// 	// 	if err != nil {
		// 	// 		return err
		// 	// 	}
		// 	// 	ConfigTypesOffset += int64(n)
		// 	// }
		// 	if acesCC == true {
		// 		b := []byte("acesCC\n")
		// 		n, err := MissingConfigTypes.WriteAt(b, ConfigTypesOffset)
		// 		if err != nil {
		// 			return err
		// 		}
		// 		ConfigTypesOffset += int64(n)
		// 	}
		// 	if acesCID == true {
		// 		b := []byte("acesCID\n")
		// 		n, err := MissingConfigTypes.WriteAt(b, ConfigTypesOffset)
		// 		if err != nil {
		// 			return err
		// 		}
		// 		ConfigTypesOffset += int64(n)
		// 	}
		// 	if acesCyl == true {
		// 		b := []byte("acesCyl\n")
		// 		n, err := MissingConfigTypes.WriteAt(b, ConfigTypesOffset)
		// 		if err != nil {
		// 			return err
		// 		}
		// 		ConfigTypesOffset += int64(n)
		// 	}
		// 	if acesBlockType == true {
		// 		b := []byte("acesBlockType\n")
		// 		n, err := MissingConfigTypes.WriteAt(b, ConfigTypesOffset)
		// 		if err != nil {
		// 			return err
		// 		}
		// 		ConfigTypesOffset += int64(n)
		// 	}
		// 	if regionID == true {
		// 		b := []byte("regionID\n")
		// 		n, err := MissingConfigTypes.WriteAt(b, ConfigTypesOffset)
		// 		if err != nil {
		// 			return err
		// 		}
		// 		ConfigTypesOffset += int64(n)
		// 	}
		// 	if bodyStyleConfigID == true {
		// 		b := []byte("bodyStyleConfigID\n")
		// 		n, err := MissingConfigTypes.WriteAt(b, ConfigTypesOffset)
		// 		if err != nil {
		// 			return err
		// 		}
		// 		ConfigTypesOffset += int64(n)
		// 	}
		// 	if blockType == true {
		// 		b := []byte("blockType\n")
		// 		n, err := MissingConfigTypes.WriteAt(b, ConfigTypesOffset)
		// 		if err != nil {
		// 			return err
		// 		}
		// 		ConfigTypesOffset += int64(n)
		// 	}
		// 	if engineConfigID == true {
		// 		b := []byte("engineConfigID\n")
		// 		n, err := MissingConfigTypes.WriteAt(b, ConfigTypesOffset)
		// 		if err != nil {
		// 			return err
		// 		}
		// 		ConfigTypesOffset += int64(n)
		// 	}

	} //end of spot-checking each attribute

	// //remove duplicates
	// err = RemoveDuplicates("exports/MissingConfigTypes.csv")
	// err = RemoveDuplicates("exports/MissingConfigs.csv")
	// err = RemoveDuplicates("exports/VehicleConfigurationsNeeded.csv")

	log.Print("MADE IT TO THE END. ", err)
	return err
}

//TODO replace audit
// is there a vehicle with these x configs?
// Y: add part
// N: are there these configattributes?
// Y: {create joins in vehicleconfigattribute, create vehicle, add part}
// N: Are there the configattributes' configattributetypes?
// Y: {create configattributes,create joins in vehicleconfigattribute, create vehicle, add part}
// N: --there should be every config attribute. some are combos

func ProcessConfigs(configVehicleGroup *ConfigVehicleGroup, configsToProcess map[string][]string) error {
	//configVehicleGroup has aaiaBaseID, aaiaSubmodelID, vehicleID
	//configsToProcess map is map for the above vehicle of partNumber []aaiaConfigType:aaiaConfigValue
	var err error
	initMaps.Do(initMap)

	//7 is weird - engine
	for partNumber, cons := range configsToProcess {
		for _, aaiaCon := range cons {
			aaiaConTypeID := strings.Split(aaiaCon, ",")[0]
			aaiaConValID := strings.Split(aaiaCon, ",")[1]
			log.Print("CURT ", aaiaConTypeID, ",", aaiaConValID, " part-", partNumber)
			curtCon := configMap[aaiaCon]
			//TODO if "" - like 7 ,13,14,15
			log.Print(curtCon)
			curtConTypeID, err := strconv.Atoi(strings.Split(curtCon, ",")[0])
			if err != nil {
				return err
			}
			//find vehicleJoin
			err = CheckVehicleJoin(configVehicleGroup.BaseID, configVehicleGroup.SubID, curtConTypeID, partNumber)
			if err == sql.ErrNoRows {
				//first 'NO'
				if curtCon != "" {
					//create joins in vca, vehicle, vehiclepart
				} else {
					//create ca, vca, vehcile vehiclepart
				}

			}
			log.Print(err)
		}
	}

	return err
}

func CheckVehicleJoin(cBaseID int, cSubmodelID int, curtAttr int, partNumber string) error {
	db, err := sql.Open("mysql", database.ConnectionString())
	if err != nil {
		return err
	}
	defer db.Close()

	stmt, err := db.Prepare(checkVehicleJoin)
	if err != nil {
		return err
	}
	defer stmt.Close()
	var vId, vcId int
	err = stmt.QueryRow(cBaseID, cSubmodelID, curtAttr).Scan(&vId, &vcId)
	if err != nil {
		if err == sql.ErrNoRows {
			return err
		}
	}
	//join part
	stmt, err = db.Prepare("insert into vcdb_VehiclePart (VehicleID, PartNumber) values (?,(select partID from Part where oldPartNumber = ?))")
	if err != nil {
		return err
	}
	// _, err = stmt.Exec(vId, partNumber)
	// if err != nil {
	// 	return err
	// }
	return err
}

func auditConfigs(acesType int, acesValue int, configMap map[string]string, vehicleJoinMap map[string]string, c ConfigVehicleRaw, MissingVehicleConfigs *os.File, MissingConfigs *os.File, config_PartsNeeded *os.File) error {
	var err error
	//search for this configAttribute and configAttributeType. If there are no Curt versions, write the needed aaia configAttibute type and configAttribute to csv
	typeID, valueID, err := checkConfigID(acesValue, acesType, configMap)

	//TODO
	if err != nil {
		if err.Error() == "noconfigs" {
			//MissingConfigs.csv
			b := []byte(strconv.Itoa(c.VehicleID) + "," + strconv.Itoa(c.BaseID) + "," + strconv.Itoa(c.SubmodelID) + "," + c.PartNumber + "," + strconv.Itoa(acesValue) + "," + strconv.Itoa(acesType) + "\n")
			n, err := MissingConfigs.WriteAt(b, ConfigOffset)
			if err != nil {
				log.Print("configAudit err; writing MissingConfigs ", err)
				return err
			}
			ConfigOffset += int64(n)

			//TODO - Instead, create configs,
			return nil
		} else {

			return err
		}

	} else {
		//curt configAttribute and configAttributeType found - check for vehicle and join in vehicleConfigAttribute tables
		//if there are no vehicle/vehicleConfigAttribute join, write this miss to csv
		// vehicleID, vehicleConfigID, err := CheckVehicleConfig(typeID, c.BaseID, c.SubmodelID)
		vehicleID, vehicleConfigID, err := CheckVehicleConfigMap(typeID, c.BaseID, c.SubmodelID, vehicleJoinMap)
		if err != nil {
			if err.Error() == "novehicleconfig" {
				b := []byte(strconv.Itoa(typeID) + "," + strconv.Itoa(valueID) + "," + strconv.Itoa(c.BaseID) + "," + strconv.Itoa(c.SubmodelID) + "\n")
				n, err := MissingVehicleConfigs.WriteAt(b, VehicleConfigOffset)

				if err != nil {
					log.Print("configAudit error; writing MissingVehicleConfigs ", err)
					return err
				}
				VehicleConfigOffset += int64(n)
			} else {
				return err
			}
		} else {
			log.Print(vehicleID, " has config ", vehicleConfigID, " already.")
			//JOIN part
			configPartJoinSql := []byte("(" + strconv.Itoa(vehicleID) + ", (select partID from Part where oldPartNumber = '" + c.PartNumber + "')),\n")
			n, err := config_PartsNeeded.WriteAt(configPartJoinSql, configPartNeededOffset)
			if err != nil {
				return err
			}
			configPartNeededOffset += int64(n)
		}

	}
	return err
}

func createMissingConfigTypesFile() (*os.File, int64, error) {
	//files - missing configTypes - there is no Curt ConfigAttributeType for this AAIAConfigType, but vehicles are differenetiated by it
	missingConfigTypes, err := os.Create("exports/MissingConfigTypes.csv")
	if err != nil {
		return missingConfigTypes, 0, err
	}
	// configTypesOffset := int64(0)
	h := []byte("AAIAConfigTypeID\n")
	n, err := missingConfigTypes.WriteAt(h, ConfigTypesOffset)
	if err != nil {
		return missingConfigTypes, ConfigTypesOffset, err
	}
	ConfigTypesOffset += int64(n)
	return missingConfigTypes, ConfigTypesOffset, err
}

func createMissingConfigsFile() (*os.File, int64, error) {
	//files - missing aces configs - there is a curt ConfigType, but no curt configValue corresponding to the AAIAConfig value
	missingConfigs, err := os.Create("exports/MissingConfigs.csv")
	if err != nil {
		return missingConfigs, 0, err
	}
	// configOffset := int64(0)
	h := []byte("AAIAVehicleID,AAIABaseVehicleID,AAIASubmodelID,PartNumber,AAIAConfigID,AAIAConfigTypeID,\n")
	n, err := missingConfigs.WriteAt(h, ConfigOffset)
	if err != nil {
		return missingConfigs, ConfigOffset, err
	}
	ConfigOffset += int64(n)
	return missingConfigs, ConfigOffset, err
}
func createMissingVehicleConfigurationsFile() (*os.File, int64, error) {
	//files - configs needed in VehicleConfigAttribute (join table and vcdb_Vehicle table)
	missingVehicleConfigs, err := os.Create("exports/VehicleConfigurationsNeeded.csv")
	if err != nil {
		return missingVehicleConfigs, 0, err
	}
	// vehicleConfigOffset := int64(0)
	h := []byte("TypeID,ConfigID,AAIABaseID,AAIASubmodelID\n")
	n, err := missingVehicleConfigs.WriteAt(h, VehicleConfigOffset)
	if err != nil {
		return missingVehicleConfigs, VehicleConfigOffset, err
	}
	VehicleConfigOffset += int64(n)
	return missingVehicleConfigs, VehicleConfigOffset, err
}

func checkConfigID(aaiaConfigId, aaiaConfigTypeId int, configMap map[string]string) (int, int, error) {
	var err error
	acesStr := strconv.Itoa(aaiaConfigTypeId) + "," + strconv.Itoa(aaiaConfigId)
	configStr := configMap[acesStr]
	if configStr == "" {
		err = errors.New("noconfigs")
		return 0, 0, err
	}
	output := strings.Split(configStr, ",")
	typeID, err := strconv.Atoi(output[0])
	if err != nil {
		return 0, 0, err
	}
	valID, err := strconv.Atoi(output[1])
	if err != nil {
		return 0, 0, err
	}
	return typeID, valID, err
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
