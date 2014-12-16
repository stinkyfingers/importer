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
	ConfigTypesOffset   int64 = 0
	ConfigOffset        int64 = 0
	VehicleConfigOffset int64 = 0
)

//For all mongodb entries, returns BaseVehicleRaws
func MongoToConfig(subIds []int, dbCollection string) ([]ConfigVehicleRaw, error) {
	var err error
	var cgs []ConfigVehicleRaw
	session, err := mgo.Dial(database.MongoConnectionString().Addrs[0])
	if err != nil {
		return cgs, err
	}
	defer session.Close()
	collection := session.DB("importer").C(dbCollection)
	err = collection.Find(bson.M{"submodelId": bson.M{"$in": subIds}}).All(&cgs)
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

func AuditConfigs(configVehicleGroups []ConfigVehicleGroup) error {
	configMap, err := GetConfigMap()
	if err != nil {
		return err
	}
	vehicleJoinMap, err := CreateVehicleJoinMap()
	if err != nil {
		return err
	}

	MissingConfigTypes, _, err := createMissingConfigTypesFile()
	MissingConfigs, _, err := createMissingConfigsFile()
	MissingVehicleConfigs, _, err := createMissingVehicleConfigurationsFile()
	if err != nil {
		return err
	}

	for _, configVehicleGroup := range configVehicleGroups {
		fuelType := false
		fuelDeliveryID := false
		acesLiter := false
		acesCC := false
		acesCID := false
		acesCyl := false
		acesBlockType := false
		aspirationID := false
		driveTypeID := false
		bodyTypeID := false
		bodyNumDoorsID := false
		engineVinID := false
		regionID := false
		powerOutputID := false
		fuelDelConfigID := false
		bodyStyleConfigID := false
		valvesID := false
		cylHeadTypeID := false
		blockType := false
		engineBaseID := false
		engineConfigID := false
		for i, configs := range configVehicleGroup.ConfigVehicles {
			if i > 0 { //not the first configVehicle

				if configs.FuelTypeID != configVehicleGroup.ConfigVehicles[i-1].FuelTypeID {
					fuelType = true
				}
				if configs.FuelDeliveryID != configVehicleGroup.ConfigVehicles[i-1].FuelDeliveryID {
					fuelDeliveryID = true
				}
				if configs.AcesLiter != configVehicleGroup.ConfigVehicles[i-1].AcesLiter {
					acesLiter = true
				}
				if configs.AcesCC != configVehicleGroup.ConfigVehicles[i-1].AcesCC {
					acesCC = true
				}
				if configs.AcesCID != configVehicleGroup.ConfigVehicles[i-1].AcesCID {
					acesCID = true
				}
				if configs.AcesCyl != configVehicleGroup.ConfigVehicles[i-1].AcesCyl {
					acesCyl = true
				}
				if configs.AcesBlockType != configVehicleGroup.ConfigVehicles[i-1].AcesBlockType {
					acesBlockType = true
				}
				if configs.AspirationID != configVehicleGroup.ConfigVehicles[i-1].AspirationID {
					aspirationID = true
				}
				if configs.DriveTypeID != configVehicleGroup.ConfigVehicles[i-1].DriveTypeID {
					driveTypeID = true
				}
				if configs.BodyTypeID != configVehicleGroup.ConfigVehicles[i-1].BodyTypeID {
					bodyTypeID = true
				}
				if configs.BodyNumDoorsID != configVehicleGroup.ConfigVehicles[i-1].BodyNumDoorsID {
					bodyNumDoorsID = true
				}
				if configs.EngineVinID != configVehicleGroup.ConfigVehicles[i-1].EngineVinID {
					engineVinID = true
				}
				if configs.RegionID != configVehicleGroup.ConfigVehicles[i-1].RegionID {
					regionID = true
				}
				if configs.PowerOutputID != configVehicleGroup.ConfigVehicles[i-1].PowerOutputID {
					powerOutputID = true
				}
				if configs.FuelDelConfigID != configVehicleGroup.ConfigVehicles[i-1].FuelDelConfigID {
					fuelDelConfigID = true
				}
				if configs.BodyStyleConfigID != configVehicleGroup.ConfigVehicles[i-1].BodyStyleConfigID {
					bodyStyleConfigID = true
				}
				if configs.ValvesID != configVehicleGroup.ConfigVehicles[i-1].ValvesID {
					valvesID = true
				}
				if configs.CylHeadTypeID != configVehicleGroup.ConfigVehicles[i-1].CylHeadTypeID {
					cylHeadTypeID = true
				}
				if configs.BlockType != configVehicleGroup.ConfigVehicles[i-1].BlockType {
					blockType = true
				}
				if configs.EngineBaseID != configVehicleGroup.ConfigVehicles[i-1].EngineBaseID {
					engineBaseID = true
				}
				if configs.EngineConfigID != configVehicleGroup.ConfigVehicles[i-1].EngineConfigID {
					engineConfigID = true
				}

			}

		}

		//fueltype
		if fuelType == true {
			log.Print("fuelType")
			acesType := 6
			for _, c := range configVehicleGroup.ConfigVehicles {
				acesValue := int(c.FuelTypeID)
				err = auditConfigs(acesType, acesValue, configMap, vehicleJoinMap, c, MissingVehicleConfigs, MissingConfigs)
				if err != nil {
					log.Print(err)
					return err
				}
			}
		}

		//fueldelivery
		if fuelDeliveryID == true {
			log.Print("fuelDeliveryID ")
			acesType := 20
			for _, c := range configVehicleGroup.ConfigVehicles {
				acesValue := int(c.FuelDeliveryID)
				err = auditConfigs(acesType, acesValue, configMap, vehicleJoinMap, c, MissingVehicleConfigs, MissingConfigs)
				if err != nil {
					log.Print(err)
					return err
				}
			}
		}
		//aspiration
		if aspirationID == true {
			log.Print("aspirationID")
			acesType := 8
			for _, c := range configVehicleGroup.ConfigVehicles {
				acesValue := int(c.AspirationID)
				err = auditConfigs(acesType, acesValue, configMap, vehicleJoinMap, c, MissingVehicleConfigs, MissingConfigs)
				if err != nil {
					log.Print(err)
					return err
				}
			}
		}
		//drive type
		if driveTypeID == true {
			log.Print("driveTypeID")
			acesType := 8
			for _, c := range configVehicleGroup.ConfigVehicles {
				acesValue := int(c.DriveTypeID)
				err = auditConfigs(acesType, acesValue, configMap, vehicleJoinMap, c, MissingVehicleConfigs, MissingConfigs)
				if err != nil {
					log.Print(err)
					return err
				}
			}
		}
		//body type
		if bodyTypeID == true {
			log.Print("bodyTypeID")
			acesType := 2
			for _, c := range configVehicleGroup.ConfigVehicles {
				acesValue := int(c.BodyTypeID)
				err = auditConfigs(acesType, acesValue, configMap, vehicleJoinMap, c, MissingVehicleConfigs, MissingConfigs)
				if err != nil {
					log.Print(err)
					return err
				}
			}
		}
		//body num doors
		if bodyNumDoorsID == true {
			log.Print("bodyNumDoorsID")
			acesType := 4
			for _, c := range configVehicleGroup.ConfigVehicles {
				acesValue := int(c.BodyNumDoorsID)
				err = auditConfigs(acesType, acesValue, configMap, vehicleJoinMap, c, MissingVehicleConfigs, MissingConfigs)
				if err != nil {
					log.Print(err)
					return err
				}
			}
		}
		//engine vin
		if engineVinID == true {
			log.Print("engineVinID")
			acesType := 16
			for _, c := range configVehicleGroup.ConfigVehicles {
				acesValue := int(c.PowerOutputID)
				err = auditConfigs(acesType, acesValue, configMap, vehicleJoinMap, c, MissingVehicleConfigs, MissingConfigs)
				if err != nil {
					log.Print(err)
					return err
				}
			}
		}
		//power output
		if powerOutputID == true {
			log.Print("powerOutputID")
			acesType := 25
			for _, c := range configVehicleGroup.ConfigVehicles {
				acesValue := int(c.PowerOutputID)
				err = auditConfigs(acesType, acesValue, configMap, vehicleJoinMap, c, MissingVehicleConfigs, MissingConfigs)
				if err != nil {
					log.Print(err)
					return err
				}
			}
		}
		//fuel del - TODO is this that same as subtype
		if fuelDelConfigID == true {
			log.Print("fuelDelConfigID")
			acesType := 19
			for _, c := range configVehicleGroup.ConfigVehicles {
				acesValue := int(c.FuelDelConfigID)
				err = auditConfigs(acesType, acesValue, configMap, vehicleJoinMap, c, MissingVehicleConfigs, MissingConfigs)
				if err != nil {
					log.Print(err)
					return err
				}
			}
		}
		//valves
		if valvesID == true {
			log.Print("valvesID")
			acesType := 40
			for _, c := range configVehicleGroup.ConfigVehicles {
				acesValue := int(c.ValvesID)
				err = auditConfigs(acesType, acesValue, configMap, vehicleJoinMap, c, MissingVehicleConfigs, MissingConfigs)
				if err != nil {
					log.Print(err)
					return err
				}
			}
		}
		//cyl head type
		if cylHeadTypeID == true {
			log.Print("cylHeadTypeID")
			acesType := 12
			for _, c := range configVehicleGroup.ConfigVehicles {
				acesValue := int(c.CylHeadTypeID)
				err = auditConfigs(acesType, acesValue, configMap, vehicleJoinMap, c, MissingVehicleConfigs, MissingConfigs)
				if err != nil {
					log.Print(err)
					return err
				}
			}
		}
		//engine base - TODO - is this "Engine?"
		if engineBaseID == true {
			log.Print("engineBaseID")
			acesType := 7
			for _, c := range configVehicleGroup.ConfigVehicles {
				acesValue := int(c.EngineBaseID)
				err = auditConfigs(acesType, acesValue, configMap, vehicleJoinMap, c, MissingVehicleConfigs, MissingConfigs)
				if err != nil {
					log.Print(err)
					return err
				}
			}
		}

		// //cylHeadTypeID
		// if cylHeadTypeID == true {
		// 	log.Print("CYL")
		// 	//create vehicles with diff config values for this config type
		// 	for _, c := range configVehicleGroup.ConfigVehicles {
		// 		acesTypeID := 12
		// 		//search for this configAttribute and configAttributeType. If there are no Curt versions, write the needed aaia configAttibute type and configAttribute to csv
		// 		typeID, valueID, err := checkConfigID(int(c.CylHeadTypeID), acesTypeID, configMap)
		// 		if err != nil {
		// 			if err.Error() == "noconfigs" {
		// 				b := []byte(strconv.Itoa(int(c.CylHeadTypeID)) + "," + strconv.Itoa(acesTypeID) + "\n")
		// 				n, err := MissingConfigs.WriteAt(b, ConfigOffset)
		// 				if err != nil {
		// 					return err
		// 				}
		// 				ConfigOffset += int64(n)
		// 				continue
		// 			} else {
		// 				return err
		// 			}
		// 		} else {
		// 			//curt configAttribute and configAttributeType found - check for vehicle and join in vehicleConfigAttribute tables
		// 			//if there are no vehicle/vehicleConfigAttribute join, write this miss to csv
		// 			vehicleID, vehicleConfigID, err := CheckVehicleConfig(typeID, c.BaseID, c.SubmodelID)
		// 			if err != nil {
		// 				if err.Error() == "novehicleconfig" {
		// 					b := []byte(strconv.Itoa(typeID) + "," + strconv.Itoa(valueID) + "," + strconv.Itoa(c.BaseID) + "," + strconv.Itoa(c.SubmodelID))
		// 					n, err := MissingVehicleConfigs.WriteAt(b, VehicleConfigOffset)
		// 					if err != nil {
		// 						return err
		// 					}
		// 					VehicleConfigOffset += int64(n)
		// 				} else {
		// 					return err
		// 				}
		// 			} else {
		// 				log.Print(vehicleID, " has config ", vehicleConfigID, " already.")
		// 			}

		// 		}
		// 	}
		// } //end cylHeadTypeID

		//NON - CURT->ACES CONFIGS
		if acesLiter == true {
			b := []byte("acesLiter\n")
			n, err := MissingConfigTypes.WriteAt(b, ConfigTypesOffset)
			if err != nil {
				return err
			}
			ConfigTypesOffset += int64(n)
		}
		if acesCC == true {
			b := []byte("acesCC\n")
			n, err := MissingConfigTypes.WriteAt(b, ConfigTypesOffset)
			if err != nil {
				return err
			}
			ConfigTypesOffset += int64(n)
		}
		if acesCID == true {
			b := []byte("acesCID\n")
			n, err := MissingConfigTypes.WriteAt(b, ConfigTypesOffset)
			if err != nil {
				return err
			}
			ConfigTypesOffset += int64(n)
		}
		if acesCyl == true {
			b := []byte("acesCyl\n")
			n, err := MissingConfigTypes.WriteAt(b, ConfigTypesOffset)
			if err != nil {
				return err
			}
			ConfigTypesOffset += int64(n)
		}
		if acesBlockType == true {
			b := []byte("acesBlockType\n")
			n, err := MissingConfigTypes.WriteAt(b, ConfigTypesOffset)
			if err != nil {
				return err
			}
			ConfigTypesOffset += int64(n)
		}
		if regionID == true {
			b := []byte("regionID\n")
			n, err := MissingConfigTypes.WriteAt(b, ConfigTypesOffset)
			if err != nil {
				return err
			}
			ConfigTypesOffset += int64(n)
		}
		if bodyStyleConfigID == true {
			b := []byte("bodyStyleConfigID\n")
			n, err := MissingConfigTypes.WriteAt(b, ConfigTypesOffset)
			if err != nil {
				return err
			}
			ConfigTypesOffset += int64(n)
		}
		if blockType == true {
			b := []byte("blockType\n")
			n, err := MissingConfigTypes.WriteAt(b, ConfigTypesOffset)
			if err != nil {
				return err
			}
			ConfigTypesOffset += int64(n)
		}
		if engineConfigID == true {
			b := []byte("engineConfigID\n")
			n, err := MissingConfigTypes.WriteAt(b, ConfigTypesOffset)
			if err != nil {
				return err
			}
			ConfigTypesOffset += int64(n)
		}

	} //end of spot-checking each attribute
	log.Print("MADE IT TO THE END. ", err)
	return err
}

func auditConfigs(acesType int, acesValue int, configMap map[string]string, vehicleJoinMap map[string]string, c ConfigVehicleRaw, MissingVehicleConfigs *os.File, MissingConfigs *os.File) error {
	var err error

	//search for this configAttribute and configAttributeType. If there are no Curt versions, write the needed aaia configAttibute type and configAttribute to csv
	typeID, valueID, err := checkConfigID(acesValue, acesType, configMap)
	if err != nil {
		if err.Error() == "noconfigs" {
			b := []byte(strconv.Itoa(acesValue) + "," + strconv.Itoa(acesType) + "\n")
			n, err := MissingConfigs.WriteAt(b, ConfigOffset)
			if err != nil {
				log.Print("configAudit err; writing MissingConfigs ", err)
				return err
			}
			ConfigOffset += int64(n)
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
	h := []byte("AAIAConfigID,AAIAConfigTypeID\n")
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

// func CheckVehicleConfig(typeID, baseID, subID int) (int, int, error) {
// 	var err error
// 	var vehicleID, vehicleConfigID int
// 	db, err := sql.Open("mysql", database.ConnectionString())
// 	if err != nil {
// 		log.Print("615", err)
// 		return vehicleID, vehicleConfigID, err
// 	}
// 	defer db.Close()

// 	stmt, err := db.Prepare(checkVehicleJoin)
// 	if err != nil {
// 		log.Print("622", err)
// 		return 0, 0, err
// 	}
// 	defer stmt.Close()
// 	err = stmt.QueryRow(baseID, subID, typeID).Scan(&vehicleID, &vehicleConfigID)
// 	if err != nil {
// 		if err == sql.ErrNoRows {
// 			return 0, 0, errors.New("novehicleconfig")
// 		}
// 		log.Print("632", err)
// 		return 0, 0, err
// 	}
// 	return vehicleID, vehicleConfigID, err
// }

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
