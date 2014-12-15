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
	// "reflect"
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

// type ConfigVehicleGroup struct {
// 	SubID    int `bson:"submodelId,omitempty"`
// 	BaseID   int `bson:"baseVehicleId,omitempty"`
// 	Vehicles []Vehicle
// }

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
	// checkConfigAttributeTypeStmt = `select ID from ConfigAttributeType where AcesTypeID = ?`
	// checkConfigAttributeStmt     = `select ca.ID from ConfigAttribute as ca
	// 	where ca.ConfigAttributeTypeID = ?
	// 	and vcdbID = ?`

	checkVehicleJoin = `select v.ID, vca.VehicleConfigID from vcdb_Vehicle as v 
		join BaseVehicle as b on b.ID = v.BaseVehicleID
		join Submodel as s on s.ID = v.SubmodelID
		join VehicleConfigAttribute as vac on vca.VehicleConfigID = v.ConfigID
		where b.AAIABaseVehicleID = ?
		and s.AAIASubmodelID = ?
		and vca.AttributeID = ?`

	// checkVehicleConfigAttributeStmt = `  select vca.VehicleConfigID from VehicleConfigAttribute as vca
	// 	join vcdb_Vehicle as v on v.ConfigID = vca.VehicleConfigID
	// 	join BaseVehicle as b on b.ID = v.BaseVehicleID
	// 	join Submodel as s on s.ID = v.SubmodelID
	// 	where vca.AttributeID = ?
	// 	and b.AAIABaseModelID = ?
	// 	and s.AAIASubmodelID = ?`
	// checkVehicleConfig = `select v.ID from vcdb_Vehicle as v
	// 	join BaseVehicle as b on b.ID = v.BaseVehicleID
	// 	join Submodel as s on s.ID = v.SubmodelID
	// 	where b.AAIABaseVehicleID = ?
	// 	and s.AAIASubmodelID = ?
	// 	and v.ConfigID = ?`
)

//For all mongodb entries, returns BaseVehicleRaws
func MongoToConfig(subIds []int) ([]ConfigVehicleRaw, error) {
	var err error
	var cgs []ConfigVehicleRaw
	session, err := mgo.Dial(database.MongoConnectionString().Addrs[0])
	if err != nil {
		return cgs, err
	}
	defer session.Close()
	collection := session.DB("importer").C("ariesTest")
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

	//files - missing aces configs
	missingConfigs, err := os.Create("MissingConfigs.csv")
	if err != nil {
		return err
	}
	configOffset := int64(0)
	h := []byte("AAIAConfigID,AAIAConfigTypeID\n")
	n, err := missingConfigs.WriteAt(h, configOffset)
	if err != nil {
		return err
	}
	configOffset += int64(n)

	//files - configs needed in VehicleConfigAttribute
	missingVehicleConfigs, err := os.Create("VehicleConfigurationsNeeded")
	if err != nil {
		return err
	}
	vehicleConfigOffset := int64(0)
	h = []byte("TypeID,ConfigID,AAIABaseID,AAIASubmodelID\n")
	n, err = missingVehicleConfigs.WriteAt(h, vehicleConfigOffset)
	if err != nil {
		return err
	}
	vehicleConfigOffset += int64(n)

	for _, configVehicleGroup := range configVehicleGroups {
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
		cylHeadTypeID := false
		// blockType := false
		// engineBaseID := false
		// engineConfigID := false
		for i, configs := range configVehicleGroup.ConfigVehicles {
			if i > 0 { //not the first configVehicle

				// if configs.FuelTypeID != configVehicleGroup.ConfigVehicles[i-1].FuelTypeID {
				// 	fuelType = true
				// }
				// if configs.FuelDeliveryID != configVehicleGroup.ConfigVehicles[i-1].FuelDeliveryID {
				// 	fuelDeliveryID = true
				// }
				// if configs.AcesLiter != configVehicleGroup.ConfigVehicles[i-1].AcesLiter {
				// 	acesLiter = true
				// }
				// if configs.AcesCC != configVehicleGroup.ConfigVehicles[i-1].AcesCC {
				// 	acesCC = true
				// }
				// if configs.AcesCID != configVehicleGroup.ConfigVehicles[i-1].AcesCID {
				// 	acesCID = true
				// }
				// if configs.AcesCyl != configVehicleGroup.ConfigVehicles[i-1].AcesCyl {
				// 	acesCyl = true
				// }
				// if configs.AcesBlockType != configVehicleGroup.ConfigVehicles[i-1].AcesBlockType {
				// 	acesBlockType = true
				// }
				// if configs.AspirationID != configVehicleGroup.ConfigVehicles[i-1].AspirationID {
				// 	aspirationID = true
				// }
				// if configs.DriveTypeID != configVehicleGroup.ConfigVehicles[i-1].DriveTypeID {
				// 	driveTypeID = true
				// }
				// if configs.BodyTypeID != configVehicleGroup.ConfigVehicles[i-1].BodyTypeID {
				// 	bodyTypeID = true
				// }
				// if configs.BodyNumDoorsID != configVehicleGroup.ConfigVehicles[i-1].BodyNumDoorsID {
				// 	bodyNumDoorsID = true
				// }
				// if configs.EngineVinID != configVehicleGroup.ConfigVehicles[i-1].EngineVinID {
				// 	engineVinID = true
				// }
				// if configs.RegionID != configVehicleGroup.ConfigVehicles[i-1].RegionID {
				// 	regionID = true
				// }
				// if configs.PowerOutputID != configVehicleGroup.ConfigVehicles[i-1].PowerOutputID {
				// 	powerOutputID = true
				// }
				// if configs.FuelDelConfigID != configVehicleGroup.ConfigVehicles[i-1].FuelDelConfigID {
				// 	fuelDelConfigID = true
				// }
				// if configs.BodyStyleConfigID != configVehicleGroup.ConfigVehicles[i-1].BodyStyleConfigID {
				// 	bodyStyleConfigID = true
				// }
				// if configs.ValvesID != configVehicleGroup.ConfigVehicles[i-1].ValvesID {
				// 	valvesID = true
				// }
				if configs.CylHeadTypeID != configVehicleGroup.ConfigVehicles[i-1].CylHeadTypeID {
					cylHeadTypeID = true
				}
				// if configs.BlockType != configVehicleGroup.ConfigVehicles[i-1].BlockType {
				// 	blockType = true
				// }
				// if configs.EngineBaseID != configVehicleGroup.ConfigVehicles[i-1].EngineBaseID {
				// 	engineBaseID = true
				// }
				// if configs.EngineConfigID != configVehicleGroup.ConfigVehicles[i-1].EngineConfigID {
				// 	engineConfigID = true
				// }

			}

		}
		// if engineConfigID == true {
		// 	//create vehicles with diff config values for this config type
		// 	for _, c := range configVehicleGroup.ConfigVehicles {
		// 		sql := "insert into vcdb_Vehicle (BaseVehicleID, SubModelID, ConfigID) values(" + strconv.Itoa(configVehicleGroup.BaseID) + "," + strconv.Itoa(configVehicleGroup.SubID) + "," + c.EngineConfigID //no

		// 	}
		// }
		if cylHeadTypeID == true {
			log.Print("Diff in cylHeadType")
			//create vehicles with diff config values for this config type
			for _, c := range configVehicleGroup.ConfigVehicles {
				acesTypeID := 12
				//search for this configAttribute and configAttributeType. If there are no Curt versions, write the needed aaia configAttibute type and configAttribute to csv
				typeID, valueID, err := checkConfigID(int(c.CylHeadTypeID), acesTypeID, configMap)
				if err != nil {
					if err.Error() == "noconfigs" {
						b := []byte(strconv.Itoa(int(c.CylHeadTypeID)) + "," + strconv.Itoa(acesTypeID) + "\n")
						n, err = missingConfigs.WriteAt(b, configOffset)
						configOffset += int64(n)
						continue
					} else {
						return err
					}
				} else {
					//curt configAttribute and configAttributeType found - check for vehicle and join in vehicleConfigAttribute tables
					//if there are no vehicle/vehicleConfigAttribute join, write this miss to csv
					vehicleID, vehicleConfigID, err := CheckVehicleConfig(typeID, c.BaseID, c.SubmodelID)
					if err != nil {
						if err.Error() == "novehicleconfig" {
							b := []byte(strconv.Itoa(typeID) + "," + strconv.Itoa(valueID) + "," + strconv.Itoa(c.BaseID) + "," + strconv.Itoa(c.SubmodelID))
							n, err = missingVehicleConfigs.WriteAt(b, vehicleConfigOffset)
							if err != nil {
								return err
							}
							vehicleConfigOffset += int64(n)
						} else {
							return err
						}
					}
					log.Print(vehicleID, " has config ", vehicleConfigID, " already.")
				}
			}
		}

		//end of spot-checking each attribute
	}
	return err
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

func CheckVehicleConfig(typeID, baseID, subID int) (int, int, error) {
	var err error
	var vehicleID, vehicleConfigID int
	db, err := sql.Open("mysql", database.ConnectionString())
	if err != nil {
		return vehicleID, vehicleConfigID, err
	}
	defer db.Close()

	stmt, err := db.Prepare(checkVehicleJoin)
	if err != nil {
		return 0, 0, err
	}
	defer stmt.Close()
	err = stmt.QueryRow(baseID, subID, typeID).Scan(&vehicleID, &vehicleConfigID)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, 0, errors.New("novehicleconfig")
		}
		return 0, 0, err
	}
	return vehicleID, vehicleConfigID, err
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
