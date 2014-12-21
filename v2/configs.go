package v2

import (
	"github.com/curt-labs/polkImporter/helpers/database"
	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/mgo.v2"
	// "gopkg.in/mgo.v2/bson"

	"database/sql"
	"errors"
	"log"
	// "os"
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
	insertVehiclePartStmt   = `insert into vcdb_VehiclePart (VehicleID, PartNumber) values (?, (select partID from Part where oldPartNumber = ?))`
	insertVehicleConfigStmt = `insert into VehicleConfig (AAIAVehicleConfigID) values (0)`
)

var (
	ConfigTypesOffset      int64 = 0
	ConfigOffset           int64 = 0
	VehicleConfigOffset    int64 = 0
	configPartNeededOffset int64 = 0
)

var acesTypeCurtTypeMap map[int]int
var configMap map[string]string
var initConfigMaps sync.Once

func initConfigMap() {
	configMap, _ = GetConfigMap()
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

//for each ConfigVehicleGroup in the array, compare config arrays

func AuditConfigs(configVehicleGroups []ConfigVehicleGroup) error {
	var err error
	// config_PartsNeeded, err := os.Create("exports/Config_PartsNeeded.txt")
	// if err != nil {
	// 	return err
	// }
	// b := []byte("insert into vcdb_VehiclePart (VehicleID, PartNumber) values ")
	// n, err := config_PartsNeeded.WriteAt(b, int64(0))
	// if err != nil {
	// 	return err
	// }
	// configPartNeededOffset += int64(n)

	for _, configVehicleGroup := range configVehicleGroups {
		log.Print("VEHICLE GROUP: ", configVehicleGroup)

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

		//remove duplicates
		for _, partsConfigs := range configsToProcess {
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
			partsConfigs = tempList
		}

		//process configs
		log.Print("FOR ", configVehicleGroup.VehicleID, configsToProcess)
		err = ProcessConfigs(&configVehicleGroup, configsToProcess)

		// 	initMaps.Do(initMap)

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
// Y: is there a part join? N: add part Y: continue
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
		var configAttributeArray []int //array of config attributes associated with this part
		for _, aaiaCon := range cons {
			aaiaConTypeID := strings.Split(aaiaCon, ",")[0]
			aaiaConValID := strings.Split(aaiaCon, ",")[1]
			log.Print("AAIA Type: ", aaiaConTypeID, ", AAIA val:", aaiaConValID, " part-", partNumber)
			curtCon := configMap[aaiaCon]

			log.Print("CURT CONFIG: ", curtCon)
			// curtConTypeID, err := strconv.Atoi(strings.Split(curtCon, ",")[0])
			// if err != nil {
			// 	return err
			// }
			var curtConValID int
			curtConSplitArray := strings.Split(curtCon, ",")
			if len(curtConSplitArray) > 1 {
				curtConValID, err = strconv.Atoi(curtConSplitArray[1])
				if err != nil {
					return err
				}
			} else {
				//TODO - there are no ConfigAttiributes for this aaia atribute yet  7,13,14,15
			}
			configAttributeArray = append(configAttributeArray, curtConValID)

			//find vehicleJoin

			// err = CheckVehicleJoin(configVehicleGroup.BaseID, configVehicleGroup.SubID, curtConTypeID, partNumber)
			// if err == sql.ErrNoRows {
			// 	//first 'NO'
			// 	if curtCon != "" {
			// 		//create joins in vca, vehicle, vehiclepart
			// 	} else {
			// 		//create ca, vca, vehcile vehiclepart
			// 	}

			// }

		} //end config loop
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
	sqlStmt := `insert into VehicleConfigAttribute (AttributeID, VehicleConfigID) values (`
	log.Print("LEN ", len(configAttributeArray))
	for i := 1; i < len(configAttributeArray); i++ {
		// sqlStmt += sqlAddOns
		if configAttributeArray[i] != 0 {
			sqlStmt += `(` + strconv.Itoa(configAttributeArray[i]) + `,` + strconv.Itoa(vConfigId) + `),`
		}
	}

	sqlStmt = strings.TrimRight(sqlStmt, ",")
	sqlStmt += ")"
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
