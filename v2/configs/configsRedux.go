package configs

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
	CurtAttributeIDs  []int
}

type ConfigVehicleGroup struct {
	VehicleID      int `bson:"vehicleId,omitempty"`
	SubID          int `bson:"submodelId,omitempty"`
	BaseID         int `bson:"baseVehicleId,omitempty"`
	DiffConfigs    []int
	ConfigVehicles []ConfigVehicleRaw
}

var newCvgs []ConfigVehicleGroup //static var
var configPartNeededOffset int64 = 0
var processedConfigVehicles int = 0
var insertQueriesOffset int64 = 0
var configErrorOffset int64 = 0
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
	insertVehiclePartStmt    = `insert into vcdb_VehiclePart (VehicleID, PartNumber) values (?, ?)`
	insertVehicleConfigStmt  = `insert into VehicleConfig (AAIAVehicleConfigID) values (0)`
	insertCurtConfigTypeStmt = `insert into ConfigAttributeType(name, AcesTypeID, sort) values (?,?,?)`
	insertCurtConfigStmt     = `insert into ConfigAttribute(ConfigAttributeTypeID, parentID, vcdbID, value) values(?,0,?,?)`
	getCurtConfigValueIdStmt = `select ID from ConfigAttribute where ConfigAttributeTypeID = ? and vcdbID = ? `
	findVehicleStmt          = `select  v.ID from vcdb_Vehicle as v
		join BaseVehicle as b on b.ID = v.BaseVehicleID
		join Submodel as s on s.ID = v.SubmodelID
		join VehicleConfig as vc on vc.ID = v.ConfigID
		join VehicleConfigAttribute as vca on vca.VehicleConfigID = v.ConfigID
		where b.AAIABaseVehicleID = ?
		and s.AAIASubmodelID = ?
		and vca.AttributeID = ? `
	insertBaseVehicle = `insert into BaseVehicle(AAIABaseVehicleID, YearID, MakeID, ModelID) values(?,?,?,?)`
	insertSubmodel    = `insert into Submodel (AAIASubmodelID, SubmodelName) values (?,?)`
	findSubmodelStmt  = `select  v.ID from vcdb_Vehicle as v
		join BaseVehicle as b on b.ID = v.BaseVehicleID
		join Submodel as s on s.ID = v.SubmodelID
		where b.ID = ?
		and s.ID = ?
		and (v.ConfigID is null or v.ConfigID = 0)`
	findPartStmt              = "select VehicleID from vcdb_VehiclePart where VehicleID = ? and PartNumber = (select p.partID from Part as p where p.oldPartNumber = ? limit 1)"
	vehicleInsertStmt         = `insert into vcdb_Vehicle (BaseVehicleID, SubModelID, ConfigID, AppID) values (?,?,?,0)`
	submodelVehicleInsertStmt = `insert into vcdb_Vehicle (BaseVehicleID, SubModelID, ConfigID, AppID) values (?,?,NULL,0)`
)

var acesTypeCurtTypeMap map[int]int
var configMap map[string]string
var configAttributeTypeMap map[int]int
var configAttributeMap map[string]int
var baseMap map[int]int
var subMap map[int]int
var partMap map[string]int
var missingPartNumbers *os.File
var insertVehiclePartQueries *os.File
var configErrorFile *os.File
var initConfigMaps sync.Once
var vehicleOldPartArray []string
var submodelVehicleMap map[string]int

func initConfigMap() {
	var err error
	configAttributeTypeMap, err = getConfigAttriguteTypeMap()
	if err != nil {
		log.Print(err)
	}
	configAttributeMap, err = getConfigAttributeMap()
	if err != nil {
		log.Print(err)
	}
	baseMap, err = getBaseMap()
	if err != nil {
		log.Print(err)
	}
	subMap, err = getSubMap()
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
	vehicleOldPartArray, err = GetVehicleOldPartArray()
	if err != nil {
		log.Print(err)
	}
	insertVehiclePartQueries, insertQueriesOffset, err = createInsertStatementsFile("InsertStatements_VehiclePart_configs")
	if err != nil {
		log.Print("err creating insertStatements ", err)
	}
	submodelVehicleMap, err = getSubmodelInVehicleTableMap()
	if err != nil {
		log.Print(err)
	}
	configErrorFile, err = createConfigErrorFile("ConfigErrorFile")
	if err != nil {
		log.Print(err)
	}
}

//For all mongodb entries, returns ConfigVehicleRaws
func MongoToConfigurations(dbCollection string) ([]ConfigVehicleRaw, error) {
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

//process things in batches
func MongoToConfigurationsBatch(dbCollection string, limit, skip int) ([]ConfigVehicleRaw, error) {
	var err error
	var cgs []ConfigVehicleRaw

	session, err := mgo.Dial(database.MongoConnectionString().Addrs[0])
	if err != nil {
		return cgs, err
	}
	defer session.Close()
	collection := session.DB("importer").C(dbCollection)

	//write to csv raw vehicles
	err = collection.Find(nil).Sort("$natural").Skip(skip).Limit(limit).All(&cgs)

	return cgs, err
}

func ConfigArray(cgs []ConfigVehicleRaw) []ConfigVehicleGroup {
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

//NEW
// foreach VehicleID's array of configs:
// for each config compile array of unique config values
// get length of each
//split into x groups of vehicles for lowest number of array values x, that is greater than 1
//repeat for each subgroup until there is only arrays of 1 unique config value

func ReduceConfigs(configVehicleGroups []ConfigVehicleGroup) error {
	var err error
	newCvgs = configVehicleGroups

	err = ReduceFuelType()
	if err != nil {
		return err
	}
	err = ReduceFuelDelivery()
	if err != nil {
		return err
	}
	err = ReduceDriveType()
	if err != nil {
		return err
	}
	err = ReduceBodyNumDoors()
	if err != nil {
		return err
	}
	err = ReduceEngineVin()
	if err != nil {
		return err
	}
	err = ReduceBodyType()
	if err != nil {
		return err
	}
	err = ReducePowerOutput()
	if err != nil {
		return err
	}
	err = ReduceValves()
	if err != nil {
		return err
	}
	err = ReduceCylHead()
	if err != nil {
		return err
	}
	err = ReduceEngineBase()
	if err != nil {
		return err
	}

	// err = ReduceAcesLiter()//SKIP - Redundant w/ EngineBase
	// err = ReduceAcesBlock()//SKIP - Redundant w/ EngineBase
	// err = ReduceBodyStyle() //SKIP - Redundant w/ Body Type + Num Doors

	err = ReduceFuelDelConfig()
	if err != nil {
		return err
	}

	err = ReduceEngineConfig()
	if err != nil {
		return err
	}

	//These two just panic - no Corresponding curt type
	err = ReduceAcesCC()
	if err != nil {
		return err
	}
	err = ReduceAcesCid()
	if err != nil {
		return err
	}

	//Sanity Check
	log.Print("LENGTH:", len(newCvgs), "\n\n")

	//Check for duplicates
	for _, r := range newCvgs {
		for i, c := range r.ConfigVehicles {
			if i > 1 {
				comparison, err := CompareConfigFields(c, r.ConfigVehicles[i-1])
				if err != nil {
					return err
				}
				if comparison == false {
					log.Print("FALSE ", r, "\n\n")
					err = errors.New("Configs didn't reduce out completely")
					return err
				}
			}
		}
	}

	err = ProcessReducedConfigs(newCvgs)
	return err
}
func ProcessReducedConfigs(newCvgs []ConfigVehicleGroup) error {
	var err error

	//all good above? Begin checking/writing vehicles/vehicleConfigs and checking/writing vehicleparts
	for _, cvg := range newCvgs {
		// log.Print(cvg)
		err = Process(cvg.ConfigVehicles) //send array of ConfigVehiclesRaw (which contain array of attributes to link to vehicle and part)
		if err != nil {
			return err
		}
	}
	log.Print("ALL DONE ", err)
	return err
}

//for each RawVehicle (w/ array of configs)
//check for existence of BaseVehicle
//check for existence of Submodel
//if no attributes, process as submodel
//else, process with configs
func Process(cvg []ConfigVehicleRaw) error {
	var err error
	for _, raw := range cvg { //for each vehicle
		processedConfigVehicles++
		log.Print(processedConfigVehicles)
		//make attribute array
		var attrs []int
		for _, attr := range raw.CurtAttributeIDs {
			attrs = append(attrs, attr)
		}
		//get curt base and submodel
		cBaseID := baseMap[raw.BaseID]
		if cBaseID == 0 {
			//create base
			cBaseID, err = InsertBaseVehicle(raw.BaseID)
			if err != nil {
				if err == sql.ErrNoRows {
					b := []byte("Missing Base Vehicle: " + strconv.Itoa(cBaseID))
					n, err := configErrorFile.WriteAt(b, configErrorOffset)
					if err != nil {
						return err
					}
					configErrorOffset += int64(n)
					return nil //exit without processing vehicle
				} else {
					return err
				}
			}
		}
		cSubmodelID := subMap[raw.SubmodelID]
		if cSubmodelID == 0 {
			//create submodel
			cSubmodelID, err = InsertSubmodel(raw.SubmodelID)
			if err != nil {
				return err
			}
		}

		//Begin actually processing
		//submodels that slipped through
		if len(attrs) == 0 {
			//enter as a submodel
			// _, err = CheckSubmodelAndParts(raw.SubmodelID, raw.BaseID, raw.PartNumber, "ariesConfigs")
			err = FindSubmodelWithParts(cBaseID, cSubmodelID, raw.PartNumber, attrs)
			if err != nil {
				return err
			}
		} else {
			//else insert as vehicle with configs
			err = FindVehicleWithAttributes(cBaseID, cSubmodelID, raw.PartNumber, attrs)
			if err != nil {
				return err
			}
		}
	}
	return err
}

//search for this vehicle (submodel) in vcdb_Vehicle
//if it does not exist, create
//check for vehiclePart, insert if it does not exist
func FindSubmodelWithParts(cBaseID int, cSubmodelID int, partNumber string, configAttributeArray []int) error {
	var err error
	var vId int
	db, err := sql.Open("mysql", database.ConnectionString())
	if err != nil {
		return err
	}
	defer db.Close()

	a := []string{strconv.Itoa(cBaseID), strconv.Itoa(cSubmodelID)}
	str := strings.Join(a, ":")
	vId = submodelVehicleMap[str]
	if vId == 0 {
		log.Print("Submodel not found")

		//create vehicle
		stmt, err := db.Prepare(submodelVehicleInsertStmt)
		if err != nil {
			return err
		}
		defer stmt.Close()
		res, err := stmt.Exec(cBaseID, cSubmodelID)
		if err != nil {
			return err
		}
		id, err := res.LastInsertId()
		if err != nil {
			return err
		}
		vId = int(id)

	}
	err = FindPart(vId, partNumber)
	return err
}

//search for this vehicle in vcdb_Vehicle with it's array of 1-inifity attributes
//if it does not exist, create
//check for vehiclePart, insert if it does not exist
func FindVehicleWithAttributes(cBaseID int, cSubmodelID int, partNumber string, configAttributeArray []int) error {
	//build goddamn query
	//find vehicle with these attri
	if len(configAttributeArray) == 0 {
		log.Panic("NO CONFIGS TO INSERT")
	}
	sqlStmt := `select  v.ID from vcdb_Vehicle as v
		join BaseVehicle as b on b.ID = v.BaseVehicleID
		join Submodel as s on s.ID = v.SubmodelID
		join VehicleConfigAttribute as vca on vca.VehicleConfigID = v.ConfigID
		where b.ID = ?
		and s.ID = ?
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

	//missing vehicle with these config(s)
	if err != nil || vId == 0 {
		if err == sql.ErrNoRows {
			//no matching vehicle, must create
			vId, err = createVehicleConfigAttributes(cBaseID, cSubmodelID, partNumber, configAttributeArray)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}
	//find/insert vehiclePart
	err = FindPart(vId, partNumber)

	return err
}

func FindPart(vId int, partNum string) error {
	//Check for existence of vehiclePart
	var err error
	a := []string{strconv.Itoa(vId), partNum}
	str := strings.Join(a, ":")
	for _, vp := range vehicleOldPartArray {
		if vp == str {
			log.Print("VehiclePart found.")
			return nil
		}
	}
	err = InsertVehiclePart(vId, partNum)
	return err
}

func createVehicleConfigAttributes(cBaseID int, cSubmodelID int, partNumber string, configAttributeArray []int) (int, error) {
	var vId int
	db, err := sql.Open("mysql", database.ConnectionString())
	if err != nil {
		return vId, err
	}
	defer db.Close()
	log.Print("insert vehicleConfig")
	//new vehicleConfig
	stmt, err := db.Prepare(insertVehicleConfigStmt)
	if err != nil {
		return vId, err
	}
	res, err := stmt.Exec()
	if err != nil {
		return vId, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return vId, err
	}
	vConfigId := int(id)

	log.Print("insert vehicle. baseID: ", cBaseID, "   sub: ", cSubmodelID, "   confg: ", vConfigId)
	//insert new vehicle first

	stmt, err = db.Prepare(vehicleInsertStmt)
	if err != nil {
		return vId, err
	}
	res, err = stmt.Exec(cBaseID, cSubmodelID, vConfigId)
	if err != nil {
		return vId, err
	}
	id, err = res.LastInsertId()
	if err != nil {
		return vId, err
	}
	vId = int(id)

	//insert vehicleConfigAttribute
	sqlStmt := `insert into VehicleConfigAttribute (AttributeID, VehicleConfigID) values `
	for i := 0; i < len(configAttributeArray); i++ {
		// sqlStmt += sqlAddOns
		// if configAttributeArray[i] != 0 {
		log.Print(configAttributeArray[i], "__", vConfigId)
		sqlStmt += `(` + strconv.Itoa(configAttributeArray[i]) + `,` + strconv.Itoa(vConfigId) + `),`
		// }
	}

	sqlStmt = strings.TrimRight(sqlStmt, ",")

	stmt, err = db.Prepare(sqlStmt)
	if err != nil {
		return vId, err
	}
	defer stmt.Close()

	_, err = stmt.Exec()
	if err != nil {
		return vId, err
	}
	return vId, err
	// err = FindPart(vId, partNumber)
	// // err = InsertVehiclePart(vId, partNumber)
	// if err != nil {
	// 	return err
	// }
	// return err
}

func InsertVehiclePart(vId int, partNum string) error {

	//check part Number
	partId := partMap[partNum]
	if partId == 0 {
		b := []byte(partNum + "\n")
		n, err := missingPartNumbers.WriteAt(b, configPartNeededOffset)
		if err != nil {
			return err
		}
		configPartNeededOffset += int64(n)
		return nil
	}
	// //Write to txt - the SQL version below works fine when processing in batches
	// b := []byte("(" + strconv.Itoa(vId) + "," + strconv.Itoa(partId) + "),\n")
	// n, err := insertVehiclePartQueries.WriteAt(b, insertQueriesOffset)
	// if err != nil {
	// 	return err
	// }
	// insertQueriesOffset += int64(n)

	//SQL version
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
	_, err = stmt.Exec(vId, partId)
	if err != nil {
		return err
	}

	//add vehicle:OldPart to vehiclePartArray
	a := []string{strconv.Itoa(vId), partNum}
	str := strings.Join(a, ":")
	vehicleOldPartArray = append(vehicleOldPartArray, str)

	return err
}

//SUBMODELS AND BASEVEHICLES that need to be created
//Crap for inserting into baseVehicle and Submodel tables
type VehicleInfo struct {
	Make     string `bson:"makeId,omitempty"`
	Model    string `bson:"modelId,omitempty"`
	Year     int    `bson:"yearId,omitempty"`
	Submodel string `bson:"submodel,omitempty"`
}

func InsertBaseVehicle(aaiaBaseId int) (int, error) {
	var err error
	var cBaseId int
	session, err := mgo.Dial(database.MongoConnectionString().Addrs[0])
	if err != nil {
		return cBaseId, err
	}
	defer session.Close()
	collection := session.DB("importer").C("aries")

	b := VehicleInfo{}

	//write to csv raw vehicles
	err = collection.Find(bson.M{"baseVehicleId": aaiaBaseId}).One(&b)

	db, err := sql.Open("mysql", database.ConnectionString())
	if err != nil {
		return cBaseId, err
	}
	defer db.Close()
	var makeId, modelId int

	stmt, err := db.Prepare("select ID from vcdb_Model where AAIAModelID = ?")
	if err != nil {
		return cBaseId, err
	}
	err = stmt.QueryRow(b.Model).Scan(&modelId)
	if err != nil {
		log.Print("Missing Model in Model table", b.Model)
		return cBaseId, err
	}

	stmt, err = db.Prepare("select ID from vcdb_Make where AAIAMakeID = ?")
	if err != nil {
		return cBaseId, err
	}
	err = stmt.QueryRow(b.Make).Scan(&makeId)
	if err != nil {
		log.Print("Missing Make in make table", b.Make)
		return cBaseId, err
	}

	stmt, err = db.Prepare(insertBaseVehicle)
	if err != nil {
		return cBaseId, err
	}
	defer stmt.Close()
	res, err := stmt.Exec(aaiaBaseId, b.Year, makeId, modelId)
	if err != nil {
		return cBaseId, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return cBaseId, err
	}
	cBaseId = int(id)
	return cBaseId, err
}

func InsertSubmodel(aaiaSubdmodel int) (int, error) {
	var err error
	var cSubId int
	session, err := mgo.Dial(database.MongoConnectionString().Addrs[0])
	if err != nil {
		return cSubId, err
	}
	defer session.Close()
	collection := session.DB("importer").C("aries")

	b := VehicleInfo{}

	//write to csv raw vehicles
	err = collection.Find(bson.M{"submodelId": aaiaSubdmodel}).One(&b)

	db, err := sql.Open("mysql", database.ConnectionString())
	if err != nil {
		return cSubId, err
	}
	defer db.Close()

	stmt, err := db.Prepare(insertSubmodel)
	if err != nil {
		return cSubId, err
	}
	defer stmt.Close()
	res, err := stmt.Exec(aaiaSubdmodel, b.Submodel)
	if err != nil {
		return cSubId, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return cSubId, err
	}
	cSubId = int(id)
	return cSubId, err
}

//Begin REDUCTION FUNCTIONS
func ReduceFuelType() error {
	var err error
	var cvgsArray []ConfigVehicleGroup
	initConfigMap()

	for _, cvg := range newCvgs {

		//loop through fields
		var ftype []int
		for _, c := range cvg.ConfigVehicles {
			//field type
			ftype = append(ftype, int(c.FuelTypeID))
		}
		ftype = removeDuplicatesFromIntArray(ftype)

		//FUEL TYPE
		//if more than one fueltype, we'll split the ConfigVehicles on this value into multiple cvgs
		if len(ftype) > 1 {
			mmm := make(map[uint8][]ConfigVehicleRaw)

			for _, c := range cvg.ConfigVehicles {
				mmm[c.FuelTypeID] = append(mmm[c.FuelTypeID], c)
			}
			for _, m := range mmm {
				var tempCvg ConfigVehicleGroup
				tempCvg.BaseID = cvg.BaseID
				tempCvg.SubID = cvg.SubID
				tempCvg.VehicleID = cvg.VehicleID
				for _, mm := range m {
					//Can we get Curt Attribute ID here?
					//CurtAttrId <- CurtTypeId, AcesValueID, ValueString
					//CurtTypeId <- acesTypeId
					//AcesValueID <- from mm -or- get if provided value
					//Have: acesType, acesValue

					//GetCurtTypeId
					curtConfigTypeId := configAttributeTypeMap[6]
					if curtConfigTypeId == 0 {
						log.Print("Missing Type: FuelType")
						//TODO CREATE  configType
						log.Panic("Missing type")
					}
					// log.Print("Config TYPE ", curtConfigTypeId)
					if mm.FuelTypeID > 0 {
						curtConfigId, err := getCurtConfigValue(curtConfigTypeId, int(mm.FuelTypeID))
						if err != nil {
							if err == sql.ErrNoRows {
								err = nil
								AcesValue, err := getAcesConfigurationValueName(6, int(mm.FuelTypeID))
								if err != nil {
									return err
								}
								curtConfigId, err = createCurtConfigValue(curtConfigTypeId, int(mm.FuelTypeID), AcesValue)
								if err != nil {
									return err
								}
							} else {
								return err
							}
						}
						// log.Print("CURT CONFIG ", curtConfigId)
						mm.CurtAttributeIDs = append(mm.CurtAttributeIDs, curtConfigId)

						//Resume
						tempCvg.ConfigVehicles = append(tempCvg.ConfigVehicles, mm)
					}
				}
				tempCvg.DiffConfigs = cvg.DiffConfigs                //previous diffCOnfigs
				tempCvg.DiffConfigs = append(tempCvg.DiffConfigs, 6) // FUEL TYPE
				cvgsArray = append(cvgsArray, tempCvg)
			}
		} else {
			cvgsArray = append(cvgsArray, cvg)
		}
	}
	newCvgs = cvgsArray
	return err
}

func ReduceFuelDelivery() error {
	var err error
	var cvgsArray []ConfigVehicleGroup

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
					//GetCurtTypeId
					curtConfigTypeId := configAttributeTypeMap[20]
					if curtConfigTypeId == 0 {
						log.Print("Missing Type: FuelDel")
						//TODO CREATE  configType
						log.Panic("Missing type")
					}
					// log.Print("Config TYPE ", curtConfigTypeId)
					if mm.FuelDeliveryID > 0 {
						curtConfigId, err := getCurtConfigValue(curtConfigTypeId, int(mm.FuelDeliveryID))
						if err != nil {
							if err == sql.ErrNoRows {
								err = nil
								AcesValue, err := getAcesConfigurationValueName(20, int(mm.FuelDeliveryID))
								if err != nil {
									return err
								}
								curtConfigId, err = createCurtConfigValue(curtConfigTypeId, int(mm.FuelDeliveryID), AcesValue)
								if err != nil {
									return err
								}
							} else {
								return err
							}
						}
						// log.Print("CURT CONFIG ", curtConfigId)
						mm.CurtAttributeIDs = append(mm.CurtAttributeIDs, curtConfigId)
						tempCvg.ConfigVehicles = append(tempCvg.ConfigVehicles, mm)
					}
				}
				tempCvg.DiffConfigs = cvg.DiffConfigs                 //previous diffCOnfigs
				tempCvg.DiffConfigs = append(tempCvg.DiffConfigs, 20) // FUEL DELIVERY

				cvgsArray = append(cvgsArray, tempCvg)
			}
		} else {
			cvgsArray = append(cvgsArray, cvg)
		}
	}
	newCvgs = cvgsArray
	return err
}

func ReduceDriveType() error {
	var err error
	var cvgsArray []ConfigVehicleGroup

	for _, cvg := range newCvgs {
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
					//GetCurtTypeId
					curtConfigTypeId := configAttributeTypeMap[3]
					if curtConfigTypeId == 0 {
						log.Print("Missing Type: DriveType")
						//TODO CREATE  configType
						log.Panic("Missing type")
					}
					// log.Print("Config TYPE ", curtConfigTypeId)
					if mm.DriveTypeID > 0 {
						curtConfigId, err := getCurtConfigValue(curtConfigTypeId, int(mm.DriveTypeID))
						if err != nil {
							if err == sql.ErrNoRows {
								err = nil
								AcesValue, err := getAcesConfigurationValueName(3, int(mm.DriveTypeID))
								if err != nil {
									return err
								}
								curtConfigId, err = createCurtConfigValue(curtConfigTypeId, int(mm.DriveTypeID), AcesValue)
								if err != nil {
									return err
								}
							} else {
								return err
							}
						}
						// log.Print("CURT CONFIG ", curtConfigId)
						mm.CurtAttributeIDs = append(mm.CurtAttributeIDs, curtConfigId)
						tempCvg.ConfigVehicles = append(tempCvg.ConfigVehicles, mm)
					}
				}
				tempCvg.DiffConfigs = cvg.DiffConfigs                //previous diffCOnfigs
				tempCvg.DiffConfigs = append(tempCvg.DiffConfigs, 3) // DRIVE TYPE

				cvgsArray = append(cvgsArray, tempCvg)
			}
		} else {
			cvgsArray = append(cvgsArray, cvg)
		}
	}
	newCvgs = cvgsArray

	return err
}

func ReduceBodyNumDoors() error {
	var err error
	var cvgsArray []ConfigVehicleGroup

	for _, cvg := range newCvgs {
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
					//GetCurtTypeId
					curtConfigTypeId := configAttributeTypeMap[4]
					if curtConfigTypeId == 0 {
						log.Print("Missing Type: FuelType")
						//TODO CREATE  configType
						log.Panic("Missing type")
					}
					// log.Print("Config TYPE ", curtConfigTypeId)
					if mm.BodyNumDoorsID > 0 {
						curtConfigId, err := getCurtConfigValue(curtConfigTypeId, int(mm.BodyNumDoorsID))
						if err != nil {
							if err == sql.ErrNoRows {
								err = nil
								AcesValue, err := getAcesConfigurationValueName(4, int(mm.BodyNumDoorsID))
								if err != nil {
									return err
								}
								curtConfigId, err = createCurtConfigValue(curtConfigTypeId, int(mm.BodyNumDoorsID), AcesValue)
								if err != nil {
									return err
								}
							} else {
								return err
							}
						}
						// log.Print("CURT CONFIG ", curtConfigId)
						mm.CurtAttributeIDs = append(mm.CurtAttributeIDs, curtConfigId)
						tempCvg.ConfigVehicles = append(tempCvg.ConfigVehicles, mm)
					}
				}
				tempCvg.DiffConfigs = cvg.DiffConfigs                //previous diffCOnfigs
				tempCvg.DiffConfigs = append(tempCvg.DiffConfigs, 4) // NUM DOORS

				cvgsArray = append(cvgsArray, tempCvg)
			}
		} else {
			cvgsArray = append(cvgsArray, cvg)
		}
	}
	newCvgs = cvgsArray
	return err
}

func ReduceEngineVin() error {
	var err error
	var cvgsArray []ConfigVehicleGroup

	for _, cvg := range newCvgs {
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
					//GetCurtTypeId
					curtConfigTypeId := configAttributeTypeMap[16]
					if curtConfigTypeId == 0 {
						log.Print("Missing Type: Vin")
						//TODO CREATE  configType
						log.Panic("Missing type")
					}
					// log.Print("Config TYPE ", curtConfigTypeId)
					if mm.EngineVinID > 0 {
						// log.Print(mm.EngineVinID)
						curtConfigId, err := getCurtConfigValue(curtConfigTypeId, int(mm.EngineVinID))
						if err != nil {
							if err == sql.ErrNoRows {
								err = nil
								AcesValue, err := getAcesConfigurationValueName(16, int(mm.EngineVinID))
								if err != nil {
									return err
								}
								curtConfigId, err = createCurtConfigValue(curtConfigTypeId, int(mm.EngineVinID), AcesValue)
								if err != nil {
									return err
								}
							} else {
								return err
							}
						}

						mm.CurtAttributeIDs = append(mm.CurtAttributeIDs, curtConfigId)
						tempCvg.ConfigVehicles = append(tempCvg.ConfigVehicles, mm)
					}
				}
				tempCvg.DiffConfigs = cvg.DiffConfigs //previous diffCOnfigs
				tempCvg.DiffConfigs = append(tempCvg.DiffConfigs, 16)

				cvgsArray = append(cvgsArray, tempCvg)
			}
		} else {
			cvgsArray = append(cvgsArray, cvg)
		}

	}
	newCvgs = cvgsArray

	return err
}

func ReduceBodyType() error {
	var err error
	var cvgsArray []ConfigVehicleGroup

	for _, cvg := range newCvgs {
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
					//GetCurtTypeId
					curtConfigTypeId := configAttributeTypeMap[2]
					if curtConfigTypeId == 0 {
						log.Print("Missing Type: BodyType")
						//TODO CREATE  configType
						log.Panic("Missing type")
					}
					// log.Print("Config TYPE ", curtConfigTypeId)
					if mm.BodyTypeID > 0 {
						curtConfigId, err := getCurtConfigValue(curtConfigTypeId, int(mm.BodyTypeID))
						if err != nil {
							if err == sql.ErrNoRows {
								err = nil
								AcesValue, err := getAcesConfigurationValueName(2, int(mm.BodyTypeID))
								if err != nil {
									return err
								}
								curtConfigId, err = createCurtConfigValue(curtConfigTypeId, int(mm.BodyTypeID), AcesValue)
								if err != nil {
									return err
								}
							} else {
								return err
							}
						}
						// log.Print("CURT CONFIG ", curtConfigId)
						mm.CurtAttributeIDs = append(mm.CurtAttributeIDs, curtConfigId)
						tempCvg.ConfigVehicles = append(tempCvg.ConfigVehicles, mm)
					}
				}
				tempCvg.DiffConfigs = cvg.DiffConfigs //previous diffCOnfigs
				tempCvg.DiffConfigs = append(tempCvg.DiffConfigs, 2)

				cvgsArray = append(cvgsArray, tempCvg)
			}
		} else {
			cvgsArray = append(cvgsArray, cvg)
		}
	}
	newCvgs = cvgsArray

	return err
}

func ReduceAcesLiter() error {
	var err error
	var cvgsArray []ConfigVehicleGroup

	for _, cvg := range newCvgs {
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
					//GetCurtTypeId
					curtConfigTypeId := configAttributeTypeMap[106]
					if curtConfigTypeId == 0 {
						log.Print("Missing Type: AcesLIter")
						//TODO CREATE  configType
						log.Panic("Missing type")
					}
					// log.Print("Config TYPE ", curtConfigTypeId)
					if mm.AcesLiter > 0 {
						curtConfigId, err := getCurtConfigValue(curtConfigTypeId, int(mm.AcesLiter))
						if err != nil {
							if err == sql.ErrNoRows {
								err = nil
								AcesValue, err := getAcesConfigurationValueName(106, int(mm.AcesLiter))
								if err != nil {
									return err
								}
								curtConfigId, err = createCurtConfigValue(curtConfigTypeId, int(mm.AcesLiter), AcesValue)
								if err != nil {
									return err
								}
							} else {
								return err
							}
						}
						// log.Print("CURT CONFIG ", curtConfigId)
						mm.CurtAttributeIDs = append(mm.CurtAttributeIDs, curtConfigId)
						tempCvg.ConfigVehicles = append(tempCvg.ConfigVehicles, mm)
					}
				}
				tempCvg.DiffConfigs = cvg.DiffConfigs                  //previous diffCOnfigs
				tempCvg.DiffConfigs = append(tempCvg.DiffConfigs, 106) // 6 - Aces Liter - special case

				cvgsArray = append(cvgsArray, tempCvg)
			}
		} else {
			cvgsArray = append(cvgsArray, cvg)
		}
	}
	newCvgs = cvgsArray

	return err
}

// func ReduceAcesCC() error {
// 	var err error
// 	var cvgsArray []ConfigVehicleGroup

// 	for _, cvg := range newCvgs {
// 		//loop through fields
// 		var ftype []int
// 		for _, c := range cvg.ConfigVehicles {
// 			//fuel type
// 			ftype = append(ftype, int(c.AcesCC))
// 		}
// 		ftype = removeDuplicatesFromIntArray(ftype)

// 		//FUEL TYPE
// 		if len(ftype) > 1 {
// 			mmm := make(map[float64][]ConfigVehicleRaw)

// 			for _, c := range cvg.ConfigVehicles {
// 				// log.Print(c, mmm)
// 				mmm[c.AcesCC] = append(mmm[c.AcesCC], c)
// 			}
// 			// log.Print(mmm)
// 			for _, m := range mmm {
// 				var tempCvg ConfigVehicleGroup
// 				tempCvg.BaseID = cvg.BaseID
// 				tempCvg.SubID = cvg.SubID
// 				tempCvg.VehicleID = cvg.VehicleID
// 				for _, mm := range m {
// 					//GetCurtTypeId
// 					curtConfigTypeId := configAttributeTypeMap[206]
// 					if curtConfigTypeId == 0 {
// 						log.Print("Missing Type: AcesCC")
// 						//TODO CREATE  configType
// 						log.Panic("Missing type")
// 					}
// 					// log.Print("Config TYPE ", curtConfigTypeId)
// 					if mm.AcesCC > 0 {
// 						curtConfigId, err := getCurtConfigValue(curtConfigTypeId, int(mm.AcesCC))
// 						if err != nil {
// 							if err == sql.ErrNoRows {
// 								err = nil
// 								AcesValue, err := getAcesConfigurationValueName(206, int(mm.AcesCC))
// 								if err != nil {
// 									return err
// 								}
// 								curtConfigId, err = createCurtConfigValue(curtConfigTypeId, int(mm.AcesCC), AcesValue)
// 								if err != nil {
// 									return err
// 								}
// 							} else {
// 								return err
// 							}
// 						}
// 						// log.Print("CURT CONFIG ", curtConfigId)
// 						mm.CurtAttributeIDs = append(mm.CurtAttributeIDs, curtConfigId)
// 						tempCvg.ConfigVehicles = append(tempCvg.ConfigVehicles, mm)
// 					}
// 				}
// 				tempCvg.DiffConfigs = cvg.DiffConfigs                  //previous diffCOnfigs
// 				tempCvg.DiffConfigs = append(tempCvg.DiffConfigs, 206) // 6 - Aces Liter - special case

// 				cvgsArray = append(cvgsArray, tempCvg)
// 			}
// 		} else {
// 			cvgsArray = append(cvgsArray, cvg)
// 		}
// 	}
// 	newCvgs = cvgsArray

// 	return err
// }

// func ReduceAcesCid() error {
// 	var err error
// 	var cvgsArray []ConfigVehicleGroup

// 	for _, cvg := range newCvgs {
// 		//loop through fields
// 		var ftype []int
// 		for _, c := range cvg.ConfigVehicles {
// 			//fuel type
// 			ftype = append(ftype, int(c.AcesCID))
// 		}
// 		ftype = removeDuplicatesFromIntArray(ftype)

// 		//FUEL TYPE
// 		if len(ftype) > 1 {
// 			mmm := make(map[uint16][]ConfigVehicleRaw)

// 			for _, c := range cvg.ConfigVehicles {
// 				// log.Print(c, mmm)
// 				mmm[c.AcesCID] = append(mmm[c.AcesCID], c)
// 			}
// 			// log.Print(mmm)
// 			for _, m := range mmm {
// 				var tempCvg ConfigVehicleGroup
// 				tempCvg.BaseID = cvg.BaseID
// 				tempCvg.SubID = cvg.SubID
// 				tempCvg.VehicleID = cvg.VehicleID
// 				for _, mm := range m {
// 					//GetCurtTypeId
// 					curtConfigTypeId := configAttributeTypeMap[306]
// 					if curtConfigTypeId == 0 {
// 						log.Print("Missing Type: AcesCid")
// 						//TODO CREATE  configType
// 						log.Panic("Missing type")
// 					}
// 					// log.Print("Config TYPE ", curtConfigTypeId)
// 					if mm.AcesCID > 0 {
// 						curtConfigId, err := getCurtConfigValue(curtConfigTypeId, int(mm.AcesCID))
// 						if err != nil {
// 							if err == sql.ErrNoRows {
// 								err = nil
// 								AcesValue, err := getAcesConfigurationValueName(306, int(mm.AcesCID))
// 								if err != nil {
// 									return err
// 								}
// 								curtConfigId, err = createCurtConfigValue(curtConfigTypeId, int(mm.AcesCID), AcesValue)
// 								if err != nil {
// 									return err
// 								}
// 							} else {
// 								return err
// 							}
// 						}
// 						// log.Print("CURT CONFIG ", curtConfigId)
// 						mm.CurtAttributeIDs = append(mm.CurtAttributeIDs, curtConfigId)
// 						tempCvg.ConfigVehicles = append(tempCvg.ConfigVehicles, mm)
// 					}
// 				}
// 				tempCvg.DiffConfigs = cvg.DiffConfigs                  //previous diffCOnfigs
// 				tempCvg.DiffConfigs = append(tempCvg.DiffConfigs, 306) // 6 - Aces Liter - special case

// 				cvgsArray = append(cvgsArray, tempCvg)
// 			}
// 		} else {
// 			cvgsArray = append(cvgsArray, cvg)
// 		}
// 	}
// 	newCvgs = cvgsArray

// 	return err
// }

func ReduceAcesCC() error {
	var err error
	// var cvgsArray []ConfigVehicleGroup

	for _, cvg := range newCvgs {
		//loop through fields
		var ftype []int
		for _, c := range cvg.ConfigVehicles {
			//fuel type
			ftype = append(ftype, int(c.AcesCC))
		}
		ftype = removeDuplicatesFromIntArray(ftype)

		//FUEL TYPE
		if len(ftype) > 1 {
			log.Panic("Diff on CC")
		}
	}
	return err
}

func ReduceAcesCid() error {
	var err error
	// var cvgsArray []ConfigVehicleGroup

	for _, cvg := range newCvgs {
		//loop through fields
		var ftype []int
		for _, c := range cvg.ConfigVehicles {
			//fuel type
			ftype = append(ftype, int(c.AcesCID))
		}
		ftype = removeDuplicatesFromIntArray(ftype)

		//FUEL TYPE
		if len(ftype) > 1 {
			log.Panic("Diff on CID")
		}
	}
	return err
}

// func ReduceAcesBlock() error {
// 	var err error
// 	var cvgsArray []ConfigVehicleGroup

// 	for _, cvg := range newCvgs {
// 		//loop through fields
// 		var ftype []string
// 		for _, c := range cvg.ConfigVehicles {
// 			//fuel type
// 			ftype = append(ftype, c.AcesBlockType)
// 		}
// 		ftype = removeDuplicatesFromStringArray(ftype)

// 		//FUEL TYPE
// 		if len(ftype) > 1 {
// 			mmm := make(map[string][]ConfigVehicleRaw)

// 			for _, c := range cvg.ConfigVehicles {
// 				// log.Print(c, mmm)
// 				mmm[c.AcesBlockType] = append(mmm[c.AcesBlockType], c)
// 			}
// 			// log.Print(mmm)
// 			for _, m := range mmm {
// 				var tempCvg ConfigVehicleGroup
// 				tempCvg.BaseID = cvg.BaseID
// 				tempCvg.SubID = cvg.SubID
// 				tempCvg.VehicleID = cvg.VehicleID
// 				for _, mm := range m {
// 					//GetCurtTypeId
// 					curtConfigTypeId := configAttributeTypeMap[999]
// 					if curtConfigTypeId == 0 {
// 						log.Print("Missing Type: AcesBlockType")
// 						//TODO CREATE  configType
// 						log.Panic("Missing type")
// 					}
// 					// log.Print("Config TYPE ", curtConfigTypeId)
// 					//getAcesValueID from Value
// 					//get AcesValueId
// 					AcesValueId, err := getAcesConfigurationValueID(999, strconv.Itoa(int(mm.AcesBlockType)))
// 					if err != nil {
// 						log.Print(err)
// 						return err
// 					}
// 					log.Print("CurtTypeID", AcesValueId)
// if mm.AcesBlockType > 0{
// curtConfigId, err := getCurtConfigValue(curtConfigTypeId, AcesValueId)
// 					if err != nil {
// 						if err == sql.ErrNoRows {
// 							err = nil
// 							AcesValue, err := getAcesConfigurationValueName(999, AcesValueId)
// 							if err != nil {
// 								return err
// 							}
// 							curtConfigId, err = createCurtConfigValue(curtConfigTypeId, AcesValueId, AcesValue)
// 							if err != nil {
// 								return err
// 							}
// 						} else {
// 	return err
// }
// 					}
// 					// log.Print("CURT CONFIG ", curtConfigId)
// 					mm.CurtAttributeIDs = append(mm.CurtAttributeIDs, curtConfigId)
// 					tempCvg.ConfigVehicles = append(tempCvg.ConfigVehicles, mm)
// 				}}
// 				tempCvg.DiffConfigs = cvg.DiffConfigs                  //previous diffCOnfigs
// 				tempCvg.DiffConfigs = append(tempCvg.DiffConfigs, 999) // HELP

// 				cvgsArray = append(cvgsArray, tempCvg)
// 			}
// 		} else {
// 			cvgsArray = append(cvgsArray, cvg)
// 		}
// 	}
// 	newCvgs = cvgsArray
// 	return err
// }

func ReducePowerOutput() error {
	var err error
	var cvgsArray []ConfigVehicleGroup

	for _, cvg := range newCvgs {
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
					//GetCurtTypeId
					curtConfigTypeId := configAttributeTypeMap[25]
					if curtConfigTypeId == 0 {
						log.Print("Missing Type: PowerOutput")
						//TODO CREATE  configType
						log.Panic("Missing type")
					}
					// log.Print("Config TYPE ", curtConfigTypeId)
					if mm.PowerOutputID > 0 {
						curtConfigId, err := getCurtConfigValue(curtConfigTypeId, int(mm.PowerOutputID))
						if err != nil {
							if err == sql.ErrNoRows {
								err = nil
								AcesValue, err := getAcesConfigurationValueName(25, int(mm.PowerOutputID))
								if err != nil {
									return err
								}
								curtConfigId, err = createCurtConfigValue(curtConfigTypeId, int(mm.PowerOutputID), AcesValue)
								if err != nil {
									return err
								}
							} else {
								return err
							}
						}
						// log.Print("CURT CONFIG ", curtConfigId)
						mm.CurtAttributeIDs = append(mm.CurtAttributeIDs, curtConfigId)
						tempCvg.ConfigVehicles = append(tempCvg.ConfigVehicles, mm)
					}
				}
				tempCvg.DiffConfigs = cvg.DiffConfigs                 //previous diffCOnfigs
				tempCvg.DiffConfigs = append(tempCvg.DiffConfigs, 25) // 6 - Aces Liter - special case

				cvgsArray = append(cvgsArray, tempCvg)
			}
		} else {
			cvgsArray = append(cvgsArray, cvg)
		}
	}
	newCvgs = cvgsArray

	return err
}

//Unique - Crosses 4 Curt Configs w/ one Aces ID
func ReduceFuelDelConfig() error {
	var err error
	var cvgsArray []ConfigVehicleGroup

	for _, cvg := range newCvgs {
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
					//get fuel subtype and design ids
					var err error
					db, err := sql.Open("mysql", database.VcdbConnectionString())
					if err != nil {
						return err
					}
					defer db.Close()

					stmt, err := db.Prepare("select FuelDeliveryTypeID, FuelDeliverySubTypeID, FuelSystemControlTypeID, FuelSystemDesignID from FuelDeliveryConfig where FuelDeliveryConfigID = ?")
					if err != nil {
						return err
					}
					defer stmt.Close()
					var fdt, fdst, fsct, fsd int
					err = stmt.QueryRow(mm.FuelDelConfigID).Scan(&fdt, &fdst, &fsct, &fsd)
					if err != nil {
						log.Print("Err finding fuel del configID")
						return err
					}

					//Get several CurtType Ids
					curtConfigTypeIdSubType := configAttributeTypeMap[19]
					curtConfigTypeIdType := configAttributeTypeMap[20]
					curtConfigTypeIdControl := configAttributeTypeMap[21]
					curtConfigTypeIdDesign := configAttributeTypeMap[22]

					if mm.FuelDelConfigID > 0 {
						//SubType
						curtConfigId, err := getCurtConfigValue(curtConfigTypeIdSubType, fdt)
						if err != nil {
							if err == sql.ErrNoRows {
								err = nil
								AcesValue, err := getAcesConfigurationValueName(19, fdt)
								if err != nil {
									return err
								}
								curtConfigId, err = createCurtConfigValue(curtConfigTypeIdSubType, fdt, AcesValue)
								if err != nil {
									return err
								}
							} else {
								return err
							}
						}
						mm.CurtAttributeIDs = append(mm.CurtAttributeIDs, curtConfigId)

						//Type
						curtConfigId, err = getCurtConfigValue(curtConfigTypeIdType, fdst)
						if err != nil {
							if err == sql.ErrNoRows {
								err = nil
								AcesValue, err := getAcesConfigurationValueName(20, fdst)
								if err != nil {
									return err
								}
								curtConfigId, err = createCurtConfigValue(curtConfigTypeIdType, fdst, AcesValue)
								if err != nil {
									return err
								}
							} else {
								return err
							}
						}
						mm.CurtAttributeIDs = append(mm.CurtAttributeIDs, curtConfigId)

						//Control
						curtConfigId, err = getCurtConfigValue(curtConfigTypeIdControl, fsct)
						if err != nil {
							if err == sql.ErrNoRows {
								err = nil
								AcesValue, err := getAcesConfigurationValueName(21, fsct)
								if err != nil {
									return err
								}
								curtConfigId, err = createCurtConfigValue(curtConfigTypeIdControl, fsct, AcesValue)
								if err != nil {
									return err
								}
							} else {
								return err
							}
						}
						mm.CurtAttributeIDs = append(mm.CurtAttributeIDs, curtConfigId)

						//Design
						curtConfigId, err = getCurtConfigValue(curtConfigTypeIdDesign, fsd)
						if err != nil {
							if err == sql.ErrNoRows {
								err = nil
								AcesValue, err := getAcesConfigurationValueName(22, fsd)
								if err != nil {
									return err
								}
								curtConfigId, err = createCurtConfigValue(curtConfigTypeIdDesign, fsd, AcesValue)
								if err != nil {
									return err
								}
							} else {
								return err
							}
						}
						mm.CurtAttributeIDs = append(mm.CurtAttributeIDs, curtConfigId)

						//append all these configs
						tempCvg.ConfigVehicles = append(tempCvg.ConfigVehicles, mm)
					}
				}
				tempCvg.DiffConfigs = cvg.DiffConfigs //previous diffCOnfigs
				tempCvg.DiffConfigs = append(tempCvg.DiffConfigs, 19)
				tempCvg.DiffConfigs = append(tempCvg.DiffConfigs, 20)
				tempCvg.DiffConfigs = append(tempCvg.DiffConfigs, 21)
				tempCvg.DiffConfigs = append(tempCvg.DiffConfigs, 22)

				cvgsArray = append(cvgsArray, tempCvg)
			}
		} else {
			cvgsArray = append(cvgsArray, cvg)
		}
	}
	newCvgs = cvgsArray

	return err
}

func ReduceBodyStyle() error {
	var err error
	var cvgsArray []ConfigVehicleGroup

	for _, cvg := range newCvgs {
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
					//GetCurtTypeId
					if mm.BodyStyleConfigID > 0 {
						curtConfigTypeId := configAttributeTypeMap[999]
						if curtConfigTypeId == 0 {
							log.Print("Missing Type: BodyStyleConfigID")
							//TODO CREATE  configType
							log.Panic("Missing type")
						}
						// log.Print("Config TYPE ", curtConfigTypeId)

						curtConfigId, err := getCurtConfigValue(curtConfigTypeId, int(mm.BodyStyleConfigID))
						if err != nil {
							if err == sql.ErrNoRows {
								err = nil
								AcesValue, err := getAcesConfigurationValueName(999, int(mm.BodyStyleConfigID))
								if err != nil {
									return err
								}
								curtConfigId, err = createCurtConfigValue(curtConfigTypeId, int(mm.BodyStyleConfigID), AcesValue)
								if err != nil {
									return err
								}
							} else {
								return err
							}
						}
						// log.Print("CURT CONFIG ", curtConfigId)
						mm.CurtAttributeIDs = append(mm.CurtAttributeIDs, curtConfigId)
						tempCvg.ConfigVehicles = append(tempCvg.ConfigVehicles, mm)
					}
				}
				tempCvg.DiffConfigs = cvg.DiffConfigs                  //previous diffCOnfigs
				tempCvg.DiffConfigs = append(tempCvg.DiffConfigs, 999) // HELP

				cvgsArray = append(cvgsArray, tempCvg)
			}
		} else {
			cvgsArray = append(cvgsArray, cvg)
		}
	}
	newCvgs = cvgsArray
	return err
}

func ReduceValves() error {
	var err error
	var cvgsArray []ConfigVehicleGroup

	for _, cvg := range newCvgs {
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
					//GetCurtTypeId
					curtConfigTypeId := configAttributeTypeMap[40]
					if curtConfigTypeId == 0 {
						log.Print("Missing Type: Valves")
						//TODO CREATE  configType
						log.Panic("Missing type")
					}
					// log.Print("Config TYPE ", curtConfigTypeId)
					if mm.ValvesID > 0 {
						curtConfigId, err := getCurtConfigValue(curtConfigTypeId, int(mm.ValvesID))
						if err != nil {
							if err == sql.ErrNoRows {
								err = nil
								AcesValue, err := getAcesConfigurationValueName(40, int(mm.ValvesID))
								if err != nil {
									return err
								}
								curtConfigId, err = createCurtConfigValue(curtConfigTypeId, int(mm.ValvesID), AcesValue)
								if err != nil {
									return err
								}
							} else {
								return err
							}
						}
						// log.Print("CURT CONFIG ", curtConfigId)
						mm.CurtAttributeIDs = append(mm.CurtAttributeIDs, curtConfigId)
						tempCvg.ConfigVehicles = append(tempCvg.ConfigVehicles, mm)
					}
				}
				tempCvg.DiffConfigs = cvg.DiffConfigs                 //previous diffCOnfigs
				tempCvg.DiffConfigs = append(tempCvg.DiffConfigs, 40) // HELP

				cvgsArray = append(cvgsArray, tempCvg)
			}
		} else {
			cvgsArray = append(cvgsArray, cvg)
		}
	}
	newCvgs = cvgsArray

	return err
}

func ReduceCylHead() error {
	var err error
	var cvgsArray []ConfigVehicleGroup

	for _, cvg := range newCvgs {
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
					//GetCurtTypeId
					curtConfigTypeId := configAttributeTypeMap[12]
					if curtConfigTypeId == 0 {
						log.Print("Missing Type: Cylhead")
						//TODO CREATE  configType
						log.Panic("Missing type")
					}
					// log.Print("Config TYPE ", curtConfigTypeId)
					if mm.CylHeadTypeID > 0 {
						curtConfigId, err := getCurtConfigValue(curtConfigTypeId, int(mm.CylHeadTypeID))
						if err != nil {
							if err == sql.ErrNoRows {
								err = nil
								AcesValue, err := getAcesConfigurationValueName(12, int(mm.CylHeadTypeID))
								if err != nil {
									return err
								}
								curtConfigId, err = createCurtConfigValue(curtConfigTypeId, int(mm.CylHeadTypeID), AcesValue)
								if err != nil {
									return err
								}
							} else {
								return err
							}
						}
						// log.Print("CURT CONFIG ", curtConfigId)
						mm.CurtAttributeIDs = append(mm.CurtAttributeIDs, curtConfigId)
						tempCvg.ConfigVehicles = append(tempCvg.ConfigVehicles, mm)
					}
				}
				tempCvg.DiffConfigs = cvg.DiffConfigs                 //previous diffCOnfigs
				tempCvg.DiffConfigs = append(tempCvg.DiffConfigs, 12) // HELP

				cvgsArray = append(cvgsArray, tempCvg)
			}
		} else {
			cvgsArray = append(cvgsArray, cvg)
		}
	}
	newCvgs = cvgsArray

	return err
}

func ReduceEngineBase() error {
	var err error
	var cvgsArray []ConfigVehicleGroup

	for _, cvg := range newCvgs {
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
					//GetCurtTypeId
					curtConfigTypeId := configAttributeTypeMap[7]
					if curtConfigTypeId == 0 {
						log.Print("Missing Type: EngineBaseID")
						//TODO CREATE  configType
						log.Panic("Missing type")
					}
					// log.Print("Config TYPE ", curtConfigTypeId)
					if mm.EngineBaseID > 0 {
						curtConfigId, err := getCurtConfigValue(curtConfigTypeId, int(mm.EngineBaseID))
						if err != nil {
							if err == sql.ErrNoRows {
								err = nil
								AcesValue, err := getAcesConfigurationValueName(7, int(mm.EngineBaseID))
								if err != nil {
									return err
								}
								curtConfigId, err = createCurtConfigValue(curtConfigTypeId, int(mm.EngineBaseID), AcesValue)
								if err != nil {
									return err
								}
							} else {
								return err
							}
						}
						// log.Print("CURT CONFIG ", curtConfigId)
						mm.CurtAttributeIDs = append(mm.CurtAttributeIDs, curtConfigId)
						tempCvg.ConfigVehicles = append(tempCvg.ConfigVehicles, mm)
					}
				}
				tempCvg.DiffConfigs = cvg.DiffConfigs //previous diffCOnfigs
				tempCvg.DiffConfigs = append(tempCvg.DiffConfigs, 7)

				cvgsArray = append(cvgsArray, tempCvg)
			}
		} else {
			cvgsArray = append(cvgsArray, cvg)
		}
	}
	newCvgs = cvgsArray

	return err
}

func ReduceEngineConfig() error {
	var err error
	var cvgsArray []ConfigVehicleGroup

	for _, cvg := range newCvgs {
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
					//GetCurtTypeIds
					curtConfigTypeIdDesignation := configAttributeTypeMap[13]
					curtConfigTypeIdIgnitionSystem := configAttributeTypeMap[23]
					curtConfigTypeIdMfr := configAttributeTypeMap[14]
					curtConfigTypeIdVersion := configAttributeTypeMap[15]

					// curtConfigTypeId := configAttributeTypeMap[7]
					// if curtConfigTypeId == 0 {
					// 	log.Print("Missing Type: EngineConfig")
					// 	//TODO CREATE  configType
					// 	log.Panic("Missing type")
					// }
					// log.Print("Config TYPE ", curtConfigTypeId)
					if mm.EngineConfigID > 0 {

						var err error
						db, err := sql.Open("mysql", database.VcdbConnectionString())
						if err != nil {
							return err
						}
						defer db.Close()

						stmt, err := db.Prepare("select EngineDesignationID, IgnitionSystemTypeID, EngineMfrID, EngineVersionID from EngineConfig where EngineConfigID = ?")
						if err != nil {
							return err
						}
						defer stmt.Close()
						var des, ignit, mfr, version int
						err = stmt.QueryRow(mm.EngineConfigID).Scan(&des, &ignit, &mfr, &version)
						if err != nil {
							return err
						}

						//Designation
						curtConfigId, err := getCurtConfigValue(curtConfigTypeIdDesignation, des)
						if err != nil {
							if err == sql.ErrNoRows {
								err = nil
								AcesValue, err := getAcesConfigurationValueName(13, des)
								if err != nil {
									return err
								}
								curtConfigId, err = createCurtConfigValue(curtConfigTypeIdDesignation, des, AcesValue)
								if err != nil {
									return err
								}
							} else {
								return err
							}
						}
						mm.CurtAttributeIDs = append(mm.CurtAttributeIDs, curtConfigId)

						//Ignition
						curtConfigId, err = getCurtConfigValue(curtConfigTypeIdIgnitionSystem, ignit)
						if err != nil {
							if err == sql.ErrNoRows {
								err = nil
								AcesValue, err := getAcesConfigurationValueName(23, ignit)
								if err != nil {
									return err
								}
								curtConfigId, err = createCurtConfigValue(curtConfigTypeIdIgnitionSystem, ignit, AcesValue)
								if err != nil {
									return err
								}
							} else {
								return err
							}
						}
						mm.CurtAttributeIDs = append(mm.CurtAttributeIDs, curtConfigId)

						//Mfr
						curtConfigId, err = getCurtConfigValue(curtConfigTypeIdMfr, mfr)
						if err != nil {
							if err == sql.ErrNoRows {
								err = nil
								AcesValue, err := getAcesConfigurationValueName(14, mfr)
								if err != nil {
									return err
								}
								curtConfigId, err = createCurtConfigValue(curtConfigTypeIdMfr, mfr, AcesValue)
								if err != nil {
									return err
								}
							} else {
								return err
							}
						}
						mm.CurtAttributeIDs = append(mm.CurtAttributeIDs, curtConfigId)

						//Version
						curtConfigId, err = getCurtConfigValue(curtConfigTypeIdVersion, version)
						if err != nil {
							if err == sql.ErrNoRows {
								err = nil
								AcesValue, err := getAcesConfigurationValueName(15, version)
								if err != nil {
									return err
								}
								curtConfigId, err = createCurtConfigValue(curtConfigTypeIdVersion, version, AcesValue)
								if err != nil {
									return err
								}
							} else {
								return err
							}
						}
						mm.CurtAttributeIDs = append(mm.CurtAttributeIDs, curtConfigId)

						//append to vehicleConfig array
						tempCvg.ConfigVehicles = append(tempCvg.ConfigVehicles, mm)
					}
				}

				tempCvg.DiffConfigs = cvg.DiffConfigs //previous diffCOnfigs
				tempCvg.DiffConfigs = append(tempCvg.DiffConfigs, 13)
				tempCvg.DiffConfigs = append(tempCvg.DiffConfigs, 14)
				tempCvg.DiffConfigs = append(tempCvg.DiffConfigs, 15)
				tempCvg.DiffConfigs = append(tempCvg.DiffConfigs, 23)

				cvgsArray = append(cvgsArray, tempCvg)
			}
		} else {
			cvgsArray = append(cvgsArray, cvg)
		}
	}
	newCvgs = cvgsArray

	return err
}

//Begin UTILITY FUNCTIONS
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

func getCurtConfigValue(curtConfigTypeId, aaiaConfigValueId int) (int, error) {
	var err error
	strArray := []string{strconv.Itoa(curtConfigTypeId), strconv.Itoa(aaiaConfigValueId)}
	caStr := strings.Join(strArray, ":")
	CurtValueId := configAttributeMap[caStr]
	if CurtValueId == 0 {
		err = sql.ErrNoRows
	}
	return CurtValueId, err
}

func createCurtConfigValue(CurtConfigTypeId, AcesValueId int, AcesValue string) (int, error) {
	var err error
	var CurtConfigId int
	db, err := sql.Open("mysql", database.ConnectionString())
	if err != nil {
		return CurtConfigId, err
	}
	defer db.Close()
	//`insert into ConfigAttribute(ConfigAttributeTypeID, parentID, vcdbID, value) values(?,0,?,?)`
	stmt, err := db.Prepare(insertCurtConfigStmt)
	if err != nil {
		return CurtConfigId, err
	}
	defer stmt.Close()
	res, err := stmt.Exec(CurtConfigTypeId, AcesValueId, AcesValue)
	if err != nil {
		return CurtConfigId, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return CurtConfigId, err
	}
	CurtConfigId = int(id)
	//insert into map
	strArray := []string{strconv.Itoa(CurtConfigTypeId), strconv.Itoa(AcesValueId)}
	caStr := strings.Join(strArray, ":")
	configAttributeMap[caStr] = CurtConfigId

	return CurtConfigId, err
}

func getAcesConfigurationValueID(aaiaConfigTypeID int, value string) (string, error) {
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
	sqlStmt := "select " + idField + " from " + table + " where " + valueField + " = " + value
	// log.Print("stmt ", sqlStmt)
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

func getAcesConfigurationValueName(aaiaConfigTypeID, aaiaConfigValueID int) (string, error) {
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
		valueField = "CONCAT(Liter, ' Liter ', BlockType,'-',Cylinders)"
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
	sqlStmt := "select " + valueField + " from " + table + " where " + idField + " = " + strconv.Itoa(aaiaConfigValueID)
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
			log.Print("ERR", err)
			err = nil
		}
		return valueName, err
	}
	return valueName, err
}
