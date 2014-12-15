package importer

import (
	"encoding/csv"
	"github.com/curt-labs/polkImporter/helpers/database"
	"gopkg.in/mgo.v2"
	"os"
	"strconv"
)

type CsvVehicle struct {
	Make                       string  `bson:"make,omitempty"`
	Model                      string  `bson:"model,omitempty"`
	SubModel                   string  `bson:"submodel,omitempty"`
	Year                       string  `bson:"year,omitempty"`
	GVW                        uint8   `bson:"gvw,omitempty"`
	VehicleID                  int     `bson:"vehicleId,omitempty"`
	BaseVehicleID              int     `bson:"baseVehicleId,omitempty"`
	YearID                     int     `bson:"yearId,omitempty"`
	MakeID                     int     `bson:"makeId,omitempty"`
	ModelID                    int     `bson:"modelId,omitempty"`
	SubmodelID                 int     `bson:"submodelId,omitempty"`
	VehicleTypeID              uint8   `bson:"vehicleTypeId,omitempty"`
	FuelTypeID                 uint8   `bson:"fuelTypeId,omitempty"`       // 6 FuelType
	FuelDeliveryID             uint8   `bson:"fuelDeliveryId,omitempty"`   //20 FuelDeliveryType
	AcesLiter                  float64 `bson:"acesLiter,omitempty"`        //EngineBase.Liter
	AcesCC                     float64 `bson:"acesCc,omitempty"`           //EngineBase.CC
	AcesCID                    uint16  `bson:"acesCid,omitempty"`          //EngineBase.CID
	AcesCyl                    uint8   `bson:"acesCyl,omitempty"`          //EngineBase.Cylinders
	AcesBlockType              string  `bson:"acesBlockType,omitempty"`    //EngineBase.BlockType
	AspirationID               uint8   `bson:"aspirationId,omitempty"`     // 8 Aspiration
	DriveTypeID                uint8   `bson:"driveId,omitempty"`          // 3 DriveType
	BodyTypeID                 uint8   `bson:"bodyTypeId,omitempty"`       // 2 BodyType
	BodyNumDoorsID             uint8   `bson:"bodyNumDoors,omitempty"`     // 4 BodyNumDoors
	EngineVinID                uint8   `bson:"engineVin,omitempty"`        // 16 EngineVIN
	RegionID                   uint8   `bson:"regionId,omitempty"`         //Region
	PowerOutputID              uint16  `bson:"powerOutputId,omitempty"`    // 25 PowerOutput
	FuelDelConfigID            uint8   `bson:"fuelDelConfigId,omitempty"`  //FuelDeliveryConfig
	BodyStyleConfigID          uint8   `bson:"bodyStyeConfigId,omitempty"` //BodyStyleConfig
	ValvesID                   uint8   `bson:"valvesId,omitempty"`         // 40 Valves
	CylHeadTypeID              uint8   `bson:"cylHeadTypeId,omitempty"`    // 12 CylinderHeadType
	BlockType                  string  `bson:"blockType,omitempty"`        //EngineBase.BlockType
	EngineBaseID               uint16  `bson:"engineBaseId,omitempty"`     // 7 EngineBase
	EngineConfigID             uint16  `bson:"engineConfigId,omitempty"`   //EngineConfig
	PCDBPartTerminologyName    string  `bson:"pcdbPartTerminalogyName,omitempty"`
	Position                   []byte  `bson:"position,omitempty"`
	PartNumber                 string  `bson:"partNumber,omitempty"`
	PartDesc                   string  `bson:"partDesc,omitempty"`
	VehicleCount               int     `bson:"vehicleCount,omitempty"`
	DistributedPartOpportunity int     `bson:"distributedPartOpportunity,omitempty"`
	MaximumPartOpportunity     int     `bson:"maximumPartOpportunity,omitempty"`
}

func CaptureCsv(filename string, headerLines int) error {
	var err error

	session, err := mgo.Dial(database.MongoConnectionString().Addrs[0])
	defer session.Close()

	csvfile, err := os.Open(filename)
	if err != nil {
		return err
	}
	collection := session.DB("importer").C("ariesTest")
	err = collection.DropCollection()
	if err != nil {
		return err
	}

	defer csvfile.Close()
	reader := csv.NewReader(csvfile)
	reader.FieldsPerRecord = -1 //flexible number of fields

	lines, err := reader.ReadAll()
	if err != nil {
		return err
	}

	lines = lines[headerLines:] //axe header

	for _, line := range lines {
		//get values
		Make := line[0]
		Model := line[1]
		SubModel := line[2]
		Year := line[3]
		GVW, err := strconv.Atoi(line[4])
		VehicleID, err := strconv.Atoi(line[5])
		BaseVehicleID, err := strconv.Atoi(line[6])
		YearID, err := strconv.Atoi(line[7])
		MakeID, err := strconv.Atoi(line[8])
		ModelID, err := strconv.Atoi(line[9])
		SubmodelID, err := strconv.Atoi(line[10])
		VehicleTypeID, err := strconv.Atoi(line[11])
		FuelTypeID, err := strconv.Atoi(line[12])
		FuelDeliveryID, err := strconv.Atoi(line[13])
		AcesLiter, err := strconv.ParseFloat(line[14], 64)
		AcesCC, err := strconv.ParseFloat(line[15], 64)
		AcesCID, err := strconv.Atoi(line[16])
		AcesCyl, err := strconv.Atoi(line[17])
		AcesBlockType := line[18]
		AspirationID, err := strconv.Atoi(line[19])
		DriveTypeID, err := strconv.Atoi(line[20])
		BodyTypeID, err := strconv.Atoi(line[21])
		BodyNumDoorsID, err := strconv.Atoi(line[22])
		EngineVinID, err := strconv.Atoi(line[23])
		RegionID, err := strconv.Atoi(line[24])
		PowerOutputID, err := strconv.Atoi(line[25])
		FuelDelConfigID, err := strconv.Atoi(line[26])
		BodyStyleConfigID, err := strconv.Atoi(line[27])
		ValvesID, err := strconv.Atoi(line[28])
		CylHeadTypeID, err := strconv.Atoi(line[29])
		BlockType := line[30]
		EngineBaseID, err := strconv.Atoi(line[31])
		EngineConfigID, err := strconv.Atoi(line[32])
		PCDBPartTerminologyName := line[33]
		Position := []byte(line[34])
		PartNumber := line[35]
		PartDesc := line[36]
		VehicleCount, err := strconv.Atoi(line[37])
		DistributedPartOpportunity, err := strconv.Atoi(line[38])
		MaximumPartOpportunity, err := strconv.Atoi(line[39])
		if err != nil {
			return err
		}
		//assign to struct
		c := CsvVehicle{
			Make:                       Make,
			Model:                      Model,
			SubModel:                   SubModel,
			Year:                       Year,
			GVW:                        uint8(GVW),
			VehicleID:                  VehicleID,
			BaseVehicleID:              BaseVehicleID,
			YearID:                     YearID,
			MakeID:                     MakeID,
			ModelID:                    ModelID,
			SubmodelID:                 SubmodelID,
			VehicleTypeID:              uint8(VehicleTypeID),
			FuelTypeID:                 uint8(FuelTypeID),
			FuelDeliveryID:             uint8(FuelDeliveryID),
			AcesLiter:                  AcesLiter,
			AcesCC:                     AcesCC,
			AcesCID:                    uint16(AcesCID),
			AcesCyl:                    uint8(AcesCyl),
			AcesBlockType:              AcesBlockType,
			AspirationID:               uint8(AspirationID),
			DriveTypeID:                uint8(DriveTypeID),
			BodyTypeID:                 uint8(BodyTypeID),
			BodyNumDoorsID:             uint8(BodyNumDoorsID),
			EngineVinID:                uint8(EngineVinID),
			RegionID:                   uint8(RegionID),
			PowerOutputID:              uint16(PowerOutputID),
			FuelDelConfigID:            uint8(FuelDelConfigID),
			BodyStyleConfigID:          uint8(BodyStyleConfigID),
			ValvesID:                   uint8(ValvesID),
			CylHeadTypeID:              uint8(CylHeadTypeID),
			BlockType:                  BlockType,
			EngineBaseID:               uint16(EngineBaseID),
			EngineConfigID:             uint16(EngineConfigID),
			PCDBPartTerminologyName:    PCDBPartTerminologyName,
			Position:                   Position,
			PartNumber:                 PartNumber,
			PartDesc:                   PartDesc,
			VehicleCount:               VehicleCount,
			DistributedPartOpportunity: DistributedPartOpportunity,
			MaximumPartOpportunity:     MaximumPartOpportunity,
		}
		err = collection.Insert(&c)
	}
	return err
}
