package importer

import (
	// "database/sql"
	// "encoding/csv"
	// "github.com/curt-labs/polkImporter/helpers/database"
	_ "github.com/go-sql-driver/mysql"
	// "log"
	// "os"
	// "reflect"
	// "runtime"
	// "strconv"
	// "strings"
)

type BaseVehicleGroup struct {
	ID       int
	Vehicles []Vehicle
}

type Vehicle struct {
	ID          int
	PartNumbers []int
}

func Run(filename string, headerLines int, useOldPartNumbers bool, insertMissingData bool) error {
	var err error
	err = CaptureCsv(filename, headerLines)
	if err != nil {
		return err
	}
	return err
}

// func Csv(filename string, headerLines int, useOldPartNumbers bool, insertMissingData bool) {
// 	csvfile, err := os.Open(filename)
// 	if err != nil {
// 		return
// 	}
// 	defer csvfile.Close()

// 	reader := csv.NewReader(csvfile)
// 	reader.FieldsPerRecord = -1 //flexible number of fields

// 	lines, err := reader.ReadAll()
// 	if err != nil {
// 		return
// 	}

// 	lines = lines[headerLines:] //axe header

// 	for _, line := range lines {
// 		AAIABaseID, err := strconv.Atoi(line[6])
// 		VehicleID, err := strconv.Atoi(line[5])
// 		PartNumber, err := strconv.Atoi(line[35])
// 	}

// }
