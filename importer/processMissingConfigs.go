package importer

import (
	// "github.com/curt-labs/polkImporter/helpers/database"
	_ "github.com/go-sql-driver/mysql"
	// "gopkg.in/mgo.v2"
	// "gopkg.in/mgo.v2/bson"

	// "database/sql"
	"encoding/csv"
	// "errors"
	"log"
	"os"
	"reflect"
	"strconv"
	// "strings"
)

type ConfigTypeAndValue struct {
	ConfigID     int
	ConfigTypeID int
}

//TODO - Experimental; Do Not Use (yet?)

func ProcessMissingConfigs() error {
	vMap, err := GetMissingConfigsCsv()
	if err != nil {
		return err
	}
	err = CompareConfigs(vMap)

	return err
}

func GetMissingConfigsCsv() (map[string][]map[string][]ConfigTypeAndValue, error) {

	var ctav ConfigTypeAndValue

	vMap := make(map[string][]map[string][]ConfigTypeAndValue)
	pMap := make(map[string][]ConfigTypeAndValue)
	missingConfigs, err := os.Open("exports/MissingConfigs.csv")
	if err != nil {
		return vMap, err
	}
	csvFile := csv.NewReader(missingConfigs)
	if err != nil {
		return vMap, err
	}
	lines, err := csvFile.ReadAll()
	if err != nil {
		return vMap, err
	}
	lines = lines[1:] //axe header
	for _, line := range lines {

		ctav.ConfigID, err = strconv.Atoi(line[4])
		ctav.ConfigTypeID, err = strconv.Atoi(line[5])

		pMap[line[3]] = append(pMap[line[3]], ctav)

		vBaseSub := line[0] + ":" + line[1] + ":" + line[2]

		vMap[vBaseSub] = append(vMap[vBaseSub], pMap)

	}
	// log.Print(vMap)
	return vMap, err
}

func CompareConfigs(vMap map[string][]map[string][]ConfigTypeAndValue) error {
	var err error
	// sql := "insert into vcdb_VehiclePart"

	for _, pMap := range vMap { //map of vehicleID:BaseID:SubmodelID to parts+configs
		match := true
		for j, cons := range pMap { //map of partNumber to ConfigTypeAndValue struct
			if j > 1 {
				match := reflect.DeepEqual(cons, pMap[j-1])
				if match == false {
					continue
				}
			}
		}
		if match == true {
			//add parts to submodel
			log.Print(pMap)
		} else {
			log.Print(match)
			//note config difference
		}

	}
	return err
}
