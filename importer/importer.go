package importer

import (
	"log"
)

func Run(filename string, headerLines int, useOldPartNumbers bool, insertMissingData bool) error {
	log.Print("Running")
	var err error
	err = CaptureCsv(filename, headerLines)
	if err != nil {
		return err
	}
	return err
}

func RunAfterCsvMongoed() error {
	bvs, err := MongoToBase()
	if err != nil {
		return err
	}
	log.Print("Total baseVehicles to check: ", len(bvs))

	bases := BvgArray(bvs)

	baseIds, err := AuditBaseVehicles(bases)
	log.Print("Number of groups of base models to pass into submodels: ", len(baseIds))

	sbs, err := MongoToSubmodel(baseIds)
	if err != nil {
		return err
	}
	log.Print("Total submodels to check: ", len(sbs))

	subs := SmgArray(sbs)
	subIds, err := AuditSubmodels(subs)
	if err != nil {
		return err
	}
	log.Print("Number of groups of submodels to pass into configurations: ", len(subIds))

	configVehicles, err := MongoToConfig(subIds)
	log.Print("Total vehicles to check: ", len(configVehicles))

	cons := CgArray(configVehicles)
	log.Print("Number of vehicles (grouped by VehicleID) to audit the configurations of: ", len(cons))

	err = AuditConfigs(cons)
	if err != nil {
		return err
	}

	return err
}
