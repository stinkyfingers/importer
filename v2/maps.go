package v2

import (
	"github.com/curt-labs/polkImporter/helpers/database"
	_ "github.com/go-sql-driver/mysql"

	"database/sql"
	"strconv"
)

var (
	partMapStmt                = "select oldPartNumber, partID from Part where oldPartNumber is not null"
	baseMapStmt                = "select AAIABaseVehicleID, ID from BaseVehicle where AAIABaseVehicleID is not null and AAIABaseVehicleID > 0"
	makeMapStmt                = "select AAIAMakeID, ID from vcdb_Make where AAIAMakeID > 0"
	modelMapStmt               = "select AAIAModelID, ID from vcdb_Model where AAIAModelID > 0"
	baseVehicleToVehicleMap    = "select BaseVehicleID, ID from vcdb_Vehicle where (SubmodelID = 0 or SubmodelID is null) and (ConfigID = 0 or ConfigID is null)"
	vehiclePartStmt            = "select VehicleID, PartNumber from vcdb_VehiclePart where VehicleID is not null and PartNumber is not null"
	subMapStmt                 = "select AAIASubmodelID, ID from Submodel where AAIASubmodelID > 0 and AAIASubmodelID is not null"
	submodelToVehicleMapStmt   = "select SubmodelID, BaseVehicleID, ID from vcdb_Vehicle where (ConfigID = 0 or ConfigID is null) and SubmodelID > 0"
	configAttributeTypeMapStmt = "select AcesTypeID, ID from ConfigAttributeType where AcesTypeID is not null and AcesTypeID > 0"
	configAttributeMapStmt     = "select ID, ConfigAttributeTypeID, vcdbID from ConfigAttribute where vcdbID > 0 and vcdbID is not null"
)

func getConfigAttributeMap() (map[string]int, error) {
	var err error
	aMap := make(map[string]int)
	db, err := sql.Open("mysql", database.ConnectionString())
	if err != nil {
		return aMap, err
	}
	defer db.Close()

	stmt, err := db.Prepare(configAttributeMapStmt)
	if err != nil {
		return aMap, err
	}
	defer stmt.Close()
	res, err := stmt.Query()
	var i, c, v int

	for res.Next() {
		err = res.Scan(&i, &c, &v)
		if err != nil {
			return aMap, err
		}
		aMap[strconv.Itoa(c)+":"+strconv.Itoa(v)] = i
	}
	return aMap, err
}

func getConfigAttriguteTypeMap() (map[int]int, error) {
	var err error
	aMap := make(map[int]int)
	db, err := sql.Open("mysql", database.ConnectionString())
	if err != nil {
		return aMap, err
	}
	defer db.Close()

	stmt, err := db.Prepare(configAttributeTypeMapStmt)
	if err != nil {
		return aMap, err
	}
	defer stmt.Close()
	res, err := stmt.Query()
	var a, i int

	for res.Next() {
		err = res.Scan(&a, &i)
		if err != nil {
			return aMap, err
		}
		aMap[a] = i
	}
	return aMap, err
}

func getSubmodelBaseToVehicleMap() (map[string]int, error) {
	var err error
	sbMap := make(map[string]int)
	db, err := sql.Open("mysql", database.ConnectionString())
	if err != nil {
		return sbMap, err
	}
	defer db.Close()

	stmt, err := db.Prepare(submodelToVehicleMapStmt)
	if err != nil {
		return sbMap, err
	}
	defer stmt.Close()
	res, err := stmt.Query()
	var s, b, v int

	for res.Next() {
		err = res.Scan(&s, &b, &v)
		if err != nil {
			return sbMap, err
		}
		sbMap[strconv.Itoa(s)+":"+strconv.Itoa(b)] = v
	}
	return sbMap, err
}

func getSubMap() (map[int]int, error) {
	var err error
	subMap := make(map[int]int)
	db, err := sql.Open("mysql", database.ConnectionString())
	if err != nil {
		return subMap, err
	}
	defer db.Close()

	stmt, err := db.Prepare(subMapStmt)
	if err != nil {
		return subMap, err
	}
	defer stmt.Close()
	res, err := stmt.Query()
	var p int
	var o int
	for res.Next() {
		err = res.Scan(&o, &p)
		if err != nil {
			return subMap, err
		}
		subMap[o] = p
	}
	return subMap, err
}

func getVehiclePartArray() ([]string, error) {
	var err error
	var vp []string
	db, err := sql.Open("mysql", database.ConnectionString())
	if err != nil {
		return vp, err
	}
	defer db.Close()

	stmt, err := db.Prepare(vehiclePartStmt)
	if err != nil {
		return vp, err
	}
	defer stmt.Close()
	res, err := stmt.Query()
	var p int
	var o int
	for res.Next() {
		err = res.Scan(&o, &p)
		if err != nil {
			return vp, err
		}

		vp = append(vp, strconv.Itoa(o)+":"+strconv.Itoa(p))
	}
	return vp, err
}

// func getVehiclePartMap() (map[int]int, error) {
// 	var err error
// 	vpMap := make(map[int]int)
// 	db, err := sql.Open("mysql", database.ConnectionString())
// 	if err != nil {
// 		return vpMap, err
// 	}
// 	defer db.Close()

// 	stmt, err := db.Prepare(vehiclePartStmt)
// 	if err != nil {
// 		return vpMap, err
// 	}
// 	defer stmt.Close()
// 	res, err := stmt.Query()
// 	var p int
// 	var o int
// 	for res.Next() {
// 		err = res.Scan(&o, &p)
// 		if err != nil {
// 			return vpMap, err
// 		}
// 		vpMap[o] = p
// 	}
// 	return vpMap, err
// }

func getBaseVehicleToVehicleMap() (map[int]int, error) {
	var err error
	bvMap := make(map[int]int)
	db, err := sql.Open("mysql", database.ConnectionString())
	if err != nil {
		return bvMap, err
	}
	defer db.Close()

	stmt, err := db.Prepare(baseVehicleToVehicleMap)
	if err != nil {
		return bvMap, err
	}
	defer stmt.Close()
	res, err := stmt.Query()
	var p int
	var o int
	for res.Next() {
		err = res.Scan(&o, &p)
		if err != nil {
			return bvMap, err
		}
		bvMap[o] = p
	}
	return bvMap, err
}

func getMakeMap() (map[int]int, error) {
	var err error
	makeMap := make(map[int]int)
	db, err := sql.Open("mysql", database.ConnectionString())
	if err != nil {
		return makeMap, err
	}
	defer db.Close()

	stmt, err := db.Prepare(makeMapStmt)
	if err != nil {
		return makeMap, err
	}
	defer stmt.Close()
	res, err := stmt.Query()
	var p int
	var o int
	for res.Next() {
		err = res.Scan(&o, &p)
		if err != nil {
			return makeMap, err
		}
		makeMap[o] = p
	}
	return makeMap, err
}

func getModelMap() (map[int]int, error) {
	var err error
	modelMap := make(map[int]int)
	db, err := sql.Open("mysql", database.ConnectionString())
	if err != nil {
		return modelMap, err
	}
	defer db.Close()

	stmt, err := db.Prepare(modelMapStmt)
	if err != nil {
		return modelMap, err
	}
	defer stmt.Close()
	res, err := stmt.Query()
	var p int
	var o int
	for res.Next() {
		err = res.Scan(&o, &p)
		if err != nil {
			return modelMap, err
		}
		modelMap[o] = p
	}
	return modelMap, err
}

func getPartMap() (map[string]int, error) {
	var err error
	partMap := make(map[string]int)
	db, err := sql.Open("mysql", database.ConnectionString())
	if err != nil {
		return partMap, err
	}
	defer db.Close()

	stmt, err := db.Prepare(partMapStmt)
	if err != nil {
		return partMap, err
	}
	defer stmt.Close()
	res, err := stmt.Query()
	var p int
	var o string
	for res.Next() {
		err = res.Scan(&o, &p)
		if err != nil {
			return partMap, err
		}

		// if o != "" {
		partMap[o] = p
		// }
	}
	return partMap, err
}

func getBaseMap() (map[int]int, error) {
	var err error
	baseMap := make(map[int]int)
	db, err := sql.Open("mysql", database.ConnectionString())
	if err != nil {
		return baseMap, err
	}
	defer db.Close()

	stmt, err := db.Prepare(baseMapStmt)
	if err != nil {
		return baseMap, err
	}
	defer stmt.Close()
	res, err := stmt.Query()
	var o, p int
	for res.Next() {
		err = res.Scan(&o, &p)
		if err != nil {
			return baseMap, err
		}
		// if o > 0 {
		baseMap[o] = p
		// }
	}
	return baseMap, err
}

func GetArrayOfAAIABaseVehicleIDsForWhichThereExistsACurtBaseID() ([]int, error) {
	var err error
	var id int
	var arrayIDs []int
	db, err := sql.Open("mysql", database.ConnectionString())
	if err != nil {
		return arrayIDs, err
	}
	defer db.Close()

	stmt, err := db.Prepare(arrayOfAAIABaseIDs)
	if err != nil {
		return arrayIDs, err
	}
	defer stmt.Close()
	res, err := stmt.Query()
	if err != nil {
		return arrayIDs, err
	}
	for res.Next() {
		err = res.Scan(&id)
		if err != nil {
			return arrayIDs, err
		}
		arrayIDs = append(arrayIDs, id)
	}
	return arrayIDs, err
}

func GetArrayOfOldPartNumbersForWhichThereExistsACurtPartID() ([]string, error) {
	var err error
	var id string
	var arrayIDs []string
	db, err := sql.Open("mysql", database.ConnectionString())
	if err != nil {
		return arrayIDs, err
	}
	defer db.Close()

	stmt, err := db.Prepare(arrayOfOldPartNumbers)
	if err != nil {
		return arrayIDs, err
	}
	defer stmt.Close()
	res, err := stmt.Query()
	if err != nil {
		return arrayIDs, err
	}
	for res.Next() {
		err = res.Scan(&id)
		if err != nil {
			return arrayIDs, err
		}
		arrayIDs = append(arrayIDs, id)
	}
	return arrayIDs, err
}
