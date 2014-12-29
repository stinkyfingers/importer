package configs

import (
	"github.com/curt-labs/polkImporter/helpers/database"
	_ "github.com/go-sql-driver/mysql"

	"database/sql"
	"os"
	"strconv"
	"strings"
)

var (
	configAttributeTypeMapStmt = "select AcesTypeID, ID from ConfigAttributeType where AcesTypeID is not null and AcesTypeID > 0"
	configAttributeMapStmt     = "select ID, ConfigAttributeTypeID, vcdbID from ConfigAttribute where vcdbID > 0 and vcdbID is not null"
	baseMapStmt                = "select AAIABaseVehicleID, ID from BaseVehicle where AAIABaseVehicleID is not null and AAIABaseVehicleID > 0"
	subMapStmt                 = "select AAIASubmodelID, ID from Submodel where AAIASubmodelID > 0 and AAIASubmodelID is not null"
	partMapStmt                = "select oldPartNumber, partID from Part where oldPartNumber is not null"
	vehicleOldPartArrayStmt    = `select vp.VehicleID, p.oldPartNumber from vcdb_VehiclePart  as vp
		join Part as p on p.partID = vp.PartNumber
		group by concat(VehicleID, PartNumber)`
	submodelInVehicleTableMapStmt = `select v.ID, v.BaseVehicleID, v.SubModelID from vcdb_Vehicle  as v 
		where (v.SubmodelID  > 0 and v.SubmodelID is not null)
		and (v.ConfigID = 0 or v.ConfigID is null)`
)

func getSubmodelInVehicleTableMap() (map[string]int, error) {
	var err error
	sMap := make(map[string]int)
	db, err := sql.Open("mysql", database.ConnectionString())
	if err != nil {
		return sMap, err
	}
	defer db.Close()

	stmt, err := db.Prepare(submodelInVehicleTableMapStmt)
	if err != nil {
		return sMap, err
	}
	defer stmt.Close()
	var v, b, s int
	res, err := stmt.Query()
	if err != nil {
		return sMap, err
	}
	for res.Next() {
		err = res.Scan(&v, &b, &s)
		if err != nil {
			return sMap, err
		}
		sarray := []string{strconv.Itoa(b), strconv.Itoa(s)}
		str := strings.Join(sarray, ":")
		sMap[str] = v
	}
	return sMap, err
}

func getVehicleOldPartArray() ([]string, error) {
	var err error
	var a []string
	db, err := sql.Open("mysql", database.ConnectionString())
	if err != nil {
		return a, err
	}
	defer db.Close()

	stmt, err := db.Prepare(vehicleOldPartArrayStmt)
	if err != nil {
		return a, err
	}
	defer stmt.Close()
	var v, p int
	res, err := stmt.Query()
	if err != nil {
		return a, err
	}
	for res.Next() {
		err = res.Scan(&v, &p)
		if err != nil {
			return a, err
		}
		x := []string{strconv.Itoa(v), strconv.Itoa(p)}
		str := strings.Join(x, ":")
		a = append(a, str)
	}
	return a, err
}
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

func createMissingPartNumbers(title string) (*os.File, error) {
	missingPartNumbers, err := os.Create("exports/" + title + ".csv")
	if err != nil {
		return missingPartNumbers, err
	}
	// h := []byte("Missing Part Numbers \n")
	// n, err := missingPartNumbers.WriteAt(h, missingPartNumbersOffset)
	// missingPartNumbersOffset += int64(n)

	return missingPartNumbers, err
}

func createInsertStatementsFile(title string) (*os.File, int64, error) {
	var off int64 = 0
	f, err := os.Create("exports/" + title + ".csv")
	if err != nil {
		return f, off, err
	}
	h := []byte("insert into vcdb_VehiclePart (VehicleID, PartNumber) values \n")
	n, err := f.WriteAt(h, off)
	if err != nil {
		return f, off, err
	}
	off += int64(n)

	return f, off, err
}

func createConfigErrorFile(title string) (*os.File, error) {
	configErrorFile, err := os.Create("exports/" + title + ".txt")
	if err != nil {
		return configErrorFile, err
	}

	return configErrorFile, err
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
