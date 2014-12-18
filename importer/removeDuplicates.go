package importer

import (
	"encoding/csv"
	"os"
	"sort"
)

func RemoveDuplicates(sortableFile string) error {
	var theArray []string

	csvfile, err := os.Open(sortableFile)
	if err != nil {
		return err
	}
	defer csvfile.Close()
	reader := csv.NewReader(csvfile)
	reader.FieldsPerRecord = -1     //flexible number of fields
	reader.Comma = int32(987654321) //ignore commas, treat as lines

	lines, err := reader.ReadAll()
	if err != nil {
		return err
	}

	for _, line := range lines {
		theArray = append(theArray, line[0])
	}
	if len(theArray) < 2 {
		return err
	}

	//re-create files to write
	file, err := os.Create(sortableFile)
	if err != nil {
		return err
	}
	off := int64(0)

	sort.Strings(theArray[1:])
	for i, record := range theArray {
		if i == 0 {
			n, err := file.WriteAt([]byte(record+"\n"), off)
			if err != nil {
				return err
			}
			off += int64(n)
		}
		if i > 0 {
			if record != theArray[i-1] {
				n, err := file.WriteAt([]byte(record+"\n"), off)
				if err != nil {
					return err
				}
				off += int64(n)
			}
		}
	}

	return err
}
