package main

import (
	"github.com/curt-labs/polkImporter/importer"
	"log"
)

func main() {
	err = importer.Run()
	if err != nil {
		log.Print("Error: ", err)
	}
}
