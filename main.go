package main

import (
	"github.com/curt-labs/polkImporter/v2"
	"log"
)

func main() {
	var err error
	err = v2.Run()
	if err != nil {
		log.Print("Error: ", err)
	}
}
