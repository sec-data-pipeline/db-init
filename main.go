package main

import (
	"log"
	"time"

	"github.com/sec-data-pipeline/db-init/request"
	"github.com/sec-data-pipeline/db-init/storage"
)

func main() {
	connParams, err := storage.GetAWSConnParams()
	if err != nil {
		panic(err)
	}
	db, err := storage.New(connParams)
	if err != nil {
		panic(err)
	}
	err = db.CreateTables()
	if err != nil {
		panic(err)
	}
	ciks, err := request.GetSP()
	if err != nil {
		panic(err)
	}
	ciks = append(ciks, []string{"0001477333", "0001543151"}...)
	for _, cik := range ciks {
		time.Sleep(100 * time.Millisecond)
		company, err := request.GetCompany(cik)
		if err != nil {
			log.Println(err.Error())
			continue
		}
		err = db.InsertCompany(*company)
		if err != nil {
			log.Println(err.Error())
		}
	}
}
