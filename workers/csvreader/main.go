package main

import (
	"log"

	_ "github.com/joho/godotenv/autoload"
	"starbyte.io/core/worker"
	w "starbyte.io/workers/csvreader/worker"
)

func main() {
	customConfig := w.NewCsvReaderConfig()
	worker.ReadConfigEnvVar(&customConfig)
	worker, err := worker.NewInputWorker(
		w.CsvReader{Config: customConfig},
	)

	if err != nil {
		log.Fatal(err)
	}
	err = worker.Run()

	if err != nil {
		log.Fatal(err)
	}
}
