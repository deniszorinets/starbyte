package main

import (
	"log"

	"starbyte.io/core/worker"
	w "starbyte.io/workers/pgsqlcopysink/worker"
)

func main() {
	customConfig := w.Config{}
	worker.ReadConfigEnvVar(&customConfig)

	sink := w.NewPgsqlCopySink(customConfig)
	worker, err := worker.NewStepWorker(
		sink,
	)

	if err != nil {
		log.Fatal(err)
	}
	err = worker.Run()

	if err != nil {
		log.Fatal(err)
	}
}
