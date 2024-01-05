package main

import (
	"log"

	"starbyte.io/core/worker"
	extractor "starbyte.io/workers/extractor/step"
)

func main() {
	customConfig := &extractor.ExtractorConfig{}
	worker.ReadConfigEnvVar(&customConfig)
	parsers, err := extractor.BuildParser(customConfig)
	if err != nil {
		log.Fatal(err)
	}
	worker, err := worker.NewStepWorker(
		extractor.Extractor{Parsers: parsers},
	)

	if err != nil {
		log.Fatal(err)
	}
	err = worker.Run()

	if err != nil {
		log.Fatal(err)
	}
}
