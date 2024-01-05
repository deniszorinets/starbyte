package extractor

import (
	"log"
)

type Extractor struct {
	Parsers map[string]Parser
}

func remapMap(data map[any]any) map[string]any {
	res := map[string]any{}
	for k, v := range data {
		res[k.(string)] = v
	}
	return res
}

func (step Extractor) Transform(data any) (any, error) {
	res := map[string]any{}
	for col, parse := range step.Parsers {
		var val any
		var err error

		val, err = RunJsonPathAndCast(parse, remapMap(data.(map[any]any)))

		if err != nil {
			log.Fatal(err)
			return nil, err
		}
		res[col] = val
	}
	return res, nil

}
