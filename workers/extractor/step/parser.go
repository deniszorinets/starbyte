package extractor

import (
	"github.com/itchyny/gojq"
)

const (
	Int      string = "int"
	Double   string = "double"
	Boolean  string = "bool"
	String   string = "str"
	DateTime string = "datetime"
	Pass     string = "pass"
)

type Parser struct {
	CastFunc     func(data any) (any, error)
	JsonPath     *gojq.Code
	JsonPathText string
}

func Passthrough(data any) (any, error) {
	return data, nil
}

func StrtodatetimeWrapper(datefmt string) func(data any) (any, error) {
	return func(data any) (any, error) {
		if data != nil {
			return Strtodatetime(data.(string), datefmt)
		} else {
			return nil, nil
		}
	}
}

func BuildParser(config *ExtractorConfig) (map[string]Parser, error) {
	parserMap := make(map[string]Parser)
	for fieldName, field := range config.Columns {

		jpath, err := gojq.Parse(field.JsonPath)
		if err != nil {
			return nil, err
		}

		code, err := gojq.Compile(jpath)

		if err != nil {
			return nil, err
		}

		parserInstance := Parser{
			JsonPath:     code,
			JsonPathText: field.JsonPath,
		}

		switch field.CastTo {
		case Pass:
			parserInstance.CastFunc = Passthrough
		case Int:
			parserInstance.CastFunc = Strtoint
		case Double:
			parserInstance.CastFunc = Strtodouble
		case Boolean:
			parserInstance.CastFunc = Strtobool
		case String:
			parserInstance.CastFunc = Strtostr
		case DateTime:
			datefmt := field.DateFormat
			parserInstance.CastFunc = StrtodatetimeWrapper(datefmt)
		}
		parserMap[fieldName] = parserInstance
	}
	return parserMap, nil
}

func RunJsonPathAndCast(parse Parser, data any) (any, error) {
	iter := parse.JsonPath.Run(data)

	v, ok := iter.Next()
	if !ok {
		return nil, nil
	}
	if err, ok := v.(error); ok {
		return nil, err
	}

	val, err := parse.CastFunc(v)

	return val, err
}
