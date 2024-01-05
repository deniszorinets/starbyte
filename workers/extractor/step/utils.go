package extractor

import (
	"fmt"
	"strconv"

	"github.com/itchyny/timefmt-go"
)

func Strtoint(data any) (any, error) {

	if dataStr, ok := data.(string); ok {
		if data == "" {
			return 0, nil
		}
		return strconv.Atoi(dataStr)
	} else if dataFloat, ok := data.(float64); ok {
		return int(dataFloat), nil
	} else if dataInt, ok := data.(int); ok {
		return dataInt, nil
	}

	return nil, fmt.Errorf("cannot cast %s to int", data)
}

func Strtodouble(data any) (any, error) {
	if dataStr, ok := data.(string); ok {
		if data == "" {
			return 0., nil
		}
		return strconv.ParseFloat(dataStr, 64)
	} else if dataFloat, ok := data.(float64); ok {
		return dataFloat, nil
	}

	return nil, fmt.Errorf("cannot cast %s to double", data)
}

func Strtobool(data any) (any, error) {
	if dataStr, ok := data.(string); ok {
		if data == "" {
			return false, nil
		}
		return strconv.ParseBool(dataStr)
	} else if dataFloat, ok := data.(float64); ok {
		return dataFloat == 0., nil
	} else if dataBool, ok := data.(bool); ok {
		return dataBool, nil
	}

	return nil, fmt.Errorf("cannot cast %s to boolean", data)
}

func Strtostr(data any) (any, error) {
	if data == "" {
		return nil, nil
	}
	return data, nil
}

func Strtodatetime(data any, format string) (any, error) {
	if data == "" {
		return nil, nil
	}
	datetime, err := timefmt.Parse(data.(string), format)
	return datetime, err
}
