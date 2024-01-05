package worker

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"

	s3io "starbyte.io/core/s3"
)

type CsvReaderConfig struct {
	HasHeader bool   `json:"has_header,omitempty" mapstructure:"has_header"`
	Delimiter string `json:"delimiter,omitempty"  mapstructure:"delimiter"`
	FileUri   string `json:"file_uri" mapstructure:"file_uri"`
}

type CsvReader struct {
	Config CsvReaderConfig
}

func NewCsvReaderConfig() CsvReaderConfig {
	return CsvReaderConfig{
		HasHeader: true,
		Delimiter: ",",
	}
}

func mapRecordToHeader(record []string, header []string) (map[string]string, error) {
	value := make(map[string]string)
	if len(record) != len(header) {
		return nil, errors.New("record can not be mapped to header")
	}

	for idx, item := range header {
		value[item] = record[idx]
	}
	return value, nil
}

func (input CsvReader) Read(ctx context.Context, output chan<- any) {
	var csvHeader []string = nil
	defer close(output)

	ctx, cancel := context.WithCancelCause(ctx)

	hasHeader := input.Config.HasHeader

	reader, err := s3io.Read(ctx, input.Config.FileUri)

	if err != nil {
		cancel(err)
		return
	}

	csvReader := csv.NewReader(reader)
	csvReader.LazyQuotes = true
	csvReader.TrimLeadingSpace = true
	csvReader.FieldsPerRecord = -1

	for {
		record, err := csvReader.Read()
		if csvHeader == nil {
			if hasHeader {
				csvHeader = record
				continue
			} else {
				csvHeader = make([]string, len(record))
				for idx := range record {
					csvHeader[idx] = fmt.Sprintf("column%d", idx)
				}
			}
		}

		if err == io.EOF {
			break
		}

		if err != nil {
			cancel(err)
			break
		}

		mappedRecord, err := mapRecordToHeader(record, csvHeader)
		if err != nil {
			cancel(err)
			break
		}
		output <- mappedRecord

	}
}
