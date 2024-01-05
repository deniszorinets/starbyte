package s3io

import (
	"compress/gzip"
	"errors"
	"io"

	"github.com/fxamacker/cbor/v2"
)

type CborReader struct {
	reader     io.ReadCloser
	decoder    *cbor.Decoder
	gzipReader *gzip.Reader
}

func NewReader(reader io.ReadCloser) (*CborReader, error) {
	gzipReader, err := gzip.NewReader(reader)

	if err != nil {
		return nil, err
	}

	opts, _ := cbor.DecOptions{
		TimeTag: cbor.DecTagRequired,
	}.DecMode()

	decoder := opts.NewDecoder(gzipReader)

	return &CborReader{
		reader:     reader,
		decoder:    decoder,
		gzipReader: gzipReader,
	}, nil
}

func (reader *CborReader) Read() (any, error) {
	var data interface{}
	err := reader.decoder.Decode(&data)
	if errors.Is(err, io.EOF) {
		return nil, io.EOF
	}
	if err != nil {
		return nil, err
	}

	return data, err
}

func (reader *CborReader) Close() error {
	return reader.gzipReader.Close()
}
