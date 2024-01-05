package s3io

import (
	"compress/gzip"
	"io"

	"github.com/fxamacker/cbor/v2"
)

type CborWriter struct {
	writer     io.WriteCloser
	encoder    *cbor.Encoder
	gzipWriter *gzip.Writer
}

func NewWriter(writer io.WriteCloser) (*CborWriter, error) {
	opts := cbor.CoreDetEncOptions()
	opts.Time = cbor.TimeRFC3339Nano
	em, _ := opts.EncMode()

	gzipWriter := gzip.NewWriter(writer)
	encoder := em.NewEncoder(gzipWriter)
	return &CborWriter{
		writer:     writer,
		encoder:    encoder,
		gzipWriter: gzipWriter,
	}, nil
}

func (writer *CborWriter) Write(data any) error {
	return writer.encoder.Encode(data)
}

func (writer *CborWriter) Close() error {
	return writer.gzipWriter.Close()
}
