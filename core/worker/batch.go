package worker

import (
	"context"
	"log/slog"
	"time"
)

type Batch struct {
	Data []any
}

func ChunkToBatch(ctx context.Context, input <-chan any, output chan<- Batch, batchSize int, batchTimeout int) {
	defer close(output)

	buffer := make([]any, batchSize)
	idx := 0
	delay := time.NewTicker(time.Millisecond * time.Duration(batchTimeout))
	prevTimeMark := time.Now()
L:
	for {
		select {
		case <-ctx.Done():
			break L
		case data, ok := <-input:
			if !ok {
				break L
			}

			buffer[idx] = data
			idx++

			if idx >= batchSize {
				output <- Batch{Data: buffer}
				slog.Debug("batch created", "Size", idx)

				buffer = make([]any, batchSize)
				idx = 0
			}
		case t := <-delay.C:
			if idx > 0 {
				output <- Batch{Data: buffer[:idx]}

				slog.Debug("batch created", "Size", idx, "Timeout", t.Sub(prevTimeMark))
				prevTimeMark = t

				buffer = make([]any, batchSize)
				idx = 0
			}
		}
	}

	if idx > 0 {
		output <- Batch{Data: buffer[:idx]}
		slog.Debug("batch created", "Size", idx)
	}
}
