package utils

import (
	"context"
	"sync"
)

func ReadChannelWithContext(ctx context.Context, input <-chan any) chan any {
	output := make(chan any, 1)

	go func() {
	L:
		for {
			select {
			case <-ctx.Done():
				break L
			case record, ok := <-input:
				if !ok {
					break L
				}
				output <- record
			}
		}
		close(output)
	}()

	return output
}

func CastChannelWithContext[T any, S any](ctx context.Context, input <-chan S, castFunc func(record S) T) chan T {
	output := make(chan T, 1)

	go func() {
	L:
		for {
			select {
			case <-ctx.Done():
				break L
			case record, ok := <-input:
				if !ok {
					break L
				}
				output <- castFunc(record)
			}
		}
		close(output)
	}()

	return output
}

func RunInWg(wg *sync.WaitGroup, f func()) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		f()
	}()
}
