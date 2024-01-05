package sdk

import "context"

type Output interface {
	Sink(context.Context, <-chan any)
}
