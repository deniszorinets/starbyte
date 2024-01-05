package sdk

import "context"

type Input interface {
	Read(context.Context, chan<- any)
}
