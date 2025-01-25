package spi

import "context"

type Executor interface {
	Open(context.Context)
}
