package rpc

import (
	"github.com/google/uuid"
	"starbyte.io/core/amqp"
)

type ProcessResult string

const (
	OK      ProcessResult = "OK"
	ERROR   ProcessResult = "ERROR"
	ALLDONE ProcessResult = "ALLDONE"
)

type ProcessRequest struct {
	amqp.AmqpMessage
	ResourceUri   string
	ResultUri     string
	Termination   bool
	CorrelationId uuid.UUID
}

type ProcessResponse struct {
	amqp.AmqpMessage
	CorrelationId uuid.UUID
	Status        ProcessResult
	ResultUri     string
	Error         string
}
