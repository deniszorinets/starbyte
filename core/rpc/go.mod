module starbyte.io/core/rpc

go 1.21.3

replace starbyte.io/core/utils => ../utils

replace starbyte.io/core/amqp => ../amqp

require (
	github.com/google/uuid v1.5.0
	starbyte.io/core/amqp v0.0.0-00010101000000-000000000000
)

require (
	github.com/rabbitmq/amqp091-go v1.9.0 // indirect
	starbyte.io/core/utils v0.0.0-00010101000000-000000000000 // indirect
)
