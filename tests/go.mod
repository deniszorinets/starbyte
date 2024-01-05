module starbyte.io/tests

go 1.21.3

replace starbyte.io/core/s3 => ../core/s3

replace starbyte.io/core/rpc => ../core/rpc

replace starbyte.io/core/amqp => ../core/amqp

replace starbyte.io/core/utils => ../core/utils

replace starbyte.io/core/worker => ../core/worker

replace starbyte.io/sdk => ../sdk

replace starbyte.io/coordinator => ../coordinator

replace starbyte.io/workers/csvreader => ../workers/csvreader

replace starbyte.io/workers/extractor => ../workers/extractor

replace starbyte.io/workers/pgsqlcopysink => ../workers/pgsqlcopysink

require (
	github.com/golang-migrate/migrate/v4 v4.17.0
	github.com/google/uuid v1.5.0
	github.com/mitchellh/mapstructure v1.5.0
	github.com/stretchr/testify v1.8.4
	starbyte.io/coordinator v0.0.0-00010101000000-000000000000
	starbyte.io/core/amqp v0.0.0-00010101000000-000000000000
	starbyte.io/core/utils v0.0.0-00010101000000-000000000000
	starbyte.io/core/worker v0.0.0-00010101000000-000000000000
	starbyte.io/workers/csvreader v0.0.0-00010101000000-000000000000
	starbyte.io/workers/extractor v0.0.0-00010101000000-000000000000
	starbyte.io/workers/pgsqlcopysink v0.0.0-00010101000000-000000000000
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/fxamacker/cbor/v2 v2.5.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/itchyny/gojq v0.12.13 // indirect
	github.com/itchyny/timefmt-go v0.1.5 // indirect
	github.com/jmoiron/sqlx v1.3.5 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.16.7 // indirect
	github.com/klauspost/cpuid/v2 v2.2.5 // indirect
	github.com/lib/pq v1.10.9 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/minio/minio-go/v7 v7.0.63 // indirect
	github.com/minio/sha256-simd v1.0.1 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/rabbitmq/amqp091-go v1.9.0 // indirect
	github.com/rs/xid v1.5.0 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/x448/float16 v0.8.4 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	golang.org/x/crypto v0.17.0 // indirect
	golang.org/x/net v0.18.0 // indirect
	golang.org/x/sys v0.15.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	gopkg.in/ini.v1 v1.67.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	starbyte.io/core/rpc v0.0.0-00010101000000-000000000000 // indirect
	starbyte.io/core/s3 v0.0.0-00010101000000-000000000000 // indirect
	starbyte.io/sdk v0.0.0-00010101000000-000000000000 // indirect
)
