install:
	go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@latest
migrate:
	migrate -path database/migration/ -database "postgresql://postgres:postgres@localhost:5432/lightbyte_coordinator_test?sslmode=disable" -verbose up