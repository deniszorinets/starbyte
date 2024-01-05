package tests

import (
	"context"
	"database/sql"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"
	"github.com/stretchr/testify/suite"
	"starbyte.io/coordinator/db"
	"starbyte.io/coordinator/pipeline"
	"starbyte.io/core/amqp"
	"starbyte.io/core/utils"
	"starbyte.io/core/worker"

	csv "starbyte.io/workers/csvreader/worker"
	extractor "starbyte.io/workers/extractor/step"
	sink "starbyte.io/workers/pgsqlcopysink/worker"
)

const (
	TEST_DB_CONN_STR     = "postgres://postgres:postgres@localhost:5432/coordinator_test_db?sslmode=disable"
	POSTGRES_DB_CONN_STR = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
	AMQP_CONN_STR        = "amqp://user:password@localhost"
	S3_CONN_STR          = "http://minioadmin:minioadmin@localhost:9000/pipeline"
)

var (
	PIPELINE_ID     = uuid.MustParse("148fd0b6-f51f-4a95-b904-41a59dfcb62b")
	INPUT_STEP_ID   = uuid.MustParse("84225863-da7a-4b44-bb6a-bd3e3f801682")
	EXTRACT_STEP_ID = uuid.MustParse("84225863-da7a-4b44-bb6a-bd3e3f801683")
)

type TestSuite struct {
	suite.Suite
}

func (suite *TestSuite) SetupTest() {
	db, err := sql.Open("postgres", POSTGRES_DB_CONN_STR)
	if err != nil {
		suite.FailNow(err.Error())
	}

	if _, err = db.Exec("SELECT pg_terminate_backend(pid) from pg_stat_activity where datname='coordinator_test_db'"); err != nil {
		suite.FailNow(err.Error())
	}

	if _, err = db.Exec("DROP DATABASE IF EXISTS coordinator_test_db"); err != nil {
		suite.FailNow(err.Error())
	}

	if _, err = db.Exec("CREATE DATABASE coordinator_test_db"); err != nil {
		suite.FailNow(err.Error())
	}

	db, err = sql.Open("postgres", TEST_DB_CONN_STR)

	if err != nil {
		suite.FailNow(err.Error())
	}

	driver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		suite.FailNow(err.Error())
	}

	m, err := migrate.NewWithDatabaseInstance(
		"file://../coordinator/database/migrations",
		"postgres", driver)
	if err != nil {
		suite.FailNow(err.Error())
	}

	if err = m.Up(); err != nil {
		suite.FailNow(err.Error())
	}

	if _, err = db.Exec("DELETE FROM schema_migrations"); err != nil {
		suite.FailNow(err.Error())
	}

	m, err = migrate.NewWithDatabaseInstance(
		"file://stubs",
		"postgres", driver)

	if err != nil {
		suite.FailNow(err.Error())
	}

	if err = m.Up(); err != nil {
		suite.FailNow(err.Error())
	}

	m.Close()
}

func runExtractor(step db.Step) error {
	os.Setenv(worker.LIGHTBYTE_WORKER_AMQP_URI, AMQP_CONN_STR)
	os.Setenv(worker.LIGHTBYTE_WORKER_LISTEN_QUEUE, step.GetReqQueueName())
	os.Setenv(worker.LIGHTBYTE_WORKER_RESPONSE_QUEUE, step.GetRespQueueName())

	conf := extractor.ExtractorConfig{}
	mapstructure.Decode(step.Config, &conf)

	parsers, err := extractor.BuildParser(&conf)
	if err != nil {
		log.Fatal(err)
	}
	worker, err := worker.NewStepWorker(
		extractor.Extractor{Parsers: parsers},
	)

	if err != nil {
		return err
	}

	return worker.Run()
}

func runCsvReader(step db.Step) error {
	os.Setenv(worker.LIGHTBYTE_WORKER_BATCH_SIZE, "1")
	os.Setenv(worker.LIGHTBYTE_WORKER_BATCH_TIMEOUT, "1000")
	os.Setenv(worker.LIGHTBYTE_WORKER_AMQP_URI, AMQP_CONN_STR)
	os.Setenv(worker.LIGHTBYTE_WORKER_LISTEN_QUEUE, step.GetReqQueueName())
	os.Setenv(worker.LIGHTBYTE_WORKER_RESPONSE_QUEUE, step.GetRespQueueName())

	conf := csv.CsvReaderConfig{}
	mapstructure.Decode(step.Config, &conf)
	worker, err := worker.NewInputWorker(
		csv.CsvReader{Config: conf},
	)
	if err != nil {
		return err
	}
	return worker.Run()
}

func runSink(step db.Step) error {
	os.Setenv(worker.LIGHTBYTE_WORKER_AMQP_URI, AMQP_CONN_STR)
	os.Setenv(worker.LIGHTBYTE_WORKER_LISTEN_QUEUE, step.GetReqQueueName())
	os.Setenv(worker.LIGHTBYTE_WORKER_RESPONSE_QUEUE, step.GetRespQueueName())

	conf := sink.Config{}
	mapstructure.Decode(step.Config, &conf)

	worker, err := worker.NewStepWorker(
		sink.NewPgsqlCopySink(conf),
	)

	if err != nil {
		return err
	}

	return worker.Run()
}

func (suite *TestSuite) TestCoordinatorPipeline() {

	repository, err := db.NewPgRepository(TEST_DB_CONN_STR)

	if err != nil {
		suite.FailNow(err.Error())
	}

	p, err := repository.GetPipeline(PIPELINE_ID)

	if err != nil {
		suite.FailNow(err.Error())
	}

	amqpConn := amqp.RabbitMqAmqp{}
	err = amqpConn.Connect(context.Background(), AMQP_CONN_STR)

	if err != nil {
		suite.FailNow(err.Error())
	}

	coordinator, err := pipeline.NewExecutor(p, &amqpConn, S3_CONN_STR, repository)
	if err != nil {
		suite.FailNow(err.Error())
	}

	wg := &sync.WaitGroup{}
	errs := make(chan error, 4)

	utils.RunInWg(wg, func() { errs <- coordinator.Run() })
	time.Sleep(1 * time.Second)

	utils.RunInWg(wg, func() { errs <- runExtractor(p.Steps["extract"]) })

	time.Sleep(1 * time.Second)

	utils.RunInWg(wg, func() { errs <- runCsvReader(p.Steps["input"]) })

	time.Sleep(1 * time.Second)

	utils.RunInWg(wg, func() { errs <- runSink(p.Steps["sink"]) })

	go func() {
		for err := range errs {
			if err != nil {
				suite.FailNow(err.Error())
			}
		}
	}()

	wg.Wait()

	close(errs)

}

func TestExampleTestSuite(t *testing.T) {
	suite.Run(t, new(TestSuite))
}
