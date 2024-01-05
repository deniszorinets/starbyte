package db

import (
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type BatchRetries struct {
	MaxRetries int
	Retries    int
}

type Repository interface {
	GetMaxRetriesByCorrelationId(uuid.UUID) (*BatchRetries, error)
	GetResourceUriByCorrelationId(uuid.UUID) (string, error)
	CreateNewBatch(*uuid.UUID, uuid.UUID, string) (*Batch, error)
	CreateNewBatchProcessAttempt(uuid.UUID, uuid.UUID) error
	SetBatchProcessStatus(uuid.UUID, string, string) error
	GetBatchByCorrelationId(uuid.UUID) (*Batch, error)
	DiscardBatchByCorrelationId(uuid.UUID) error
	GetPipeline(uuid.UUID) (*Pipeline, error)
}

type PgRepository struct {
	ConnStr string
	conn    *sqlx.DB
}

func NewPgRepository(connStr string) (*PgRepository, error) {
	db, err := sqlx.Connect("postgres", connStr)
	if err != nil {
		return nil, err
	}

	return &PgRepository{
		ConnStr: connStr,
		conn:    db,
	}, nil
}

type batchContainer struct {
	BatchId uuid.UUID `db:"batch_id"`
	Uri     string    `db:"uri"`
}

func (pg *PgRepository) GetMaxRetriesByCorrelationId(correlationId uuid.UUID) (*BatchRetries, error) {
	retries := &BatchRetries{}
	batch := &batchContainer{}
	err := pg.conn.Get(batch, `select batch_id, uri from batches b join batch_process_log bpl using(batch_id) where correlation_id = $1`, correlationId)
	if err != nil {
		return nil, err
	}
	err = pg.conn.Get(
		retries,
		`select count(*) as retires, (s.runtime_config -> 'max_retries')::int as max_retries
		from batch_process_log bpl
		join batches b using (batch_id) 
		join steps s on s.step_id = b.step_to_id
		where batch_id = $1 
		group by s.runtime_config`,
		batch.BatchId,
	)
	if err != nil {
		return nil, err
	}
	return retries, nil
}

func (pg *PgRepository) GetResourceUriByCorrelationId(correlationId uuid.UUID) (string, error) {
	batch := &batchContainer{}
	err := pg.conn.Get(batch, `select batch_id, uri from batches b join batch_process_log bpl using(batch_id) where correlation_id = $1`, correlationId)
	if err != nil {
		return "", err
	}
	return batch.Uri, nil
}

type batchCreateResultContainer struct {
	BatchId  uuid.UUID `db:"batch_id"`
	IssuedAt time.Time `db:"issued_at"`
}

func (pg *PgRepository) CreateNewBatch(stepFromId *uuid.UUID, stepToId uuid.UUID, uri string) (*Batch, error) {
	batch := &batchCreateResultContainer{}

	err := pg.conn.Get(batch, `insert into batches (step_from_id, step_to_id, issued_at, uri) values($1, $2, now() at time zone 'utc', $3) returning batch_id, issued_at`, stepFromId, stepToId, uri)
	if err != nil {
		return nil, err
	}
	return &Batch{
		BatchId:    batch.BatchId,
		IssuedAt:   batch.IssuedAt,
		StepFromId: stepFromId,
		StepToId:   stepToId,
		Uri:        uri,
	}, nil
}

func (pg *PgRepository) CreateNewBatchProcessAttempt(batchId uuid.UUID, correlationId uuid.UUID) error {
	_, err := pg.conn.Exec(`insert into batch_process_log (correlation_id, batch_id, started_at) values($1, $2, now() at time zone 'utc')`, correlationId, batchId)
	return err
}

func (pg *PgRepository) SetBatchProcessStatus(correlationId uuid.UUID, state string, err string) error {
	_, dbErr := pg.conn.Exec(`update batch_process_log set finished_at=now() at time zone 'utc', state=$1, error=$2 where correlation_id = $3`, state, err, correlationId)
	return dbErr
}

func (pg *PgRepository) GetBatchByCorrelationId(correlationId uuid.UUID) (*Batch, error) {
	batch := &Batch{}

	err := pg.conn.Get(batch, `select batches.* from batches join batch_process_log using(batch_id) where correlation_id = $1`, correlationId)
	if err != nil {
		return nil, err
	}
	return batch, err
}

func (pg *PgRepository) DiscardBatchByCorrelationId(correlationId uuid.UUID) error {
	batch, err := pg.GetBatchByCorrelationId(correlationId)
	if err != nil {
		return err
	}

	_, err = pg.conn.Exec(`delete from batch_process_log where batch_id = $1`, batch.BatchId)

	if err != nil {
		return err
	}

	_, err = pg.conn.Exec(`delete from batches where batch_id = $1`, batch.BatchId)

	return err
}

func (pg *PgRepository) GetPipeline(pipelineId uuid.UUID) (*Pipeline, error) {
	pipeline := &Pipeline{}
	err := pg.conn.Get(pipeline,
		`select
			p.*,
			jsonb_object_agg(sq.name,
			row_to_json(sq)) as steps
		from
			pipelines p
		join (
			select
				steps.*,
				parent.name as input_step_name
			from
				steps
			left join steps as parent on
				parent.step_id = steps.input_step_id) as sq
				using(pipeline_id)
		where pipeline_id = $1
		group by
			pipeline_id`,
		pipelineId,
	)
	if err != nil {
		return nil, err
	}
	return pipeline, nil
}
