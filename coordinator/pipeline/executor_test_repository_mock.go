package pipeline

import (
	"errors"

	"github.com/google/uuid"
	"starbyte.io/coordinator/db"
)

type RepositoryMock struct {
	Retries    int
	MaxRetries int
}

var ErrRetries = errors.New("retries error")
var ErrResourceUri = errors.New("resource error")
var ErrDb = errors.New("db error")

func (m *RepositoryMock) GetMaxRetriesByCorrelationId(CorrelationId uuid.UUID) (*db.BatchRetries, error) {
	return &db.BatchRetries{MaxRetries: m.MaxRetries, Retries: m.Retries}, nil
}

func (m *RepositoryMock) GetResourceUriByCorrelationId(CorrelationId uuid.UUID) (string, error) {
	return "test_uri", nil
}

func (m *RepositoryMock) CreateNewBatch(*uuid.UUID, uuid.UUID, string) (*db.Batch, error) {
	return &db.Batch{BatchId: uuid.UUID{}}, nil
}
func (m *RepositoryMock) CreateNewBatchProcessAttempt(uuid.UUID, uuid.UUID) error {
	return nil
}
func (m *RepositoryMock) SetBatchProcessStatus(uuid.UUID, string, string) error {
	return nil
}

func (m *RepositoryMock) GetBatchByCorrelationId(uuid.UUID) (*db.Batch, error) {
	return nil, nil
}

func (m *RepositoryMock) DiscardBatchByCorrelationId(uuid.UUID) error {
	return nil
}

func (pg *RepositoryMock) GetPipeline(pipelineId uuid.UUID) (*db.Pipeline, error) {
	pipeline := &db.Pipeline{}
	return pipeline, nil
}

type RepositoryErrorMock struct {
}

func (m *RepositoryErrorMock) GetMaxRetriesByCorrelationId(CorrelationId uuid.UUID) (*db.BatchRetries, error) {
	return nil, ErrRetries
}

func (m *RepositoryErrorMock) GetResourceUriByCorrelationId(CorrelationId uuid.UUID) (string, error) {
	return "", ErrResourceUri
}

func (m *RepositoryErrorMock) CreateNewBatch(*uuid.UUID, uuid.UUID, string) (*db.Batch, error) {
	return nil, ErrDb
}
func (m *RepositoryErrorMock) CreateNewBatchProcessAttempt(uuid.UUID, uuid.UUID) error {
	return ErrDb
}
func (m *RepositoryErrorMock) SetBatchProcessStatus(uuid.UUID, string, string) error {
	return ErrDb
}
func (m *RepositoryErrorMock) GetBatchByCorrelationId(uuid.UUID) (*db.Batch, error) {
	return nil, ErrDb
}

func (m *RepositoryErrorMock) DiscardBatchByCorrelationId(uuid.UUID) error {
	return ErrDb
}

func (pg *RepositoryErrorMock) GetPipeline(pipelineId uuid.UUID) (*db.Pipeline, error) {
	return nil, ErrDb
}

type RepositoryResourceErrorMock struct {
}

func (m *RepositoryResourceErrorMock) GetMaxRetriesByCorrelationId(CorrelationId uuid.UUID) (*db.BatchRetries, error) {
	return &db.BatchRetries{MaxRetries: 1, Retries: 0}, nil
}

func (m *RepositoryResourceErrorMock) GetResourceUriByCorrelationId(CorrelationId uuid.UUID) (string, error) {
	return "", ErrResourceUri
}

func (m *RepositoryResourceErrorMock) CreateNewBatch(*uuid.UUID, uuid.UUID, string) (*db.Batch, error) {
	return nil, ErrDb
}
func (m *RepositoryResourceErrorMock) CreateNewBatchProcessAttempt(uuid.UUID, uuid.UUID) error {
	return ErrDb
}
func (m *RepositoryResourceErrorMock) SetBatchProcessStatus(uuid.UUID, string, string) error {
	return ErrDb
}
func (m *RepositoryResourceErrorMock) GetBatchByCorrelationId(uuid.UUID) (*db.Batch, error) {
	return nil, ErrDb
}
func (m *RepositoryResourceErrorMock) DiscardBatchByCorrelationId(uuid.UUID) error {
	return ErrDb
}

func (pg *RepositoryResourceErrorMock) GetPipeline(pipelineId uuid.UUID) (*db.Pipeline, error) {
	return nil, ErrDb
}
