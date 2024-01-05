package db

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type RuntimeConfig struct {
	BatchSize    int `db:"batch_size" json:"batch_size,omitempty"`
	BatchTimeout int `db:"batch_timeout" json:"batch_timeout,omitempty"`
	MaxRetries   int `db:"max_retries" json:"max_retries,omitempty"`
}

type StepConf map[string]Step

type Step struct {
	Id            uuid.UUID     `db:"step_id" json:"step_id"`
	Name          string        `db:"name" json:"name"`
	PipelineId    uuid.UUID     `db:"pipeline_id" json:"pipeline_id"`
	Input         string        `db:"input_step_name" json:"input_step_name"`
	Image         string        `db:"image" json:"image"`
	RuntimeConfig RuntimeConfig `db:"runtime_config" json:"runtime_config,omitempty"`
	Config        any           `db:"config" json:"config,omitempty"`
}

func (s StepConf) Value() (driver.Value, error) {
	return json.Marshal(s)
}

func (s *StepConf) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}

	return json.Unmarshal(b, &s)
}

func (s *Step) GetRespQueueName() string {
	return fmt.Sprintf("responses_%s_%s", s.Id, s.Name)
}

func (s *Step) GetReqQueueName() string {
	return fmt.Sprintf("requests_%s_%s", s.Id, s.Name)
}

type Pipeline struct {
	Id    uuid.UUID `db:"pipeline_id"`
	Name  string    `db:"name"`
	Steps StepConf  `db:"steps"`
}

type Batch struct {
	BatchId    uuid.UUID  `db:"batch_id"`
	StepFromId *uuid.UUID `db:"step_from_id"`
	StepToId   uuid.UUID  `db:"step_to_id"`
	IssuedAt   time.Time  `db:"issued_at"`
	Uri        string     `db:"uri"`
}

type BatchProcessLog struct {
	BatchId       uuid.UUID `db:"batch_id"`
	CorrelationId uuid.UUID `db:"correlation_id"`
	StartedAt     time.Time `db:"started_at"`
	FinishedAt    time.Time `db:"finished_at"`
	State         string    `db:"state"`
	Error         string    `db:"error"`
}
