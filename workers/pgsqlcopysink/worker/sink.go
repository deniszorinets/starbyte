package worker

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/lib/pq"
)

type Config struct {
	User     string   `mapstructure:"user"`
	Password string   `mapstructure:"password"`
	Host     string   `mapstructure:"host"`
	Port     int      `mapstructure:"port"`
	Dbname   string   `mapstructure:"db_name"`
	Table    string   `mapstructure:"table"`
	Columns  []string `mapstructure:"columns"`
}

type PgsqlCopySink struct {
	Config  *Config
	ConnStr string
	Db      *sql.DB
	Stmt    *sql.Stmt
	Txn     *sql.Tx
}

func NewPgsqlCopySink(customConfig Config) *PgsqlCopySink {
	connStr := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		customConfig.Host,
		customConfig.Port,
		customConfig.User,
		customConfig.Password,
		customConfig.Dbname,
	)

	return &PgsqlCopySink{ConnStr: connStr, Config: &customConfig}
}

func getTableSchema(tableName string) (string, string) {
	splittedTableName := strings.Split(tableName, ".")
	if len(splittedTableName) == 1 {
		return "public", splittedTableName[0]
	} else {
		return splittedTableName[0], splittedTableName[1]

	}
}

func getQuery(schema string, tableName string, columns []string) string {
	return pq.CopyInSchema(schema, tableName, columns...)
}

func (p *PgsqlCopySink) BeforeTransform() error {
	var err error = nil

	p.Db, err = sql.Open("postgres", p.ConnStr)
	if err != nil {
		return err
	}

	p.Txn, err = p.Db.Begin()
	if err != nil {
		return err
	}

	schema, tableName := getTableSchema(p.Config.Table)
	p.Stmt, err = p.Txn.Prepare(getQuery(schema, tableName, p.Config.Columns))
	return err
}

func (p *PgsqlCopySink) AfterTransform() error {
	_, err := p.Stmt.Exec()
	if err != nil {
		return err
	}

	err = p.Stmt.Close()
	if err != nil {
		return err
	}

	err = p.Txn.Commit()
	if err != nil {
		return err
	}

	return p.Db.Close()
}

func (p PgsqlCopySink) Transform(data any) (any, error) {
	inputData := make([]any, len(p.Config.Columns))
	for idx, columnName := range p.Config.Columns {
		inputData[idx] = data.(map[any]any)[columnName]
	}

	_, err := p.Stmt.Exec(inputData...)
	if err != nil {
		return nil, err
	}
	return data, nil
}
