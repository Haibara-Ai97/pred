package datasource

import (
	"context"
	"pred/sql"
	"time"
)

type DataSourceManager interface {
	Initialize(ctx context.Context) error
	GetDataSource() DataSource
}

type DataSource interface {
	GetPrepareData(ctx context.Context, tableName, pointName string, startTime, endTime time.Time) ([]*sql.YSBase, error)
	GetOriginData(ctx context.Context, tableName, pointName string, startTime, endTime time.Time) ([]*sql.YSBase, error)
	GetLastOriginData(ctx context.Context, tableName, pointName string, startTime, endTime time.Time) ([]*sql.YSBase, error)
	GetMaxTime(ctx context.Context, tableName string) (*sql.YSBase, error)
	GetPointType(ctx context.Context, pointName string) (string, error)
}
