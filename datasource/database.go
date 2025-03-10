package datasource

import (
	"context"
	"fmt"
	"gorm.io/gorm"
	"pred/sql"
	"time"
)

type DBDataSource struct {
	DB *gorm.DB
}

func NewDBDataSource(db *gorm.DB) *DBDataSource {
	return &DBDataSource{DB: db}
}

func (d *DBDataSource) GetPrepareData(ctx context.Context, tableName,
	pointName string, startTime, endTime time.Time) ([]*sql.YSBase, error) {
	return sql.FromRawData(d.DB, tableName, pointName, startTime, endTime)
}

func (d *DBDataSource) GetOriginData(ctx context.Context, tableName,
	pointName string, startTime, endTime time.Time) ([]*sql.YSBase, error) {
	return sql.FromRawDataOrigin(d.DB, tableName, pointName, startTime, endTime)
}

func (d *DBDataSource) GetLastOriginData(ctx context.Context, tableName,
	pointName string, startTime, endTime time.Time) ([]*sql.YSBase, error) {
	return sql.FromLastRawDataOrigin(d.DB, tableName, pointName, startTime, endTime)
}

func (d *DBDataSource) GetMaxTime(ctx context.Context, tableName string) (result *sql.YSBase, err error) {
	err = d.DB.Raw(fmt.Sprintf("SELECT MAX(DT) AS DT FROM %s", tableName)).Scan(&result).Error
	if err != nil {
		return nil, fmt.Errorf("failed to get MaxTime from table %s: %v", tableName, err)
	}
	return result, nil
}

func (d *DBDataSource) GetPointType(ctx context.Context, pointName string) (string, error) {
	return sql.GetKindsChinese(d.DB, pointName)
}
