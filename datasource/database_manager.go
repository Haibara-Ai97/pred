package datasource

import (
	"context"
	"gorm.io/gorm"
)

type DatabaseManager struct {
	datasource *DBDataSource
	db         *gorm.DB
}

func NewDatabaseManager(db *gorm.DB) *DatabaseManager {
	return &DatabaseManager{
		db: db,
	}
}

func (m *DatabaseManager) Initialize(ctx context.Context) error {
	m.datasource = &DBDataSource{
		DB: m.db,
	}
	return nil
}

func (m *DatabaseManager) GetDataSource() DataSource {
	return m.datasource
}
