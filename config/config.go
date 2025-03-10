package config

import (
	"encoding/json"
	"fmt"
	"os"
	"pred/mq"
	"pred/process"
	"time"
)

type DataSourceMode string

const (
	DataSourceModeDB   DataSourceMode = "db"
	DataSourceModeHTTP DataSourceMode = "http"
)

type Config struct {
	Task struct {
		Task        string `json:"task"`
		IsEmergency string `json:"is_emergency"`
	} `json:"task"`

	App struct {
		LogLevel string `json:"log_level"`
		TimeZone string `json:"time_zone"`
	} `json:"app"`

	Database struct {
		Host         string        `json:"host"`
		Port         string        `json:"port"`
		User         string        `json:"user"`
		Password     string        `json:"password"`
		DBName       string        `json:"db_name"`
		MaxIdleConns int           `json:"max_idle_conns"`
		MaxOpenConns int           `json:"max_open_conns"`
		MaxLifeTime  time.Duration `json:"max_life_time"`
		MaxIdleTime  time.Duration `json:"max_idle_time"`
	} `json:"database"`

	DataSource struct {
		Mode string `json:"mode"`
		HTTP struct {
			BaseURL string            `json:"base_url"`
			Headers map[string]string `json:"headers"`
			Timeout time.Duration     `json:"timeout"`
		} `json:"http"`
	} `json:"data_source"`

	RabbitMQ struct {
		Host     string `json:"host"`
		Port     int    `json:"port"`
		Username string `json:"username"`
		Password string `json:"password"`
		Vhost    string `json:"vhost"`
	} `json:"rabbitmq"`

	Process struct {
		BatchSize    int           `json:"batch_size"`
		Workers      int           `json:"workers"`
		Timeout      time.Duration `json:"timeout"`
		CacheTimeout time.Duration `json:"cache_timeout"`
	} `json:"process"`

	Schedule struct {
		ScheduleCron string `json:"schedule_cron"`
	} `json:"schedule"`
}

func LoadConfig(configPath string) (*Config, error) {

	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("read config failed: %w", err)
	}

	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("unmarshal config failed: %w", err)
	}

	if err := validateConfig(&config); err != nil {
		return nil, fmt.Errorf("validate config failed: %w", err)
	}

	return &config, nil
}

func validateConfig(config *Config) error {
	if config.Database.MaxIdleConns <= 0 {
		return fmt.Errorf("invalid max_idle_conns")
	}
	if config.Database.MaxOpenConns <= 0 {
		return fmt.Errorf("invalid max_open_conns")
	}

	if config.DataSource.Mode != "db" && config.DataSource.Mode != "http" {
		return fmt.Errorf("invalid data source mode")
	}
	if config.DataSource.Mode == "http" && config.DataSource.HTTP.BaseURL == "" {
		return fmt.Errorf("http base url is required")
	}

	if config.Process.BatchSize <= 0 {
		return fmt.Errorf("invalid process.batch_size")
	}
	return nil
}

func (c *Config) GetDSN() string {
	return fmt.Sprintf(
		"sqlserver://%s:%s@%s:%s?database=%s&encrypt=disable",
		c.Database.User, c.Database.Password, c.Database.Host, c.Database.Port, c.Database.DBName)
}

func (c *Config) GetProcessConfig(evalDay time.Time) process.ProcessConfig {
	return process.ProcessConfig{
		BatchSize: c.Process.BatchSize,
		Workers:   c.Process.Workers,
		Timeout:   c.Process.Timeout,
		Day:       time.Now(),
		EvalDay:   evalDay,
		DataSource: process.DataSourceConfig{
			Mode: process.DataSourceMode(c.DataSource.Mode),
			HTTP: struct {
				BaseURL string            `json:"base_url"`
				Headers map[string]string `json:"headers"`
			}{
				BaseURL: c.DataSource.HTTP.BaseURL,
				Headers: c.DataSource.HTTP.Headers,
			},
		},
	}
}

func (c *Config) GetRabbitMQConfig() mq.RabbitMQConfig {
	return mq.RabbitMQConfig{
		Host:     c.RabbitMQ.Host,
		Port:     c.RabbitMQ.Port,
		Username: c.RabbitMQ.Username,
		Password: c.RabbitMQ.Password,
		VHost:    c.RabbitMQ.Vhost,
	}
}
