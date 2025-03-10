package scheduler

import (
	"context"
	"fmt"
	"github.com/robfig/cron/v3"
	"gorm.io/gorm"
	"log"
	"pred/config"
	"pred/datasource"
	"pred/logger"
	"pred/models"
	"pred/process"
	"pred/sql"
	"time"
)

type DataSourceMode string

const (
	DataSourceModeDB   DataSourceMode = "db"
	DataSourceModeHTTP DataSourceMode = "http"
)

type Scheduler struct {
	cron *cron.Cron
}

func DoGrossSchedule(config DoScheduleConfig) error {
	ctx, cancel := context.WithTimeout(context.Background(), config.cfg.Process.Timeout)
	defer cancel()

	// 初始化数据管理器 目前实现了从数据库读取数据和从http请求中读取数据
	_ = config.dataManager.Initialize(ctx)
	dataSource := config.dataManager.GetDataSource()

	// 更新最大收集时间-YS_**表
	err := sql.UpdateCollectTime(ctx, config.db, dataSource.GetMaxTime)
	if err != nil {
		return fmt.Errorf("failed to update collect time: %v", err)
	}

	// 得到处理时间序列-PointProgress表
	grossTimes, err := sql.GetGrossTime(config.db)
	if err != nil {
		return err
	}

	// 模型参数缓存
	cache := models.NewModelParamsCache(config.cfg.Process.CacheTimeout)

	createProcessConfig := &process.CreateManagerConfig{
		Db:          config.db,
		Cache:       cache,
		DataManager: config.dataManager,
		Task:        "gross",
		ErrLogger:   config.errorLog,
		InfoLogger:  config.infoLog,
	}

	for _, grossTime := range grossTimes {
		processTime, _ := time.Parse("2006-01-02", grossTime)
		createProcessConfig.Config = config.cfg.GetProcessConfig(processTime)
		createProcessConfig.RmqConfig = config.cfg.GetRabbitMQConfig()

		manager, _ := process.NewProcessManager(createProcessConfig)
		if err := manager.Start(ctx); err != nil {
			log.Fatal("fatal to process: ", err)
			return err
		}
		log.Printf("successfully gross date: %s", grossTime)
	}
	return nil
}

func DoPredictSchedule(config DoScheduleConfig) error {
	ctx, cancel := context.WithTimeout(context.Background(), config.cfg.Process.Timeout)
	defer cancel()

	// 初始化数据管理器 目前实现了从数据库读取数据和从http请求中读取数据
	_ = config.dataManager.Initialize(ctx)
	dataSource := config.dataManager.GetDataSource()

	// 更新最大收集时间-YS_**表
	err := sql.UpdateCollectTime(ctx, config.db, dataSource.GetMaxTime)
	if err != nil {
		return fmt.Errorf("failed to update collect time: %v", err)
	}

	predictTimes, err := sql.GetPredictTime(config.db)
	if err != nil {
		return err
	}
	cache := models.NewModelParamsCache(config.cfg.Process.CacheTimeout)

	createProcessConfig := &process.CreateManagerConfig{
		Db:          config.db,
		Cache:       cache,
		DataManager: config.dataManager,
		Task:        "predict",
		ErrLogger:   config.errorLog,
		InfoLogger:  config.infoLog,
	}

	for _, grossTime := range predictTimes {
		processTime, _ := time.Parse("2006-01-02", grossTime)
		createProcessConfig.Config = config.cfg.GetProcessConfig(processTime)
		createProcessConfig.RmqConfig = config.cfg.GetRabbitMQConfig()

		manager, _ := process.NewProcessManager(createProcessConfig)
		if err := manager.Start(ctx); err != nil {
			log.Fatal("fatal to process: ", err)
			return err
		}
		log.Printf("successfully predict date: %s", grossTime)
	}
	return nil
}

func DoTotalSchedule(config DoScheduleConfig) error {
	ctx, cancel := context.WithTimeout(context.Background(), config.cfg.Process.Timeout)
	defer cancel()

	cache := models.NewModelParamsCache(config.cfg.Process.CacheTimeout)

	// 初始化数据管理器 目前实现了从数据库读取数据和从http请求中读取数据
	_ = config.dataManager.Initialize(ctx)
	dataSource := config.dataManager.GetDataSource()

	// 更新最大收集时间-YS_**表
	err := sql.UpdateCollectTime(ctx, config.db, dataSource.GetMaxTime)
	if err != nil {
		return fmt.Errorf("failed to update collect time: %v", err)
	}

	createProcessConfig := &process.CreateManagerConfig{
		Db:          config.db,
		Cache:       cache,
		DataManager: config.dataManager,
		Task:        "gross&predict",
		ErrLogger:   config.errorLog,
		InfoLogger:  config.infoLog,
	}

	var handleTimes []string
	if config.cfg.Task.IsEmergency == "true" {
		// 测试用 指定应急情况下的处理日期
		todayTime, _ := time.Parse("2006-01-02", "2023-01-01")
		today := todayTime.Format("2006-01-02")

		//today := time.Now().Format("2006-01-02")
		handleTimes = []string{today}
		log.Printf("date: %v", handleTimes)
		createProcessConfig.IsEmergency = true
	} else {
		handleTimes, err = sql.GetGrossTime(config.db)
		if err != nil {
			return err
		}
	}

	for _, grossTime := range handleTimes {
		processTime, _ := time.Parse("2006-01-02", grossTime)
		createProcessConfig.Config = config.cfg.GetProcessConfig(processTime)
		createProcessConfig.RmqConfig = config.cfg.GetRabbitMQConfig()

		manager, _ := process.NewProcessManager(createProcessConfig)
		if err := manager.Start(ctx); err != nil {
			log.Fatal("fatal to process: ", err)
			return err
		}
		log.Printf("successfully gross & predict date: %s", grossTime)
	}
	return nil
}

func LoadLog(task string) (errorLog, infoLog *logger.Logger, err error) {
	switch task {
	case "gross":
		errorLog, err = logger.NewLogger("logger/gross/ErrorLog.log", logger.INFO)
		infoLog, err = logger.NewLogger("logger/gross/InfoLog.log", logger.INFO)
	case "predict":
		errorLog, err = logger.NewLogger("logger/predict/ErrorLog.log", logger.INFO)
		infoLog, err = logger.NewLogger("logger/predict/InfoLog.log", logger.INFO)
	case "gross&predict":
		errorLog, err = logger.NewLogger("logger/total/ErrorLog.log", logger.INFO)
		infoLog, err = logger.NewLogger("logger/total/InfoLog.log", logger.INFO)
	}
	if err != nil {
		return nil, nil, err
	}
	return errorLog, infoLog, nil
}

type DoScheduleConfig struct {
	db          *gorm.DB
	cfg         *config.Config
	errorLog    *logger.Logger
	infoLog     *logger.Logger
	dataManager datasource.DataSourceManager
}
