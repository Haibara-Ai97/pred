package scheduler

import (
	"context"
	"fmt"
	"github.com/robfig/cron/v3"
	"gorm.io/driver/sqlserver"
	"gorm.io/gorm"
	"log"
	"os"
	"os/signal"
	"pred/config"
	"pred/datasource"
	"pred/mq"
	"pred/sql"
	"strings"
	"syscall"
	"time"
)

func NewScheduler() *Scheduler {
	// 加载时间
	log.Printf("scheduler starting...")

	// 读取config
	cfg, err := config.LoadConfig("D:\\GoProject\\pred\\config.json")
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	// 加载时间
	loc, err := time.LoadLocation(cfg.App.TimeZone)
	if err != nil {
		log.Fatalf("failed to load timezone: %v", err)
	}
	time.Local = loc

	// 创建定时任务
	c := cron.New(cron.WithLocation(time.Local))

	// 在定时器中添加任务
	_, err = c.AddFunc(cfg.Schedule.ScheduleCron, func() {
		log.Printf("starting task...")

		var scheduleConfig DoScheduleConfig
		scheduleConfig.cfg = cfg

		// 数据库连接
		db, err := gorm.Open(sqlserver.Open(cfg.GetDSN()), &gorm.Config{})
		if err != nil {
			log.Fatal("fail to connect database: ", err)
		}

		sqlDB, err := db.DB()
		if err != nil {
			log.Fatal("fail to connect database: ", err)
		}

		defer func() {
			if err := sqlDB.Close(); err != nil {
				log.Printf("fail to close database: %v", err)
			}
		}()

		sqlDB.SetMaxIdleConns(cfg.Database.MaxIdleConns)
		sqlDB.SetMaxOpenConns(cfg.Database.MaxOpenConns)
		sqlDB.SetConnMaxLifetime(cfg.Database.MaxLifeTime)
		sqlDB.SetConnMaxIdleTime(cfg.Database.MaxIdleTime)

		err = sqlDB.Ping()
		if err != nil {
			log.Fatal("fail to connect database: ", err)
		}
		fmt.Println("success to connect database")

		scheduleConfig.db = db

		// 加载log
		errorLog, infoLog, err := LoadLog(cfg.Task.Task)
		if err != nil {
			log.Fatal("fail to create newLogger: ", err)
		}
		defer func() {
			errorLog.Close()
			infoLog.Close()
		}()

		scheduleConfig.errorLog = errorLog
		scheduleConfig.infoLog = infoLog

		// 初始化数据源 直接读取数据库/http请求
		var dataManager datasource.DataSourceManager
		switch cfg.DataSource.Mode {
		case string(DataSourceModeDB):
			dataManager = datasource.NewDatabaseManager(db)
		case string(DataSourceModeHTTP):
			dataManager = datasource.NewHTTPManager(
				cfg.DataSource.HTTP.BaseURL,
				cfg.DataSource.HTTP.Headers)
		}

		scheduleConfig.dataManager = dataManager

		// 按照不同任务开始执行
		switch cfg.Task.Task {
		case "gross":
			if err := DoGrossSchedule(scheduleConfig); err != nil {
				log.Printf("fail to do gross schedule: %v", err)
				return
			}
		case "predict":
			if err := DoPredictSchedule(scheduleConfig); err != nil {
				log.Printf("fail to do predict schedule: %v", err)
			}
		case "gross&predict":
			if err := DoTotalSchedule(scheduleConfig); err != nil {
				log.Printf("fail to do gross & predict schedule: %v", err)
			}
		}

		log.Printf("success to do task")
	})

	_, err = c.AddFunc("monthly", func() {
		// 数据库连接
		db, err := gorm.Open(sqlserver.Open(cfg.GetDSN()), &gorm.Config{})
		if err != nil {
			log.Fatal("fail to connect database: ", err)
		}

		sqlDB, err := db.DB()
		if err != nil {
			log.Fatal("fail to connect database: ", err)
		}

		defer func() {
			if err := sqlDB.Close(); err != nil {
				log.Printf("fail to close database: %v", err)
			}
		}()

		sqlDB.SetMaxIdleConns(cfg.Database.MaxIdleConns)
		sqlDB.SetMaxOpenConns(cfg.Database.MaxOpenConns)
		sqlDB.SetConnMaxLifetime(cfg.Database.MaxLifeTime)
		sqlDB.SetConnMaxIdleTime(cfg.Database.MaxIdleTime)

		err = sqlDB.Ping()
		if err != nil {
			log.Fatal("fail to connect database: ", err)
			return
		}
		fmt.Println("success to connect database")

		// 初始化数据源 直接读取数据库/http请求
		var dataManager datasource.DataSourceManager
		switch cfg.DataSource.Mode {
		case string(DataSourceModeDB):
			dataManager = datasource.NewDatabaseManager(db)
		case string(DataSourceModeHTTP):
			dataManager = datasource.NewHTTPManager(
				cfg.DataSource.HTTP.BaseURL,
				cfg.DataSource.HTTP.Headers)
		}

		ctx, cancel := context.WithTimeout(context.Background(), cfg.Process.Timeout)
		defer cancel()

		_ = dataManager.Initialize(ctx)
		dataSource := dataManager.GetDataSource()

		points, err := sql.GetAllPointNames(db)
		if err != nil {
			log.Fatal("Updating Models: fail to get points: ", err)
			return
		}

		rmqConfig := cfg.GetRabbitMQConfig()

		rmq, err := mq.NewRabbitMQService(rmqConfig)
		if err != nil {
			log.Printf("NewRabbitMQService failed: %v", err)
			return
		}
		defer rmq.Close()

		for _, point := range points {
			pointType, err := dataSource.GetPointType(ctx, point.PointName)
			if err != nil {
				log.Fatal("Updating Models: fail to get point type: ", err)
				return
			}

			req := &mq.UpdateRequest{
				RequestID: mq.GenerateUUID(),
				PointName: point.PointName,
				PointType: pointType,
			}

			if err = rmq.SendUpdateRequest(ctx, req); err != nil {
				log.Printf("fail to send update request: %v", err)
				return
			}

			resp, err := rmq.ReceiveUpdateResponse(ctx, req.RequestID)
			if err != nil {
				log.Printf("fail to receive update response: %v", err)
				return
			}
			if strings.TrimSpace(resp.PreProModel) == "" || resp.Error != "" {
				log.Printf("fail to receive update response: %v", resp.Error)
				return
			}

			err = db.Transaction(func(tx *gorm.DB) error {
				err := sql.UpdateModels(tx, resp.PreProModel, resp.PredictModel, resp.AssessModel, &point)
				if err != nil {
					return err
				}
				return nil
			})
			if err != nil {
				log.Printf("fail to update models: %v", err)
			}
		}
	})

	if err != nil {
		log.Printf("failed to add gross task: %v", err)
	}

	scheduler := &Scheduler{
		cron: c,
	}
	return scheduler
}

func (s *Scheduler) Start() {
	s.cron.Start()
	log.Printf("scheduler started...")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Printf("scheduler shutting down...")
	s.cron.Stop()
}

func (s *Scheduler) Emergency() {
	log.Printf("emergency starting...")

	// 读取config
	cfg, err := config.LoadConfig("D:\\GoProject\\pred\\emergencyConfig.json")
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	// 直接执行任务
	log.Printf("starting emergency task...")

	var scheduleConfig DoScheduleConfig
	scheduleConfig.cfg = cfg

	// 数据库连接
	db, err := gorm.Open(sqlserver.Open(cfg.GetDSN()), &gorm.Config{})
	if err != nil {
		log.Fatal("fail to connect database: ", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		log.Fatal("fail to connect database: ", err)
	}

	defer func() {
		if err := sqlDB.Close(); err != nil {
			log.Printf("fail to close database: %v", err)
		}
	}()

	sqlDB.SetMaxIdleConns(cfg.Database.MaxIdleConns)
	sqlDB.SetMaxOpenConns(cfg.Database.MaxOpenConns)
	sqlDB.SetConnMaxLifetime(cfg.Database.MaxLifeTime)
	sqlDB.SetConnMaxIdleTime(cfg.Database.MaxIdleTime)

	err = sqlDB.Ping()
	if err != nil {
		log.Fatal("fail to connect database: ", err)
	}
	fmt.Println("success to connect database")

	scheduleConfig.db = db

	// 加载log
	errorLog, infoLog, err := LoadLog(cfg.Task.Task)
	if err != nil {
		log.Fatal("fail to create newLogger: ", err)
	}
	defer func() {
		errorLog.Close()
		infoLog.Close()
	}()

	scheduleConfig.errorLog = errorLog
	scheduleConfig.infoLog = infoLog

	// 初始化数据源 直接读取数据库/http请求
	var dataManager datasource.DataSourceManager
	switch cfg.DataSource.Mode {
	case string(DataSourceModeDB):
		dataManager = datasource.NewDatabaseManager(db)
	case string(DataSourceModeHTTP):
		dataManager = datasource.NewHTTPManager(
			cfg.DataSource.HTTP.BaseURL,
			cfg.DataSource.HTTP.Headers)
	}

	scheduleConfig.dataManager = dataManager

	// 按照不同任务开始执行
	switch cfg.Task.Task {
	case "gross":
		if err := DoGrossSchedule(scheduleConfig); err != nil {
			log.Printf("fail to do gross schedule: %v", err)
			return
		}
	case "predict":
		if err := DoPredictSchedule(scheduleConfig); err != nil {
			log.Printf("fail to do predict schedule: %v", err)
		}
	case "gross&predict":
		if err := DoTotalSchedule(scheduleConfig); err != nil {
			log.Printf("fail to do gross & predict schedule: %v", err)
		}
	}

	log.Printf("success to do task")
}

func (s *Scheduler) Update() {
	log.Printf("emergency starting...")

	// 读取config
	cfg, err := config.LoadConfig("D:\\GoProject\\pred\\emergencyConfig.json")
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	// 直接执行任务
	log.Printf("starting emergency task...")

	// 数据库连接
	db, err := gorm.Open(sqlserver.Open(cfg.GetDSN()), &gorm.Config{})
	if err != nil {
		log.Fatal("fail to connect database: ", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		log.Fatal("fail to connect database: ", err)
	}

	defer func() {
		if err := sqlDB.Close(); err != nil {
			log.Printf("fail to close database: %v", err)
		}
	}()

	sqlDB.SetMaxIdleConns(cfg.Database.MaxIdleConns)
	sqlDB.SetMaxOpenConns(cfg.Database.MaxOpenConns)
	sqlDB.SetConnMaxLifetime(cfg.Database.MaxLifeTime)
	sqlDB.SetConnMaxIdleTime(cfg.Database.MaxIdleTime)

	err = sqlDB.Ping()
	if err != nil {
		log.Fatal("fail to connect database: ", err)
		return
	}
	fmt.Println("success to connect database")

	// 初始化数据源 直接读取数据库/http请求
	var dataManager datasource.DataSourceManager
	switch cfg.DataSource.Mode {
	case string(DataSourceModeDB):
		dataManager = datasource.NewDatabaseManager(db)
	case string(DataSourceModeHTTP):
		dataManager = datasource.NewHTTPManager(
			cfg.DataSource.HTTP.BaseURL,
			cfg.DataSource.HTTP.Headers)
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Process.Timeout)
	defer cancel()

	_ = dataManager.Initialize(ctx)
	dataSource := dataManager.GetDataSource()

	points, err := sql.GetAllPointNames(db)
	if err != nil {
		log.Fatal("Updating Models: fail to get points: ", err)
		return
	}

	rmqConfig := cfg.GetRabbitMQConfig()

	rmq, err := mq.NewRabbitMQService(rmqConfig)
	if err != nil {
		log.Printf("NewRabbitMQService failed: %v", err)
		return
	}
	defer rmq.Close()

	for _, point := range points {
		pointType, err := dataSource.GetPointType(ctx, point.PointName)
		if err != nil {
			log.Fatal("Updating Models: fail to get point type: ", err)
			return
		}

		req := &mq.UpdateRequest{
			RequestID: mq.GenerateUUID(),
			PointName: point.PointName,
			PointType: pointType,
		}

		if err = rmq.SendUpdateRequest(ctx, req); err != nil {
			log.Printf("fail to send update request: %v", err)
			return
		}

		resp, err := rmq.ReceiveUpdateResponse(ctx, req.RequestID)
		if err != nil {
			log.Printf("fail to receive update response: %v", err)
			return
		}
		if strings.TrimSpace(resp.PreProModel) == "" || resp.Error != "" {
			log.Printf("fail to receive update response: %v", resp.Error)
			return
		}

		err = db.Transaction(func(tx *gorm.DB) error {
			err := sql.UpdateModels(tx, resp.PreProModel, resp.PredictModel, resp.AssessModel, &point)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			log.Printf("fail to update models: %v", err)
		}
	}

	log.Printf("success to update models")
}

func (s *Scheduler) DoScheduler(task string) {
	log.Printf("service starting...")

	// 读取config
	cfg, err := config.LoadConfig("D:\\GoProject\\pred\\config.json")
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}
	cfg.Task.Task = task
	cfg.Task.IsEmergency = "false"

	// 直接执行任务
	log.Printf("starting task...")

	var scheduleConfig DoScheduleConfig
	scheduleConfig.cfg = cfg

	// 数据库连接
	db, err := gorm.Open(sqlserver.Open(cfg.GetDSN()), &gorm.Config{})
	if err != nil {
		log.Fatal("fail to connect database: ", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		log.Fatal("fail to connect database: ", err)
	}

	defer func() {
		if err := sqlDB.Close(); err != nil {
			log.Printf("fail to close database: %v", err)
		}
	}()

	sqlDB.SetMaxIdleConns(cfg.Database.MaxIdleConns)
	sqlDB.SetMaxOpenConns(cfg.Database.MaxOpenConns)
	sqlDB.SetConnMaxLifetime(cfg.Database.MaxLifeTime)
	sqlDB.SetConnMaxIdleTime(cfg.Database.MaxIdleTime)

	err = sqlDB.Ping()
	if err != nil {
		log.Fatal("fail to connect database: ", err)
	}
	fmt.Println("success to connect database")

	scheduleConfig.db = db

	// 加载log
	errorLog, infoLog, err := LoadLog(cfg.Task.Task)
	if err != nil {
		log.Fatal("fail to create newLogger: ", err)
	}
	defer func() {
		errorLog.Close()
		infoLog.Close()
	}()

	scheduleConfig.errorLog = errorLog
	scheduleConfig.infoLog = infoLog

	// 初始化数据源 直接读取数据库/http请求
	var dataManager datasource.DataSourceManager
	switch cfg.DataSource.Mode {
	case string(DataSourceModeDB):
		dataManager = datasource.NewDatabaseManager(db)
	case string(DataSourceModeHTTP):
		dataManager = datasource.NewHTTPManager(
			cfg.DataSource.HTTP.BaseURL,
			cfg.DataSource.HTTP.Headers)
	}

	scheduleConfig.dataManager = dataManager

	// 按照不同任务开始执行
	switch cfg.Task.Task {
	case "gross":
		if err := DoGrossSchedule(scheduleConfig); err != nil {
			log.Printf("fail to do gross schedule: %v", err)
			return
		}
	case "predict":
		if err := DoPredictSchedule(scheduleConfig); err != nil {
			log.Printf("fail to do predict schedule: %v", err)
		}
	case "gross&predict":
		if err := DoTotalSchedule(scheduleConfig); err != nil {
			log.Printf("fail to do gross & predict schedule: %v", err)
		}
	}

	log.Printf("success to do task")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Printf("service shutting down...")
}
