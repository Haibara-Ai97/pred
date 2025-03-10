package process

import (
	"context"
	"fmt"
	"gorm.io/gorm"
	"log"
	"pred/datasource"
	"pred/models"
	"pred/mq"
	"pred/sql"
	"strconv"
	"sync"
	"time"
)

// NewProcessManager 创建处理管理器
func NewProcessManager(cfg *CreateManagerConfig) (*ProcessManager, error) {
	return &ProcessManager{
		db:          cfg.Db,
		config:      cfg.Config,
		rmqConfig:   cfg.RmqConfig,
		cache:       cfg.Cache,
		dataManager: cfg.DataManager,
		task:        cfg.Task,
		errLogger:   cfg.ErrLogger,
		infoLogger:  cfg.InfoLogger,
		isEmergency: cfg.IsEmergency,
	}, nil
}

func (pm *ProcessManager) Start(ctx context.Context) error {
	// 记录一天处理时间
	start := time.Now()
	defer func() {
		duration := time.Since(start)
		fmt.Printf("Predict Operation took %v\n", duration)
	}()

	var err error

	// 创建处理记录-ProgressT表
	switch pm.task {
	case "gross":
		err = sql.ToGrossJudge(pm.db, pm.config.EvalDay, 0)
	case "predict":
		err = sql.ToDataHandle(pm.db, pm.config.EvalDay, 0)
	case "gross&predict":
		err = sql.ToGrossJudge(pm.db, pm.config.EvalDay, 0)
		err = sql.ToDataHandle(pm.db, pm.config.EvalDay, 0)
	}
	if err != nil {
		return err
	}

	// 获取所有点位 points为sql.ThresholdPoints类型-ThresholdPoints表
	points, err := sql.GetAllPointNames(pm.db)
	if err != nil {
		log.Printf("Get all points failed: %v", err)
		return err
	}

	// 处理结果和error的统计
	var totalProcessed, totalFailed int
	var allErrorStats []ErrorStats
	errorTypeCount := make(map[string]int)
	var totalStatic TotalStatic

	// 分批处理 BatchSize在pm的config中配置
	batchSize := pm.config.BatchSize
	batches := make([][]sql.ThresholdPoints, 0, (len(points)+batchSize-1)/batchSize)

	for i := 0; i < len(points); i += batchSize {
		end := i + batchSize
		if end > len(points) {
			end = len(points)
		}
		batches = append(batches, points[i:end])
	}

	log.Printf("Split into %d batches of size %d", len(batches), batchSize)

	for batchIndex, batch := range batches {
		select {
		case <-ctx.Done():
			return fmt.Errorf("process has been canceled: %v", ctx.Err())
		default:
			log.Printf("Processing batch %d/%d (size: %d)", batchIndex+1, len(batches), len(batch))
			// 分批处理
			processed, failed, errorStats, staticStats := pm.BatchProcessPoints(ctx, batch)

			// 处理结果统计
			totalProcessed += processed
			totalFailed += failed
			allErrorStats = append(allErrorStats, errorStats...)

			if pm.task == "predict" || pm.task == "gross&predict" {
				for _, static := range staticStats {
					if static.Sigma1 {
						totalStatic.Sigma1++
					}
					if static.Sigma2 {
						totalStatic.Sigma2++
					}
					if static.Sigma3 {
						totalStatic.Sigma3++
					}
					if static.SigmaN {
						totalStatic.SigmaN++
					}
					if static.Abnormal {
						if totalStatic.Abnormal == "" {
							totalStatic.Abnormal = strconv.Itoa(static.ThId)
						} else {
							totalStatic.Abnormal += "," + strconv.Itoa(static.ThId)
						}
					}
				}
			}

			for _, stat := range errorStats {
				errorTypeCount[stat.ErrorType]++
			}
		}
	}

	// log记录
	log.Printf("Process completed - Total processed: %d, Total failed: %d", totalProcessed, totalFailed)
	pm.infoLogger.Info("EvalDay: %s Process completed - Total processed: %d, Total failed: %d",
		pm.config.EvalDay.Format("2006-01-02"), totalProcessed, totalFailed)
	log.Printf("Error type statistics:")
	for errorType, count := range errorTypeCount {
		log.Printf("  %s: %d errors", errorType, count)
		pm.infoLogger.Info(fmt.Sprintf("  %s: %d errors", errorType, count))
	}

	for _, errorStat := range allErrorStats {
		pm.errLogger.Warn("Point: %s ErrorType: %s error: %s",
			errorStat.PointName, errorStat.ErrorType, errorStat.ErrorMessage)
	}

	// 结束记录
	switch pm.task {
	case "gross":
		// 记录处理状态-ProgressT表
		err = sql.ToGrossJudge(pm.db, pm.config.EvalDay, 1)
		if err != nil {
			return err
		}
	case "predict":
		// 记录处理状态-ProgressT表
		err = sql.ToDataHandle(pm.db, pm.config.EvalDay, 1)
		if err != nil {
			return err
		}
		// 更新处理结果-SigmaT AbnormPointsT PointProgress表
		err = sql.Complete(pm.db, totalStatic.Sigma1, totalStatic.Sigma2, totalStatic.Sigma3, totalStatic.SigmaN,
			totalStatic.Abnormal, pm.config.EvalDay, false)
		if err != nil {
			return err
		}
	case "gross&predict":
		// 记录处理状态-ProgressT表
		err = sql.ToGrossJudge(pm.db, pm.config.EvalDay, 1)
		if err != nil {
			return err
		}
		// 记录处理状态-ProgressT表
		err = sql.ToDataHandle(pm.db, pm.config.EvalDay, 1)
		if err != nil {
			return err
		}
		// 更新处理结果-SigmaT AbnormPointsT PointProgress表
		err = sql.Complete(pm.db, totalStatic.Sigma1, totalStatic.Sigma2, totalStatic.Sigma3, totalStatic.SigmaN,
			totalStatic.Abnormal, pm.config.EvalDay, pm.isEmergency)
		if err != nil {
			return err
		}
	}

	return nil
}

func (pm *ProcessManager) BatchProcessPoints(ctx context.Context,
	batch []sql.ThresholdPoints) (processed int, failed int, errorStats []ErrorStats, staticStats []StaticStats) {
	workChan := make(chan ProcessPointTask, pm.config.BatchSize)
	resultChan := make(chan error, pm.config.BatchSize)
	errorStatsChan := make(chan ErrorStats, pm.config.BatchSize)
	staticStatsChan := make(chan StaticStats, pm.config.BatchSize)

	// 初始化数据源
	dataSource := pm.dataManager.GetDataSource()

	var wg sync.WaitGroup
	for i := 0; i < pm.config.Workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// 初始化消息队列
			rmq, err := mq.NewRabbitMQService(pm.rmqConfig)
			if err != nil {
				log.Printf("NewRabbitMQService failed: %v", err)
				resultChan <- fmt.Errorf("worker %d: fail to create RabbitMQ connection: %v", workerID, err)
				return
			}
			defer rmq.Close()

			for task := range workChan {
				select {
				case <-ctx.Done():
					return
				default:
					//log.Printf("Worker %d: Processing point %s", workerID, pointName)
					switch pm.task {
					case "gross":
						err = GrossProcessPoint(ctx, pm.db, task, rmq, pm.cache, dataSource, pm.config.EvalDay, errorStatsChan)
					case "predict":
						err = PredictProcessPoint(ctx, pm.db, task, rmq, pm.cache, dataSource,
							pm.config.EvalDay, errorStatsChan, staticStatsChan)
					case "gross&predict":
						err = FullProcessPoint(ctx, pm.db, task, rmq, pm.cache, dataSource,
							pm.config.EvalDay, errorStatsChan, staticStatsChan, pm.isEmergency)
					}
					resultChan <- err
				}
			}
		}(i)
	}

	go func() {
		for i := range batch {
			select {
			case <-ctx.Done():
				close(workChan)
				return
			case workChan <- ProcessPointTask{
				Point:     &batch[i],
				BatchSize: pm.config.BatchSize,
				Day:       pm.config.Day,
				EvalLen:   1,
			}:
				//log.Printf("Queued point %s for processing", batch[i].PointName)
			}
		}
		close(workChan)
	}()

	go func() {
		wg.Wait()
		close(resultChan)
		close(errorStatsChan)
		close(staticStatsChan)
	}()

	for err := range resultChan {
		if err != nil {
			failed++
		} else {
			processed++
		}
	}

	for stat := range errorStatsChan {
		errorStats = append(errorStats, stat)
	}

	for static := range staticStatsChan {
		staticStats = append(staticStats, static)
	}

	log.Printf("Batch complete - processed: %d, failed: %d", processed, failed)
	return processed, failed, errorStats, staticStats
}

func GrossProcessPoint(ctx context.Context, db *gorm.DB, task ProcessPointTask,
	rmq *mq.RabbitMQService, cache *models.ModelParamsCache, dataSource datasource.DataSource, evalDay time.Time, errorChan chan<- ErrorStats) error {
	//log.Printf("Processing point: %v", task.Point.PointName)
	//log.Printf("Start processing point: %s for eval day: %v", task.Point.PointName, evalDay)

	// error管理
	reportError := func(errType string, err error) error {
		errorChan <- ErrorStats{
			PointName:    task.Point.PointName,
			ErrorType:    errType,
			ErrorMessage: err.Error(),
			TimeStamp:    time.Now(),
		}
		return err
	}

	// 读取点位处理进度信息-PointProgress表
	point := task.Point
	pointInfo, err := sql.GetPointInfo(db, point)
	if err != nil {
		log.Printf("Get start time failed: %v", err)
		return reportError("GetPointInfo", err)
	}

	startDay := pointInfo.GrossTime // 粗差进度
	endDay := pointInfo.CollectTime // 收集日期
	isBan := pointInfo.IsBan
	preproModel := point.PreProModel   // 预处理模型
	predictModel := point.PredictModel // 预测模型
	assessModel := point.AssessModel   // 评判模型
	if isBan == 1 {
		if startDay.After(endDay) {
			// 如果粗查日期大于收集日期 修改handle_Time-PointProgress表
			err = db.Transaction(func(tx *gorm.DB) error {
				return sql.ModifyTime(tx, pointInfo)
			})
			if err != nil {
				return reportError("ModifyTime", err)
			}
			log.Printf("point: %s eval time after collect time: %v", point.PointName, err)
			return nil
		}

		modelKey := models.ModelKey{
			PreProModel:  preproModel,
			PredictModel: predictModel,
			AssessModel:  assessModel,
		}
		var modelParams models.ModelParams
		var found bool

		// 获取模型参数
		if modelParams, found = cache.Get(modelKey); !found {
			// 如果缓存没有命中 向python端发送ModelRequest请求
			req := &mq.ModelRequest{
				PointName:    point.PointName,
				PreProModel:  preproModel,
				PredictModel: predictModel,
				AssessModel:  assessModel,
			}

			if err := rmq.SendModelRequest(ctx, req); err != nil {
				return reportError("SendModelRequest", err)
			}

			resp, err := rmq.ReceiveModelResponse(ctx)
			if err != nil {
				return reportError("ReceiveModelResponse", err)
			}
			if resp.Error != "" {
				return reportError("PythonService", err)
			}

			if resp.PredOut < task.EvalLen {
				return reportError("PredictionLength",
					fmt.Errorf("prediction length (%d) is less than required evaluation length (%d)",
						resp.PredOut, task.EvalLen))
			}
			modelParams.PredOut = resp.PredOut
			modelParams.PredIn = resp.PredIn
			modelParams.Baseline = resp.Baseline
			cache.Set(modelKey, modelParams)
		}
		// 预处理和粗差值操作
		errStats, err := gross(ctx, db, task.Point, rmq, pointInfo, modelParams, dataSource, evalDay)
		if err == nil {
			//log.Printf("successfully processed point: %s", point.PointName)
			return nil
		}
		return reportError(errStats.ErrorType, err)

	}
	return nil
}

func PredictProcessPoint(ctx context.Context, db *gorm.DB, task ProcessPointTask,
	rmq *mq.RabbitMQService, cache *models.ModelParamsCache, dataSource datasource.DataSource,
	evalDay time.Time, errorChan chan<- ErrorStats, staticChan chan<- StaticStats) error {
	//log.Printf("Processing point: %v", task.Point.PointName)
	//log.Printf("Start processing point: %s for eval day: %v", task.Point.PointName, evalDay)

	reportError := func(errType string, err error) error {
		errorChan <- ErrorStats{
			PointName:    task.Point.PointName,
			ErrorType:    errType,
			ErrorMessage: err.Error(),
			TimeStamp:    time.Now(),
		}
		return err
	}

	point := task.Point
	pointInfo, err := sql.GetPointInfo(db, point)
	if err != nil {
		log.Printf("Get start time failed: %v", err)
		return reportError("GetPointInfo", err)
	}
	startDay := pointInfo.HandleTime
	endDay := pointInfo.CollectTime
	//today := task.Day
	isBan := pointInfo.IsBan
	preproModel := point.PreProModel
	predictModel := point.PredictModel
	assessModel := point.AssessModel
	if isBan == 1 {
		if startDay.After(endDay) {
			//err = sql.ModifyTime(db, pointInfo)
			err = db.Transaction(func(tx *gorm.DB) error {
				return sql.ModifyTime(tx, pointInfo)
			})
			if err != nil {
				return reportError("ModifyTime", err)
			}
			log.Printf("point: %s eval time after collect time: %v", point.PointName, err)
			return nil
		}
		modelKey := models.ModelKey{
			PreProModel:  preproModel,
			PredictModel: predictModel,
			AssessModel:  assessModel,
		}
		var modelParams models.ModelParams
		var found bool
		if modelParams, found = cache.Get(modelKey); !found {
			req := &mq.ModelRequest{
				PointName:    point.PointName,
				PreProModel:  preproModel,
				PredictModel: predictModel,
				AssessModel:  assessModel,
			}

			if err := rmq.SendModelRequest(ctx, req); err != nil {
				return reportError("SendModelRequest", err)
			}

			resp, err := rmq.ReceiveModelResponse(ctx)
			if err != nil {
				return reportError("ReceiveModelResponse", err)
			}
			if resp.Error != "" {
				return reportError("PythonService", err)
			}

			if resp.PredOut < task.EvalLen {
				return reportError("PredictionLength",
					fmt.Errorf("prediction length (%d) is less than required evaluation length (%d)",
						resp.PredOut, task.EvalLen))
			}
			modelParams.PredOut = resp.PredOut
			modelParams.PredIn = resp.PredIn
			modelParams.Baseline = resp.Baseline
			cache.Set(modelKey, modelParams)
		}

		errStats, err := predict(ctx, db, task.Point, rmq, pointInfo, modelParams, dataSource, evalDay, staticChan)
		if err == nil {
			//log.Printf("successfully processed point: %s", point.PointName)
			return nil
		}
		return reportError(errStats.ErrorType, err)

	}
	return nil
}

func FullProcessPoint(ctx context.Context, db *gorm.DB, task ProcessPointTask,
	rmq *mq.RabbitMQService, cache *models.ModelParamsCache, dataSource datasource.DataSource,
	evalDay time.Time, errorChan chan<- ErrorStats, staticChan chan<- StaticStats, isEmergency bool) error {
	//log.Printf("Processing point: %v", task.Point.PointName)
	//log.Printf("Start processing point: %s for eval day: %v", task.Point.PointName, evalDay)

	reportError := func(errType string, err error) error {
		errorChan <- ErrorStats{
			PointName:    task.Point.PointName,
			ErrorType:    errType,
			ErrorMessage: err.Error(),
			TimeStamp:    time.Now(),
		}
		return err
	}

	point := task.Point

	_ = CheckModels(point)

	pointInfo, err := sql.GetPointInfo(db, point)
	if err != nil {
		log.Printf("Get start time failed: %v", err)
		return reportError("GetPointInfo", err)
	}

	startDay := pointInfo.GrossTime
	endDay := pointInfo.CollectTime
	//today := task.Day
	isBan := pointInfo.IsBan
	preproModel := point.PreProModel
	predictModel := point.PredictModel
	assessModel := point.AssessModel
	if isBan == 1 {
		if startDay.After(endDay) {
			//err = sql.ModifyTime(db, pointInfo)
			err = db.Transaction(func(tx *gorm.DB) error {
				return sql.ModifyTime(tx, pointInfo)
			})
			if err != nil {
				return reportError("ModifyTime", err)
			}
			log.Printf("point: %s eval time after collect time: %v", point.PointName, err)
			return nil
		}
		modelKey := models.ModelKey{
			PreProModel:  preproModel,
			PredictModel: predictModel,
			AssessModel:  assessModel,
		}
		var modelParams models.ModelParams
		var found bool
		if modelParams, found = cache.Get(modelKey); !found {
			req := &mq.ModelRequest{
				PointName:    point.PointName,
				PreProModel:  preproModel,
				PredictModel: predictModel,
				AssessModel:  assessModel,
			}

			if err := rmq.SendModelRequest(ctx, req); err != nil {
				return reportError("SendModelRequest", err)
			}

			resp, err := rmq.ReceiveModelResponse(ctx)
			if err != nil {
				return reportError("ReceiveModelResponse", err)
			}
			if resp.Error != "" {
				return reportError("PythonService", err)
			}

			if resp.PredOut < task.EvalLen {
				return reportError("PredictionLength",
					fmt.Errorf("prediction length (%d) is less than required evaluation length (%d)",
						resp.PredOut, task.EvalLen))
			}
			modelParams.PredOut = resp.PredOut
			modelParams.PredIn = resp.PredIn
			modelParams.Baseline = resp.Baseline
			cache.Set(modelKey, modelParams)
		}

		errStats, err := total(ctx, db, task.Point, rmq, pointInfo, modelParams, dataSource, evalDay, staticChan, isEmergency)
		if err == nil {
			//log.Printf("successfully processed point: %s", point.PointName)
			return nil
		}
		return reportError(errStats.ErrorType, err)

	}
	return nil
}
