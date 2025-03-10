package process

import (
	"context"
	"fmt"
	"gorm.io/gorm"
	"log"
	"math"
	"pred/datasource"
	"pred/models"
	"pred/mq"
	"pred/sql"
	"time"
)

func gross(ctx context.Context, db *gorm.DB, point *sql.ThresholdPoints, rmq *mq.RabbitMQService,
	pointInfo *sql.PointProgress, modelParams models.ModelParams, dataSource datasource.DataSource, evalDay time.Time) (ErrorStats, error) {
	// error处理
	makeError := func(errType string, err error) (ErrorStats, error) {
		return ErrorStats{
			PointName:    point.PointName,
			ErrorType:    errType,
			ErrorMessage: err.Error(),
			TimeStamp:    time.Now(),
		}, err
	}

	evalStep := 24 * time.Hour
	prepareDay := pointInfo.PreprocessTime // 已处理进度
	prepareDay = prepareDay.Add(evalStep)
	prepareStepsRequired := math.Max(float64(modelParams.PredIn+modelParams.PredOut-1), float64(modelParams.Baseline))

	startTime := evalDay.Add(-time.Duration(prepareStepsRequired) * 24 * time.Hour)
	startTime = startTime.Add(24 * time.Hour) // 开始时间为评判日期前30天
	endTime1 := evalDay
	endTime2 := endTime1.Add(24 * time.Hour) // 结束时间是评判日期
	//endTime3 := endTime2.Add(24 * time.Hour)

	if prepareDay.IsZero() || prepareDay.Before(endTime1) {
		if point.CalFlag == 0 {
			return makeError("PointExcluded", fmt.Errorf("point: %s is excluded", point.PointName))
		}

		// 获得原始数据-YS_**表
		DBPrepare, err := dataSource.GetPrepareData(ctx, point.Tablename, point.PointName, startTime, endTime1)
		if err != nil {
			return makeError("PrepareSQLFailed", err)
		}
		DBOrigin, err := dataSource.GetOriginData(ctx, point.Tablename, point.PointName, startTime, endTime2)
		if err != nil {
			return makeError("OriginDataFailed", err)
		}

		// 处理数据
		databasePrepare, err := filterRecords(db, DBPrepare, point)
		if err != nil {
			return makeError("FilterPrepareRecordsFailed", err)
		}
		databaseOrigin, idMap, err := filterOriginRecords(db, DBOrigin, point)
		if err != nil {
			return makeError("FilterOriginRecordsFailed", err)
		}

		allX := make([]string, 0, len(databaseOrigin)+len(databasePrepare))
		allY := make([][]float64, 0, len(databaseOrigin)+len(databasePrepare))

		for k, v := range databasePrepare {
			allX = append(allX, k)
			allY = append(allY, v)
		}
		for k, v := range databaseOrigin {
			allX = append(allX, k)
			allY = append(allY, v)
		}

		prepareX := make([]string, 0, len(databasePrepare))
		prepareY := make([][]float64, 0, len(databasePrepare))
		for k, v := range databasePrepare {
			prepareX = append(prepareX, k)
			prepareY = append(prepareY, v)
		}

		// 转换时间为时间戳
		x, _ := TimeStringsToFloat64(prepareX)
		y := prepareY

		// 转换合并后的时间为时间戳
		x1, _ := TimeStringsToFloat64(allX)
		y1 := allY

		// 生成新的采样时间点序列
		xNew := make([]float64, 0)
		for t := startTime; t.Before(endTime1) || t.Equal(endTime1); t = t.Add(24 * time.Hour) {
			xNew = append(xNew, float64(t.Unix()))
		}

		// 向python端发送CalculateRequest请求
		req := &mq.CalculateRequest{
			RequestID:    mq.GenerateUUID(),
			X:            x,
			Y:            y,
			X1:           x1,
			Y1:           y1,
			XNew:         xNew,
			PreProModel:  point.PreProModel,
			PredictModel: point.PredictModel,
			AssessModel:  point.AssessModel,
			PointName:    point.PointName,
			DataOrigin:   databaseOrigin,
		}

		if err = rmq.SendCalculateRequest(ctx, req); err != nil {
			log.Printf("failed to send CalculateRequest: %v", err)
			return makeError("SendCalculateRequest", err)
		}

		resp, err := rmq.ReceiveCalculateResponse(ctx, req.RequestID)
		if err != nil {
			log.Printf("failed to receive CalculateResponse: %v", err)
			return makeError("ReceiveCalculateResponse", err)
		}

		if len(resp.DataPrepare) == 0 {
			return makeError("NoneDataPrepareFromPython", fmt.Errorf("no prepare data"))
		}

		fullOrigin, fullPrepare, _ := PreprocessDailyData(x1, xNew, y1, resp.DataPrepare)

		if len(resp.AbnormPoints) > 0 {
			dts, _ := StringsToTimes(resp.AbnormPoints, "2006-01-02 15:04:05")
			err = sql.UpdateJudgeResult(db, point, pointInfo, dts)
			if err != nil {
				return makeError("UpdateJudgeResult", err)
			}
		}

		// 更新预处理数据-View_T_ZB_**表
		err = db.Transaction(func(tx *gorm.DB) error {
			return sql.ToPreprocess(tx, point, fullOrigin, fullPrepare, idMap)
		})
		if err != nil {
			return makeError("UpdatePreprocessRecord", err)
		}

		if resp.DataIntegrity >= 0.5 {
			// 更新预处理时间-PointProgress表
			err = sql.UpdatePreprocessTime(db, pointInfo, evalDay)
			if err != nil {
				return makeError("UpdatePreprocessTime", err)
			}
		} else {
			return makeError("EfficientOriginRecords",
				fmt.Errorf("OriginRecords not enough: %f", resp.DataIntegrity))
		}
		// 更新粗差值时间-PointProgress表
		err = sql.UpdateGrossTime(db, pointInfo, evalDay)
		if err != nil {
			return makeError("UpdateGrossTime", err)
		}

		if resp.IsModelChange == 1 {
			err = db.Transaction(func(tx *gorm.DB) error {
				err = sql.UpdateModels(tx, resp.PreProModel, resp.PredictModel, resp.AssessModel, point)
				if err != nil {
					return err
				}
				return nil
			})
			if err != nil {
				return makeError("FailUpdateModels", err)
			}
		}

		log.Printf("Successfully gross point: %s", point.PointName)
	}
	return ErrorStats{}, nil
}
