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

func predict(ctx context.Context, db *gorm.DB, point *sql.ThresholdPoints, rmq *mq.RabbitMQService,
	pointInfo *sql.PointProgress, modelParams models.ModelParams, dataSource datasource.DataSource,
	evalDay time.Time, staticChan chan<- StaticStats) (ErrorStats, error) {
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
	if prepareDay.Before(evalDay) {
		//log.Printf("Point: %s have no enough prepared data", point.PointName)
		return makeError("UnpreparedPoint", fmt.Errorf("point: %s Date: %v no prepared data", point.PointName, evalDay))
	}
	prepareDay = prepareDay.Add(evalStep)
	prepareStepsRequired := math.Max(float64(modelParams.PredIn+modelParams.PredOut-1), float64(modelParams.Baseline))

	startTime := evalDay.Add(-time.Duration(prepareStepsRequired) * 24 * time.Hour) // 开始时间为评判日期前30天
	//startTime = startTime.Add(24 * time.Hour)
	endTime1 := evalDay
	endTime2 := endTime1.Add(24 * time.Hour) // 结束时间是评判日期
	//endTime3 := endTime2.Add(24 * time.Hour)

	if point.CalFlag == 0 {
		return makeError("PointExcluded", fmt.Errorf("point: %s is excluded", point.PointName))
	}
	// 获取已预处理的数据-View_T_ZB_**表
	dataPrepare, err := sql.GetPreparedData(db, point.Tablename, point.ThId, startTime, endTime2)
	if err != nil {
		return makeError("GetPreparedData", err)
	}
	if len(dataPrepare) < 31 {
		return makeError("InsufficientData", fmt.Errorf("point: %s insufficient data", point.PointName))
	}

	// 处理数据
	dataPrepareX := make([]time.Time, 0, len(dataPrepare))
	dataPrepareY := make([][]float64, 0, len(dataPrepare))
	for _, v := range dataPrepare {
		dataPrepareX = append(dataPrepareX, v.DT)
		dataPrepareY = append(dataPrepareY, AppendStringToFloats(v.Preprocess))
	}

	// 向python端发送PredictRequest请求
	req := &mq.PredictRequest{
		RequestID:    mq.GenerateUUID(),
		DataPrepare:  dataPrepareY,
		PreProModel:  point.PreProModel,
		PredictModel: point.PredictModel,
		AssessModel:  point.AssessModel,
		PointName:    point.PointName,
	}

	if err = rmq.SendPredictRequest(ctx, req); err != nil {
		log.Printf("failed to send PredictRequest: %v", err)
		return makeError("SendPredictRequest", err)
	}

	resp, err := rmq.ReceivePredictResponse(ctx, req.RequestID)
	if err != nil {
		log.Printf("failed to receive PredictResponse: %v", err)
		return makeError("ReceivePredictResponse", err)
	}

	if resp.Error != "" {
		return makeError("ErrorFromPython", fmt.Errorf(resp.Error))
	}

	if len(resp.DataPredict) == 0 {
		return makeError("NoneDataPredictFromPython", fmt.Errorf("no prepare data"))
	}
	if len(resp.DataAssess) == 0 {
		return makeError("NoneDataAssessFromPython", fmt.Errorf("no prepare data"))
	}

	var assessResult sql.AssessResult

	// 更新预测和评判数据-View_T_ZB_**表
	err = db.Transaction(func(tx *gorm.DB) error {
		assessResult, err = sql.ToPredict(tx, point, pointInfo, resp.DataPredict, resp.DataAssess, evalDay)
		if err != nil {
			return err
		}
		return nil
	})
	// 更新评判结果-T_DATA_PREDICT_RESULT表
	//err = sql.UpdatePredictResult(db, point, pointInfo, evalDay)

	//if err != nil {
	//	return makeError("UpdatePredictRecord", err)
	//}

	// 统计评判数据
	staticStats := CountAssessResult(point, assessResult)
	staticChan <- staticStats

	// 更新预测和评判时间-PointProgress表
	err = sql.UpdatePredictTime(db, pointInfo, evalDay)
	if err != nil {
		return makeError("UpdatePredictTime", err)
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

	//log.Printf("Successfully predict point: %s", point.PointName)

	return ErrorStats{}, nil
}
