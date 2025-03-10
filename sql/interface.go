package sql

import (
	"context"
	"errors"
	"fmt"
	"gorm.io/gorm"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"
)

type AssessResult struct {
	Eval  []float64
	Sigma float64
}

type MaxTimeGetterFunc func(ctx context.Context, tableName string) (*YSBase, error)

func GetAllTableNames(db *gorm.DB) ([]string, error) {
	var tableNames []string

	result := db.Model(&ThresholdPoints{}).Distinct().Pluck("TableName", &tableNames)
	if result.Error != nil {
		return nil, fmt.Errorf("fail to get table names: %v", result.Error)
	}

	return tableNames, nil
}

func UpdateCollectTime(ctx context.Context, db *gorm.DB, getMaxTime MaxTimeGetterFunc) error {
	tableNames, err := GetAllTableNames(db)
	if err != nil {
		return err
	}
	now := time.Now()
	nowSTR := now.Format("2006-01-02 15:04:05.000")
	maxTime := time.Time{}

	for _, tableName := range tableNames {
		tableName = strings.Replace(tableName, "View_T_ZB", "YS", 1)

		//var result YSBase
		//err := db.Raw(fmt.Sprintf("SELECT MAX(DT) as DT FROM %s", tableName)).
		//	Scan(&result).Error
		result, err := getMaxTime(ctx, tableName)

		if err != nil {
			log.Printf("fail to get max DT from table %s: %v", tableName, err)
			continue
		}

		if !result.DT.IsZero() {
			dtStr := result.DT.Format("2006-01-02 15:04:05.000")
			if dtStr > "00" && dtStr < nowSTR && result.DT.After(maxTime) {
				maxTime = result.DT
			}
		}
	}

	if !maxTime.IsZero() {
		if maxTime.After(now) {
			maxTime = now
		}

		maxTimeStr := maxTime.Format("2006-01-02 15:04:05.000")
		log.Printf("max time is %s", maxTimeStr)

		result := db.Session(&gorm.Session{AllowGlobalUpdate: true}).
			Model(&PointProgress{}).Update("collect_Time", maxTime)
		if result.Error != nil {
			return result.Error
		}

		log.Printf("successfully update max time %s", maxTimeStr)
	}
	return nil
}

func GetGrossTime(db *gorm.DB) ([]string, error) {
	var dates []string

	var progressT ProgressT
	err := db.Where("work = ? AND flag = ?", "gross_judge", 1).
		Order("dt desc").First(&progressT).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}

	var pointProgress PointProgress
	err = db.Order("collect_Time desc").First(&pointProgress).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}

	if !progressT.Dt.IsZero() && !pointProgress.CollectTime.IsZero() {
		startDay := progressT.Dt.Format("2006-01-02")
		endDay := pointProgress.CollectTime.Format("2006-01-02")

		startDayTime, _ := time.Parse("2006-01-02", startDay)
		earliestStart, _ := time.Parse("2006-01-02", "2023-01-01")
		endDayTime, _ := time.Parse("2006-01-02", endDay)

		if startDayTime.Before(earliestStart) {
			startDay = "2023-01-01"
			startDayTime = earliestStart
		}

		if !startDayTime.After(endDayTime) {
			dates = append(dates, startDay)

			for current := startDayTime.AddDate(0, 0, 1); !current.After(endDayTime); current = current.AddDate(0, 0, 1) {
				dates = append(dates, current.Format("2006-01-02"))
			}
		}
	}

	log.Printf("date: %v", dates)
	return dates, nil
}

func GetPredictTime(db *gorm.DB) ([]string, error) {
	var dates []string

	var startPointProgress PointProgress
	err := db.Model(&PointProgress{}).Where("isban = ?", 1).
		Where("CONVERT(DATE, handle_Time) <= CONVERT(DATE, collect_Time)").Order("handle_Time desc").First(&startPointProgress).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}

	var endPointProgress PointProgress
	err = db.Model(&PointProgress{}).Where("isban = ?", 1).
		Where("CONVERT(DATE, preprocess_Time) <= CONVERT(DATE, collect_Time)").Order("gross_Time desc").First(&endPointProgress).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}

	if !startPointProgress.HandleTime.IsZero() && !endPointProgress.PreprocessTime.IsZero() {
		startDay := startPointProgress.HandleTime.Add(24 * time.Hour).Format("2006-01-02")
		endDay := endPointProgress.PreprocessTime.Format("2006-01-02")

		startDayTime, _ := time.Parse("2006-01-02", startDay)
		earliestStart, _ := time.Parse("2006-01-02", "2023-01-01")
		endDayTime, _ := time.Parse("2006-01-02", endDay)

		if startDayTime.Before(earliestStart) {
			startDay = "2023-01-01"
			startDayTime = earliestStart
		}

		if !startDayTime.After(endDayTime) {
			dates = append(dates, startDay)

			for current := startDayTime.AddDate(0, 0, 1); !current.After(endDayTime); current = current.AddDate(0, 0, 1) {
				dates = append(dates, current.Format("2006-01-02"))
			}
		}
	}

	log.Printf("date: %v", dates)
	return dates, nil
}

func GetAllPointNames(db *gorm.DB) ([]ThresholdPoints, error) {
	var points []ThresholdPoints
	err := db.Model(&ThresholdPoints{}).
		Where("CalFlag = ?", 1).
		Where("th_id IN (?)",
			db.Model(&ThresholdPoints{}).
				Select("MIN(th_id)").
				Where("CalFlag = ?", 1).
				Group("PointName")).
		Find(&points).Error
	if err != nil {
		return nil, err
	}
	return points, nil
}

func GetPointInfo(db *gorm.DB, point *ThresholdPoints) (*PointProgress, error) {
	var pointInfo PointProgress
	result := db.Model(&PointProgress{}).Where("DesignCode = ?", point.PointName).Find(&pointInfo)
	if result.Error != nil {
		return nil, fmt.Errorf("failed to get point progress: %v", result.Error)
	}
	return &pointInfo, nil
}

func ModifyTime(tx *gorm.DB, pointInfo *PointProgress) error {
	result := tx.Model(&PointProgress{}).Where("th_id = ?", pointInfo.ThId).
		Update("handle_Time", pointInfo.CollectTime)
	if result.Error != nil {
		return fmt.Errorf("fail to modify thid %s handle time: %v", pointInfo.ThId, result.Error)
	}
	return nil
}

func FromRawData(tx *gorm.DB, table, pointName string,
	startTime, endTime time.Time) ([]*YSBase, error) {

	tableName := strings.Replace(table, "View_T_ZB", "YS", 1)
	var records []*YSBase

	err := tx.Table(tableName).Select("ID, INSTR_NO, DT, R11, R12, R13, R21, R22, R23").
		Where("INSTR_NO = ? AND DT >= ? AND DT <= ?", pointName, startTime, endTime).
		Find(&records).Error
	if err != nil {
		return nil, fmt.Errorf("fail to get point: %s records: %v", pointName, err)
	}

	return records, nil
}

func FromRawDataOrigin(tx *gorm.DB, table, pointName string,
	startTime, endTime time.Time) ([]*YSBase, error) {

	tableName := strings.Replace(table, "View_T_ZB", "YS", 1)
	var records []*YSBase

	err := tx.Table(tableName).Select("ID, INSTR_NO, DT, R11, R12, R13, R21, R22, R23").
		Where("INSTR_NO = ? AND DT >= ? AND DT <= ?", pointName, startTime, endTime).
		Find(&records).Error
	if err != nil {
		return nil, fmt.Errorf("fail to get point: %s origin records: %v", pointName, err)
	}

	return records, nil
}

func FromLastRawDataOrigin(tx *gorm.DB, table, pointName string,
	startTime, endTime time.Time) ([]*YSBase, error) {

	tableName := strings.Replace(table, "View_T_ZB", "YS", 1)
	var records []*YSBase

	err := tx.Table(tableName).Select("ID, INSTR_NO, DT, R11, R12, R13, R21, R22, R23").
		Where("INSTR_NO = ? AND DT >= ? AND DT <= ?", pointName, startTime, endTime).
		Order("DT desc").
		First(&records).Error
	if err != nil {
		return nil, fmt.Errorf("fail to get point: %s origin records: %v", pointName, err)
	}

	return records, nil
}

func AddJudgeFlag(tx *gorm.DB, table, pointName, res string, dt time.Time) error {
	tableName := strings.Replace(table, "View_T_ZB", "YS", 1)

	result := tx.Table(tableName).
		Where("INSTR_NO = ? AND DT = ?", pointName, dt).
		Update("AUTO_JUDGE", res)
	if result.Error != nil {
		return fmt.Errorf("fail to add AUTO_JUDGE flag: %v", result.Error)
	}
	return nil
}

func ToPreprocess(tx *gorm.DB, point *ThresholdPoints, dataOrigin, dataPrepare map[time.Time][][]float64, idMap map[string]int) error {
	timePoints := make([]time.Time, 0, len(dataPrepare))
	for t := range dataPrepare {
		timePoints = append(timePoints, t)
	}
	sort.Slice(timePoints, func(i, j int) bool {
		return timePoints[i].Before(timePoints[j])
	})

	table := &ViewTZBTable{tableName: point.Tablename}
	maxRetries := 3

	id := 0

	for _, dt := range timePoints {
		// 准备预处理数据
		var predictVals []string
		for _, val := range dataPrepare[dt][0] {
			predictVals = append(predictVals, fmt.Sprintf("%.4f", val))
		}
		predictValsStr := strings.Join(predictVals, ",")

		// 准备原始数据
		date := dt.Format("2006-01-02")
		originValsStr := "NULL"
		for origDt, origVals := range dataOrigin {
			if origDt.Format("2006-01-02") == date {
				var origValStrs []string
				for _, val := range origVals[0] {
					origValStrs = append(origValStrs, fmt.Sprintf("%.4f", val))
				}
				originValsStr = strings.Join(origValStrs, ",")
				break
			}
		}

		// SQL 参数
		pname := strconv.Itoa(int(point.ThId))
		dtStr := dt.Format("2006-01-02 15:04:05")

		if val, exists := idMap[date]; exists {
			id = val
		} else {
			id = 0
		}

		var lastErr error
		for i := 0; i < maxRetries; i++ {
			err := tx.Transaction(func(ttx *gorm.DB) error {
				// 先尝试更新
				updateResult := ttx.Exec(`
                    UPDATE `+table.TableName()+`
                    SET origin = ?, preprocess = ?, ID = ?
                    WHERE pname = ? AND dt = ?`,
					originValsStr, predictValsStr, id, pname, dtStr,
				)

				if updateResult.Error != nil {
					return updateResult.Error
				}

				// 如果没有更新任何记录，尝试插入
				if updateResult.RowsAffected == 0 {
					insertResult := ttx.Exec(`
                        INSERT INTO `+table.TableName()+` 
                        (pname, dt, comp, origin, preprocess, ID)
                        SELECT ?, ?, ?, ?, ?, ?
                        WHERE NOT EXISTS (
                            SELECT 1 FROM `+table.TableName()+`
                            WHERE pname = ? AND dt = ?
                        )`,
						pname, dtStr, point.Comp, originValsStr, predictValsStr, id,
						pname, dtStr,
					)

					if insertResult.Error != nil {
						return insertResult.Error
					}
				}
				return nil
			})

			if err == nil {
				break
			}

			lastErr = err
			// 只有在发生主键冲突时才重试
			if !strings.Contains(err.Error(), "PRIMARY KEY") {
				return fmt.Errorf("database error: %v", err)
			}

			if i < maxRetries-1 {
				time.Sleep(time.Millisecond * 100 * time.Duration(i+1))
			}
		}

		if lastErr != nil {
			return fmt.Errorf("failed after %d retries: %v", maxRetries, lastErr)
		}
	}

	return nil
}

func UpdatePreprocessTime(db *gorm.DB, pointInfo *PointProgress, dt time.Time) error {
	result := db.Model(&PointProgress{}).Where("th_id = ?", pointInfo.ThId).
		Update("preprocess_time", dt)
	if result.Error != nil {
		return fmt.Errorf("fail to update preprocess time: %v", result.Error)
	}
	return nil
}

func findIDFromYS(db *gorm.DB, point *ThresholdPoints, dt time.Time) (ID int, err error) {
	table := point.Tablename
	tableName := strings.Replace(table, "View_T_ZB", "YS", 1)
	var result *YSBase
	err = db.Table(tableName).Select("ID").Where("INSTR_NO = ? AND DT = ?", point.PointName, dt).Find(&result).Error
	if err != nil {
		return 0, err
	}
	return result.ID, err
}

func findIDFromYSPredict(db *gorm.DB, point *ThresholdPoints, dt time.Time) (ID int, err error) {
	table := point.Tablename
	tableName := strings.Replace(table, "View_T_ZB", "YS", 1)

	startOfDay := dt.Truncate(24 * time.Hour)
	endOfDay := startOfDay.Add(24 * time.Hour)

	var result *YSBase
	err = db.Table(tableName).Select("ID").
		Where("INSTR_NO = ? AND DT> = ? AND DT < ?", point.PointName, startOfDay, endOfDay).
		Order("DT ASC").First(&result).Error
	if err != nil {
		return 0, err
	}
	return result.ID, err
}

func UpdateJudgeResult(db *gorm.DB, point *ThresholdPoints, pointInfo *PointProgress, dts []time.Time) error {
	for _, dt := range dts {
		ID, err := findIDFromYS(db, point, dt)
		if err != nil {
			return err
		}
		result := &TDataJudgeResult{
			MethodCode: "2",
			DataID:     ID,
			InstrNo:    point.PointName,
			DT:         dt,
		}

		tx := db.Begin()
		if tx.Error != nil {
			return fmt.Errorf("begin transaction failed: %v", tx.Error)
		}

		if err := tx.Where("INSTR_NO = ? AND DT = ?", point.PointName, dt).Delete(&TDataJudgeResult{}).Error; err != nil {
			tx.Rollback()
			return fmt.Errorf("delete judge result failed: %v", err)
		}

		if err := tx.Create(&result).Error; err != nil {
			tx.Rollback()
			return fmt.Errorf("insert judge result failed: %v", err)
		}

		if err := tx.Commit().Error; err != nil {
			return fmt.Errorf("commit judge result failed: %v", err)
		}
	}
	return nil
}

func UpdatePredictResult(db *gorm.DB, point *ThresholdPoints, pointInfo *PointProgress, evalDay time.Time) error {
	// 开启事务
	tx := db.Begin()
	if tx.Error != nil {
		return fmt.Errorf("failed to begin transaction: %v", tx.Error)
	}

	// 使用defer确保事务最终会被处理
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
			log.Printf("Panic in UpdatePredictResult: %v", r)
		}
	}()

	// 查找ID
	ID, err := findIDFromYSPredict(tx, point, evalDay)
	if err != nil {
		tx.Rollback()
		return err
	}

	// 创建预测结果
	result := &TDataPredictResult{
		MethodCode: "2",
		DataID:     ID,
		InstrNo:    point.PointName,
		DT:         evalDay,
		JudgeTime:  pointInfo.GrossTime,
		CreateTime: time.Now(),
	}

	// 创建记录
	if err := tx.Create(&result).Error; err != nil {
		tx.Rollback()
		return fmt.Errorf("insert judge result failed: %v", err)
	}

	// 提交事务
	if err := tx.Commit().Error; err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	return nil
}

func UpdateGrossTime(db *gorm.DB, pointInfo *PointProgress, dt time.Time) error {
	result := db.Model(&PointProgress{}).Where("th_id = ?", pointInfo.ThId).
		Update("gross_time", dt)
	if result.Error != nil {
		return fmt.Errorf("fail to update gross time: %v", result.Error)
	}
	return nil
}

func ToGrossJudge(db *gorm.DB, evalDay time.Time, flag int) error {
	date := time.Now()
	record := &ProgressT{
		Dt:     evalDay,
		Work:   "gross_judge",
		Optime: date,
		Flag:   flag,
	}
	err := db.Model(&ProgressT{}).Create(record).Error
	if err != nil {
		return fmt.Errorf("failed to update gross_judge in Progress_T: %v", err)
	}
	return nil
}

func ToDataHandle(db *gorm.DB, evalDay time.Time, flag int) error {
	date := time.Now()
	record := &ProgressT{
		Dt:     evalDay,
		Work:   "datahandle",
		Optime: date,
		Flag:   flag,
	}
	err := db.Model(&ProgressT{}).Create(record).Error
	if err != nil {
		return fmt.Errorf("failed to update gross_judge in Progress_T: %v", err)
	}
	return nil
}

func GetPreparedData(tx *gorm.DB, table string, pointName int,
	startTime, endTime time.Time) ([]*ViewTZBBase, error) {

	var records []*ViewTZBBase

	err := tx.Table(table).Select("pname, dt, comp, origin, preprocess").
		Where("pname = ? AND dt >= ? AND dt < ?", pointName, startTime, endTime).
		Find(&records).Error
	if err != nil {
		return nil, fmt.Errorf("fail to get point: %s records: %v", pointName, err)
	}

	return records, nil
}

func ToPredict(tx *gorm.DB, point *ThresholdPoints, pointInfo *PointProgress,
	dataPredict [][]float64, dataAssess []interface{}, evalDay time.Time) (AssessResult, error) {
	var record ViewTZBBase
	var assessResult AssessResult

	record.Predict = floatSliceToString(dataPredict[0])
	record.OptModel = point.AssessModel
	record.Eval = interfaceSliceToString(dataAssess[0], 0)

	assessResult.Eval = interfaceSliceToFloat(dataAssess[0])
	sigmaList, _ := dataAssess[1].([]interface{})
	sigmaList1, _ := sigmaList[0].([]interface{})
	sigmaFloat, _ := sigmaList1[0].(float64)
	assessResult.Sigma = sigmaFloat

	sigmaStr, _ := dataAssess[1].([]interface{})
	var builder strings.Builder
	builder.WriteString("[[")
	builder.WriteString(interfaceSliceToString(sigmaStr[0], 2))
	builder.WriteString("]]")
	record.Sigma = builder.String()
	record.Coef = interfaceSliceToString(dataAssess[2], 4)

	parsedDate, _ := time.Parse("2006-01-02", evalDay.Format("2006-01-02"))
	startOfDay := parsedDate
	endOfDay := parsedDate.Add(24 * time.Hour)

	err := tx.Table(point.Tablename).Where("pname = ? AND dt >= ? AND dt < ?", point.ThId, startOfDay, endOfDay).
		Omit("dt", "pname").Updates(record).Error
	if err != nil {
		log.Printf("fail to update viewtable: ", err)
		return assessResult, fmt.Errorf("fail to update record: %v", err)
	}

	return assessResult, nil
}

func Complete(db *gorm.DB, sigma1, sigma2, sigma3, sigmaN int, abnormal string, dt time.Time, isEmergency bool) error {
	sigmaT := SigmaT{
		Sigma_1: sigma1,
		Sigma_2: sigma2,
		Sigma_3: sigma3,
		Sigma_n: sigmaN,
		Dt:      dt,
	}

	abnormPointsT := AbnormPointsT{
		AbPoints: abnormal,
		Dt:       dt,
	}

	if isEmergency {
		sigmaT.EmergencyFlag = 1
		abnormPointsT.EmergencyFlag = 1
	}

	err := db.Model(&SigmaT{}).Create(&sigmaT).Error
	if err != nil {
		return fmt.Errorf("fail to insert SigmaT: %v", err)
	}

	err = db.Model(&AbnormPointsT{}).Create(&abnormPointsT).Error
	if err != nil {
		return fmt.Errorf("fail to insert AbnormPointsT: %v", err)
	}

	if !isEmergency {
		err = db.Model(&PointProgress{}).Where("1=1").Update("handle_Time", dt).Error
		if err != nil {
			return fmt.Errorf("fail to update handle_Time: %v", err)
		}
	}
	return nil
}

func UpdateEmergencyFlag(tx *gorm.DB, point *ThresholdPoints, id int) error {
	table := point.Tablename
	pname := strconv.Itoa(point.ThId)

	err := tx.Table(table).Where("pname = ? AND ID = ?", pname, id).Update("EmergencyFlag", 1).Error
	if err != nil {
		return fmt.Errorf("fail to update Emergency Flag: %v", err)
	}
	return nil
}

func UpdateModels(tx *gorm.DB, preproModel, predictModel, assessModel string, point *ThresholdPoints) error {
	record := &ThresholdPoints{
		PreProModel:  preproModel,
		PredictModel: predictModel,
		AssessModel:  assessModel,
	}

	err := tx.Model(&ThresholdPoints{}).
		Where("PointName = ? AND th_id = ?", point.PointName, point.ThId).
		Updates(record).Error
	if err != nil {
		return fmt.Errorf("fail to update models: %v", err)
	}
	return nil
}

func GetKindsChinese(db *gorm.DB, pointName string) (string, error) {
	var result *IMPINSINF
	err := db.Model(&IMPINSINF{}).Where("INSTR_NO = ?", pointName).First(&result).Error
	if err != nil {
		return "", err
	}
	return result.KindsChi, nil
}

func floatSliceToString(numbers []float64) string {
	strNumbers := make([]string, len(numbers))
	for i, number := range numbers {
		strNumbers[i] = fmt.Sprint(number)
	}
	return strings.Join(strNumbers, ",")
}

func interfaceSliceToString(v interface{}, precision int) string {
	slice, _ := v.([]interface{})

	var builder strings.Builder
	for i, value := range slice {
		floatValue, _ := value.(float64)
		if i > 0 {
			builder.WriteString(",")
		}

		builder.WriteString(fmt.Sprintf("%.*f", precision, floatValue))
	}
	return builder.String()
}

func UpdatePredictTime(db *gorm.DB, pointInfo *PointProgress, dt time.Time) error {
	result := db.Model(&PointProgress{}).Where("th_id = ?", pointInfo.ThId).
		Updates(map[string]interface{}{
			"predict_Time": dt,
			"assess_Time":  dt,
		})
	if result.Error != nil {
		return fmt.Errorf("fail to update preprocess time: %v", result.Error)
	}
	return nil
}

func interfaceSliceToFloat(v interface{}) []float64 {
	slice, _ := v.([]interface{})

	var result []float64
	for _, value := range slice {
		floatValue, _ := value.(float64)
		result = append(result, floatValue)
	}
	return result
}
