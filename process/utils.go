package process

import (
	"fmt"
	"gorm.io/gorm"
	"pred/datasource"
	"pred/logger"
	"pred/models"
	"pred/mq"
	"pred/sql"
	"strconv"
	"strings"
	"time"
)

type DataSourceMode string

type ProcessManager struct {
	db          *gorm.DB
	config      ProcessConfig
	rmqConfig   mq.RabbitMQConfig
	cache       *models.ModelParamsCache
	dataManager datasource.DataSourceManager
	task        string
	errLogger   *logger.Logger
	infoLogger  *logger.Logger
	isEmergency bool
}

type CreateManagerConfig struct {
	Db          *gorm.DB
	Config      ProcessConfig
	RmqConfig   mq.RabbitMQConfig
	Cache       *models.ModelParamsCache
	Task        string
	ErrLogger   *logger.Logger
	InfoLogger  *logger.Logger
	DataManager datasource.DataSourceManager
	IsEmergency bool
}

type DataSourceConfig struct {
	Mode DataSourceMode `json:"mode"`
	HTTP struct {
		BaseURL string            `json:"base_url"`
		Headers map[string]string `json:"headers"`
	} `json:"http"`
}

// ProcessConfig 处理配置
type ProcessConfig struct {
	Day              time.Time
	BatchSize        int           // 每批处理的点位数量
	HistoryBatchSize int           // 每个点位的历史数据批量
	Workers          int           // 并发工作协程数
	Timeout          time.Duration // 整体处理超时时间
	EvalDay          time.Time
	DataSource       DataSourceConfig
}

type ProcessPointTask struct {
	Point     *sql.ThresholdPoints
	BatchSize int
	Day       time.Time
	EvalLen   int
}

type ErrorStats struct {
	PointName    string
	ErrorType    string
	ErrorMessage string
	TimeStamp    time.Time
}

type StaticStats struct {
	ThId     int
	Sigma1   bool
	Sigma2   bool
	Sigma3   bool
	SigmaN   bool
	Abnormal bool
}

type TotalStatic struct {
	Sigma1   int
	Sigma2   int
	Sigma3   int
	SigmaN   int
	Abnormal string
}

func AppendStringToFloats(str string) []float64 {
	numbers := make([]float64, 0)

	if len(str) == 0 {
		return numbers
	}

	strNumbers := strings.Split(str, ",")
	for _, s := range strNumbers {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}

		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return numbers
		}

		numbers = append(numbers, f)
	}

	return numbers
}

func CountAssessResult(point *sql.ThresholdPoints, result sql.AssessResult) StaticStats {
	stats := StaticStats{
		ThId:     point.ThId,
		Sigma1:   false,
		Sigma2:   false,
		Sigma3:   false,
		SigmaN:   false,
		Abnormal: false,
	}
	if result.Sigma >= 0.0 && result.Sigma < 1.0 {
		stats.Sigma1 = true
	} else if result.Sigma >= 1.0 && result.Sigma < 2.0 {
		stats.Sigma2 = true
	} else if result.Sigma >= 2.0 && result.Sigma < 3.0 {
		stats.Sigma3 = true
	} else if result.Sigma >= 3.0 {
		stats.SigmaN = true
	}

	for _, eval := range result.Eval {
		if eval >= 3.0 {
			stats.Abnormal = true
		}
	}

	return stats
}

func filterRecords(db *gorm.DB, records []*sql.YSBase, point *sql.ThresholdPoints) (map[string][]float64, error) {
	result := make(map[string][]float64)
	components := strings.Split(point.Comp, ",")
	deleteKeys := make([]string, 0)

	for _, record := range records {
		timeStr := record.DT.Format("2006-01-02 15:04:05.000")
		signvar := make([]float64, 0, len(components))
		flag := false

		for _, c := range components {
			c = strings.TrimSpace(c)
			var val float64
			switch c {
			case "R11":
				val = record.R11
			case "R12":
				val = record.R12
			case "R13":
				val = record.R13
			case "R21":
				val = record.R21
			case "R22":
				val = record.R22
			case "R23":
				val = record.R23
			default:
				continue
			}

			if val == 0 {
				signvar = nil
				break
			}
			flag = true
			signvar = append(signvar, val)
		}

		if len(signvar) != len(components) {
			// 记录不符合要求的数据
			//err := sql.AddJudgeFlag(tx, point.Tablename, point.PointName, "1", record.DT)
			//if err != nil {
			//	return nil, fmt.Errorf("添加标记失败: %v", err)
			//}
			deleteKeys = append(deleteKeys, timeStr)
			continue
		}

		if len(signvar) > 0 && flag {
			result[timeStr] = signvar
		}
	}

	// 检查数据是否足够
	if len(result) < 23 {
		//log.Printf("no valid data for point %s", point.PointName)
		return nil, fmt.Errorf("no valid prepare data for point %s", point.PointName)
	}

	return result, nil
}

func filterOriginRecords(tx *gorm.DB, records []*sql.YSBase, point *sql.ThresholdPoints) (map[string][]float64, map[string]int, error) {
	result := make(map[string][]float64)
	components := strings.Split(point.Comp, ",")
	deleteKeys := make([]string, 0)
	idMap := make(map[string]int)

	for _, record := range records {
		timeStr := record.DT.Format("2006-01-02 15:04:05.000")
		timeStr1 := record.DT.Format("2006-01-02")
		signvar := make([]float64, 0, len(components))

		if _, exists := idMap[timeStr1]; !exists {
			idMap[timeStr1] = record.ID
		}

		for _, c := range components {
			c = strings.TrimSpace(c)
			var val float64
			switch c {
			case "R11":
				val = record.R11
			case "R12":
				val = record.R12
			case "R13":
				val = record.R13
			case "R21":
				val = record.R21
			case "R22":
				val = record.R22
			case "R23":
				val = record.R23
			default:
				continue
			}

			if val == 0 {
				continue
			}
			signvar = append(signvar, val)
		}

		if len(signvar) != len(components) {
			// 记录不符合要求的数据
			//err := sql.AddJudgeFlag(tx, point.Tablename, point.PointName, "1", record.DT)
			//if err != nil {
			//	return nil, fmt.Errorf("添加标记失败: %v", err)
			//}
			deleteKeys = append(deleteKeys, timeStr)
			continue
		}

		result[timeStr] = signvar
	}

	// 检查数据是否足够
	if len(result) < 1 {
		//log.Printf("no valid origin data for point %s", point.PointName)
		return nil, nil, fmt.Errorf("no valid origin data for point %s", point.PointName)
	}

	return result, idMap, nil
}

func TimeStringsToFloat64(times []string) ([]float64, error) {
	result := make([]float64, len(times))
	for i, timeStr := range times {
		t, err := time.Parse("2006-01-02 15:04:05.000", timeStr)
		if err != nil {
			return nil, fmt.Errorf("time %s is invalid", timeStr)
		}
		result[i] = float64(t.Unix())
	}

	return result, nil
}

func PreprocessDailyData(x1 []float64, xNew []float64, y [][]float64,
	dataPrepare [][]float64) (map[time.Time][][]float64, map[time.Time][][]float64, error) {

	// 初始化返回的两个map
	dataOrigin := make(map[time.Time][][]float64)
	dataPrepareResult := make(map[time.Time][][]float64)

	// Step 1: 按日期分组原始数据
	dailyOrigin := make(map[string]struct {
		t     time.Time
		value []float64
	})

	for i, timestamp := range x1 {
		if i >= len(y) {
			return nil, nil, fmt.Errorf("index out of range: y array length is %d, but trying to access index %d", len(y), i)
		}

		if timestamp == 0 {
			continue
		}
		t := time.Unix(int64(timestamp), 0)
		date := t.Format("2006-01-02")

		if _, exists := dailyOrigin[date]; !exists {
			dailyOrigin[date] = struct {
				t     time.Time
				value []float64
			}{
				t:     t,
				value: y[i],
			}
		}
	}

	// Step 2: 按日期分组xNew数据
	dailyXNew := make(map[string]time.Time)
	dataPrepareDayIndex := 0 // 用于追踪dataPrepare的索引

	for _, timestamp := range xNew {
		if timestamp == 0 {
			continue
		}

		t := time.Unix(int64(timestamp), 0)
		date := t.Format("2006-01-02")

		// 只保留每天的第一条记录
		if _, exists := dailyXNew[date]; !exists {
			if dataPrepareDayIndex >= len(dataPrepare) {
				return nil, nil, fmt.Errorf("dataPrepare array length (%d) is less than number of days in xNew", len(dataPrepare))
			}

			dailyXNew[date] = t

			// 添加预测数据
			dataPrepareResult[t] = [][]float64{dataPrepare[dataPrepareDayIndex]}

			// 如果这一天有原始数据，添加到dataOrigin
			if orig, exists := dailyOrigin[date]; exists {
				dataOrigin[t] = [][]float64{orig.value}
			}

			dataPrepareDayIndex++
		}
	}

	return dataOrigin, dataPrepareResult, nil
}

func StringsToTimes(dates []string, layout string) ([]time.Time, error) {
	times := make([]time.Time, len(dates))
	for i, dateStr := range dates {
		t, err := time.Parse(layout, dateStr)
		if err != nil {
			return nil, fmt.Errorf("parse time error at index %d: %v", i, err)
		}
		times[i] = t
	}
	return times, nil
}

func CheckModels(point *sql.ThresholdPoints) error {
	if strings.TrimSpace(point.PreProModel) == "" {
		point.PreProModel = "ND,ESA,LSR,ML30,NF"
	}

	if strings.TrimSpace(point.PredictModel) == "" {
		point.PreProModel = "LSR,30,1"
	}

	if strings.TrimSpace(point.AssessModel) == "" {
		point.AssessModel = "ESA,1.28"
	}

	return nil
}
