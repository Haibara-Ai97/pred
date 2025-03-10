package sql

import (
	"time"
)

type ThresholdPoints struct {
	PointName    string `gorm:"column:PointName;type:varchar(50);primaryKey"`
	Tablename    string `gorm:"column:TableName;type:varchar(59)"`
	Comp         string `gorm:"column:Comp;type:varchar(20)"`
	CompName     string `gorm:"column:CompName;type:varchar(100)"`
	CalFlag      int    `gorm:"column:CalFlag;type:int"`
	ThId         int    `gorm:"column:th_id;type:int;primaryKey"`
	PreProModel  string `gorm:"column:prepromodel;type:varchar(50)"`
	PredictModel string `gorm:"column:predictmodel;type:varchar(50)"`
	AssessModel  string `gorm:"column:assessmodel;type:varchar(50)"`
}

func (ThresholdPoints) TableName() string {
	return "ThresholdPoints"
}

type YSBase struct {
	ID      int       `gorm:"column:ID;type:int;primaryKey"`
	InstrNo string    `gorm:"column:INSTR_NO;type:varchar(50)"`
	DT      time.Time `gorm:"column:DT;type:datetime;not null"`
	R11     float64   `gorm:"column:R11;type:decimal(32,8)"`
	R12     float64   `gorm:"column:R12;type:decimal(32,8)"`
	R13     float64   `gorm:"column:R13;type:decimal(32,8)"`
	R21     float64   `gorm:"column:R21;type:decimal(32,8)"`
	R22     float64   `gorm:"column:R22;type:decimal(32,8)"`
	R23     float64   `gorm:"column:R23;type:decimal(32,8)"`
	//R31       float64   `gorm:"column:R31;type:decimal(32,8)"`
	//R32       float64   `gorm:"column:R32;type:decimal(32,8)"`
	//R33       float64   `gorm:"column:R33;type:decimal(32,8)"`
	InpDT     time.Time `gorm:"column:INP_DT;type:datetime;not null"`
	AutoJudge string    `gorm:"column:AUTO_JUDGE;type:nchar(1)"`
}

type YSTable struct {
	YSBase
	tableName string
}

func NewYSTable(tableName string) *YSTable {
	return &YSTable{
		tableName: tableName,
	}
}

func (y *YSTable) TableName() string {
	if y.tableName == "" {
		return "YS_NONE"
	}
	return y.tableName
}

type PointProgress struct {
	DesignCode     string    `gorm:"column:DesignCode;type:varchar(50)"`
	CollectTime    time.Time `gorm:"column:collect_Time;type:datetime"`
	HandleTime     time.Time `gorm:"column:handle_Time;type:datetime"`
	JudgeTime      time.Time `gorm:"column:judge_Time;type:datetime"`
	PreprocessTime time.Time `gorm:"column:preprocess_Time;type:datetime"`
	GrossTime      time.Time `gorm:"column:gross_Time;type:datetime"`
	ThId           int       `gorm:"column:th_id;type:int;primaryKey"`
	IsBan          int16     `gorm:"column:isban;type:smallint"`
	PointId        int16     `gorm:"column:pointId;type:smallint"`
	PredictTime    time.Time `gorm:"column:predict_Time;type:datetime"`
	AssessTime     time.Time `gorm:"column:assess_Time;type:datetime"`
}

func (PointProgress) TableName() string {
	return "PointProgress"
}

type ProgressT struct {
	Id     int       `gorm:"column:id;primaryKey"`
	Dt     time.Time `gorm:"column:dt;type:datetime;not null"`
	Work   string    `gorm:"column:work;type:varchar(20)"`
	Optime time.Time `gorm:"column:optime;type:datetime"`
	Flag   int       `gorm:"column:flag;type:int"`
}

func (ProgressT) TableName() string {
	return "Progress_T"
}

type ViewTZBBase struct {
	PName         string    `gorm:"column:pname;type:varchar(30);primaryKey"`
	DT            time.Time `gorm:"column:dt;type:datetime;primaryKey"`
	OptModel      string    `gorm:"column:optmodel;type:varchar(20)"`
	Eval          string    `gorm:"column:eval;type:varchar(10)"`
	Sigma         string    `gorm:"column:sigma;type:varchar(200)"`
	Coef          string    `gorm:"column:coef;type:varchar(30)"`
	Comp          string    `gorm:"column:comp;type:varchar(90)"`
	Origin        string    `gorm:"column:origin;type:varchar(90)"`
	Preprocess    string    `gorm:"column:preprocess;type:varchar(90)"`
	Predict       string    `gorm:"column:predict;type:varchar(90)"`
	ID            int       `gorm:"column:ID;type:int"`
	EmergencyFlag int       `gorm:"column:EmergencyFlag;type:int"`
}

type ViewTZBTable struct {
	ViewTZBBase
	tableName string `gorm:"-"`
}

func NewViewTZBTable(tableName string) *ViewTZBTable {
	return &ViewTZBTable{
		tableName: tableName,
	}
}

func (y *ViewTZBTable) TableName() string {
	if y.tableName == "" {
		return "View_T_ZB_NONE"
	}
	//return "KhidiDam_MW" + ".dbo." + y.tableName
	return y.tableName

}

type TDataJudgeResult struct {
	ID         int       `gorm:"column:ID;primaryKey"`
	MethodCode string    `gorm:"column:METHOD_CODE;type:char(1)"`
	DataID     int       `gorm:"column:DATA_ID"`
	InstrNo    string    `gorm:"column:INSTR_NO;type:varchar(50)"`
	DT         time.Time `gorm:"column:DT;type:datetime"`
}

func (TDataJudgeResult) TableName() string {
	return "T_DATA_JUDGE_RESULT"
}

type SigmaT struct {
	Dt            time.Time `gorm:"column:dt;type:date"`
	Sigma_1       int       `gorm:"column:sigma_1;type:int"`
	Sigma_2       int       `gorm:"column:sigma_2;type:int"`
	Sigma_3       int       `gorm:"column:sigma_3;type:int"`
	Sigma_n       int       `gorm:"column:sigma_n;type:int"`
	Id            int       `gorm:"column:id;type:int;primaryKey"`
	EmergencyFlag int       `gorm:"column:EmergencyFlag;type:int"`
}

func (SigmaT) TableName() string {
	return "Sigma_T"
}

type AbnormPointsT struct {
	Dt            time.Time `gorm:"column:dt;type:date"`
	AbPoints      string    `gorm:"column:abpoints;type:varchar(max)"`
	Id            int       `gorm:"column:id;type:int;primaryKey"`
	EmergencyFlag int       `gorm:"column:EmergencyFlag;type:int"`
}

func (AbnormPointsT) TableName() string {
	return "AbnormPoints_T"
}

type TDataPredictResult struct {
	ID         int       `gorm:"column:ID;primaryKey"`
	MethodCode string    `gorm:"column:METHOD_CODE;type:char(1)"`
	DataID     int       `gorm:"column:DATA_ID"`
	InstrNo    string    `gorm:"column:INSTR_NO;type:varchar(50)"`
	DT         time.Time `gorm:"column:DT;type:datetime"`
	JudgeTime  time.Time `gorm:"column:JUDGE_TIME;type:datetime"`
	CreateTime time.Time `gorm:"column:CREATE_TIME;type:datetime"`
}

func (TDataPredictResult) TableName() string {
	return "T_DATA_PREDICT_RESULT"
}

type IMPINSINF struct {
	InstrNO    string `gorm:"column:INSTR_NO;type:varchar(50);primaryKey"`
	KindsChi   string `gorm:"column:KINDSCHI;type:nvarchar(30)"`
	InstrModel string `gorm:"column:INSTR_MODEL;type:varchar(10)"`
}

func (IMPINSINF) TableName() string {
	return "IMP_INSINF"
}
