package logger

import (
	"fmt"
	"gopkg.in/natefinch/lumberjack.v2"
	"io"
	"log"
	"os"
	"runtime"
	"sync"
	"time"
)

type LogLevel int

const (
	DEBUG LogLevel = iota
	INFO
	WARN
	ERROR
	FATAL
)

type Logger struct {
	mu       sync.Mutex
	level    LogLevel
	debugLog *log.Logger
	infoLog  *log.Logger
	warnLog  *log.Logger
	errorLog *log.Logger
	fatalLog *log.Logger
	logFile  *lumberjack.Logger
}

func NewLogger(logFilePath string, level LogLevel) (*Logger, error) {
	// 创建日志文件
	//logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	//if err != nil {
	//	return nil, fmt.Errorf("无法打开日志文件: %v", err)
	//}

	logFile := &lumberjack.Logger{
		Filename:   logFilePath,
		MaxSize:    100,
		MaxBackups: 3,
		MaxAge:     28,
		Compress:   true,
	}

	logger := &Logger{
		level:   level,
		logFile: logFile,
		debugLog: log.New(io.MultiWriter(logFile, os.Stdout), "DEBUG: ",
			log.Ldate|log.Ltime|log.Lshortfile),
		infoLog: log.New(io.MultiWriter(logFile, os.Stdout), "INFO: ",
			log.Ldate|log.Ltime|log.Lshortfile),
		//warnLog: log.New(io.MultiWriter(logFile, os.Stdout), "WARN: ",
		//	log.Ldate|log.Ltime|log.Lshortfile),
		warnLog: log.New(logFile, "WARN:", log.Ldate|log.Ltime|log.Lshortfile),
		errorLog: log.New(io.MultiWriter(logFile, os.Stderr), "ERROR: ",
			log.Ldate|log.Ltime|log.Lshortfile),
		fatalLog: log.New(io.MultiWriter(logFile, os.Stderr), "FATAL: ",
			log.Ldate|log.Ltime|log.Lshortfile),
	}

	return logger, nil
}

// Debug 记录调试信息
func (l *Logger) Debug(format string, v ...interface{}) {
	if l.level <= DEBUG {
		l.log(l.debugLog, format, v...)
	}
}

// Info 记录普通信息
func (l *Logger) Info(format string, v ...interface{}) {
	if l.level <= INFO {
		l.log(l.infoLog, format, v...)
	}
}

// Warn 记录警告信息
func (l *Logger) Warn(format string, v ...interface{}) {
	if l.level <= WARN {
		l.log(l.warnLog, format, v...)
	}
}

// Error 记录错误信息
func (l *Logger) Error(format string, v ...interface{}) {
	if l.level <= ERROR {
		l.log(l.errorLog, format, v...)
	}
}

// Fatal 记录致命错误并退出程序
func (l *Logger) Fatal(format string, v ...interface{}) {
	if l.level <= FATAL {
		l.log(l.fatalLog, format, v...)
		os.Exit(1)
	}
}

// log 内部通用日志记录方法
func (l *Logger) log(logger *log.Logger, format string, v ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// 获取调用者信息
	_, file, line, ok := runtime.Caller(2)
	if !ok {
		file = "???"
		line = 0
	}

	// 格式化日志消息
	msg := fmt.Sprintf(format, v...)
	logEntry := fmt.Sprintf("[%s] %s:%d %s",
		time.Now().Format("2006-01-02 15:04:05"),
		file, line, msg)

	logger.Println(logEntry)
}

// Close 关闭日志文件
func (l *Logger) Close() error {
	return l.logFile.Close()
}
