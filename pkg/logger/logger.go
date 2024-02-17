package logger

import (
	"log"
	"os"
	"path/filepath"
	"runtime"
)

const logName = "crdb.log"

var logFileDir = logName

const (
	Error = iota
	Debug
	Info
)

type Logger struct {
	l *log.Logger
}

func SetupLogger(dir string) error {
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return err
	}
	logFileDir = filepath.Join(dir, logName)
	return nil
}

func (logger *Logger) logWithDetials(logType int, format string, a ...interface{}) {
	_, file, line, _ := runtime.Caller(2)
	filename := filepath.Base(file)
	var logTypeStr = ""
	switch logType {
	case Error:
		logTypeStr = "[Error] "
	case Debug:
		logTypeStr = "[Debug] "
	case Info:
		logTypeStr = "[Info] "
	default:
		logTypeStr = ""
	}
	logger.l.Printf(logTypeStr+"[%s:%d] "+format+"\n", append([]interface{}{filename, line}, a...)...)
}

func GetLogger(name string) *Logger {
	logFile, err := os.OpenFile(logFileDir, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Error opening log file: %v", err)
	}
	logName := "[" + name + "]" + " "
	logger := log.New(logFile, logName, log.LstdFlags)
	return &Logger{l: logger}
}

func (logger *Logger) Error(format string, a ...interface{}) {
	logger.logWithDetials(Error, format, a...)
}

func (logger *Logger) Debug(format string, a ...interface{}) {
	logger.logWithDetials(Debug, format, a...)
}

func (logger *Logger) Info(format string, a ...interface{}) {
	logger.logWithDetials(Info, format, a...)
}
