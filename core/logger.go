package core

import (
	"os"
	"sync"

	"github.com/op/go-logging"
)

// LoggerInterface contains methods which should be present in logger implementation.
type LoggerInterface interface {
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
	Panic(args ...interface{})
	Panicf(format string, args ...interface{})
	Critical(args ...interface{})
	Criticalf(format string, args ...interface{})
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
	Warning(args ...interface{})
	Warningf(format string, args ...interface{})
	Notice(args ...interface{})
	Noticef(format string, args ...interface{})
	Info(args ...interface{})
	Infof(format string, args ...interface{})
	Debug(args ...interface{})
	Debugf(format string, args ...interface{})
}

// Logger component. Uses github.com/op/go-logging under the hood.
// This logger can prevent any write operations (disabled by default, use .Exclusive() method to enable).
type Logger struct {
	logger *logging.Logger
	mutex  *sync.RWMutex
}

// NewLogger will create new goroutine-safe logger with specified formatter.
// Usage:
//	    logger := NewLogger("telegram", logging.ERROR, DefaultLogFormatter())
func NewLogger(transportCode string, logLevel logging.Level, logFormat logging.Formatter) *Logger {
	return &Logger{
		logger: newInheritedLogger(transportCode, logLevel, logFormat),
	}
}

// newInheritedLogger is a constructor for underlying logger in Logger struct.
func newInheritedLogger(transportCode string, logLevel logging.Level, logFormat logging.Formatter) *logging.Logger {
	logger := logging.MustGetLogger(transportCode)
	logBackend := logging.NewLogBackend(os.Stdout, "", 0)
	formatBackend := logging.NewBackendFormatter(logBackend, logFormat)
	backend1Leveled := logging.AddModuleLevel(logBackend)
	backend1Leveled.SetLevel(logLevel, "")
	logging.SetBackend(formatBackend)

	return logger
}

// DefaultLogFormatter will return default formatter for logs.
func DefaultLogFormatter() logging.Formatter {
	return logging.MustStringFormatter(
		`%{time:2006-01-02 15:04:05.000} %{level:.4s} => %{message}`,
	)
}

// Exclusive makes logger goroutine-safe.
func (l *Logger) Exclusive() *Logger {
	if l.mutex == nil {
		l.mutex = &sync.RWMutex{}
	}

	return l
}

// lock locks logger.
func (l *Logger) lock() {
	if l.mutex != nil {
		l.mutex.Lock()
	}
}

// unlock unlocks logger.
func (l *Logger) unlock() {
	if l.mutex != nil {
		l.mutex.Unlock()
	}
}

// Fatal is equivalent to l.Critical(fmt.Sprint()) followed by a call to os.Exit(1).
func (l *Logger) Fatal(args ...interface{}) {
	l.lock()
	defer l.unlock()
	l.logger.Fatal(args...)
}

// Fatalf is equivalent to l.Critical followed by a call to os.Exit(1).
func (l *Logger) Fatalf(format string, args ...interface{}) {
	l.lock()
	defer l.unlock()
	l.logger.Fatalf(format, args...)
}

// Panic is equivalent to l.Critical(fmt.Sprint()) followed by a call to panic().
func (l *Logger) Panic(args ...interface{}) {
	l.lock()
	defer l.unlock()
	l.logger.Panic(args...)
}

// Panicf is equivalent to l.Critical followed by a call to panic().
func (l *Logger) Panicf(format string, args ...interface{}) {
	l.lock()
	defer l.unlock()
	l.logger.Panicf(format, args...)
}

// Critical logs a message using CRITICAL as log level.
func (l *Logger) Critical(args ...interface{}) {
	l.lock()
	defer l.unlock()
	l.logger.Critical(args...)
}

// Criticalf logs a message using CRITICAL as log level.
func (l *Logger) Criticalf(format string, args ...interface{}) {
	l.lock()
	defer l.unlock()
	l.logger.Criticalf(format, args...)
}

// Error logs a message using ERROR as log level.
func (l *Logger) Error(args ...interface{}) {
	l.lock()
	defer l.unlock()
	l.logger.Error(args...)
}

// Errorf logs a message using ERROR as log level.
func (l *Logger) Errorf(format string, args ...interface{}) {
	l.lock()
	defer l.unlock()
	l.logger.Errorf(format, args...)
}

// Warning logs a message using WARNING as log level.
func (l *Logger) Warning(args ...interface{}) {
	l.lock()
	defer l.unlock()
	l.logger.Warning(args...)
}

// Warningf logs a message using WARNING as log level.
func (l *Logger) Warningf(format string, args ...interface{}) {
	l.lock()
	defer l.unlock()
	l.logger.Warningf(format, args...)
}

// Notice logs a message using NOTICE as log level.
func (l *Logger) Notice(args ...interface{}) {
	l.lock()
	defer l.unlock()
	l.logger.Notice(args...)
}

// Noticef logs a message using NOTICE as log level.
func (l *Logger) Noticef(format string, args ...interface{}) {
	l.lock()
	defer l.unlock()
	l.logger.Noticef(format, args...)
}

// Info logs a message using INFO as log level.
func (l *Logger) Info(args ...interface{}) {
	l.lock()
	defer l.unlock()
	l.logger.Info(args...)
}

// Infof logs a message using INFO as log level.
func (l *Logger) Infof(format string, args ...interface{}) {
	l.lock()
	defer l.unlock()
	l.logger.Infof(format, args...)
}

// Debug logs a message using DEBUG as log level.
func (l *Logger) Debug(args ...interface{}) {
	l.lock()
	defer l.unlock()
	l.logger.Debug(args...)
}

// Debugf logs a message using DEBUG as log level.
func (l *Logger) Debugf(format string, args ...interface{}) {
	l.lock()
	defer l.unlock()
	l.logger.Debugf(format, args...)
}
