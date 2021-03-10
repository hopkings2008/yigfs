package log

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"strings"
)

type Level int

const (
	ErrorLevel Level = 0 // Errors should be properly handled
	WarnLevel  Level = 1 // Errors could be ignored; messages might need noticed
	InfoLevel  Level = 2 // Informational messages
)

const RequestIdKeyString = "RequestId"

func ParseLevel(levelString string) Level {
	switch strings.ToLower(levelString) {
	case "info":
		return InfoLevel
	case "warn":
		return WarnLevel
	case "error":
		return ErrorLevel
	default:
		return InfoLevel
	}
}

type Logger struct {
	out       io.WriteCloser
	level     Level
	logger    *log.Logger
	requestID string
}

var logFlags = log.Ldate | log.Ltime | log.Lmicroseconds

func NewFileLogger(path string, logLevel Level) Logger {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic("Failed to open log file " + path)
	}
	return NewLogger(f, logLevel)
}

func NewLogger(out io.WriteCloser, logLevel Level) Logger {
	l := Logger{
		out:    out,
		level:  logLevel,
		logger: log.New(out, "", logFlags),
	}
	return l
}

func (l Logger) NewWithRequestID(requestID string) Logger {
	return Logger{
		out:       l.out,
		level:     l.level,
		logger:    l.logger,
		requestID: requestID,
	}
}

func getCaller(skipCallDepth int) string {
	_, fullPath, line, ok := runtime.Caller(skipCallDepth)
	if !ok {
		return ""
	}
	fileParts := strings.Split(fullPath, "/")
	file := fileParts[len(fileParts)-1]
	return fmt.Sprintf("%s:%d", file, line)
}

func (l Logger) prefixArray() []interface{} {
	array := make([]interface{}, 0, 3)
	array = append(array, getCaller(3))
	if len(l.requestID) > 0 {
		array = append(array, l.requestID)
	}
	return array
}

func (l Logger) Info(ctx context.Context, args ...interface{}) {
	if l.level < InfoLevel {
		return
	}
	prefixArray := l.prefixArray()
	prefixArray = append(prefixArray, "[INFO]")
	prefixArray = append(prefixArray, "[ "+RequestIdFromContext(ctx)+" ]")
	args = append(prefixArray, args...)
	l.logger.Println(args...)
}

func (l Logger) Warn(ctx context.Context, args ...interface{}) {
	if l.level < WarnLevel {
		return
	}
	prefixArray := l.prefixArray()
	prefixArray = append(prefixArray, "[WARN]")
	prefixArray = append(prefixArray, "[ "+RequestIdFromContext(ctx)+" ]")
	args = append(prefixArray, args...)
	l.logger.Println(args...)
}

func (l Logger) Error(ctx context.Context, args ...interface{}) {
	if l.level < ErrorLevel {
		return
	}
	prefixArray := l.prefixArray()
	prefixArray = append(prefixArray, "[ERROR]")
	prefixArray = append(prefixArray, "[ "+RequestIdFromContext(ctx)+" ]")
	args = append(prefixArray, args...)
	l.logger.Println(args...)
}

// Write a new line with args. Unless you really want to customize
// output format, use "Info", "Warn", "Error" instead
func (l Logger) Println(args ...interface{}) {
	_, _ = l.out.Write([]byte(fmt.Sprintln(args...)))
}

func (l Logger) Close() error {
	return l.out.Close()
}

// To avoid looped dependency.
// api.RequestIdFromContext() requires RequestContext, while RequestContext depends on meta package.
func RequestIdFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}

	if result, ok := ctx.Value(RequestIdKeyString).(string); ok {
		return result
	}

	return ""
}