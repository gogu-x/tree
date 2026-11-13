// Package tlog provides a small leveled logger with colored console output and
// optional plain-text file output.
package tlog

import (
	"errors"
	"fmt"
	"io"
	stdlog "log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"time"
)

// DefaultFlags print the date, time with microseconds, and the caller's full
// file path and line number. Passing 0 to New selects these flags.
const DefaultFlags = stdlog.Ldate | stdlog.Ltime | stdlog.Lmicroseconds | stdlog.Llongfile

// NoFlags can be passed to New or SetFlags to explicitly disable the standard
// tlog header. A literal 0 means DefaultFlags.
const NoFlags = -1

// Level is a logging severity level.
type Level int

const (
	DebugLevel Level = iota
	ReleaseLevel
	WarnLevel
	ErrorLevel
	FatalLevel
)

const (
	colorReset   = "\x1b[0m"
	colorCyan    = "\x1b[36m"
	colorGreen   = "\x1b[32m"
	colorYellow  = "\x1b[33m"
	colorRed     = "\x1b[31m"
	colorBoldRed = "\x1b[1;31m"
)

func (level Level) String() string {
	switch level {
	case DebugLevel:
		return "debug"
	case ReleaseLevel:
		return "release"
	case WarnLevel:
		return "warn"
	case ErrorLevel:
		return "error"
	case FatalLevel:
		return "fatal"
	default:
		return "unknown"
	}
}

func parseLevel(value string) (Level, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "debug":
		return DebugLevel, nil
	case "release", "info":
		return ReleaseLevel, nil
	case "warn", "warning":
		return WarnLevel, nil
	case "error":
		return ErrorLevel, nil
	case "fatal":
		return FatalLevel, nil
	default:
		return DebugLevel, errors.New("unknown level: " + value)
	}
}

func normalizeFlags(flags int) int {
	switch flags {
	case 0:
		return DefaultFlags
	case NoFlags:
		return 0
	default:
		return flags
	}
}

const (
	maxLogMessageBytes  = 16 * 1024       // 单条正文上限，避免队列持有无界的大字符串。
	DefaultFlushTimeout = 2 * time.Second // 进程结束时最多等待日志输出的时间。
)

// Logger 通过原子队列异步输出日志；调用方不等待控制台或文件 I/O。
// 队列满或 CAS 竞争超限时丢弃并计数，控制台和文件输出互不阻塞。
type Logger struct {
	level        atomic.Int32  // 当前最低日志级别。
	flags        atomic.Int64  // 当前日志头格式。
	colorEnabled atomic.Bool   // 是否为控制台着色。
	closed       atomic.Bool   // 是否已停止接收新日志。
	producers    atomic.Int64  // 已进入日志调用、尚未完成投递的生产者数。
	truncated    atomic.Uint64 // 超长正文截断次数。
	console      *logSink      // 控制台独立队列及消费者。
	file         *logSink      // 文件独立队列及消费者。
}

// logRecord 是在调用线程中完成快照的日志，不跨线程持有可变业务参数。
type logRecord struct {
	timestamp time.Time
	caller    string
	line      int
	flags     int
	label     string
	color     string
	message   string
}

// Stats 返回各输出目标的累计丢弃、写入失败及正文截断次数。
type Stats struct {
	ConsoleDropped     uint64 // 控制台队列满或竞争超限时丢弃的条数。
	FileDropped        uint64 // 文件队列满或竞争超限时丢弃的条数。
	ConsoleWriteErrors uint64 // 控制台写入失败次数。
	FileWriteErrors    uint64 // 文件写入失败次数。
	Truncated          uint64 // 超出正文长度上限的条数。
}

// New 创建异步日志器；非空 pathname 启用文件输出，文件打开失败不会启动后台协程。
func New(strLevel string, pathname string, flag int) (*Logger, error) {
	level, err := parseLevel(strLevel)
	if err != nil {
		return nil, err
	}
	var file io.Writer
	if pathname != "" {
		if err := os.MkdirAll(pathname, 0o755); err != nil {
			return nil, fmt.Errorf("create tlog directory %q: %w", pathname, err)
		}
		filename := time.Now().Format("20060102_15_04_05.000000") + ".tlog"
		output, err := os.OpenFile(filepath.Join(pathname, filename), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return nil, fmt.Errorf("create tlog file: %w", err)
		}
		file = output
	}
	return newLogger(level, os.Stdout, file, normalizeFlags(flag), consoleSupportsColor()), nil
}

// newLogger 在发布日志器前设置原子配置和独立的输出协程。
func newLogger(level Level, consoleWriter, fileWriter io.Writer, flags int, colorEnabled bool) *Logger {
	logger := &Logger{}
	logger.level.Store(int32(level))
	logger.flags.Store(int64(flags))
	logger.colorEnabled.Store(colorEnabled)
	if consoleWriter != nil {
		logger.console = newLogSink(logger, consoleWriter, false)
	}
	if fileWriter != nil {
		logger.file = newLogSink(logger, fileWriter, true)
	}
	return logger
}

func consoleSupportsColor() bool {
	return os.Getenv("NO_COLOR") == "" && !strings.EqualFold(os.Getenv("TERM"), "dumb")
}

// Close 非阻塞地停止接收新日志，后台继续排空队列并关闭自有文件。
// 关闭后的日志调用直接忽略。需要确认排空的进程退出路径应额外调用 Flush。
func (logger *Logger) Close() { logger.closed.Store(true) }

// Flush 最多等待 timeout，让调用前已经入队的日志完成输出；超时返回 false。
// 只用于进程退出或显式检查，不应在业务 Actor 的请求处理流程中调用。
func (logger *Logger) Flush(timeout time.Duration) bool {
	var consoleTarget, fileTarget uint64
	if logger.console != nil {
		consoleTarget = logger.console.queue.head.Load()
	}
	if logger.file != nil {
		fileTarget = logger.file.queue.head.Load()
	}
	deadline := time.Now().Add(timeout)
	for {
		closed := logger.closed.Load()
		if logger.sinkFlushed(logger.console, consoleTarget, closed) && logger.sinkFlushed(logger.file, fileTarget, closed) {
			return true
		}
		if !time.Now().Before(deadline) {
			return false
		}
		time.Sleep(time.Millisecond)
	}
}

// sinkFlushed 同时检查写入进度和关闭后的消费者退出状态。
func (*Logger) sinkFlushed(sink *logSink, target uint64, closed bool) bool {
	if sink == nil {
		return true
	}
	if closed {
		return sink.done.Load()
	}
	return sink.written.Load() >= target
}

// Shutdown 停止接收日志并限时等待输出，仅由进程退出路径调用。
func (logger *Logger) Shutdown(timeout time.Duration) bool {
	logger.Close()
	return logger.Flush(timeout)
}

// Stats 通过原子读取返回累计计数，不等待后台输出。
func (logger *Logger) Stats() Stats {
	stats := Stats{Truncated: logger.truncated.Load()}
	if logger.console != nil {
		stats.ConsoleDropped = logger.console.dropped.Load()
		stats.ConsoleWriteErrors = logger.console.writeErrors.Load()
	}
	if logger.file != nil {
		stats.FileDropped = logger.file.dropped.Load()
		stats.FileWriteErrors = logger.file.writeErrors.Load()
	}
	return stats
}

// SetLevel 原子更新最低日志级别，不使用日志输出锁。
func (logger *Logger) SetLevel(level string) error {
	parsed, err := parseLevel(level)
	if err != nil {
		return err
	}
	logger.level.Store(int32(parsed))
	return nil
}

// SetColor 原子更新后续日志的控制台颜色设置。
func (logger *Logger) SetColor(enabled bool) { logger.colorEnabled.Store(enabled) }

// SetFlags 原子更新后续日志的头部格式，已入队日志保留调用时的设置。
func (logger *Logger) SetFlags(flags int) { logger.flags.Store(int64(normalizeFlags(flags))) }

// doPrintf 仅在调用线程快照正文和来源，避免后台访问可变业务对象产生数据竞争。
// 有界原子入队不等待消费者；Fatal 保留退出语义，但排空等待有上限。
func (logger *Logger) doPrintf(level Level, label, color, format string, args ...interface{}) {
	logger.enqueue(level, label, color, format, args...)
	if level == FatalLevel {
		logger.Shutdown(DefaultFlushTimeout)
		os.Exit(1)
	}
}

// enqueue 登记在途生产者，关闭与投递并发时不关闭共享队列，也不会发生发送到已关闭通道的 panic。
func (logger *Logger) enqueue(level Level, label, color, format string, args ...interface{}) {
	if logger.closed.Load() || level < Level(logger.level.Load()) {
		return
	}
	logger.producers.Add(1)
	defer logger.producers.Add(-1)
	if logger.closed.Load() {
		return
	}
	flags := int(logger.flags.Load())
	record := &logRecord{timestamp: time.Now(), flags: flags, label: label}
	if logger.colorEnabled.Load() {
		record.color = color
	}
	if flags&(stdlog.Lshortfile|stdlog.Llongfile) != 0 {
		_, record.caller, record.line, _ = runtime.Caller(3)
	}
	record.message = fmt.Sprintf(format, args...)
	if len(record.message) > maxLogMessageBytes {
		record.message = strings.Clone(record.message[:maxLogMessageBytes]) + " [truncated]"
		logger.truncated.Add(1)
	}
	if logger.console != nil {
		logger.console.submit(record)
	}
	if logger.file != nil {
		logger.file.submit(record)
	}
}

func (logger *Logger) Debug(format string, args ...interface{}) {
	logger.doPrintf(DebugLevel, "[DEBUG]", colorCyan, format, args...)
}

func (logger *Logger) Release(format string, args ...interface{}) {
	logger.doPrintf(ReleaseLevel, "[RELEASE]", colorGreen, format, args...)
}

// Info is an alias-level alternative to Release.
func (logger *Logger) Info(format string, args ...interface{}) {
	logger.doPrintf(ReleaseLevel, "[INFO]", colorGreen, format, args...)
}

func (logger *Logger) Warn(format string, args ...interface{}) {
	logger.doPrintf(WarnLevel, "[WARN]", colorYellow, format, args...)
}

func (logger *Logger) Error(format string, args ...interface{}) {
	logger.doPrintf(ErrorLevel, "[ERROR]", colorRed, format, args...)
}

func (logger *Logger) Fatal(format string, args ...interface{}) {
	logger.doPrintf(FatalLevel, "[FATAL]", colorBoldRed, format, args...)
}

var globalLogger atomic.Pointer[Logger]

func init() {
	logger, err := New("debug", "", DefaultFlags)
	if err != nil {
		panic(err)
	}
	globalLogger.Store(logger)
	Log = logger
}

// Export replaces the package-level logger. It does not close the previous
// logger because callers may still own and use it.
func Export(logger *Logger) {
	if logger != nil {
		globalLogger.Store(logger)
	}
}

func Debug(format string, args ...interface{}) {
	globalLogger.Load().doPrintf(DebugLevel, "[DEBUG]", colorCyan, format, args...)
}

func Release(format string, args ...interface{}) {
	globalLogger.Load().doPrintf(ReleaseLevel, "[RELEASE]", colorGreen, format, args...)
}

func Info(format string, args ...interface{}) {
	globalLogger.Load().doPrintf(ReleaseLevel, "[INFO]", colorGreen, format, args...)
}

func Warn(format string, args ...interface{}) {
	globalLogger.Load().doPrintf(WarnLevel, "[WARN]", colorYellow, format, args...)
}

func Error(format string, args ...interface{}) {
	globalLogger.Load().doPrintf(ErrorLevel, "[ERROR]", colorRed, format, args...)
}

func Fatal(format string, args ...interface{}) {
	globalLogger.Load().doPrintf(FatalLevel, "[FATAL]", colorBoldRed, format, args...)
}

func SetLevel(level string) error {
	return globalLogger.Load().SetLevel(level)
}

func SetColor(enabled bool) {
	globalLogger.Load().SetColor(enabled)
}

func SetFlags(flags int) {
	globalLogger.Load().SetFlags(flags)
}

func Close() {
	globalLogger.Load().Close()
}

// Shutdown 在服务入口退出时统一排空当前全局日志器。
func Shutdown(timeout time.Duration) bool { return globalLogger.Load().Shutdown(timeout) }

// Flush 限时等待当前全局日志器输出已接收的记录。
func Flush(timeout time.Duration) bool { return globalLogger.Load().Flush(timeout) }
