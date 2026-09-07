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
	"strings"
	"sync"
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

// Logger writes colored records to the console. If New receives a non-empty
// pathname, it also writes the same records without ANSI color codes to a tlog
// file in that directory. Logger is safe for concurrent use.
type Logger struct {
	mu            sync.Mutex
	level         Level
	consoleLogger *stdlog.Logger
	fileLogger    *stdlog.Logger
	baseFile      *os.File
	colorEnabled  bool
	closed        bool
}

// New creates a logger at strLevel. Supported levels are debug, release/info,
// warn/warning, error, and fatal. A non-empty pathname enables additional file
// output. Passing flag=0 uses DefaultFlags; pass NoFlags to suppress headers.
func New(strLevel string, pathname string, flag int) (*Logger, error) {
	level, err := parseLevel(strLevel)
	if err != nil {
		return nil, err
	}

	flags := normalizeFlags(flag)
	logger := newLogger(level, os.Stdout, nil, flags, consoleSupportsColor())

	if pathname == "" {
		return logger, nil
	}

	if err := os.MkdirAll(pathname, 0o755); err != nil {
		return nil, fmt.Errorf("create tlog directory %q: %w", pathname, err)
	}

	filename := time.Now().Format("20060102_15_04_05.000000") + ".tlog"
	file, err := os.OpenFile(filepath.Join(pathname, filename), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("create tlog file: %w", err)
	}

	logger.fileLogger = stdlog.New(file, "", flags)
	logger.baseFile = file
	return logger, nil
}

func newLogger(level Level, consoleWriter, fileWriter io.Writer, flags int, colorEnabled bool) *Logger {
	logger := &Logger{
		level:        level,
		colorEnabled: colorEnabled,
	}
	if consoleWriter != nil {
		logger.consoleLogger = stdlog.New(consoleWriter, "", flags)
	}
	if fileWriter != nil {
		logger.fileLogger = stdlog.New(fileWriter, "", flags)
	}
	return logger
}

func consoleSupportsColor() bool {
	return os.Getenv("NO_COLOR") == "" && !strings.EqualFold(os.Getenv("TERM"), "dumb")
}

// Close flushes and closes the tlog file. Calling a logging method afterward
// panics, matching the behavior of the original implementation.
func (logger *Logger) Close() {
	logger.mu.Lock()
	defer logger.mu.Unlock()

	if logger.closed {
		return
	}
	if logger.baseFile != nil {
		_ = logger.baseFile.Close()
	}
	logger.consoleLogger = nil
	logger.fileLogger = nil
	logger.baseFile = nil
	logger.closed = true
}

// SetLevel changes the minimum severity emitted by the logger.
func (logger *Logger) SetLevel(level string) error {
	parsed, err := parseLevel(level)
	if err != nil {
		return err
	}
	logger.mu.Lock()
	logger.level = parsed
	logger.mu.Unlock()
	return nil
}

// SetColor enables or disables ANSI colors for console output. File output is
// always plain text.
func (logger *Logger) SetColor(enabled bool) {
	logger.mu.Lock()
	logger.colorEnabled = enabled
	logger.mu.Unlock()
}

// SetFlags changes the date/time and caller flags. Passing 0 selects
// DefaultFlags; pass NoFlags to disable the header.
func (logger *Logger) SetFlags(flags int) {
	flags = normalizeFlags(flags)
	logger.mu.Lock()
	defer logger.mu.Unlock()
	if logger.consoleLogger != nil {
		logger.consoleLogger.SetFlags(flags)
	}
	if logger.fileLogger != nil {
		logger.fileLogger.SetFlags(flags)
	}
}

func (logger *Logger) doPrintf(level Level, label, color, format string, args ...interface{}) {
	logger.mu.Lock()
	if logger.closed {
		logger.mu.Unlock()
		panic("logger closed")
	}
	if level < logger.level {
		logger.mu.Unlock()
		return
	}

	message := fmt.Sprintf(format, args...)
	line := label + " " + message

	if logger.consoleLogger != nil {
		if logger.colorEnabled {
			// Prefixing the standard logger colors its date/time and source header
			// as well as the level and message. Reset before the trailing newline.
			logger.consoleLogger.SetPrefix(color)
			_ = logger.consoleLogger.Output(3, line+colorReset)
		} else {
			logger.consoleLogger.SetPrefix("")
			_ = logger.consoleLogger.Output(3, line)
		}
	}
	if logger.fileLogger != nil {
		_ = logger.fileLogger.Output(3, line)
	}
	logger.mu.Unlock()

	if level == FatalLevel {
		os.Exit(1)
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
