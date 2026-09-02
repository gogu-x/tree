package log

import (
	"bytes"
	stdlog "log"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestLoggerLevelAndColor(t *testing.T) {
	var console bytes.Buffer
	var file bytes.Buffer
	logger := newLogger(ReleaseLevel, &console, &file, 0, true)

	logger.Debug("hidden=%d", 1)
	logger.Release("user=%s count=%d", "alice", 2)

	if console.String() != "\x1b[32m[RELEASE] user=alice count=2\x1b[0m\n" {
		t.Fatalf("unexpected console output: %q", console.String())
	}
	if file.String() != "[RELEASE] user=alice count=2\n" {
		t.Fatalf("unexpected file output: %q", file.String())
	}
	if strings.Contains(file.String(), "\x1b[") {
		t.Fatal("file output contains ANSI color codes")
	}
}

func assertFullCallerPath(t *testing.T, output, message string) {
	t.Helper()
	normalized := filepath.ToSlash(output)
	pattern := `^(.*/tree/log/log_test\.go):\d+: \[DEBUG\] ` + regexp.QuoteMeta(message) + `\n$`
	matches := regexp.MustCompile(pattern).FindStringSubmatch(normalized)
	if len(matches) != 2 {
		t.Fatalf("full caller path was not reported correctly: %q", output)
	}
	if !filepath.IsAbs(filepath.FromSlash(matches[1])) {
		t.Fatalf("caller path is not absolute: %q", matches[1])
	}
}

func TestLoggerCaller(t *testing.T) {
	var output bytes.Buffer
	logger := newLogger(DebugLevel, &output, nil, stdlog.Llongfile, false)

	logger.Debug("caller")

	assertFullCallerPath(t, output.String(), "caller")
}

func TestGlobalLoggerCaller(t *testing.T) {
	previous := globalLogger.Load()
	defer globalLogger.Store(previous)

	var output bytes.Buffer
	globalLogger.Store(newLogger(DebugLevel, &output, nil, stdlog.Llongfile, false))
	Debug("global caller")

	assertFullCallerPath(t, output.String(), "global caller")
}

func TestConsoleColorDefaults(t *testing.T) {
	t.Setenv("NO_COLOR", "")
	t.Setenv("TERM", "")
	if !consoleSupportsColor() {
		t.Fatal("console color should be enabled by default")
	}

	t.Setenv("NO_COLOR", "1")
	if consoleSupportsColor() {
		t.Fatal("NO_COLOR should disable console color")
	}
}

func TestDefaultFlagsAndNoFlags(t *testing.T) {
	if got := normalizeFlags(0); got != DefaultFlags {
		t.Fatalf("normalizeFlags(0)=%d, want %d", got, DefaultFlags)
	}
	if got := normalizeFlags(NoFlags); got != 0 {
		t.Fatalf("normalizeFlags(NoFlags)=%d, want 0", got)
	}
}

func TestSetLevel(t *testing.T) {
	var output bytes.Buffer
	logger := newLogger(DebugLevel, &output, nil, 0, false)

	if err := logger.SetLevel("warning"); err != nil {
		t.Fatal(err)
	}
	logger.Info("hidden")
	logger.Warn("visible")

	if output.String() != "[WARN] visible\n" {
		t.Fatalf("unexpected output: %q", output.String())
	}
	if err := logger.SetLevel("invalid"); err == nil {
		t.Fatal("SetLevel accepted an invalid level")
	}
}
