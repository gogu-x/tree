package tlog

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// stalledWriter 模拟停止消费的控制台，只阻塞后台输出线程。
type stalledWriter struct {
	entered chan struct{}
	release chan struct{}
	first   atomic.Bool
	buffer  bytes.Buffer
}

func (writer *stalledWriter) Write(data []byte) (int, error) {
	if writer.first.CompareAndSwap(false, true) {
		close(writer.entered)
	}
	<-writer.release
	return writer.buffer.Write(data)
}

// TestSlowConsoleDoesNotBlockLogging 验证慢控制台、满队列和关闭均不会卡住生产者，文件可独立输出。
func TestSlowConsoleDoesNotBlockLogging(t *testing.T) {
	console := &stalledWriter{entered: make(chan struct{}), release: make(chan struct{})}
	var file bytes.Buffer
	logger := newLogger(DebugLevel, console, &file, 0, false)
	defer func() { close(console.release); finishLogger(t, logger) }()
	logger.Info("first")
	select {
	case <-console.entered:
	case <-time.After(time.Second):
		t.Fatal("console worker did not start")
	}
	logger.Info("file continues")
	deadline := time.Now().Add(time.Second)
	for logger.file.written.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if logger.file.written.Load() != 2 {
		t.Fatal("slow console blocked file output")
	}
	completed := make(chan struct{})
	go func() {
		for i := 0; i < logQueueSize*2; i++ {
			logger.Error("queued %d", i)
		}
		logger.Close()
		close(completed)
	}()
	select {
	case <-completed:
	case <-time.After(time.Second):
		t.Fatal("logging or Close blocked on output")
	}
	if logger.Stats().ConsoleDropped == 0 {
		t.Fatal("full queue did not count dropped records")
	}
	if logger.Flush(10 * time.Millisecond) {
		t.Fatal("Flush claimed stalled console completed")
	}
}

// TestQueueConcurrentWrap 验证并发生产、槽位循环复用、无重复，以及每个生产者的顺序。
func TestQueueConcurrentWrap(t *testing.T) {
	queue := newLogQueue()
	const producers = 32
	const perProducer = 2000
	var dropped atomic.Int64
	var finished atomic.Bool
	var wg sync.WaitGroup
	for producer := 0; producer < producers; producer++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for n := 1; n <= perProducer; n++ {
				if !queue.push(&logRecord{line: id, message: fmt.Sprint(n)}) {
					dropped.Add(1)
				}
			}
		}(producer)
	}
	go func() { wg.Wait(); finished.Store(true) }()
	last := make([]int, producers)
	consumed := 0
	deadline := time.Now().Add(10 * time.Second)
	for {
		if record := queue.pop(); record != nil {
			var n int
			fmt.Sscan(record.message, &n)
			if n <= last[record.line] {
				t.Fatalf("duplicate or out of order: producer=%d value=%d previous=%d", record.line, n, last[record.line])
			}
			last[record.line] = n
			consumed++
			continue
		}
		if finished.Load() && consumed+int(dropped.Load()) == producers*perProducer {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("records lost: accepted=%d dropped=%d", consumed, dropped.Load())
		}
		time.Sleep(time.Microsecond)
	}
	if consumed == 0 {
		t.Fatal("no records consumed")
	}
}

// TestLoggerConcurrentClose 验证配置更新、日志投递和关闭并发时不产生竞态或 panic。
func TestLoggerConcurrentClose(t *testing.T) {
	logger := newLogger(DebugLevel, io.Discard, io.Discard, 0, false)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 500; n++ {
				logger.SetColor(n%2 == 0)
				logger.SetFlags(NoFlags)
				_ = logger.SetLevel("debug")
				logger.Debug("record %d", n)
				if n == 250 {
					logger.Close()
				}
			}
		}()
	}
	wg.Wait()
	finishLogger(t, logger)
}

// TestLoggerSnapshotsArguments 验证后台不再访问调用者随后修改的数据。
func TestLoggerSnapshotsArguments(t *testing.T) {
	writer := &stalledWriter{entered: make(chan struct{}), release: make(chan struct{})}
	logger := newLogger(DebugLevel, writer, nil, 0, false)
	values := []int{1, 2}
	logger.Info("values=%v", values)
	<-writer.entered
	values[0] = 99
	close(writer.release)
	finishLogger(t, logger)
	if !strings.Contains(writer.buffer.String(), "values=[1 2]") {
		t.Fatalf("not a snapshot: %q", writer.buffer.String())
	}
}

// TestLoggerTruncatesAndClosesFile 验证正文有界、文件排空关闭以及关闭后调用安全。
func TestLoggerTruncatesAndClosesFile(t *testing.T) {
	var output bytes.Buffer
	logger := newLogger(DebugLevel, &output, nil, 0, false)
	logger.Info("%s", strings.Repeat("x", maxLogMessageBytes*2))
	finishLogger(t, logger)
	if logger.Stats().Truncated != 1 || !strings.Contains(output.String(), "[truncated]") {
		t.Fatal("oversized record was not bounded")
	}
	logger.Info("ignored after close")
	directory := t.TempDir()
	diskLogger, err := New("error", directory, NoFlags)
	if err != nil {
		t.Fatal(err)
	}
	diskLogger.Error("file marker")
	finishLogger(t, diskLogger)
	files, err := filepath.Glob(filepath.Join(directory, "*.tlog"))
	if err != nil || len(files) != 1 {
		t.Fatalf("files=%v err=%v", files, err)
	}
	data, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "file marker") {
		t.Fatal("file was not drained")
	}
}

// TestFatalFlushesBeforeExit 验证 Fatal 保持退出码，并在退出前限时完成异步输出。
func TestFatalFlushesBeforeExit(t *testing.T) {
	if os.Getenv("TLOG_FATAL_CHILD") == "1" {
		logger, _ := New("debug", "", NoFlags)
		logger.Fatal("fatal marker")
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestFatalFlushesBeforeExit$")
	cmd.Env = append(os.Environ(), "TLOG_FATAL_CHILD=1")
	output, err := cmd.CombinedOutput()
	exit, ok := err.(*exec.ExitError)
	if !ok || exit.ExitCode() != 1 {
		t.Fatalf("exit=%v output=%s", err, output)
	}
	if !strings.Contains(string(output), "fatal marker") {
		t.Fatalf("fatal log lost: %s", output)
	}
}
