package tlog

import (
	"fmt"
	"io"
	stdlog "log"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"
)

// logSink 独占一个输出目标；慢控制台不会阻塞文件协程，更不会阻塞日志调用方。
type logSink struct {
	logger      *Logger
	queue       *logQueue
	writer      io.Writer
	closer      io.Closer
	plain       bool
	dropped     atomic.Uint64
	writeErrors atomic.Uint64
	written     atomic.Uint64
	done        atomic.Bool
}

// newLogSink 在输出协程启动前完成配置，文件输出目标由该协程关闭。
func newLogSink(logger *Logger, writer io.Writer, plain bool) *logSink {
	sink := &logSink{logger: logger, queue: newLogQueue(), writer: writer, plain: plain}
	if plain {
		sink.closer, _ = writer.(io.Closer)
	}
	go sink.run()
	return sink
}

// submit 仅执行有界原子入队；失败时计数，不同步写备用输出。
func (sink *logSink) submit(record *logRecord) {
	if !sink.queue.push(record) {
		sink.dropped.Add(1)
	}
}

// run 独占输出资源，无消息时短暂休眠，生产者不需要通知通道或输出锁。
func (sink *logSink) run() {
	defer sink.done.Store(true)
	if sink.closer != nil {
		defer sink.closer.Close()
	}
	var reported uint64
	idle := time.Millisecond
	for {
		record := sink.queue.pop()
		if record == nil {
			if sink.logger.closed.Load() && sink.logger.producers.Load() == 0 && sink.queue.head.Load() == sink.queue.next {
				return
			}
			time.Sleep(idle)
			if idle < 10*time.Millisecond {
				idle += time.Millisecond
			}
			continue
		}
		idle = time.Millisecond
		sink.write(record.text(sink.plain))
		if dropped := sink.dropped.Load(); dropped != reported {
			sink.write(fmt.Sprintf("[WARN] [tlog] 异步日志队列满或竞争超限，累计丢弃=%d\n", dropped))
			reported = dropped
		}
		sink.written.Add(1)
	}
}

// write 只在后台执行 I/O，失败计数，避免错误日志递归进入自身。
func (sink *logSink) write(text string) {
	n, err := io.WriteString(sink.writer, text)
	if err != nil || n != len(text) {
		sink.writeErrors.Add(1)
	}
}

// text 保留调用时的时间、源文件、级别和颜色，不把后台消费者当成日志调用者。
func (record *logRecord) text(plain bool) string {
	var header strings.Builder
	timestamp, flags := record.timestamp, record.flags
	if flags&stdlog.LUTC != 0 {
		timestamp = timestamp.UTC()
	}
	if flags&stdlog.Ldate != 0 {
		header.WriteString(timestamp.Format("2006/01/02 "))
	}
	if flags&(stdlog.Ltime|stdlog.Lmicroseconds) != 0 {
		if flags&stdlog.Lmicroseconds != 0 {
			header.WriteString(timestamp.Format("15:04:05.000000 "))
		} else {
			header.WriteString(timestamp.Format("15:04:05 "))
		}
	}
	if flags&(stdlog.Lshortfile|stdlog.Llongfile) != 0 {
		file := record.caller
		if file == "" {
			file = "???"
		}
		if flags&stdlog.Lshortfile != 0 {
			file = filepath.Base(file)
		}
		fmt.Fprintf(&header, "%s:%d: ", file, record.line)
	}
	message := record.label + " " + record.message
	color := ""
	if !plain {
		color = record.color
	}
	line := header.String() + message
	if color != "" {
		if flags&stdlog.Lmsgprefix != 0 {
			line = header.String() + color + message
		} else {
			line = color + line
		}
		line = strings.TrimSuffix(line, "\n") + colorReset
	}
	if !strings.HasSuffix(line, "\n") {
		line += "\n"
	}
	return line
}
