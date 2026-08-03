package tree

import "log"

// defaultMailboxSize 是 mailbox 用户消息缓冲的默认大小。
// Actor 可通过实现 MailboxSizer 覆盖（连接型 Actor 建议调小）。
const defaultMailboxSize = 128

// Logger is the minimal interface required for logging in the actor system.
type Logger interface {
	Error(format string, a ...interface{})
}

// stdLogger 是缺省 Logger，输出到标准 log。
type stdLogger struct{}

func (stdLogger) Error(format string, a ...interface{}) {
	log.Printf("[error] "+format, a...)
}

// defaultLogger 在 Actor 未实现 LoggerProvider 时使用。
// 必须非 nil，否则 safeCall 捕获 panic 后会二次崩溃。
var defaultLogger Logger = stdLogger{}

// SetDefaultLogger 替换全局缺省 Logger。传 nil 无效。
func SetDefaultLogger(l Logger) {
	if l != nil {
		defaultLogger = l
	}
}

// mailboxSizeOf 取 Actor 自定义的 mailbox 大小，未实现或非法则用默认值。
func mailboxSizeOf(a Actor) int {
	if s, ok := a.(MailboxSizer); ok {
		if n := s.MailboxSize(); n > 0 {
			return n
		}
	}
	return defaultMailboxSize
}

// loggerOf 取 Actor 自定义的 Logger，未实现则用全局缺省值。
func loggerOf(a Actor) Logger {
	if p, ok := a.(LoggerProvider); ok {
		if l := p.Logger(); l != nil {
			return l
		}
	}
	return defaultLogger
}
