package tlog

// Log 是服务入口初始化的日志器；NewLog 仅在启动阶段替换该引用。
var Log *Logger

// NewLog 统一实例调用与包级调用的日志器，初始化失败时保留可用的默认日志器。
func NewLog(pathname string, flag int) {
	logger, err := New("debug", pathname, flag)
	if err != nil {
		globalLogger.Load().Error("[tlog/NewLog] 初始化日志失败, err=%v", err)
		return
	}
	previous := globalLogger.Swap(logger)
	Log = logger
	if previous != nil {
		previous.Close()
	}
}
