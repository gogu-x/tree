package tree

// globalTree 是进程内唯一的运行状态，关服后不再重新初始化。
var globalTree = treeState{stopped: make(chan struct{})}

// Stop 停止指定 Actor 并等待退出清理；不得在该 Actor 自身回调中调用。
func Stop(pid PID) {
	if proc, ok := loadProc(pid); ok {
		stopAndWait(proc)
	}
}
