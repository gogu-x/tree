package tree

// defaultSystem 是进程级全局 Tree 单例。
var defaultSystem = NewTree()

// Default 返回全局 Tree 单例。
func Default() *Tree { return defaultSystem }

// Spawn 在全局系统中注册并启动一批 Actor，名字取自各 Actor 的 Name()。
// 本批次全部 OnInit 完成后，才会开始处理消息。
func Spawn(actors ...Actor) []PID {
	return defaultSystem.Spawn(actors...)
}

// SpawnOne 在全局系统中启动单个 Actor 并返回其 PID。
func SpawnOne(a Actor) PID {
	return defaultSystem.SpawnOne(a)
}

// Send 通过全局系统向目标 Actor 发送消息。
func Send(pid PID, msg interface{}) bool { return defaultSystem.Send(pid, msg) }

// SendCallback 向目标 Actor 投递回调，在目标 Actor goroutine 内执行，
// 并携带该 Actor 的 Context。
func SendCallback(pid PID, cb func(Context, interface{}, error), value interface{}, err error) bool {
	return defaultSystem.SendCallback(pid, cb, value, err)
}

// Lookup 在全局系统中按名称查找 Actor。
func Lookup(name string) (PID, bool) { return defaultSystem.Lookup(name) }

// MustLookup 在全局系统中按名称查找 Actor，未找到则 panic。
func MustLookup(name string) PID {
	return defaultSystem.MustLookup(name)
}

// Request 通过全局系统向目标 Actor 发送请求并返回 Envelope。
func Request(pid PID, msg interface{}) *Envelope { return defaultSystem.Request(pid, msg) }

// RequestCallback 通过全局系统向目标 Actor 发送请求；结果就绪后 cb 会在
// sender 所属 Actor 的 goroutine 内执行，携带该 Actor 的 Context。
// sender 必须是一个存活的 actor PID，否则 cb 不会被执行。
func RequestCallback(pid PID, msg interface{}, sender PID, cb func(Context, interface{}, error)) *Envelope {
	return defaultSystem.RequestCallback(pid, msg, sender, cb)
}

// RequestAsMessage 通过全局系统向目标 Actor 发送请求；结果就绪后会作为一条
// 普通消息投递回 sender 的 mailbox，在其 HandleMessage 内被当作新消息处理。
// sender 必须是一个存活的 actor PID，否则响应到达时会被丢弃。
// 若响应携带非 nil error，会被记录日志且不投递（HandleMessage 签名无 error 位）。
func RequestAsMessage(pid PID, msg interface{}, sender PID) *Envelope {
	return defaultSystem.RequestAsMessage(pid, msg, sender)
}
