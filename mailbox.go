package tree

// Mailbox 为每个 actor 提供消息队列。
//
// 系统消息(systemChan)与用户消息(userChan)各自独立，优先级由消费者
// (ActorSystem.run 循环)通过两段式 select 在自己的 goroutine 内裁决，
// 不再使用额外的 merge goroutine，也省去一次 channel 转发。
//
// 布局：
//
//	PushUser(msg)   ── buffered chan ──┐
//	                                   ├→ Receive() (system 优先)
//	PushSystem(msg) ── buffered(1) ────┘
type Mailbox struct {
	userChan   chan interface{} // 用户消息，带缓冲，缓冲满则 PushUser 阻塞(背压)
	systemChan chan interface{} // 系统消息(生命周期)，小缓冲，保证 Stop 不阻塞发送方
}

// NewMailbox 创建 Mailbox，userBufLen 为用户消息缓冲大小。
func NewMailbox(userBufLen int) *Mailbox {
	return &Mailbox{
		userChan:   make(chan interface{}, userBufLen),
		systemChan: make(chan interface{}, 1),
	}
}

// PushUser 投递用户消息，缓冲满时阻塞(背压)。
func (mb *Mailbox) PushUser(msg interface{}) {
	mb.userChan <- msg
}

// TryPushUser 非阻塞投递用户消息，缓冲满返回 false。
func (mb *Mailbox) TryPushUser(msg interface{}) bool {
	select {
	case mb.userChan <- msg:
		return true
	default:
		return false
	}
}

// PushSystem 投递系统消息。systemChan 带 1 缓冲，单个 Stop 信号不会阻塞发送方。
func (mb *Mailbox) PushSystem(msg interface{}) {
	mb.systemChan <- msg
}

// Receive 阻塞返回下一条消息，系统消息优先。
// 在调用者(run 循环)的 goroutine 内完成优先级裁决，无额外 goroutine。
func (mb *Mailbox) Receive() interface{} {
	// 第一段：非阻塞优先抽取系统消息。
	select {
	case m := <-mb.systemChan:
		return m
	default:
	}
	// 第二段：两者都阻塞等待；若同时就绪 Go 随机选，
	// 但下一轮 Receive 的第一段会保证系统消息先于后续用户消息被处理。
	select {
	case m := <-mb.systemChan:
		return m
	case m := <-mb.userChan:
		return m
	}
}
