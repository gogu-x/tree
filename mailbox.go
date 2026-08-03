package tree

import (
	"errors"
	"sync/atomic"
	"time"
)

// ErrTimeout is returned when an Envelope.AwaitTimeout expires.
var ErrTimeout = errors.New("actor: timeout")

// ErrActorNotFound is returned when a target PID is not registered.
var ErrActorNotFound = errors.New("actor: not found")

// envelopeKind 区分 Envelope 承载的语义，决定 run 循环如何处理它。
type envelopeKind int

const (
	kindUser     envelopeKind = iota // 普通异步消息，无需回复
	kindRequest                      // 需要回复的请求（同步 Await 或异步回调）
	kindSystem                       // 系统消息（生命周期）
	kindCallback                     // 结果回调：在目标 actor goroutine 内执行 cb
	kindTimer                        // 定时器回调：在目标 actor goroutine 内执行 cb
)

// FutureResult holds the value and error produced by the responding actor.
// 仅在构造 Envelope 时未传回调、走同步 Await 时使用。
type FutureResult struct {
	Value interface{}
	Err   error
}

// Envelope 是投递到 Mailbox 的唯一消息载体，取代原先的
// messageEnvelope / requestEnvelope / pipeCallback / Future。
//
// 三种使用形态：
//   - 普通消息：NewEnvelope(msg, sender, values)，Kind = kindUser。
//   - 请求/响应：NewRequest(msg, sender, values, cb)，Kind = kindRequest；
//     cb 为 nil 时调用方用 Await()/AwaitTimeout() 同步阻塞等待；
//     cb 非 nil 时结果就绪后以异步回调方式在调用方 actor 的 goroutine 内执行。
//   - 回调/定时器：内部使用，Kind = kindCallback / kindTimer，直接携带
//     待执行的闭包，在 run 循环内被识别并调用。
type Envelope struct {
	Msg    interface{} // 输入消息
	Sender PID
	Values map[string]interface{}

	Kind envelopeKind

	// ---- 请求/响应 ----
	resultCh chan FutureResult // 惰性同步等待通道，仅 kindRequest 且未设置回调时使用
	cb       func(Context, interface{}, error)
	replied  int32 // atomic：Respond 只能生效一次

	// ---- 系统消息 ----
	sys systemMessage

	// ---- 定时器/回调载荷 ----
	value interface{}
	err   error

	// pipeSys/pipePID 记录"响应应投递到哪个 actor"，
	// 由 Tree.requestWithValues 在创建时填入。
	pipeSys *Tree
	pipePID PID
}

// NewEnvelope 创建一条普通消息信封。
func NewEnvelope(msg interface{}, sender PID, values map[string]interface{}) *Envelope {
	return &Envelope{Msg: msg, Sender: sender, Values: values, Kind: kindUser}
}

// NewRequest 创建一条请求信封。cb 为 nil 时走同步 Await/AwaitTimeout 语义；
// cb 非 nil 时，结果就绪后会作为 kindCallback 信封投递回 pipePID 的 mailbox，
// 在其 goroutine 内执行 cb(ctx, value, err)——ctx 是 pipePID 对应 actor 的
// Context，因此 cb 内可以继续 ctx.Send/ctx.Request 等操作。
// cb 必须在构造时一次性给定（而不是事后通过 setter 修改），因为 Envelope
// 一旦被 PushUser 进目标 mailbox，就可能在另一个 goroutine 里被并发读取——
// 构造后即不可变才能避免数据竞争。
func NewRequest(msg interface{}, sender PID, values map[string]interface{}, cb func(Context, interface{}, error)) *Envelope {
	return &Envelope{Msg: msg, Sender: sender, Values: values, Kind: kindRequest, cb: cb}
}

// Respond 由响应方调用，投递结果给发起请求的一方。
// 若构造时给定了回调，结果会作为 kindCallback 信封发回调用方 mailbox，在
// 调用方 actor 的 goroutine 内执行 cb(ctx, value, err)；
// 否则写入 resultCh，供 Await/AwaitTimeout 读取。
// 只有第一次调用生效，重复调用是no-op。
//
// 若 cb 非 nil 但 sender 不是一个存活的 actor（例如 Request 是从非 actor
// 上下文发起、没有 mailbox 可投递），cb 不会被执行——因为 cb 需要的 Context
// 只能来自一个真实存活的 actor，没有该 actor 就无法构造出合法的 Context。
// 这种用法本身是编程错误：需要 cb 拿到 Context，就必须通过某个 actor（如
// ctx.RequestCallback）发起请求。
func (e *Envelope) Respond(value interface{}, err error) {
	if !atomic.CompareAndSwapInt32(&e.replied, 0, 1) {
		return
	}
	if e.cb != nil {
		e.pipeSys.sendCallback(e.pipePID, e.cb, value, err)
		return
	}
	e.ensureResultCh()
	e.resultCh <- FutureResult{Value: value, Err: err}
}

// ensureResultCh 惰性创建 resultCh，避免设置了回调的请求也分配 channel。
func (e *Envelope) ensureResultCh() {
	if e.resultCh == nil {
		e.resultCh = make(chan FutureResult, 1)
	}
}

// Await 阻塞直到结果可用并返回。仅适用于构造时未传 cb 的请求。
func (e *Envelope) Await() (interface{}, error) {
	e.ensureResultCh()
	r := <-e.resultCh
	return r.Value, r.Err
}

// AwaitTimeout 阻塞等待结果，最长等待 d。超时返回 ErrTimeout。
func (e *Envelope) AwaitTimeout(d time.Duration) (interface{}, error) {
	e.ensureResultCh()
	select {
	case r := <-e.resultCh:
		return r.Value, r.Err
	case <-time.After(d):
		return nil, ErrTimeout
	}
}

// Mailbox 为每个 actor 提供消息队列。
//
// 系统消息(systemChan)与用户消息(userChan)各自独立，优先级由消费者
// (Tree.run 循环)通过两段式 select 在自己的 goroutine 内裁决，
type Mailbox struct {
	userChan   chan *Envelope // 用户消息，带缓冲，缓冲满则 PushUser 阻塞(背压)
	systemChan chan *Envelope // 系统消息(生命周期)，小缓冲，保证 Stop 不阻塞发送方
}

// NewMailbox 创建 Mailbox，userBufLen 为用户消息缓冲大小。
func NewMailbox(userBufLen int) *Mailbox {
	return &Mailbox{
		userChan:   make(chan *Envelope, userBufLen),
		systemChan: make(chan *Envelope, 1),
	}
}

// PushUser 投递用户消息，缓冲满时阻塞(背压)。
func (mb *Mailbox) PushUser(env *Envelope) {
	mb.userChan <- env
}

// TryPushUser 非阻塞投递用户消息，缓冲满返回 false。
func (mb *Mailbox) TryPushUser(env *Envelope) bool {
	select {
	case mb.userChan <- env:
		return true
	default:
		return false
	}
}

// PushSystem 投递系统消息。systemChan 带 1 缓冲，单个 Stop 信号不会阻塞发送方。
func (mb *Mailbox) PushSystem(env *Envelope) {
	mb.systemChan <- env
}

// Receive 阻塞返回下一条消息，系统消息优先。
// 在调用者(run 循环)的 goroutine 内完成优先级裁决，无额外 goroutine。
func (mb *Mailbox) Receive() *Envelope {
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
