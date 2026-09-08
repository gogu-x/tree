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
	kindRequest                      // 需要回复的请求（同步 Await / 异步回调 / 响应转消息）
	kindSystem                       // 系统消息（生命周期）
	kindCallback                     // 结果回调：在目标 actor goroutine 内执行 cb
)

// responseMode 区分 kindRequest 信封的响应投递方式。
type responseMode int

const (
	modeAwait    responseMode = iota // 默认：写入 resultCh，供 Await/AwaitTimeout 读取
	modeCallback                     // 结果作为 kindCallback 信封投递回 sender，在其 goroutine 内执行 cb
	modeMessage                      // 结果作为 kindUser 信封投递回 sender，走 sender 的 HandleMessage
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
// 四种使用形态：
//   - 普通消息：NewEnvelope(msg, sender, values)，Kind = kindUser。
//   - 请求/响应（同步等待）：NewRequest(msg, sender, values, nil)，Kind = kindRequest，
//     Mode = modeAwait；调用方用 Await()/AwaitTimeout() 同步阻塞等待。
//   - 请求/响应（异步回调）：NewRequest(msg, sender, values, cb)，Kind = kindRequest，
//     Mode = modeCallback；结果就绪后以异步回调方式在调用方 actor 的 goroutine 内执行。
//   - 请求/响应（响应转消息）：NewRequestAsMessage(msg, sender, values)，Kind = kindRequest，
//     Mode = modeMessage；结果就绪后作为一条普通 kindUser 消息投递回调用方 actor 的
//     mailbox，在其 HandleMessage 内被当作一条新消息处理（而不是走专门的 cb）。
//     适用于"统一在 HandleMessage 里处理所有回复"的场景，如 natsrpc 转发。
//   - 回调：内部使用，Kind = kindCallback，直接携带待执行的闭包，
//     在 run 循环内被识别并调用。
type Envelope struct {
	Msg    interface{} // 输入消息
	Sender PID
	Values map[string]interface{}

	Kind envelopeKind

	// ---- 请求/响应 ----
	Mode     responseMode
	resultCh chan FutureResult // 同步等待通道，仅 Mode == modeAwait 时分配
	cb       func(Context, interface{}, error)
	replied  int32 // atomic：Respond 只能生效一次

	// ---- 系统消息 ----
	sys systemMessage

	// ---- 回调载荷 ----
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

// NewRequest 创建一条请求信封。cb 为 nil 时走同步 Await/AwaitTimeout 语义（Mode = modeAwait）；
// cb 非 nil 时 Mode = modeCallback，结果就绪后会作为 kindCallback 信封投递回 pipePID 的
// mailbox，在其 goroutine 内执行 cb(ctx, value, err)——ctx 是 pipePID 对应 actor 的
// Context，因此 cb 内可以继续 ctx.Send/ctx.Request 等操作。
// cb 必须在构造时一次性给定（而不是事后通过 setter 修改），因为 Envelope
// 一旦被 PushUser 进目标 mailbox，就可能在另一个 goroutine 里被并发读取——
// 构造后即不可变才能避免数据竞争。
func NewRequest(msg interface{}, sender PID, values map[string]interface{}, cb func(Context, interface{}, error)) *Envelope {
	mode := modeAwait
	if cb != nil {
		mode = modeCallback
	}
	e := &Envelope{Msg: msg, Sender: sender, Values: values, Kind: kindRequest, Mode: mode, cb: cb}
	if mode == modeAwait {
		// 必须在构造时分配：Respond 与 Await/AwaitTimeout 分别在响应方和
		// 请求方两个 goroutine 中运行，惰性分配会让双方各建一个 channel，
		// 响应被写进无人读取的那个，请求方一直等到超时。
		e.resultCh = make(chan FutureResult, 1)
	}
	return e
}

// NewRequestAsMessage 创建一条"响应转消息"的请求信封（Mode = modeMessage）。
// 结果就绪后不经 cb、不经 resultCh，而是作为一条 kindUser 消息重新投递回
// sender 的 mailbox，在其 HandleMessage 内被当作一条新消息处理，Values 会
// 原样保留传递。适用于希望统一在 HandleMessage 里处理所有响应的场景。
//
// 若响应携带 err != nil，Respond 不会投递消息（避免把 error 硬塞进
// HandleMessage(ctx, msg interface{}) 这种没有 error 位置的签名），仅记录日志；
// 调用方若需要感知失败，应改用 NewRequest 搭配 cb 或 Await。
func NewRequestAsMessage(msg interface{}, sender PID, values map[string]interface{}) *Envelope {
	return &Envelope{Msg: msg, Sender: sender, Values: values, Kind: kindRequest, Mode: modeMessage}
}

// Respond 由响应方调用，投递结果给发起请求的一方。
// 根据构造时的 Mode 走不同的投递方式：
//   - modeCallback：结果作为 kindCallback 信封发回调用方 mailbox，在
//     调用方 actor 的 goroutine 内执行 cb(ctx, value, err)；
//   - modeMessage：结果（value）作为 kindUser 信封发回调用方 mailbox，在
//     调用方 actor 的 HandleMessage 内被当作新消息处理；err != nil 时不投递，
//     仅记录日志；
//   - modeAwait（默认）：写入 resultCh，供 Await/AwaitTimeout 读取。
//
// 只有第一次调用生效，重复调用是no-op。
//
// 若 Mode 为 modeCallback/modeMessage 但 sender 不是一个存活的 actor（例如
// Request 是从非 actor 上下文发起、没有 mailbox 可投递），结果不会被投递——
// 因为 cb 需要的 Context、或者响应消息要投递的 mailbox，都只能来自一个真实
// 存活的 actor。这种用法本身是编程错误：需要异步接收结果，就必须通过某个
// actor（如 ctx.RequestCallback / ctx.RequestAsMessage）发起请求。
func (e *Envelope) Respond(value interface{}, err error) {
	if !atomic.CompareAndSwapInt32(&e.replied, 0, 1) {
		return
	}
	switch e.Mode {
	case modeCallback:
		e.pipeSys.sendCallback(e.pipePID, e.cb, value, err)
	case modeMessage:
		e.pipeSys.sendResponseAsMessage(e.pipePID, value, err, e.Values)
	default:
		e.resultCh <- FutureResult{Value: value, Err: err}
	}
}

// Await 阻塞直到结果可用并返回。仅适用于 Mode == modeAwait 的请求
// （即构造时未传 cb 且未使用 NewRequestAsMessage）。
func (e *Envelope) Await() (interface{}, error) {
	r := <-e.resultCh
	return r.Value, r.Err
}

// AwaitTimeout 阻塞等待结果，最长等待 d。超时返回 ErrTimeout。
func (e *Envelope) AwaitTimeout(d time.Duration) (interface{}, error) {
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
