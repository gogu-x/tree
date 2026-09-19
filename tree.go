package tree

import (
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
)

// Tree manages actor lifecycle and message routing.
type Tree struct {
	// spawnMu serializes startup batches so their OnInit sequences cannot
	// interleave. Within one batch actors initialize in argument order.
	spawnMu sync.Mutex

	// actors 使用 sync.Map：每个 PID 只在 Spawn 时写入一次、退出时删除一次，
	// 中间被消息投递读取多次。这种「写一次读多次」的模式下，sync.Map 的读路径
	// 基本无锁（atomic load），避免了 Spawn 写锁阻塞全局消息投递的锁护航问题。
	// 键类型为 PID，值类型为 *actorProcess。
	actors sync.Map

	// count 记录当前存活的 actor 数量，供 SpawnCount 使用，避免遍历 sync.Map。
	count atomic.Int64

	// registry 使用独立的锁，使 name 注册/查找不再与 actor 消息路由互相阻塞。
	regMu sync.RWMutex

	registry map[string]PID // name → PID registry

	wg sync.WaitGroup
}

// loadProc 从 actors 表中查找指定 PID 的进程。读路径基本无锁。
func (t *Tree) loadProc(pid PID) (*actorProcess, bool) {
	v, ok := t.actors.Load(pid)
	if !ok {
		return nil, false
	}
	return v.(*actorProcess), true
}

// actorProcess holds the runtime state for a single actor.
type actorProcess struct {
	pid     PID
	actor   Actor
	mailbox *Mailbox
	logger  Logger
}

// NewTree creates a new empty Tree.
func NewTree() *Tree {
	return &Tree{
		registry: make(map[string]PID),
	}
}

// Spawn 注册并启动一批 Actor，返回与入参顺序一致的 PID 列表。
//
// 名字取自 Actor.Name()，分两个阶段启动：
//
//  1. 为全部 Actor 分配 PID、创建 mailbox 并写入 registry；
//  2. 按参数顺序启动 goroutine；前一个 Actor 完成 OnInit 并进入消息循环后，
//     才启动下一个 Actor。
//
// 由此得到两个保证：
//   - 同一批 Actor 在 OnInit 中都可以 Lookup 到其他 Actor；
//   - 后启动 Actor 的 OnInit 可以同步请求前面已经初始化完成的 Actor；
//   - 多个并发 Spawn 调用的初始化过程不会交错。
//
// 注意：OnInit 只能同步等待参数顺序中位于自己之前的 Actor；后面的 Actor
// 尚未初始化，也没有开始消费 mailbox。
func (t *Tree) Spawn(actors ...Actor) []PID {
	if len(actors) == 0 {
		return nil
	}
	t.spawnMu.Lock()
	defer t.spawnMu.Unlock()

	pids := make([]PID, len(actors))
	procs := make([]*actorProcess, len(actors))

	// 阶段一：分配并注册。全部注册完成后才启动 goroutine。
	for i, a := range actors {
		if a == nil {
			panic("tree: Spawn called with nil actor")
		}
		name := a.Name()
		if name == "" {
			panic(fmt.Sprintf("tree: actor %T returned an empty Name()", a))
		}

		pid := allocatePID(name)
		proc := &actorProcess{
			pid:     pid,
			actor:   a,
			mailbox: NewMailbox(mailboxSizeOf(a)),
			logger:  loggerOf(a),
		}

		t.actors.Store(pid, proc)
		t.count.Add(1)

		t.regMu.Lock()
		t.registry[name] = pid
		t.regMu.Unlock()

		pids[i] = pid
		procs[i] = proc
	}

	// 阶段二：严格按参数顺序启动。每个 actor 完成 OnInit 并进入消息
	// 循环后，才初始化下一个 actor。这样后启动 actor 的 OnInit 可以
	// 同步请求已经启动完成的底层 actor。
	for _, proc := range procs {
		initialized := make(chan struct{})
		t.wg.Add(1)
		go t.run(proc, initialized)
		<-initialized
	}

	return pids
}

// SpawnOne 启动单个 Actor 并返回其 PID，等价于 Spawn(a)[0]。
// 用于运行时按需创建的 Actor（如每个连接、每个玩家一个）。
func (t *Tree) SpawnOne(a Actor) PID {
	return t.Spawn(a)[0]
}

// Lookup returns the PID registered under the given name.
// Returns a zero PID and false if no actor is registered with that name.
func (t *Tree) Lookup(name string) (PID, bool) {
	t.regMu.RLock()
	pid, ok := t.registry[name]
	t.regMu.RUnlock()
	return pid, ok
}

// MustLookup returns the PID registered under the given name.
// Panics if the name is not found.
func (t *Tree) MustLookup(name string) PID {
	pid, ok := t.Lookup(name)
	if !ok {
		panic(fmt.Sprintf("actor: name %q not found in registry", name))
	}
	return pid
}

// run is the main loop for an actor process.
// initialized 在 OnInit 返回后关闭，使 Spawn 可以继续初始化下一个 Actor。
func (t *Tree) run(proc *actorProcess, initialized chan<- struct{}) {
	defer t.wg.Done()
	defer func() {
		t.actors.Delete(proc.pid)
		t.count.Add(-1)
		// Clean up registry: only remove if it still points to this PID
		// (a new actor may have re-registered the same name).
		t.regMu.Lock()
		if regPID, ok := t.registry[proc.pid.Name]; ok && regPID == proc.pid {
			delete(t.registry, proc.pid.Name)
		}
		t.regMu.Unlock()
	}()

	ctx := &localContext{self: proc.pid, system: t}

	t.safeCall(proc, func() { proc.actor.OnInit(ctx) })
	if initialized != nil {
		close(initialized)
	}

	for {
		env := proc.mailbox.Receive()

		switch env.Kind {
		case kindSystem:
			if env.sys == systemStop {
				t.safeCall(proc, func() { proc.actor.OnStop(ctx) })
				return
			}

		case kindCallback:
			t.safeCall(proc, func() { env.cb(ctx, env.value, env.err) })

		case kindRequest:
			reqCtx := &localContext{
				self:    proc.pid,
				system:  t,
				sender:  env.Sender,
				msg:     env.Msg,
				request: env,
				values:  env.Values,
			}
			t.safeCall(proc, func() { proc.actor.HandleMessage(reqCtx, env.Msg) })

		default: // kindUser
			msgCtx := &localContext{
				self:   proc.pid,
				system: t,
				sender: env.Sender,
				msg:    env.Msg,
				values: env.Values,
			}
			t.safeCall(proc, func() { proc.actor.HandleMessage(msgCtx, env.Msg) })
		}
	}
}

// safeCall executes fn with panic recovery.
func (t *Tree) safeCall(proc *actorProcess, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			proc.logger.Error("actor %v panic: %v\n%s", proc.pid, r, buf[:n])
		}
	}()
	fn()
}

// Send delivers a message to the target actor asynchronously.
// Returns false if the target PID is not registered.
func (t *Tree) Send(pid PID, msg interface{}) bool {
	return t.send(pid, msg, PID{})
}

func (t *Tree) send(pid PID, msg interface{}, sender PID) bool {
	return t.sendWithValues(pid, msg, sender, nil)
}

func (t *Tree) sendWithValues(pid PID, msg interface{}, sender PID, values map[string]interface{}) bool {
	proc, ok := t.loadProc(pid)
	if !ok {
		return false
	}
	proc.mailbox.PushUser(NewEnvelope(msg, sender, values))
	return true
}

// sendCallback pushes a kindCallback envelope directly into the actor's
// mailbox, to be executed in its goroutine with that actor's Context.
// Used internally to deliver request results back to the requester when a
// callback was given to NewRequest/RequestCallback. Returns false (and does
// not execute cb) if pid is not a live actor, since cb requires a Context.
func (t *Tree) sendCallback(pid PID, cb func(Context, interface{}, error), value interface{}, err error) bool {
	proc, ok := t.loadProc(pid)
	if !ok {
		return false
	}
	proc.mailbox.PushUser(&Envelope{Kind: kindCallback, cb: cb, value: value, err: err})
	return true
}

// sendResponseAsMessage 将请求的响应结果作为一条普通 kindUser 消息投递回
// pid 的 mailbox，在其 HandleMessage 内被当作新消息处理。values 原样保留
// 传递（例如 natsrpc 转发场景中携带的 sessionID/taggerName 等路由信息）。
// 用于 NewRequestAsMessage 构造的信封被 Respond 时。
//
// err != nil 时不投递（HandleMessage(ctx, msg interface{}) 签名没有 error
// 位置，无法无损传递错误），仅记录日志；返回 false。
// pid 不是存活 actor 时同样不投递，返回 false。
func (t *Tree) sendResponseAsMessage(pid PID, value interface{}, err error, values map[string]interface{}) bool {
	if err != nil {
		defaultLogger.Error("tree: RequestAsMessage to %v got error, dropping response: %v", pid, err)
		return false
	}
	proc, ok := t.loadProc(pid)
	if !ok {
		return false
	}
	proc.mailbox.PushUser(&Envelope{Kind: kindUser, Msg: value, Values: values})
	return true
}

// TrySend delivers a message without blocking. Returns false if the target
// PID is not registered or the mailbox is full.
func (t *Tree) TrySend(pid PID, msg interface{}) bool {
	return t.trySend(pid, msg, PID{})
}

func (t *Tree) trySend(pid PID, msg interface{}, sender PID) bool {
	return t.trySendWithValues(pid, msg, sender, nil)
}

func (sys *Tree) trySendWithValues(pid PID, msg interface{}, sender PID, values map[string]interface{}) bool {
	proc, ok := sys.loadProc(pid)
	if !ok {
		return false
	}
	return proc.mailbox.TryPushUser(NewEnvelope(msg, sender, values))
}

// Request delivers a message to the target actor and returns an Envelope
// for synchronous Await/AwaitTimeout. If the target PID is not registered,
// the returned Envelope is resolved immediately with ErrActorNotFound.
func (t *Tree) Request(pid PID, msg interface{}) *Envelope {
	return t.request(pid, msg, PID{}, nil)
}

// RequestCallback delivers a message to the target actor; when the target
// responds, cb is invoked in sender's goroutine with sender's Context
// (sender must be a live actor's PID — otherwise cb is silently dropped,
// see sendCallback).
func (t *Tree) RequestCallback(pid PID, msg interface{}, sender PID, cb func(Context, interface{}, error)) *Envelope {
	return t.request(pid, msg, sender, cb)
}

// RequestAsMessage delivers a message to the target actor; when the target
// responds (via ctx.Response), the result value is redelivered to sender's
// mailbox as an ordinary kindUser message and processed by sender's
// HandleMessage, instead of via a dedicated callback or Await. Useful when
// all responses should funnel through a single HandleMessage dispatch (e.g.
// natsrpc forwarding). sender must be a live actor's PID, otherwise the
// response is silently dropped when it arrives (see sendResponseAsMessage).
// If the response carries a non-nil error, it is logged and not delivered,
// since HandleMessage(ctx, msg interface{}) has no error slot.
func (t *Tree) RequestAsMessage(pid PID, msg interface{}, sender PID) *Envelope {
	return t.requestAsMessageWithValues(pid, msg, sender, nil)
}

func (t *Tree) requestAsMessageWithValues(pid PID, msg interface{}, sender PID, values map[string]interface{}) *Envelope {
	env := NewRequestAsMessage(msg, sender, values)
	env.pipeSys = t
	env.pipePID = sender
	proc, ok := t.loadProc(pid)
	if !ok {
		env.Respond(nil, ErrActorNotFound)
		return env
	}
	proc.mailbox.PushUser(env)
	return env
}

func (t *Tree) request(pid PID, msg interface{}, sender PID, cb func(Context, interface{}, error)) *Envelope {
	return t.requestWithValues(pid, msg, sender, nil, cb)
}

func (sys *Tree) requestWithValues(pid PID, msg interface{}, sender PID, values map[string]interface{}, cb func(Context, interface{}, error)) *Envelope {
	env := NewRequest(msg, sender, values, cb)
	env.pipeSys = sys
	env.pipePID = sender
	proc, ok := sys.loadProc(pid)
	if !ok {
		env.Respond(nil, ErrActorNotFound)
		return env
	}
	proc.mailbox.PushUser(env)
	return env
}

// stop signals the actor identified by pid to shut down.
func (t *Tree) stop(pid PID) {
	proc, ok := t.loadProc(pid)
	if ok {
		proc.mailbox.PushSystem(&Envelope{Kind: kindSystem, sys: systemStop})
	}
}

// Shutdown sends a stop signal to all registered actors and waits for them
// to finish processing.
func (t *Tree) Shutdown() {
	t.actors.Range(func(_, v interface{}) bool {
		v.(*actorProcess).mailbox.PushSystem(&Envelope{Kind: kindSystem, sys: systemStop})
		return true
	})
	t.wg.Wait()
}

// SendCallback 向目标 Actor 投递一个回调，回调在目标 Actor 的 goroutine 内
// 串行执行，并携带该 Actor 的 Context。
func (t *Tree) SendCallback(pid PID, cb func(Context, interface{}, error), value interface{}, err error) bool {
	return t.sendCallback(pid, cb, value, err)
}

// Register 将 pid 注册到指定 name，用于 Actor 运行时更新自己的可寻址名称。
// 如果 name 已被其他 PID 占用，会覆盖。
func (t *Tree) Register(name string, pid PID) {
	t.regMu.Lock()
	t.registry[name] = pid
	t.regMu.Unlock()
}

// SpawnCount returns the number of currently running actors.
func (t *Tree) SpawnCount() int {
	return int(t.count.Load())
}

// Start 阻塞运行，监听 OS 关闭信号（Ctrl+C / kill）。
// 收到信号后优雅关闭所有 Actor，然后返回。
func (t *Tree) Start() {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	t.Shutdown()
}
