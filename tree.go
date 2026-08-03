package tree

import (
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gogu-x/tree/timer"
)

// Tree manages actor lifecycle and message routing.
type Tree struct {
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

	// timeWheel 是系统级共享时间轮，所有 actor 共用，避免每个 actor 独立创建
	// 带来的 2个goroutine + 1024个list.List 的巨大开销。
	// AfterFunc 通过 channel 提交，goroutine-safe，回调仍投递到各自 actor 的 mailbox。
	timeWheel *timer.TimeWheel
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
		registry:  make(map[string]PID),
		timeWheel: timer.NewTimeWheel(1024),
	}
}

// Spawn 注册并启动一批 Actor，返回与入参顺序一致的 PID 列表。
//
// 名字取自 Actor.Name()，分两个阶段启动：
//
//  1. 为全部 Actor 分配 PID、创建 mailbox 并写入 registry；
//  2. 启动各自的 goroutine，先执行 OnInit，等本批次所有 OnInit
//     都返回后，才进入消息循环消费 mailbox。
//
// 由此得到两个保证：
//   - 同一批 Actor 的 OnInit 里可以互相 Lookup / Send，与传入顺序无关；
//   - 任何 Actor 处理第一条消息时，本批次所有 Actor 都已初始化完毕。
//
// 注意：OnInit 内不要同步等待（Envelope.Await）同批次其他 Actor 的回复，
// 对方此时尚未开始消费 mailbox，会一直等到超时。跨 Actor 的初始化交互
// 请用异步 Send，或延后到第一条消息再做。
func (t *Tree) Spawn(actors ...Actor) []PID {
	if len(actors) == 0 {
		return nil
	}

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

	// 阶段二：启动 goroutine，用 WaitGroup 做一次性屏障。
	var initBarrier sync.WaitGroup
	initBarrier.Add(len(procs))
	for _, proc := range procs {
		t.wg.Add(1)
		go t.run(proc, &initBarrier)
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
// initBarrier 用于两阶段启动：OnInit 执行完先 Done，再 Wait 等齐同批次
// 其他 Actor，之后才开始消费 mailbox。
func (t *Tree) run(proc *actorProcess, initBarrier *sync.WaitGroup) {
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

	if initBarrier != nil {
		initBarrier.Done()
		initBarrier.Wait()
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

		case kindTimer:
			t.safeCall(proc, func() { env.value.(func(Context))(ctx) })

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

// afterFunc schedules cb to be delivered to pid's mailbox after duration d.
func (t *Tree) afterFunc(pid PID, d time.Duration, cb func(Context)) *timer.WheelTimer {
	_, ok := t.loadProc(pid)
	if !ok {
		return nil
	}
	return t.timeWheel.AfterFunc(d, func() {
		t.sendTimer(pid, cb)
	})
}

// cronFunc schedules cb to be delivered to pid's mailbox on the cron schedule.
func (t *Tree) cronFunc(pid PID, cronExpr *timer.CronExpr, cb func(Context)) *timer.WheelCron {
	_, ok := t.loadProc(pid)
	if !ok {
		return nil
	}
	return t.timeWheel.CronFunc(cronExpr, func() {
		t.sendTimer(pid, cb)
	})
}

// sendTimer pushes a kindTimer envelope carrying cb into pid's mailbox.
func (t *Tree) sendTimer(pid PID, cb func(Context)) bool {
	proc, ok := t.loadProc(pid)
	if !ok {
		return false
	}
	proc.mailbox.PushUser(&Envelope{Kind: kindTimer, value: cb})
	return true
}

// Shutdown sends a stop signal to all registered actors and waits for them
// to finish processing.
func (t *Tree) Shutdown() {
	t.actors.Range(func(_, v interface{}) bool {
		v.(*actorProcess).mailbox.PushSystem(&Envelope{Kind: kindSystem, sys: systemStop})
		return true
	})
	t.wg.Wait()
	t.timeWheel.Stop()
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
