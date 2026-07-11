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

// Spawn registers and starts a new actor process. The actor is automatically
// registered under the given name for Lookup. The returned PID can be used
// to send messages to the actor.
func (t *Tree) Spawn(name string, actor Actor, opts ...SpawnOption) PID {
	cfg := defaultSpawnConfig()
	for _, o := range opts {
		o(&cfg)
	}

	pid := allocatePID(name)
	mb := NewMailbox(cfg.mailboxSize)

	proc := &actorProcess{
		pid:     pid,
		actor:   actor,
		mailbox: mb,
		logger:  cfg.logger,
	}

	t.actors.Store(pid, proc)
	t.count.Add(1)

	t.regMu.Lock()
	t.registry[name] = pid
	t.regMu.Unlock()

	t.wg.Add(1)
	go t.run(proc)

	return pid
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
func (t *Tree) run(proc *actorProcess) {
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

	for {
		msg := proc.mailbox.Receive()

		switch m := msg.(type) {
		case systemMessage:
			if m == systemStop {
				t.safeCall(proc, func() { proc.actor.OnStop(ctx) })
				return
			}

		case pipeCallback:
			t.safeCall(proc, func() { m.cb(m.value, m.err) })

		case timerCallback:
			t.safeCall(proc, func() { m.cb(ctx) })

		case *requestEnvelope:
			reqCtx := &localContext{
				self:   proc.pid,
				system: t,
				sender: m.sender,
				msg:    m.msg,
				future: m.future,
				values: m.values,
			}
			t.safeCall(proc, func() { proc.actor.HandleMessage(reqCtx, m.msg) })

		case *messageEnvelope:
			msgCtx := &localContext{
				self:   proc.pid,
				system: t,
				sender: m.sender,
				msg:    m.msg,
				values: m.values,
			}
			t.safeCall(proc, func() { proc.actor.HandleMessage(msgCtx, m.msg) })

		default:
			msgCtx := &localContext{
				self:   proc.pid,
				system: t,
				msg:    m,
			}
			t.safeCall(proc, func() { proc.actor.HandleMessage(msgCtx, m) })
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
	proc.mailbox.PushUser(&messageEnvelope{msg: msg, sender: sender, values: values})
	return true
}

// sendRaw pushes a message directly into the actor's mailbox without
// wrapping it in a messageEnvelope. Used for internal message types
// like pipeCallback that need to be matched directly in the run loop.
func (t *Tree) sendRaw(pid PID, msg interface{}) bool {
	proc, ok := t.loadProc(pid)
	if !ok {
		return false
	}
	proc.mailbox.PushUser(msg)
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
	return proc.mailbox.TryPushUser(&messageEnvelope{msg: msg, sender: sender, values: values})
}

// Request delivers a message to the target actor and returns a Future.
// If the target PID is not registered, the returned Future is resolved
// immediately with ErrActorNotFound.
func (t *Tree) Request(pid PID, msg interface{}) *Future {
	return t.request(pid, msg, PID{})
}

func (t *Tree) request(pid PID, msg interface{}, sender PID) *Future {
	return t.requestWithValues(pid, msg, sender, nil)
}

func (sys *Tree) requestWithValues(pid PID, msg interface{}, sender PID, values map[string]interface{}) *Future {
	f := NewFuture()
	proc, ok := sys.loadProc(pid)
	if !ok {
		f.Respond(nil, ErrActorNotFound)
		return f
	}
	proc.mailbox.PushUser(&requestEnvelope{msg: msg, sender: sender, future: f, values: values})
	return f
}

// stop signals the actor identified by pid to shut down.
func (t *Tree) stop(pid PID) {
	proc, ok := t.loadProc(pid)
	if ok {
		proc.mailbox.PushSystem(systemStop)
	}
}

// afterFunc schedules cb to be delivered to pid's mailbox after duration d.
func (t *Tree) afterFunc(pid PID, d time.Duration, cb func(Context)) *timer.WheelTimer {
	_, ok := t.loadProc(pid)
	if !ok {
		return nil
	}
	return t.timeWheel.AfterFunc(d, func() {
		t.sendRaw(pid, timerCallback{cb: cb})
	})
}

// cronFunc schedules cb to be delivered to pid's mailbox on the cron schedule.
func (t *Tree) cronFunc(pid PID, cronExpr *timer.CronExpr, cb func(Context)) *timer.WheelCron {
	_, ok := t.loadProc(pid)
	if !ok {
		return nil
	}
	return t.timeWheel.CronFunc(cronExpr, func() {
		t.sendRaw(pid, timerCallback{cb: cb})
	})
}

// Shutdown sends a stop signal to all registered actors and waits for them
// to finish processing.
func (t *Tree) Shutdown() {
	t.actors.Range(func(_, v interface{}) bool {
		v.(*actorProcess).mailbox.PushSystem(systemStop)
		return true
	})
	t.wg.Wait()
	t.timeWheel.Stop()
}

// SendCallback 向目标 Actor 投递一个回调，回调在目标 Actor 的 goroutine 内串行执行。
func (t *Tree) SendCallback(pid PID, cb func(interface{}, error), value interface{}, err error) bool {
	return t.sendRaw(pid, pipeCallback{cb: cb, value: value, err: err})
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
