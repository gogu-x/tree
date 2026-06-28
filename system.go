package actor

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gogu-x/bigTree/timer"
)

// defaultLogger is the fallback logger when no custom logger is provided.
type stdLogger struct{}

func (l stdLogger) Error(format string, a ...interface{}) {
	log.Printf("[actor] ERROR: "+format, a...)
}

var defaultLogger Logger = stdLogger{}

// ActorSystem manages actor lifecycle and message routing.
type ActorSystem struct {
	// actors 使用 sync.Map：每个 PID 只在 Spawn 时写入一次、退出时删除一次，
	// 中间被消息投递读取多次。这种「写一次读多次」的模式下，sync.Map 的读路径
	// 基本无锁（atomic load），避免了 Spawn 写锁阻塞全局消息投递的锁护航问题。
	// 键类型为 PID，值类型为 *actorProcess。
	actors sync.Map

	// count 记录当前存活的 actor 数量，供 SpawnCount 使用，避免遍历 sync.Map。
	count atomic.Int64

	// registry 使用独立的锁，使 name 注册/查找不再与 actor 消息路由互相阻塞。
	regMu    sync.RWMutex
	registry map[string]PID // name → PID registry

	wg sync.WaitGroup

	// timeWheel 是系统级共享时间轮，所有 actor 共用，避免每个 actor 独立创建
	// 带来的 2个goroutine + 1024个list.List 的巨大开销。
	// AfterFunc 通过 channel 提交，goroutine-safe，回调仍投递到各自 actor 的 mailbox。
	timeWheel *timer.TimeWheel
}

// loadProc 从 actors 表中查找指定 PID 的进程。读路径基本无锁。
func (sys *ActorSystem) loadProc(pid PID) (*actorProcess, bool) {
	v, ok := sys.actors.Load(pid)
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

// NewActorSystem creates a new empty ActorSystem.
func NewActorSystem() *ActorSystem {
	return &ActorSystem{
		registry:  make(map[string]PID),
		timeWheel: timer.NewTimeWheel(1024),
	}
}

// Spawn registers and starts a new actor process. The actor is automatically
// registered under the given name for Lookup. The returned PID can be used
// to send messages to the actor.
func (sys *ActorSystem) Spawn(name string, actor Actor, opts ...SpawnOption) PID {
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

	sys.actors.Store(pid, proc)
	sys.count.Add(1)

	sys.regMu.Lock()
	sys.registry[name] = pid
	sys.regMu.Unlock()

	sys.wg.Add(1)
	go sys.run(proc)

	return pid
}

// Lookup returns the PID registered under the given name.
// Returns a zero PID and false if no actor is registered with that name.
func (sys *ActorSystem) Lookup(name string) (PID, bool) {
	sys.regMu.RLock()
	pid, ok := sys.registry[name]
	sys.regMu.RUnlock()
	return pid, ok
}

// MustLookup returns the PID registered under the given name.
// Panics if the name is not found.
func (sys *ActorSystem) MustLookup(name string) PID {
	pid, ok := sys.Lookup(name)
	if !ok {
		panic(fmt.Sprintf("actor: name %q not found in registry", name))
	}
	return pid
}

// run is the main loop for an actor process.
func (sys *ActorSystem) run(proc *actorProcess) {
	defer sys.wg.Done()
	defer func() {
		sys.actors.Delete(proc.pid)
		sys.count.Add(-1)
		// Clean up registry: only remove if it still points to this PID
		// (a new actor may have re-registered the same name).
		sys.regMu.Lock()
		if regPID, ok := sys.registry[proc.pid.Name]; ok && regPID == proc.pid {
			delete(sys.registry, proc.pid.Name)
		}
		sys.regMu.Unlock()
	}()

	ctx := &localContext{self: proc.pid, system: sys}

	sys.safeCall(proc, func() { proc.actor.OnInit(ctx) })

	for {
		msg := proc.mailbox.Receive()

		switch m := msg.(type) {
		case systemMessage:
			if m == systemStop {
				sys.safeCall(proc, func() { proc.actor.OnStop(ctx) })
				return
			}

		case pipeCallback:
			sys.safeCall(proc, func() { m.cb(m.value, m.err) })

		case timerCallback:
			sys.safeCall(proc, func() { m.cb(ctx) })

		case *requestEnvelope:
			reqCtx := &localContext{
				self:   proc.pid,
				system: sys,
				sender: m.sender,
				msg:    m.msg,
				future: m.future,
				values: m.values,
			}
			sys.safeCall(proc, func() { proc.actor.HandleMessage(reqCtx, m.msg) })

		case *messageEnvelope:
			msgCtx := &localContext{
				self:   proc.pid,
				system: sys,
				sender: m.sender,
				msg:    m.msg,
				values: m.values,
			}
			sys.safeCall(proc, func() { proc.actor.HandleMessage(msgCtx, m.msg) })

		default:
			msgCtx := &localContext{
				self:   proc.pid,
				system: sys,
				msg:    m,
			}
			sys.safeCall(proc, func() { proc.actor.HandleMessage(msgCtx, m) })
		}
	}
}

// safeCall executes fn with panic recovery.
func (sys *ActorSystem) safeCall(proc *actorProcess, fn func()) {
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
func (sys *ActorSystem) Send(pid PID, msg interface{}) bool {
	return sys.send(pid, msg, PID{})
}

func (sys *ActorSystem) send(pid PID, msg interface{}, sender PID) bool {
	return sys.sendWithValues(pid, msg, sender, nil)
}

func (sys *ActorSystem) sendWithValues(pid PID, msg interface{}, sender PID, values map[string]interface{}) bool {
	proc, ok := sys.loadProc(pid)
	if !ok {
		return false
	}
	proc.mailbox.PushUser(&messageEnvelope{msg: msg, sender: sender, values: values})
	return true
}

// sendRaw pushes a message directly into the actor's mailbox without
// wrapping it in a messageEnvelope. Used for internal message types
// like pipeCallback that need to be matched directly in the run loop.
func (sys *ActorSystem) sendRaw(pid PID, msg interface{}) bool {
	proc, ok := sys.loadProc(pid)
	if !ok {
		return false
	}
	proc.mailbox.PushUser(msg)
	return true
}

// TrySend delivers a message without blocking. Returns false if the target
// PID is not registered or the mailbox is full.
func (sys *ActorSystem) TrySend(pid PID, msg interface{}) bool {
	return sys.trySend(pid, msg, PID{})
}

func (sys *ActorSystem) trySend(pid PID, msg interface{}, sender PID) bool {
	return sys.trySendWithValues(pid, msg, sender, nil)
}

func (sys *ActorSystem) trySendWithValues(pid PID, msg interface{}, sender PID, values map[string]interface{}) bool {
	proc, ok := sys.loadProc(pid)
	if !ok {
		return false
	}
	return proc.mailbox.TryPushUser(&messageEnvelope{msg: msg, sender: sender, values: values})
}

// Request delivers a message to the target actor and returns a Future.
// If the target PID is not registered, the returned Future is resolved
// immediately with ErrActorNotFound.
func (sys *ActorSystem) Request(pid PID, msg interface{}) *Future {
	return sys.request(pid, msg, PID{})
}

func (sys *ActorSystem) request(pid PID, msg interface{}, sender PID) *Future {
	return sys.requestWithValues(pid, msg, sender, nil)
}

func (sys *ActorSystem) requestWithValues(pid PID, msg interface{}, sender PID, values map[string]interface{}) *Future {
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
func (sys *ActorSystem) stop(pid PID) {
	proc, ok := sys.loadProc(pid)
	if ok {
		proc.mailbox.PushSystem(systemStop)
	}
}

// afterFunc schedules cb to be delivered to pid's mailbox after duration d.
func (sys *ActorSystem) afterFunc(pid PID, d time.Duration, cb func(ActorContext)) *timer.WheelTimer {
	_, ok := sys.loadProc(pid)
	if !ok {
		return nil
	}
	return sys.timeWheel.AfterFunc(d, func() {
		sys.sendRaw(pid, timerCallback{cb: cb})
	})
}

// cronFunc schedules cb to be delivered to pid's mailbox on the cron schedule.
func (sys *ActorSystem) cronFunc(pid PID, cronExpr *timer.CronExpr, cb func(ActorContext)) *timer.WheelCron {
	_, ok := sys.loadProc(pid)
	if !ok {
		return nil
	}
	return sys.timeWheel.CronFunc(cronExpr, func() {
		sys.sendRaw(pid, timerCallback{cb: cb})
	})
}

// Shutdown sends a stop signal to all registered actors and waits for them
// to finish processing.
func (sys *ActorSystem) Shutdown() {
	sys.actors.Range(func(_, v interface{}) bool {
		v.(*actorProcess).mailbox.PushSystem(systemStop)
		return true
	})
	sys.wg.Wait()
	sys.timeWheel.Stop()
}

// Register 将 pid 注册到指定 name，用于 Actor 运行时更新自己的可寻址名称。
// 如果 name 已被其他 PID 占用，会覆盖。
func (sys *ActorSystem) Register(name string, pid PID) {
	sys.regMu.Lock()
	sys.registry[name] = pid
	sys.regMu.Unlock()
}

// SpawnCount returns the number of currently running actors.
func (sys *ActorSystem) SpawnCount() int {
	return int(sys.count.Load())
}

// Start 阻塞运行，监听 OS 关闭信号（Ctrl+C / kill）。
// 收到信号后优雅关闭所有 Actor，然后返回。
func (sys *ActorSystem) Start() {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	sys.Shutdown()
}
