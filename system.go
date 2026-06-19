package actor

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"sync"
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
	mu       sync.RWMutex
	actors   map[PID]*actorProcess
	registry map[string]PID // name → PID registry
	wg       sync.WaitGroup
}

// actorProcess holds the runtime state for a single actor.
type actorProcess struct {
	pid       PID
	actor     Actor
	mailbox   *Mailbox
	logger    Logger
	timeWheel *timer.TimeWheel
}

// NewActorSystem creates a new empty ActorSystem.
func NewActorSystem() *ActorSystem {
	return &ActorSystem{
		actors:   make(map[PID]*actorProcess),
		registry: make(map[string]PID),
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
		pid:       pid,
		actor:     actor,
		mailbox:   mb,
		logger:    cfg.logger,
		timeWheel: timer.NewTimeWheel(64),
	}

	sys.mu.Lock()
	sys.actors[pid] = proc
	sys.registry[name] = pid
	sys.mu.Unlock()

	sys.wg.Add(1)
	go sys.run(proc)

	return pid
}

// Lookup returns the PID registered under the given name.
// Returns a zero PID and false if no actor is registered with that name.
func (sys *ActorSystem) Lookup(name string) (PID, bool) {
	sys.mu.RLock()
	pid, ok := sys.registry[name]
	sys.mu.RUnlock()
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
	defer proc.timeWheel.Stop()
	defer func() {
		sys.mu.Lock()
		delete(sys.actors, proc.pid)
		// Clean up registry: only remove if it still points to this PID
		// (a new actor may have re-registered the same name).
		if regPID, ok := sys.registry[proc.pid.Name]; ok && regPID == proc.pid {
			delete(sys.registry, proc.pid.Name)
		}
		sys.mu.Unlock()
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
	sys.mu.RLock()
	proc, ok := sys.actors[pid]
	sys.mu.RUnlock()
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
	sys.mu.RLock()
	proc, ok := sys.actors[pid]
	sys.mu.RUnlock()
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
	sys.mu.RLock()
	proc, ok := sys.actors[pid]
	sys.mu.RUnlock()
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
	sys.mu.RLock()
	proc, ok := sys.actors[pid]
	sys.mu.RUnlock()
	if !ok {
		f.Respond(nil, ErrActorNotFound)
		return f
	}
	proc.mailbox.PushUser(&requestEnvelope{msg: msg, sender: sender, future: f, values: values})
	return f
}

// stop signals the actor identified by pid to shut down.
func (sys *ActorSystem) stop(pid PID) {
	sys.mu.RLock()
	proc, ok := sys.actors[pid]
	sys.mu.RUnlock()
	if ok {
		proc.mailbox.PushSystem(systemStop)
	}
}

// afterFunc schedules cb to be delivered to pid's mailbox after duration d.
func (sys *ActorSystem) afterFunc(pid PID, d time.Duration, cb func(ActorContext)) *timer.WheelTimer {
	sys.mu.RLock()
	proc, ok := sys.actors[pid]
	sys.mu.RUnlock()
	if !ok {
		return nil
	}
	return proc.timeWheel.AfterFunc(d, func() {
		sys.sendRaw(pid, timerCallback{cb: cb})
	})
}

// cronFunc schedules cb to be delivered to pid's mailbox on the cron schedule.
func (sys *ActorSystem) cronFunc(pid PID, cronExpr *timer.CronExpr, cb func(ActorContext)) *timer.WheelCron {
	sys.mu.RLock()
	proc, ok := sys.actors[pid]
	sys.mu.RUnlock()
	if !ok {
		return nil
	}
	return proc.timeWheel.CronFunc(cronExpr, func() {
		sys.sendRaw(pid, timerCallback{cb: cb})
	})
}

// Shutdown sends a stop signal to all registered actors and waits for them
// to finish processing.
func (sys *ActorSystem) Shutdown() {
	sys.mu.RLock()
	for _, proc := range sys.actors {
		proc.mailbox.PushSystem(systemStop)
	}
	sys.mu.RUnlock()
	os.Exit(1)
	sys.wg.Wait()

}

// Register 将 pid 注册到指定 name，用于 Actor 运行时更新自己的可寻址名称。
// 如果 name 已被其他 PID 占用，会覆盖。
func (sys *ActorSystem) Register(name string, pid PID) {
	sys.mu.Lock()
	sys.registry[name] = pid
	sys.mu.Unlock()
}

// SpawnCount returns the number of currently running actors.
func (sys *ActorSystem) SpawnCount() int {
	sys.mu.RLock()
	defer sys.mu.RUnlock()
	return len(sys.actors)
}

// Start 阻塞运行，监听 OS 关闭信号（Ctrl+C / kill）。
// 收到信号后优雅关闭所有 Actor，然后返回。
func (sys *ActorSystem) Start() {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	sys.Shutdown()
}
