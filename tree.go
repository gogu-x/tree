package tree

import (
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
)

// treeState 保存进程内唯一的 Actor 运行状态，不向外暴露实例。
type treeState struct {
	mods        []*actorProcess // 一次性发布的固定槽位，运行期间不再修改。
	multiMods   sync.Map        // PID → 动态进程。
	names       sync.Map        // 动态 Actor 名称 → PID。
	count       atomic.Int64    // 当前存活 Actor 数量。
	lifecycleMu sync.RWMutex    // 串行化注册、退出清理和关闭快照。
	closing     bool            // lifecycleMu 保护；一旦开始关服便不再接受注册。
	stopped     chan struct{}   // 全部退出后关闭，供并发 Shutdown 等待。
}

// fixedProcesses 获取固定数组的只读视图；发布后不再修改切片和槽位。
// 读锁只保护切片发布，不覆盖遍历、消息投递或 Actor 回调。
func fixedProcesses() []*actorProcess {
	globalTree.lifecycleMu.RLock()
	procs := globalTree.mods
	globalTree.lifecycleMu.RUnlock()
	return procs
}

// processAlive 判断进程是否已经完成退出清理。
func processAlive(proc *actorProcess) bool {
	select {
	case <-proc.done:
		return false
	default:
		return true
	}
}

// loadProc 先按完整 PID 匹配固定 Actor，未找到存活进程时查询动态 map。
func loadProc(pid PID) (*actorProcess, bool) {
	for _, proc := range fixedProcesses() {
		if proc.pid == pid && processAlive(proc) {
			return proc, true
		}
	}
	v, ok := globalTree.multiMods.Load(pid)
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
	done    chan interface{}
}

// Spawn 在服务器启动阶段一次性注册全部固定 Actor，返回顺序与参数一致。
// 数组在第一个 OnInit 前发布，之后禁止再次注册固定 Actor。
// OnInit 只能同步请求参数顺序中位于自己之前的 Actor。
func Spawn(actors ...Actor) []PID {
	return spawn(true, actors...)
}

func spawn(fixed bool, actors ...Actor) []PID {
	if len(actors) == 0 {
		return nil
	}

	pids := make([]PID, len(actors))
	procs := make([]*actorProcess, len(actors))

	// 阶段一：分配。全部参数验证完成后才注册，避免留下未启动的进程。
	for i, a := range actors {
		if a == nil {
			panic("tree: Spawn called with nil actor")
		}
		name := a.Name()
		if name == "" {
			panic(fmt.Sprintf("tree: actor %T returned an empty Name()", a))
		}

		pid := PID{Name: name}
		proc := &actorProcess{
			pid:     pid,
			actor:   a,
			mailbox: NewMailbox(mailboxSizeOf(a)),
			logger:  loggerOf(a),
			done:    make(chan interface{}, 1),
		}

		pids[i] = pid
		procs[i] = proc
	}

	globalTree.lifecycleMu.Lock()
	if globalTree.closing {
		globalTree.lifecycleMu.Unlock()
		panic("tree: Spawn called after shutdown started")
	}
	if fixed && globalTree.mods != nil {
		globalTree.lifecycleMu.Unlock()
		panic("tree: Spawn may only be called once during startup")
	}
	for i, proc := range procs {
		// 在注册锁内分配编号，使编号顺序与实际注册顺序一致。
		proc.pid = allocatePID(proc.pid.Name)
		if !fixed {
			globalTree.multiMods.Store(proc.pid, proc)
		}
		pids[i] = proc.pid
		globalTree.count.Add(1)
	}
	if fixed {
		// 必须先完成数组，再发布任何固定 PID；发布后不再修改切片和槽位。
		globalTree.mods = procs
	}
	if !fixed {
		for _, proc := range procs {
			globalTree.names.Store(proc.pid.Name, proc.pid)
		}
	}
	globalTree.lifecycleMu.Unlock()

	// 阶段二：严格按参数顺序启动。每个 actor 完成 OnInit 并进入消息
	// 循环后，才初始化下一个 actor。这样后启动 actor 的 OnInit 可以
	// 同步请求已经启动完成的底层 actor。
	for _, proc := range procs {
		initialized := make(chan struct{})
		go run(proc, initialized)
		<-initialized
	}

	return pids
}

// SpawnOne 创建单个动态 Actor 并返回其 PID。
// 用于运行时按需创建的 Actor（如每个连接、每个玩家一个）。
func SpawnOne(a Actor) PID {
	return spawn(false, a)[0]
}

// Lookup returns the PID registered under the given name.
// Returns a zero PID and false if no actor is registered with that name.
func Lookup(name string) (PID, bool) {
	for _, proc := range fixedProcesses() {
		if proc.pid.Name == name && processAlive(proc) {
			return proc.pid, true
		}
	}
	value, ok := globalTree.names.Load(name)
	if !ok {
		return PID{}, false
	}
	return value.(PID), true
}

// MustLookup returns the PID registered under the given name.
// Panics if the name is not found.
func MustLookup(name string) PID {
	pid, ok := Lookup(name)
	if !ok {
		panic(fmt.Sprintf("actor: name %q not found in registry", name))
	}
	return pid
}

// run is the main loop for an actor process.
// initialized 在 OnInit 返回后关闭，使 Spawn 可以继续初始化下一个 Actor。
func run(proc *actorProcess, initialized chan<- struct{}) {
	defer func() {
		globalTree.lifecycleMu.Lock()
		// 退出清理与快照互斥，快照中缺失的动态进程必须已经完成清理。
		globalTree.names.CompareAndDelete(proc.pid.Name, proc.pid)
		proc.actor = nil
		globalTree.count.Add(-1)
		globalTree.multiMods.Delete(proc.pid)
		close(proc.done)
		globalTree.lifecycleMu.Unlock()
	}()

	ctx := &localContext{self: proc.pid}

	safeCall(proc, func() { proc.actor.OnInit(ctx) })
	if initialized != nil {
		close(initialized)
	}

	for {
		env := proc.mailbox.Receive()

		switch env.Kind {
		case kindSystem:
			if env.sys == systemStop {
				safeCall(proc, func() { proc.actor.OnStop(ctx) })
				return
			}

		case kindCallback:
			safeCall(proc, func() { env.cb(ctx, env.value, env.err) })

		case kindRequest:
			reqCtx := &localContext{
				self:    proc.pid,
				sender:  env.Sender,
				msg:     env.Msg,
				request: env,
				values:  env.Values,
			}
			safeCall(proc, func() { proc.actor.HandleMessage(reqCtx, env.Msg) })

		default: // kindUser
			msgCtx := &localContext{
				self:   proc.pid,
				sender: env.Sender,
				msg:    env.Msg,
				values: env.Values,
			}
			safeCall(proc, func() { proc.actor.HandleMessage(msgCtx, env.Msg) })
		}
	}
}

// safeCall executes fn with panic recovery.
func safeCall(proc *actorProcess, fn func()) {
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
func Send(pid PID, msg interface{}) bool {
	return send(pid, msg, PID{})
}

func send(pid PID, msg interface{}, sender PID) bool {
	return sendWithValues(pid, msg, sender, nil)
}

func sendWithValues(pid PID, msg interface{}, sender PID, values map[string]interface{}) bool {
	proc, ok := loadProc(pid)
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
func sendCallback(pid PID, cb func(Context, interface{}, error), value interface{}, err error) bool {
	proc, ok := loadProc(pid)
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
func sendResponseAsMessage(pid PID, value interface{}, err error, values map[string]interface{}) bool {
	if err != nil {
		defaultLogger.Error("tree: RequestAsMessage to %v got error, dropping response: %v", pid, err)
		return false
	}
	proc, ok := loadProc(pid)
	if !ok {
		return false
	}
	proc.mailbox.PushUser(&Envelope{Kind: kindUser, Msg: value, Values: values})
	return true
}

// TrySend delivers a message without blocking. Returns false if the target
// PID is not registered or the mailbox is full.
func TrySend(pid PID, msg interface{}) bool {
	return trySend(pid, msg, PID{})
}

func trySend(pid PID, msg interface{}, sender PID) bool {
	return trySendWithValues(pid, msg, sender, nil)
}

func trySendWithValues(pid PID, msg interface{}, sender PID, values map[string]interface{}) bool {
	proc, ok := loadProc(pid)
	if !ok {
		return false
	}
	return proc.mailbox.TryPushUser(NewEnvelope(msg, sender, values))
}

// Request delivers a message to the target actor and returns an Envelope
// for synchronous Await/AwaitTimeout. If the target PID is not registered,
// the returned Envelope is resolved immediately with ErrActorNotFound.
func Request(pid PID, msg interface{}) *Envelope {
	return request(pid, msg, PID{}, nil)
}

// RequestCallback delivers a message to the target actor; when the target
// responds, cb is invoked in sender's goroutine with sender's Context
// (sender must be a live actor's PID — otherwise cb is silently dropped,
// see sendCallback).
func RequestCallback(pid PID, msg interface{}, sender PID, cb func(Context, interface{}, error)) *Envelope {
	return request(pid, msg, sender, cb)
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
func RequestAsMessage(pid PID, msg interface{}, sender PID) *Envelope {
	return requestAsMessageWithValues(pid, msg, sender, nil)
}

func requestAsMessageWithValues(pid PID, msg interface{}, sender PID, values map[string]interface{}) *Envelope {
	env := NewRequestAsMessage(msg, sender, values)
	env.pipePID = sender
	proc, ok := loadProc(pid)
	if !ok {
		env.Respond(nil, ErrActorNotFound)
		return env
	}
	proc.mailbox.PushUser(env)
	return env
}

func request(pid PID, msg interface{}, sender PID, cb func(Context, interface{}, error)) *Envelope {
	return requestWithValues(pid, msg, sender, nil, cb)
}

func requestWithValues(pid PID, msg interface{}, sender PID, values map[string]interface{}, cb func(Context, interface{}, error)) *Envelope {
	env := NewRequest(msg, sender, values, cb)
	env.pipePID = sender
	proc, ok := loadProc(pid)
	if !ok {
		env.Respond(nil, ErrActorNotFound)
		return env
	}
	proc.mailbox.PushUser(env)
	return env
}

// stop signals the actor identified by pid to shut down.
func stop(pid PID) {
	proc, ok := loadProc(pid)
	if ok {
		proc.mailbox.PushSystem(&Envelope{Kind: kindSystem, sys: systemStop})
	}
}

// Shutdown 先按注册逆序关闭动态 Actor，再逆序关闭固定数组。
// 每次等待 OnStop 完成后才关闭下一个；关闭期间固定服务仍可接受消息。
// 调用前应停止外部入口并完成业务异步存盘；OnStop 的异步回调不自动等待。
// 不得从本系统 Actor 的回调中同步调用 Shutdown。
func Shutdown() {
	globalTree.lifecycleMu.Lock()
	if globalTree.closing {
		globalTree.lifecycleMu.Unlock()
		<-globalTree.stopped
		return
	}
	globalTree.closing = true
	procs := make([]*actorProcess, 0)
	globalTree.multiMods.Range(func(_, value interface{}) bool {
		procs = append(procs, value.(*actorProcess))
		return true
	})
	globalTree.lifecycleMu.Unlock()
	sort.Slice(procs, func(i, j int) bool { return procs[i].pid.ID > procs[j].pid.ID })
	for _, proc := range procs {
		stopAndWait(proc)
	}
	for i := len(globalTree.mods) - 1; i >= 0; i-- {
		stopAndWait(globalTree.mods[i])
	}
	close(globalTree.stopped)
}

// stopAndWait 将停止信号放到业务队列末尾，等待退出清理完成。
func stopAndWait(proc *actorProcess) {
	select {
	case <-proc.done:
		return
	default:
	}
	select {
	case proc.mailbox.userChan <- &Envelope{Kind: kindSystem, sys: systemStop}:
	case <-proc.done:
	}
	<-proc.done
}

// SendCallback 向目标 Actor 投递一个回调，回调在目标 Actor 的 goroutine 内
// 串行执行，并携带该 Actor 的 Context。
func SendCallback(pid PID, cb func(Context, interface{}, error), value interface{}, err error) bool {
	return sendCallback(pid, cb, value, err)
}

// TrySendCallback 非阻塞投递回调；Actor 不存在或邮箱已满时返回 false。
// 回调只会在目标 Actor 的消息循环中执行，不会在调用方执行。
func TrySendCallback(pid PID, cb func(Context, interface{}, error), value interface{}, err error) bool {
	proc, ok := loadProc(pid)
	if !ok {
		return false
	}
	return proc.mailbox.TryPushUser(&Envelope{Kind: kindCallback, cb: cb, value: value, err: err})
}

// SpawnCount returns the number of currently running actors.
func SpawnCount() int {
	return int(globalTree.count.Load())
}

// Start 阻塞运行，监听 OS 关闭信号（Ctrl+C / kill）。
// 收到信号后优雅关闭所有 Actor，然后返回。
func Start() {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	Shutdown()
}
