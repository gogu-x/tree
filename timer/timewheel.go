package timer

import (
	"container/list"
	"time"

	"github.com/gogu-x/tree"
)

const (
	twTickMs        = 10 * time.Millisecond // 最小精度为 10ms。
	twSize          = 256                   // 每层槽数，必须是 2 的幂。
	twMask          = twSize - 1            // 槽下标掩码。
	twLevels        = 4                     // 四层覆盖约 497 天，超出范围的任务会重新入轮。
	twShards        = 16                    // 进程内共享的时间轮分片数。
	twDispatchLimit = 1024                  // 每个分片每轮最多投递的回调数。
)

// TimerType 标识一类定时任务；同一个 TimeWheel 内必须唯一。
type TimerType uint32

// Handler 是定时任务到期后在绑定 Actor goroutine 内执行的回调。
type Handler func(data interface{})

// CallbackSender 非阻塞地向 Actor 邮箱投递回调；邮箱满时返回 false，由时间轮重试。
// *tree.Tree 实现该接口。实现方不能同步执行回调，否则会重入分片锁。
type CallbackSender interface {
	TrySendCallback(pid tree.PID, cb func(tree.Context, interface{}, error), value interface{}, err error) bool
}

// TimeWheel 管理一个 Actor 的定时任务，底层与其他 Actor 共享固定分片。
// 回调始终在绑定 Actor 中执行；Actor 退出时必须调用 Stop。
type TimeWheel struct {
	shard    *wheelShard              // 保护本对象和所属任务的全部可变字段。
	handlers map[TimerType]Handler    // 本 Actor 的任务类型注册表。
	timers   map[*WheelTimer]struct{} // 尚未执行完成的任务，包含已投递回调。
	target   tree.PID                 // 回调接收者。
	sender   CallbackSender           // 非阻塞回调投递器。
	stopped  bool                     // 是否已停止接收和执行任务。
	batch    int                      // 单轮向本 Actor 投递的最大任务数。
	pending  list.List                // 已到期、尚未成功投递的任务。
	ready    *list.Element            // 在分片待投递 Actor 队列中的位置。
}

// WheelTimer 是可取消的任务句柄；取消会移除任务并释放业务引用。
type WheelTimer struct {
	timerType TimerType     // 任务类型，支持所属 wheel 按类型取消。
	shard     *wheelShard   // 句柄取消和执行所使用的锁。
	owner     *TimeWheel    // 所属 Actor 的定时任务管理器，结束后清空。
	handler   Handler       // 任务处理器，结束后清空。
	data      interface{}   // 业务参数，结束后清空。
	cron      *CronExpr     // 非 nil 时，在回调完成后安排下一次日历任务。
	expires   int64         // 相对分片单调时钟起点的绝对 tick。
	bucket    *list.List    // 当前所属槽或待投递队列。
	element   *list.Element // 支持取消时直接移除，不等到到期才释放。
}

// WheelCron 是日历任务句柄，共享一个可取消任务，不并发替换内部指针。
type WheelCron struct {
	timer *WheelTimer // 周期任务；无下一次执行时间时为 nil。
}

// NewTimeWheel 创建绑定到 target 的任务管理器，不为每个 Actor 创建 ticker 或 goroutine。
// chanLen 保留原调用参数，现作为每轮向该 Actor 投递任务的数量上限；0 按 1 处理。
func NewTimeWheel(chanLen int, target tree.PID, sender CallbackSender) *TimeWheel {
	if sender == nil {
		panic("timer: NewTimeWheel called with nil message sender")
	}
	if chanLen < 0 {
		panic("timer: negative queue size")
	}
	if chanLen == 0 {
		chanLen = 1
	}
	if chanLen > twDispatchLimit {
		chanLen = twDispatchLimit
	}
	return &TimeWheel{
		shard: sharedShard(target.ID), handlers: make(map[TimerType]Handler),
		timers: make(map[*WheelTimer]struct{}), target: target, sender: sender, batch: chanLen,
	}
}

// Register 注册任务类型，重复类型、nil handler 或停止后注册均会 panic。
func (tw *TimeWheel) Register(timerType TimerType, handler Handler) {
	tw.shard.mu.Lock()
	defer tw.shard.mu.Unlock()
	if handler == nil {
		panic("timer: register nil handler")
	}
	if tw.stopped {
		panic("timer: register after Stop")
	}
	if _, exists := tw.handlers[timerType]; exists {
		panic("timer: duplicate timer type registration")
	}
	tw.handlers[timerType] = handler
}

// After 创建延时任务，按实际创建时间计时，不早于指定时刻触发；停止后返回 nil。
func (tw *TimeWheel) After(timerType TimerType, d time.Duration, data interface{}) *WheelTimer {
	if d < 0 {
		d = 0
	}
	return tw.schedule(timerType, time.Now().Add(d), data, nil)
}

// Cron 创建日历任务；每次 Actor 回调完成后按当前时间计算下一次，不补发错过的周期。
func (tw *TimeWheel) Cron(timerType TimerType, expr *CronExpr, data interface{}) *WheelCron {
	if expr == nil {
		panic("timer: nil cron expression")
	}
	return &WheelCron{timer: tw.schedule(timerType, expr.Next(time.Now()), data, expr)}
}

// schedule 验证类型并将任务登记到共享时间轮。
func (tw *TimeWheel) schedule(timerType TimerType, deadline time.Time, data interface{}, cron *CronExpr) *WheelTimer {
	shard := tw.shard
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if tw.stopped {
		return nil
	}
	handler := tw.handlers[timerType]
	if handler == nil {
		panic("timer: unregistered timer type")
	}
	if deadline.IsZero() {
		return nil
	}
	task := &WheelTimer{shard: shard, owner: tw, timerType: timerType, handler: handler, data: data, cron: cron}
	tw.timers[task] = struct{}{}
	shard.schedule(task, deadline)
	return task
}

// Cancel 取消本 Actor 指定类型的全部任务，包括已投递但尚未开始的回调。
// 保留类型注册，后续可以再次 After 或 Cron；不影响其他类型和其他 Actor。
func (tw *TimeWheel) Cancel(timerType TimerType) {
	shard := tw.shard
	shard.mu.Lock()
	defer shard.mu.Unlock()
	for task := range tw.timers {
		if task.timerType == timerType {
			shard.finish(task)
		}
	}
}

// Stop 取消本 Actor 的全部任务，包括已入邮箱但尚未开始执行的回调。
// 已开始执行的回调不被中断；不会停止其他 Actor 的任务或共享分片。
func (tw *TimeWheel) Stop() {
	shard := tw.shard
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if tw.stopped {
		return
	}
	tw.stopped = true
	for task := range tw.timers {
		shard.finish(task)
	}
	tw.handlers = nil
	tw.sender = nil
	tw.timers = nil
}

// Stop 取消单个任务；调用返回后，尚未开始的回调不会再调用业务处理器。
func (task *WheelTimer) Stop() {
	if task == nil {
		return
	}
	task.shard.mu.Lock()
	defer task.shard.mu.Unlock()
	task.shard.finish(task)
}

// Stop 停止日历任务，已开始执行的回调可以完成，但不会再续订。
func (cron *WheelCron) Stop() {
	if cron != nil {
		cron.timer.Stop()
	}
}

// fire 在所属 Actor 内领取任务，取消与领取通过同一把锁确定先后顺序。
func (task *WheelTimer) fire(_ tree.Context, _ interface{}, _ error) {
	shard := task.shard
	shard.mu.Lock()
	if task.owner == nil {
		shard.mu.Unlock()
		return
	}
	handler, data, cron := task.handler, task.data, task.cron
	if cron == nil {
		shard.finish(task)
	}
	shard.mu.Unlock()
	if cron != nil {
		defer task.repeat(cron)
	}
	handler(data)
}

// repeat 在日历回调结束后续订，取消或玩家退出后不会重新注册。
func (task *WheelTimer) repeat(cron *CronExpr) {
	next := cron.Next(time.Now())
	shard := task.shard
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if task.owner == nil {
		return
	}
	if next.IsZero() {
		shard.finish(task)
		return
	}
	shard.schedule(task, next)
}
