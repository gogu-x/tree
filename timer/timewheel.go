package timer

import (
	"container/list"
	"sync"
	"time"

	"github.com/gogu-x/tree"
)

const (
	twTickMs = 10 * time.Millisecond // 最小精度 10ms
	twSize   = 256                   // 每层槽数（2的幂，便于取模）
	twMask   = twSize - 1
	twLevels = 4 // 4层：覆盖 ~11分钟
)

// WheelTimer 分级时间轮定时器句柄
type WheelTimer struct {
	cb      func()
	ticks   int64 // 相对延迟 tick 数
	expires int64 // 绝对 tick 数，由 run goroutine 赋值
	stopped bool
	mu      sync.Mutex
}

func (t *WheelTimer) Stop() {
	t.mu.Lock()
	t.stopped = true
	t.mu.Unlock()
}

// TimerType 标识一类定时任务；同一个 TimeWheel 内必须唯一。
type TimerType uint32

// Handler 是定时任务到期后在绑定 Actor goroutine 内执行的回调。
type Handler func(data interface{})

// CallbackSender 将回调投递到指定 Actor 的 mailbox。
// *tree.Tree 实现了该接口。
type CallbackSender interface {
	SendCallback(pid tree.PID, cb func(tree.Context, interface{}, error), value interface{}, err error) bool
}

// TimeWheel 分级时间轮，创建后固定绑定一个 Actor。
// run() goroutine 负责所有槽操作；到期回调由 dispatch 自动投递到该 Actor。
type TimeWheel struct {
	slots     [twLevels][twSize]*list.List
	curTick   int64
	ticker    *time.Ticker
	chanTimer chan func()
	stopCh    chan struct{}
	addCh     chan *WheelTimer
	stopOnce  sync.Once
	stateMu   sync.RWMutex
	stopped   bool
	handlers  map[TimerType]Handler
	target    tree.PID
	sender    CallbackSender
}

// NewTimeWheel 创建绑定到 target Actor 的时间轮，chanLen 为到期队列缓冲大小。
func NewTimeWheel(chanLen int, target tree.PID, sender CallbackSender) *TimeWheel {
	if sender == nil {
		panic("timer: NewTimeWheel called with nil message sender")
	}
	tw := &TimeWheel{
		ticker:    time.NewTicker(twTickMs),
		chanTimer: make(chan func(), chanLen),
		stopCh:    make(chan struct{}),
		addCh:     make(chan *WheelTimer, 256),
		handlers:  make(map[TimerType]Handler),
		target:    target,
		sender:    sender,
	}
	for i := 0; i < twLevels; i++ {
		for j := 0; j < twSize; j++ {
			tw.slots[i][j] = list.New()
		}
	}
	go tw.run()
	go tw.dispatch()
	return tw
}

// Register 注册 timerType 对应的回调。重复类型或 nil handler 会 panic。
// Register 和任务创建必须在 TimeWheel 所属 Actor goroutine 中调用。
func (tw *TimeWheel) Register(timerType TimerType, handler Handler) {
	if handler == nil {
		panic("timer: register nil handler")
	}
	if _, exists := tw.handlers[timerType]; exists {
		panic("timer: duplicate timer type registration")
	}
	tw.handlers[timerType] = handler
}

func (tw *TimeWheel) handler(timerType TimerType) Handler {
	handler := tw.handlers[timerType]
	if handler == nil {
		panic("timer: unregistered timer type")
	}
	return handler
}

// After 创建一条指定类型的延时任务，精度 10ms。
// 到期后对应回调会自动投递到绑定 Actor 的 mailbox，并在其 goroutine 中执行；
// 若时间轮已停止则返回 nil。
func (tw *TimeWheel) After(timerType TimerType, d time.Duration, data interface{}) *WheelTimer {
	handler := tw.handler(timerType)
	return tw.afterFunc(d, func() {
		tw.sender.SendCallback(tw.target, func(_ tree.Context, _ interface{}, _ error) {
			handler(data)
		}, nil, nil)
	})
}

func (tw *TimeWheel) afterFunc(d time.Duration, cb func()) *WheelTimer {
	ticks := int64(d/twTickMs) + 1
	t := &WheelTimer{cb: cb, ticks: ticks}

	tw.stateMu.RLock()
	defer tw.stateMu.RUnlock()
	if tw.stopped {
		return nil
	}
	tw.addCh <- t
	return t
}

// Stop 停止时间轮，safe to call multiple times.
func (tw *TimeWheel) Stop() {
	tw.stopOnce.Do(func() {
		tw.stateMu.Lock()
		defer tw.stateMu.Unlock()
		tw.stopped = true
		tw.ticker.Stop()
		close(tw.stopCh)
	})
}

// dispatch 消费 chanTimer，执行到期的回调函数
func (tw *TimeWheel) dispatch() {
	for {
		select {
		case <-tw.stopCh:
			return
		case cb, ok := <-tw.chanTimer:
			if !ok {
				return
			}
			cb()
		}
	}
}

// WheelCron is a recurring timer driven by a CronExpr.
type WheelCron struct {
	t *WheelTimer
}

func (c *WheelCron) Stop() {
	if c.t != nil {
		c.t.Stop()
	}
}

// Cron 创建一条按 cronExpr 周期触发的指定类型任务。
func (tw *TimeWheel) Cron(timerType TimerType, cronExpr *CronExpr, data interface{}) *WheelCron {
	handler := tw.handler(timerType)
	c := new(WheelCron)
	now := time.Now()
	next := cronExpr.Next(now)
	if next.IsZero() {
		return c
	}
	var schedule func()
	schedule = func() {
		tw.sender.SendCallback(tw.target, func(_ tree.Context, _ interface{}, _ error) {
			handler(data)
		}, nil, nil)
		now := time.Now()
		next := cronExpr.Next(now)
		if next.IsZero() {
			return
		}
		c.t = tw.afterFunc(next.Sub(now), schedule)
	}
	c.t = tw.afterFunc(next.Sub(now), schedule)
	return c
}

func (tw *TimeWheel) addTimer(t *WheelTimer) {
	diff := t.expires - tw.curTick
	var level, slot int
	switch {
	case diff < twSize:
		level = 0
		slot = int(t.expires) & twMask
	case diff < twSize*twSize:
		level = 1
		slot = int(t.expires>>8) & twMask
	case diff < twSize*twSize*twSize:
		level = 2
		slot = int(t.expires>>16) & twMask
	default:
		level = 3
		slot = int(t.expires>>24) & twMask
	}
	tw.slots[level][slot].PushBack(t)
}

// cascade 将高层到期的定时器重新分配到低层
func (tw *TimeWheel) cascade(level int, slot int) {
	l := tw.slots[level][slot]
	for e := l.Front(); e != nil; {
		t := e.Value.(*WheelTimer)
		next := e.Next()
		l.Remove(e)
		tw.addTimer(t)
		e = next
	}
}

func (tw *TimeWheel) tick() bool {
	tw.curTick++

	// 逐层 cascade
	if tw.curTick&twMask == 0 {
		slot1 := (tw.curTick >> 8) & twMask
		tw.cascade(1, int(slot1))
		if slot1 == 0 {
			slot2 := (tw.curTick >> 16) & twMask
			tw.cascade(2, int(slot2))
			if slot2 == 0 {
				slot3 := (tw.curTick >> 24) & twMask
				tw.cascade(3, int(slot3))
			}
		}
	}

	// 触发第0层当前槽
	slot := int(tw.curTick) & twMask
	l := tw.slots[0][slot]
	for e := l.Front(); e != nil; {
		t := e.Value.(*WheelTimer)
		next := e.Next()
		l.Remove(e)
		t.mu.Lock()
		stopped := t.stopped
		t.mu.Unlock()
		if !stopped {
			select {
			case tw.chanTimer <- t.cb:
			case <-tw.stopCh:
				return false
			}
		}
		e = next
	}
	return true
}

func (tw *TimeWheel) run() {
	for {
		select {
		case <-tw.stopCh:
			return
		case t := <-tw.addCh:
			t.expires = tw.curTick + t.ticks
			tw.addTimer(t)
		case <-tw.ticker.C:
			// drain pending adds before ticking
			for {
				select {
				case t := <-tw.addCh:
					t.expires = tw.curTick + t.ticks
					tw.addTimer(t)
				default:
					goto tick
				}
			}
		tick:
			if !tw.tick() {
				return
			}
		}
	}
}
