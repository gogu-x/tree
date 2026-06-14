package timer

import (
	"container/list"
	"sync"
	"time"
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
	expires int64 // 绝对 tick 数，由 run goroutine 赋值
	stopped bool
	mu      sync.Mutex
}

func (t *WheelTimer) Stop() {
	t.mu.Lock()
	t.stopped = true
	t.mu.Unlock()
}

// addReq 用于跨 goroutine 安全地向 run goroutine 提交定时器注册请求
type addReq struct {
	timer *WheelTimer
	ticks int64 // 相对延迟 tick 数
}

// TimeWheel 分级时间轮
// run() goroutine 负责所有槽操作；AfterFunc 通过 addCh 跨 goroutine 安全注册。
type TimeWheel struct {
	slots     [twLevels][twSize]*list.List
	curTick   int64
	ticker    *time.Ticker
	chanTimer chan func()
	stopCh    chan struct{}
	addCh     chan addReq
	stopOnce  sync.Once
}

// NewTimeWheel 创建时间轮，chanLen 为输出 channel 缓冲大小
func NewTimeWheel(chanLen int) *TimeWheel {
	tw := &TimeWheel{
		ticker:    time.NewTicker(twTickMs),
		chanTimer: make(chan func(), chanLen),
		stopCh:    make(chan struct{}),
		addCh:     make(chan addReq, 256),
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

// AfterFunc 注册定时器，goroutine-safe，精度 10ms。
// 若时间轮已停止则返回 nil。
func (tw *TimeWheel) AfterFunc(d time.Duration, cb func()) *WheelTimer {
	ticks := int64(d/twTickMs) + 1
	t := &WheelTimer{cb: cb}
	select {
	case tw.addCh <- addReq{timer: t, ticks: ticks}:
		return t
	case <-tw.stopCh:
		return nil
	}
}

// Stop 停止时间轮，safe to call multiple times.
func (tw *TimeWheel) Stop() {
	tw.stopOnce.Do(func() {
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

// CronFunc schedules cb according to cronExpr using the time wheel.
func (tw *TimeWheel) CronFunc(cronExpr *CronExpr, cb func()) *WheelCron {
	c := new(WheelCron)
	now := time.Now()
	next := cronExpr.Next(now)
	if next.IsZero() {
		return c
	}
	var schedule func()
	schedule = func() {
		cb()
		now := time.Now()
		next := cronExpr.Next(now)
		if next.IsZero() {
			return
		}
		c.t = tw.AfterFunc(next.Sub(now), schedule)
	}
	c.t = tw.AfterFunc(next.Sub(now), schedule)
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
			t.timer.expires = tw.curTick + t.ticks
			tw.addTimer(t.timer)
		case <-tw.ticker.C:
			// drain pending adds before ticking
			for {
				select {
				case t := <-tw.addCh:
					t.timer.expires = tw.curTick + t.ticks
					tw.addTimer(t.timer)
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
