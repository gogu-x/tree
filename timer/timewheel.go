package timer

import (
	"container/list"
	"sync"
	"time"
)

const (
	twTickMs  = 10 * time.Millisecond // 最小精度 10ms
	twSize    = 256                   // 每层槽数（2的幂，便于取模）
	twMask    = twSize - 1
	twLevels  = 4 // 4层：覆盖 ~11分钟
)

// WheelTimer 分级时间轮定时器句柄
type WheelTimer struct {
	cb      func()
	expires int64 // 绝对 tick 数
	stopped bool
	mu      sync.Mutex
}

func (t *WheelTimer) Stop() {
	t.mu.Lock()
	t.stopped = true
	t.mu.Unlock()
}

// TimeWheel 分级时间轮
// 不是 goroutine 安全的，需在同一 goroutine 内使用（与 Dispatcher 一致）
type TimeWheel struct {
	slots    [twLevels][twSize]*list.List
	curTick  int64
	ticker   *time.Ticker
	ChanTimer chan func()
	stopCh   chan struct{}
}

// NewTimeWheel 创建时间轮，chanLen 为输出 channel 缓冲大小
func NewTimeWheel(chanLen int) *TimeWheel {
	tw := &TimeWheel{
		ticker:    time.NewTicker(twTickMs),
		ChanTimer: make(chan func(), chanLen),
		stopCh:    make(chan struct{}),
	}
	for i := 0; i < twLevels; i++ {
		for j := 0; j < twSize; j++ {
			tw.slots[i][j] = list.New()
		}
	}
	go tw.run()
	return tw
}

// AfterFunc 注册定时器，d 最小精度 10ms
func (tw *TimeWheel) AfterFunc(d time.Duration, cb func()) *WheelTimer {
	ticks := int64(d/twTickMs) + 1
	t := &WheelTimer{
		cb:      cb,
		expires: tw.curTick + ticks,
	}
	tw.addTimer(t)
	return t
}

// Stop 停止时间轮
func (tw *TimeWheel) Stop() {
	tw.ticker.Stop()
	close(tw.stopCh)
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

func (tw *TimeWheel) tick() {
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
			tw.ChanTimer <- t.cb
		}
		e = next
	}
}

func (tw *TimeWheel) run() {
	for {
		select {
		case <-tw.stopCh:
			return
		case <-tw.ticker.C:
			tw.tick()
		}
	}
}
