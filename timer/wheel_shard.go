package timer

import (
	"container/list"
	"sync"
	"time"
)

// wheelShard 独占一组分级槽；锁内只操作调度数据和非阻塞投递，不执行业务回调。
type wheelShard struct {
	mu      sync.Mutex
	once    sync.Once
	epoch   time.Time
	curTick int64
	slots   [twLevels][twSize]list.List
	ready   list.List
	queued  int
	wake    chan struct{}
}

var sharedWheels [twShards]wheelShard

// sharedShard 按 Actor 编号选择分片，分片按需启动，空闲时不运行 ticker。
func sharedShard(id uint64) *wheelShard {
	shard := &sharedWheels[id%twShards]
	shard.once.Do(func() {
		shard.epoch = time.Now()
		shard.wake = make(chan struct{}, 1)
		go shard.run()
	})
	return shard
}

// schedule 使用单调时间计算到期 tick；空闲分片直接跳过此前的空闲时间。
// 调用方持有 mu。
func (shard *wheelShard) schedule(task *WheelTimer, deadline time.Time) {
	now := time.Now()
	idle := shard.queued == 0
	if idle {
		shard.curTick = int64(now.Sub(shard.epoch) / twTickMs)
	}
	// Cron 的时间可能没有单调时钟信息，先求剩余时间，再映射到单调时钟。
	remaining := deadline.Sub(now)
	if remaining < 0 {
		remaining = 0
	}
	elapsed := now.Add(remaining).Sub(shard.epoch)
	task.expires = int64(elapsed / twTickMs)
	if elapsed%twTickMs != 0 {
		task.expires++
	}
	if task.expires <= shard.curTick {
		task.expires = shard.curTick + 1
	}
	shard.add(task)
	if idle {
		select {
		case shard.wake <- struct{}{}:
		default:
		}
	}
}

// link 将任务加入队列，同时维护可取消的位置和调度任务数量。
func (shard *wheelShard) link(task *WheelTimer, bucket *list.List) {
	task.bucket = bucket
	task.element = bucket.PushBack(task)
	shard.queued++
}

// unlink 移除已排队任务，已投递到 Actor 邮箱的任务没有队列元素。
func (shard *wheelShard) unlink(task *WheelTimer) {
	if task.element == nil {
		return
	}
	task.bucket.Remove(task.element)
	task.element = nil
	task.bucket = nil
	shard.queued--
}

// finish 结束任务并清除所有业务引用，外部保留句柄也不会持有玩家对象。
func (shard *wheelShard) finish(task *WheelTimer) {
	owner := task.owner
	if owner == nil {
		return
	}
	shard.unlink(task)
	delete(owner.timers, task)
	if owner.pending.Len() == 0 && owner.ready != nil {
		shard.ready.Remove(owner.ready)
		owner.ready = nil
	}
	task.owner = nil
	task.handler = nil
	task.data = nil
	task.cron = nil
}

// add 将任务按到期距离加入对应层，已经到期的任务直接进入投递队列。
func (shard *wheelShard) add(task *WheelTimer) {
	diff := task.expires - shard.curTick
	if diff <= 0 {
		shard.enqueue(task)
		return
	}
	level := 0
	switch {
	case diff < 1<<8:
	case diff < 1<<16:
		level = 1
	case diff < 1<<24:
		level = 2
	default:
		level = 3
	}
	slot := int(task.expires>>(8*level)) & twMask
	shard.link(task, &shard.slots[level][slot])
}

// enqueue 为每个 Actor 保持独立的到期队列，邮箱拥堵只影响当前 Actor。
func (shard *wheelShard) enqueue(task *WheelTimer) {
	owner := task.owner
	shard.link(task, &owner.pending)
	if owner.ready == nil {
		owner.ready = shard.ready.PushBack(owner)
	}
}

// cascade 将当前高层槽的任务重新分配到较低层。
func (shard *wheelShard) cascade(level int) {
	bucket := &shard.slots[level][int(shard.curTick>>(8*level))&twMask]
	count := bucket.Len()
	for i := 0; i < count; i++ {
		task := bucket.Front().Value.(*WheelTimer)
		shard.unlink(task)
		shard.add(task)
	}
}

// advance 按实际经过的单调时间追赶 tick，ticker 丢失通知不会造成持续计时漂移。
func (shard *wheelShard) advance(now time.Time) {
	target := int64(now.Sub(shard.epoch) / twTickMs)
	for shard.curTick < target {
		shard.curTick++
		for level := twLevels - 1; level > 0; level-- {
			if shard.curTick&((1<<(8*level))-1) == 0 {
				shard.cascade(level)
			}
		}
		shard.cascade(0)
	}
	shard.dispatch()
}

// dispatch 按 Actor 轮转投递；邮箱满时保留任务，下个 tick 重试，不阻塞其他 Actor。
func (shard *wheelShard) dispatch() {
	owners, budget := shard.ready.Len(), twDispatchLimit
	for i := 0; i < owners && budget > 0; i++ {
		entry := shard.ready.Front()
		owner := entry.Value.(*TimeWheel)
		for sent := 0; sent < owner.batch && budget > 0 && owner.pending.Len() > 0; sent++ {
			budget--
			task := owner.pending.Front().Value.(*WheelTimer)
			if !owner.sender.TrySendCallback(owner.target, task.fire, nil, nil) {
				break
			}
			shard.unlink(task)
		}
		if owner.pending.Len() == 0 {
			shard.ready.Remove(entry)
			owner.ready = nil
		} else {
			shard.ready.MoveToBack(entry)
		}
	}
}

// run 是分片唯一的驱动协程；没有待调度任务时停掉 ticker 并等待新增任务唤醒。
func (shard *wheelShard) run() {
	for range shard.wake {
		ticker := time.NewTicker(twTickMs)
		for {
			shard.mu.Lock()
			shard.advance(time.Now())
			active := shard.queued > 0
			shard.mu.Unlock()
			if !active {
				break
			}
			<-ticker.C
		}
		ticker.Stop()
	}
}
