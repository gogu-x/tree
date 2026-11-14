package timer

import (
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/gogu-x/tree"
)

type queuedCallback struct {
	pid tree.PID
	cb  func(tree.Context, interface{}, error)
}
type controlledSender struct {
	mu        sync.Mutex
	blocked   map[uint64]bool
	callbacks []queuedCallback
}

func (s *controlledSender) TrySendCallback(pid tree.PID, cb func(tree.Context, interface{}, error), _ interface{}, _ error) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.blocked[pid.ID] {
		return false
	}
	s.callbacks = append(s.callbacks, queuedCallback{pid, cb})
	return true
}
func (s *controlledSender) take() []queuedCallback {
	s.mu.Lock()
	defer s.mu.Unlock()
	callbacks := s.callbacks
	s.callbacks = nil
	return callbacks
}
func manualWheel(shard *wheelShard, id uint64, sender callbackSender, handler Handler) *TimeWheel {
	return &TimeWheel{shard: shard, handlers: map[TimerType]Handler{testTimerType: handler}, timers: make(map[*WheelTimer]struct{}), target: tree.PID{ID: id}, sender: sender, batch: 32}
}
func manualShard() *wheelShard { return &wheelShard{epoch: time.Now(), wake: make(chan struct{}, 1)} }
func advanceTo(shard *wheelShard, tick int64) {
	shard.mu.Lock()
	defer shard.mu.Unlock()
	shard.advance(shard.epoch.Add(time.Duration(tick) * twTickMs))
}

// TestSharedWheelCatchUp 验证漏过 ticker 通知后按实际时间追赶，且不会提前执行。
func TestSharedWheelCatchUp(t *testing.T) {
	shard, sender := manualShard(), &controlledSender{}
	var values []int
	wheel := manualWheel(shard, 1, sender, func(value interface{}) { values = append(values, value.(int)) })
	first := wheel.After(testTimerType, 100*time.Millisecond, 1)
	last := wheel.After(testTimerType, 3*time.Second, 2)
	advanceTo(shard, first.expires-1)
	if len(sender.take()) != 0 {
		t.Fatal("timer executed before its deadline")
	}
	// 一次推进跨过多个 tick 和低层边界，两个任务都应到期。
	advanceTo(shard, last.expires)
	callbacks := sender.take()
	if len(callbacks) != 2 {
		t.Fatalf("callbacks=%d, want 2", len(callbacks))
	}
	for _, cb := range callbacks {
		cb.cb(nil, nil, nil)
	}
	if len(values) != 2 || values[0] != 1 || values[1] != 2 {
		t.Fatalf("execution order=%v", values)
	}
	if shard.queued != 0 || len(wheel.timers) != 0 {
		t.Fatal("completed timers retained")
	}
}

// TestSharedWheelCascadeBoundaries 验证各层进位时不会漏掉边界任务。
func TestSharedWheelCascadeBoundaries(t *testing.T) {
	for _, boundary := range []int64{1 << 8, 1 << 16, 1 << 24} {
		shard, sender := manualShard(), &controlledSender{}
		wheel := manualWheel(shard, 1, sender, func(interface{}) {})
		task := &WheelTimer{shard: shard, owner: wheel, handler: wheel.handlers[testTimerType], expires: boundary}
		wheel.timers[task] = struct{}{}
		shard.add(task)
		// 将测试时钟直接定位到进位前，避免遍历数百万个空 tick。
		shard.curTick = boundary - 1
		advanceTo(shard, boundary)
		if callbacks := sender.take(); len(callbacks) != 1 {
			t.Fatalf("boundary %d: callbacks=%d", boundary, len(callbacks))
		} else {
			callbacks[0].cb(nil, nil, nil)
		}
	}
}

// TestSharedWheelBackpressure 验证满邮箱只阻塞自身，重试不丢失或重复投递任务。
func TestSharedWheelBackpressure(t *testing.T) {
	shard := manualShard()
	sender := &controlledSender{blocked: map[uint64]bool{1: true}}
	slow := manualWheel(shard, 1, sender, func(interface{}) {})
	fast := manualWheel(shard, 17, sender, func(interface{}) {})
	a := slow.After(testTimerType, time.Millisecond, nil)
	b := fast.After(testTimerType, time.Millisecond, nil)
	deadline := max(a.expires, b.expires)
	advanceTo(shard, deadline)
	callbacks := sender.take()
	if len(callbacks) != 1 || callbacks[0].pid.ID != 17 {
		t.Fatal("slow mailbox blocked another actor")
	}
	callbacks[0].cb(nil, nil, nil)
	advanceTo(shard, deadline+1)
	if len(sender.take()) != 0 {
		t.Fatal("callback duplicated")
	}
	sender.blocked[1] = false
	advanceTo(shard, deadline+2)
	callbacks = sender.take()
	if len(callbacks) != 1 || callbacks[0].pid.ID != 1 {
		t.Fatal("blocked task was lost")
	}
	callbacks[0].cb(nil, nil, nil)
	if shard.queued != 0 || shard.ready.Len() != 0 {
		t.Fatal("delivery queue did not drain")
	}
}

// TestSharedWheelCancelDeliveredAndPending 验证取消及时释放任务，并抑制已投递但未执行的回调。
func TestSharedWheelCancelDeliveredAndPending(t *testing.T) {
	shard, sender := manualShard(), &controlledSender{}
	calls := 0
	wheel := manualWheel(shard, 1, sender, func(interface{}) { calls++ })
	future := wheel.After(testTimerType, 24*time.Hour, make([]byte, 1024))
	future.Stop()
	if future.owner != nil || future.data != nil || future.handler != nil || future.element != nil {
		t.Fatal("canceled handle retained business references")
	}
	delivered := wheel.After(testTimerType, time.Millisecond, nil)
	advanceTo(shard, delivered.expires)
	callbacks := sender.take()
	if len(callbacks) != 1 {
		t.Fatal("expected a queued callback")
	}
	delivered.Stop()
	callbacks[0].cb(nil, nil, nil)
	pending := wheel.After(testTimerType, time.Hour, make([]byte, 1024))
	wheel.Stop()
	if pending.owner != nil || pending.data != nil || shard.queued != 0 {
		t.Fatal("owner Stop retained tasks")
	}
	if calls != 0 {
		t.Fatal("canceled callback executed")
	}
}

// TestSharedWheelStopIsolation 验证同分片一个 Actor 退出不影响另一个 Actor。
func TestSharedWheelStopIsolation(t *testing.T) {
	shard, sender := manualShard(), &controlledSender{}
	a := manualWheel(shard, 1, sender, func(interface{}) { t.Error("stopped owner executed") })
	calls := 0
	b := manualWheel(shard, 17, sender, func(interface{}) { calls++ })
	taskA := a.After(testTimerType, time.Millisecond, nil)
	taskB := b.After(testTimerType, time.Millisecond, nil)
	advanceTo(shard, max(taskA.expires, taskB.expires))
	callbacks := sender.take()
	a.Stop()
	for _, cb := range callbacks {
		cb.cb(nil, nil, nil)
	}
	if calls != 1 {
		t.Fatalf("other owner calls=%d", calls)
	}
}

// TestSharedWheelCronStopDuringCallback 验证 Cron 取消与续订并发时不会复活。
func TestSharedWheelCronStopDuringCallback(t *testing.T) {
	shard, sender := manualShard(), &controlledSender{}
	entered, release := make(chan struct{}), make(chan struct{})
	wheel := manualWheel(shard, 1, sender, func(interface{}) { close(entered); <-release })
	expr, err := NewCronExpr("* * * * * *")
	if err != nil {
		t.Fatal(err)
	}
	cron := wheel.Cron(testTimerType, expr, nil)
	advanceTo(shard, cron.timer.expires)
	callbacks := sender.take()
	done := make(chan struct{})
	go func() { callbacks[0].cb(nil, nil, nil); close(done) }()
	<-entered
	cron.Stop()
	close(release)
	<-done
	if shard.queued != 0 || len(wheel.timers) != 0 || cron.timer.owner != nil {
		t.Fatal("canceled cron restarted")
	}
}

// TestSharedWheelConcurrentStop 验证添加任务和取消整个所有者的竞态。
func TestSharedWheelConcurrentStop(t *testing.T) {
	wheel := newTimeWheel(32, tree.PID{ID: 1}, &controlledSender{})
	wheel.Register(testTimerType, func(interface{}) {})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 100; n++ {
				if task := wheel.After(testTimerType, time.Hour, nil); task != nil {
					task.Stop()
				}
			}
		}()
	}
	wheel.Stop()
	wg.Wait()
	wheel.shard.mu.Lock()
	defer wheel.shard.mu.Unlock()
	if len(wheel.timers) != 0 {
		t.Fatal("stopped owner retained tasks")
	}
}

// TestSharedWheelPopulation 验证 5000 个所有者只使用固定分片，并能取消全部长延时任务。
func TestSharedWheelPopulation(t *testing.T) {
	before := runtime.NumGoroutine()
	wheels := make([]*TimeWheel, 5000)
	shards := make(map[*wheelShard]struct{})
	sender := &controlledSender{}
	for i := range wheels {
		wheel := newTimeWheel(32, tree.PID{ID: uint64(i + 1)}, sender)
		wheel.Register(testTimerType, func(interface{}) {})
		wheel.After(testTimerType, 5*time.Minute, nil)
		shards[wheel.shard] = struct{}{}
		wheels[i] = wheel
	}
	if len(shards) != twShards {
		t.Fatalf("shards=%d", len(shards))
	}
	if growth := runtime.NumGoroutine() - before; growth > twShards+4 {
		t.Errorf("goroutine growth=%d", growth)
	}
	for _, wheel := range wheels {
		wheel.Stop()
	}
	for shard := range shards {
		shard.mu.Lock()
		if shard.queued != 0 {
			t.Errorf("shard queued=%d", shard.queued)
		}
		shard.mu.Unlock()
	}
}

type mailboxActor struct {
	name    string
	entered chan struct{}
	release chan struct{}
}

func (a *mailboxActor) Name() string      { return a.name }
func (*mailboxActor) MailboxSize() int    { return 1 }
func (*mailboxActor) OnInit(tree.Context) {}
func (*mailboxActor) OnStop(tree.Context) {}
func (a *mailboxActor) HandleMessage(_ tree.Context, message interface{}) {
	if message == "block" {
		close(a.entered)
		<-a.release
	}
}

// TestSharedWheelRealMailbox 验证真实 Actor 邮箱背压不会阻塞同分片其他 Actor。
func TestSharedWheelRealMailbox(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	slowPID := tree.SpawnOne(&mailboxActor{name: "slow", entered: entered, release: release})
	fastPID := tree.SpawnOne(&mailboxActor{name: "fast"})
	slow := NewTimeWheel(32, slowPID)
	fast := NewTimeWheel(32, fastPID)
	// 将两个真实 Actor 的测试所有者放到同一分片，尚未注册任务。
	fast.shard = slow.shard
	defer func() { unblock(); slow.Stop(); fast.Stop(); tree.Stop(slowPID); tree.Stop(fastPID) }()
	slowCalled, fastCalled := make(chan struct{}, 1), make(chan struct{}, 1)
	slow.Register(testTimerType, func(interface{}) { slowCalled <- struct{}{} })
	fast.Register(testTimerType, func(interface{}) { fastCalled <- struct{}{} })
	tree.Send(slowPID, "block")
	<-entered
	tree.Send(slowPID, "fill")
	slow.After(testTimerType, time.Millisecond, nil)
	fast.After(testTimerType, time.Millisecond, nil)
	select {
	case <-fastCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("full mailbox blocked shared shard")
	}
	select {
	case <-slowCalled:
		t.Fatal("callback executed outside blocked actor")
	default:
	}
	unblock()
	select {
	case <-slowCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("callback lost after mailbox recovered")
	}
}

// TestSharedWheelBurstFairness 验证单个 Actor 大量任务到期时，另一个 Actor 不被长期饿死。
func TestSharedWheelBurstFairness(t *testing.T) {
	shard, sender := manualShard(), &controlledSender{}
	first := manualWheel(shard, 1, sender, func(interface{}) {})
	first.batch = twDispatchLimit
	second := manualWheel(shard, 17, sender, func(interface{}) {})
	lastTick := int64(0)
	for i := 0; i < twDispatchLimit+100; i++ {
		task := first.After(testTimerType, time.Millisecond, nil)
		lastTick = max(lastTick, task.expires)
	}
	task := second.After(testTimerType, time.Millisecond, nil)
	lastTick = max(lastTick, task.expires)
	advanceTo(shard, lastTick)
	callbacks := sender.take()
	count := len(callbacks)
	for _, cb := range callbacks {
		cb.cb(nil, nil, nil)
	}
	advanceTo(shard, lastTick+1)
	callbacks = sender.take()
	if len(callbacks) == 0 || callbacks[0].pid.ID != 17 {
		t.Fatal("busy actor starved another actor")
	}
	count += len(callbacks)
	for _, cb := range callbacks {
		cb.cb(nil, nil, nil)
	}
	if count != twDispatchLimit+101 || shard.queued != 0 {
		t.Fatalf("burst lost tasks: delivered=%d queued=%d", count, shard.queued)
	}
}

// TestSharedWheelCronRepeats 验证日历任务执行后再次调度，停止后立即移除。
func TestSharedWheelCronRepeats(t *testing.T) {
	shard, sender := manualShard(), &controlledSender{}
	calls := 0
	wheel := manualWheel(shard, 1, sender, func(interface{}) { calls++ })
	expr, err := NewCronExpr("* * * * * *")
	if err != nil {
		t.Fatal(err)
	}
	cron := wheel.Cron(testTimerType, expr, nil)
	advanceTo(shard, cron.timer.expires)
	callbacks := sender.take()
	if len(callbacks) != 1 {
		t.Fatal("cron did not dispatch")
	}
	callbacks[0].cb(nil, nil, nil)
	if calls != 1 || cron.timer.element == nil || shard.queued != 1 {
		t.Fatal("cron did not reschedule")
	}
	cron.Stop()
	if shard.queued != 0 || len(wheel.timers) != 0 {
		t.Fatal("cron cancellation retained task")
	}
}

// TestSharedWheelCancelType 验证按类型取消覆盖排队和已投递任务，且不影响其他类型、其他 Actor 和后续重建。
func TestSharedWheelCancelType(t *testing.T) {
	shard, sender := manualShard(), &controlledSender{}
	calls := make(map[string]int)
	wheel := manualWheel(shard, 1, sender, func(data interface{}) { calls[data.(string)]++ })
	const otherType TimerType = 2 // 用于验证取消隔离的任务类型。
	wheel.Register(otherType, func(data interface{}) { calls[data.(string)]++ })
	other := manualWheel(shard, 17, sender, func(data interface{}) { calls[data.(string)]++ })
	delivered := wheel.After(testTimerType, time.Millisecond, "canceled")
	queued := wheel.After(testTimerType, time.Hour, "future")
	kept := wheel.After(otherType, time.Millisecond, "other-type")
	neighbor := other.After(testTimerType, time.Millisecond, "other-actor")
	expr, err := NewCronExpr("* * * * * *")
	if err != nil {
		t.Fatal(err)
	}
	cron := wheel.Cron(testTimerType, expr, "cron")
	advanceTo(shard, max(delivered.expires, kept.expires, neighbor.expires))
	callbacks := sender.take()
	wheel.Cancel(testTimerType)
	wheel.Cancel(testTimerType)
	for _, callback := range callbacks {
		callback.cb(nil, nil, nil)
	}
	if calls["canceled"] != 0 || calls["other-type"] != 1 || calls["other-actor"] != 1 {
		t.Fatalf("unexpected calls: %v", calls)
	}
	if queued.owner != nil || delivered.owner != nil || cron.timer.owner != nil || shard.queued != 0 {
		t.Fatal("Cancel retained tasks")
	}
	replacement := wheel.After(testTimerType, time.Millisecond, "replacement")
	if replacement == nil {
		t.Fatal("Cancel removed type registration")
	}
	advanceTo(shard, replacement.expires)
	for _, callback := range sender.take() {
		callback.cb(nil, nil, nil)
	}
	if calls["replacement"] != 1 {
		t.Fatal("replacement timer did not execute")
	}
	wheel.Stop()
	wheel.Cancel(testTimerType)
}
