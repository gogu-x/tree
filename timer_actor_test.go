package actor

import (
	"sync/atomic"
	"testing"
	"time"
)

type serialActor struct {
	running int32
	events  []string
	done    chan struct{}
	once    int32
}

func (a *serialActor) enter(label string) {
	if !atomic.CompareAndSwapInt32(&a.running, 0, 1) {
		panic("concurrent execution in actor: " + label)
	}
	a.events = append(a.events, label)
}
func (a *serialActor) leave() { atomic.StoreInt32(&a.running, 0) }
func (a *serialActor) tryDone() {
	if len(a.events) >= 3 && atomic.CompareAndSwapInt32(&a.once, 0, 1) {
		close(a.done)
	}
}

func (a *serialActor) OnInit(ctx ActorContext) {
	// 定时器挂自身，通过 ctx 注册
	ctx.AfterFunc(20*time.Millisecond, func(ctx ActorContext) {
		a.enter("timer")
		time.Sleep(5 * time.Millisecond)
		a.leave()
		a.tryDone()
	})
}
func (a *serialActor) OnStop(ctx ActorContext) {}
func (a *serialActor) HandleMessage(ctx ActorContext, msg interface{}) {
	a.enter("msg:" + msg.(string))
	time.Sleep(5 * time.Millisecond)
	a.leave()
	a.tryDone()
}

func TestTimerSerialWithMessages(t *testing.T) {
	sys := NewActorSystem()
	defer sys.Shutdown()

	act := &serialActor{done: make(chan struct{})}
	pid := sys.Spawn("serial", act)

	sys.Send(pid, "A")
	sys.Send(pid, "B")

	select {
	case <-act.done:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out, events so far: %v", act.events)
	}

	t.Logf("events: %v", act.events)
}
