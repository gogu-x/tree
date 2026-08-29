package timer

import (
	"testing"
	"time"

	"github.com/gogu-x/tree"
)

const testTimerType = TimerType(1)

type callbackActor struct {
	timeWheel *TimeWheel
	called    chan interface{}
}

func (a *callbackActor) Name() string { return "timer-callback-test" }

func (a *callbackActor) OnInit(ctx tree.Context) {
	a.timeWheel = NewTimeWheel(1, ctx.Self(), ctx.System())
	a.timeWheel.Register(testTimerType, func(data interface{}) {
		a.called <- data
	})
	a.timeWheel.After(testTimerType, time.Millisecond, "tick")
}

func (a *callbackActor) HandleMessage(tree.Context, interface{}) {}

func (a *callbackActor) OnStop(tree.Context) {
	if a.timeWheel != nil {
		a.timeWheel.Stop()
	}
}

func TestTimeWheelDispatchesRegisteredType(t *testing.T) {
	system := tree.NewTree()
	actor := &callbackActor{called: make(chan interface{}, 1)}
	system.SpawnOne(actor)
	defer system.Shutdown()

	select {
	case got := <-actor.called:
		if got != "tick" {
			t.Fatalf("callback data = %#v, want %q", got, "tick")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("registered timer callback was not executed")
	}
}

func TestTimeWheelRejectsTimerAfterStop(t *testing.T) {
	system := tree.NewTree()
	tw := NewTimeWheel(1, tree.PID{ID: 1, Name: "test"}, system)
	tw.Register(testTimerType, func(interface{}) {})
	tw.Stop()

	if timer := tw.After(testTimerType, time.Millisecond, nil); timer != nil {
		t.Fatal("After returned a timer after TimeWheel.Stop")
	}
}

func TestTimeWheelRejectsUnregisteredType(t *testing.T) {
	system := tree.NewTree()
	tw := NewTimeWheel(1, tree.PID{ID: 1, Name: "test"}, system)
	defer tw.Stop()
	defer func() {
		if recover() == nil {
			t.Fatal("After did not panic for an unregistered timer type")
		}
	}()

	tw.After(testTimerType, time.Millisecond, nil)
}
