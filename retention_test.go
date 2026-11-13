package tree

import (
	"runtime"
	"testing"
	"time"
)

type retainedTestActor struct{ payload [1024]byte }

func (*retainedTestActor) Name() string                       { return "retention-test" }
func (*retainedTestActor) OnInit(Context)                     {}
func (*retainedTestActor) OnStop(Context)                     {}
func (*retainedTestActor) HandleMessage(Context, interface{}) {}

// TestStoppedActorCollected 验证框架存活时，已退出 Actor 不再被创建记录持有。
func TestStoppedActorCollected(t *testing.T) {
	system := NewTree()
	collected := make(chan struct{}, 1)
	func() {
		actor := &retainedTestActor{}
		runtime.SetFinalizer(actor, func(*retainedTestActor) { collected <- struct{}{} })
		system.SpawnOne(actor)
	}()
	system.Shutdown()
	deadline := time.After(3 * time.Second)
	for {
		runtime.GC()
		select {
		case <-collected:
			runtime.KeepAlive(system)
			return
		case <-deadline:
			t.Fatal("已退出 Actor 仍被引用")
		case <-time.After(10 * time.Millisecond):
		}
	}
}
