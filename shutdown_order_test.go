package tree

import (
	"reflect"
	"sync"
	"testing"
	"time"
)

type shutdownActor struct {
	name   string
	stop   func(Context)
	handle func(Context, interface{})
}

func (a *shutdownActor) Name() string { return a.name }
func (*shutdownActor) OnInit(Context) {}
func (a *shutdownActor) OnStop(ctx Context) {
	if a.stop != nil {
		a.stop(ctx)
	}
}
func (a *shutdownActor) HandleMessage(ctx Context, message interface{}) {
	if a.handle != nil {
		a.handle(ctx, message)
	}
}

// TestShutdownReverseOrder 验证跨批次逆序关闭、等待 OnStop 和关闭期间底层依赖可用。
func TestShutdownReverseOrder(t *testing.T) {
	system := NewTree()
	var order []string
	entered, release := make(chan struct{}), make(chan struct{})
	errors := make(chan error, 1)
	database := system.SpawnOne(&shutdownActor{name: "database", handle: func(ctx Context, _ interface{}) { ctx.Response(nil, nil) }, stop: func(Context) { order = append(order, "database") }})
	system.SpawnOne(&shutdownActor{name: "business", stop: func(ctx Context) {
		close(entered)
		<-release
		_, err := ctx.Request(database, "save").AwaitTimeout(time.Second)
		errors <- err
		order = append(order, "business")
	}})
	done := make(chan struct{})
	go func() { system.Shutdown(); close(done) }()
	<-entered
	if _, ok := system.Lookup("database"); !ok {
		t.Error("dependency stopped before business OnStop completed")
	}
	close(release)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown blocked")
	}
	if err := <-errors; err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(order, []string{"business", "database"}) {
		t.Fatalf("stop order: %v", order)
	}
}

// TestShutdownProcessesQueuedMessages 验证最后一个业务 Actor 投递给底层的消息不会被停止信号抢占。
func TestShutdownProcessesQueuedMessages(t *testing.T) {
	system := NewTree()
	entered, release := make(chan struct{}), make(chan struct{})
	processed := 0
	database := system.SpawnOne(&shutdownActor{name: "database", handle: func(_ Context, message interface{}) {
		if message == "block" {
			close(entered)
			<-release
			return
		}
		processed++
	}})
	system.Send(database, "block")
	<-entered
	sent := make(chan struct{})
	system.SpawnOne(&shutdownActor{name: "business", stop: func(ctx Context) {
		ctx.Send(database, "save")
		close(sent)
	}})
	done := make(chan struct{})
	go func() { system.Shutdown(); close(done) }()
	<-sent
	close(release)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown blocked")
	}
	if processed != 1 {
		t.Fatalf("processed writes = %d", processed)
	}
}

// TestShutdownConcurrentAndStoppedActor 验证已自行退出的 Actor 不阻塞关闭，并发关闭安全且只执行一次。
func TestShutdownConcurrentAndStoppedActor(t *testing.T) {
	system := NewTree()
	pid := system.SpawnOne(&shutdownActor{name: "exited", handle: func(ctx Context, _ interface{}) { ctx.Stop() }})
	proc, _ := system.loadProc(pid)
	system.Send(pid, "stop")
	<-proc.done
	stops := 0
	system.SpawnOne(&shutdownActor{name: "live", stop: func(Context) { stops++ }})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); system.Shutdown() }()
	}
	wg.Wait()
	if stops != 1 {
		t.Fatalf("OnStop count = %d", stops)
	}
	defer func() {
		if recover() == nil {
			t.Error("Spawn after Shutdown must be rejected")
		}
	}()
	system.SpawnOne(&shutdownActor{name: "late"})
}
