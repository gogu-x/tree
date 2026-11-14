package tree

import (
	"runtime"
	"sync"
	"testing"
	"time"
)

// TestFixedPIDPublication 验证读者在注册前启动，通过名称查找拿到 PID 后即可安全访问固定数组。
func TestFixedPIDPublication(t *testing.T) {
	if !isolatedGlobalTest(t) {
		return
	}
	defer Shutdown()
	var readers sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 16; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			<-start
			deadline := time.Now().Add(time.Second)
			for {
				if pid, ok := Lookup("published-fixed"); ok {
					value, err := Request(pid, "ready").AwaitTimeout(time.Second)
					if err != nil || value != "ready" {
						t.Errorf("published PID failed: value=%v err=%v", value, err)
					}
					return
				}
				if time.Now().After(deadline) {
					t.Error("fixed PID was not published")
					return
				}
				runtime.Gosched()
			}
		}()
	}
	close(start)
	Spawn(&shutdownActor{name: "published-fixed", handle: func(ctx Context, msg interface{}) { ctx.Response(msg, nil) }})
	readers.Wait()
}

// TestClosingRejectsRegistrationAndWaits 验证关闭进行中禁止两类注册，所有 Shutdown 等到固定 Actor 退出。
func TestClosingRejectsRegistrationAndWaits(t *testing.T) {
	if !isolatedGlobalTest(t) {
		return
	}
	entered, release := make(chan struct{}), make(chan struct{})
	Spawn(&shutdownActor{name: "fixed", stop: func(Context) { close(entered); <-release }})
	first, second := make(chan struct{}), make(chan struct{})
	go func() { Shutdown(); close(first) }()
	<-entered
	for _, register := range []func(){
		func() { Spawn(&shutdownActor{name: "late-fixed"}) },
		func() { SpawnOne(&shutdownActor{name: "late-dynamic"}) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Error("registration accepted during shutdown")
				}
			}()
			register()
		}()
	}
	go func() { Shutdown(); close(second) }()
	select {
	case <-second:
		t.Error("concurrent Shutdown returned before cleanup")
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	for _, done := range []chan struct{}{first, second} {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("shutdown did not complete")
		}
	}
	select {
	case <-globalTree.stopped:
	default:
		t.Fatal("shutdown completion was not published")
	}
	if SpawnCount() != 0 {
		t.Fatal("shutdown left live actors")
	}
}

// TestDynamicRegistrationShutdownRace 验证并发动态注册不会遗漏在关服快照外。
func TestDynamicRegistrationShutdownRace(t *testing.T) {
	if !isolatedGlobalTest(t) {
		return
	}
	Spawn(&shutdownActor{name: "fixed"})
	start := make(chan struct{})
	var workers sync.WaitGroup
	for i := 0; i < 64; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer func() { _ = recover() }()
			<-start
			SpawnOne(&shutdownActor{name: "dynamic"})
		}()
	}
	workers.Add(1)
	go func() { defer workers.Done(); <-start; Shutdown() }()
	close(start)
	workers.Wait()
	if SpawnCount() != 0 {
		t.Fatal("shutdown missed a concurrent registration")
	}
}
