package tree

import (
	"context"
	"os"
	"os/exec"
	"regexp"
	"testing"
	"time"
)

// isolatedGlobalTest 在独立进程中验证生命周期，不提供重置单例或创建实例的接口。
func isolatedGlobalTest(t *testing.T) bool {
	t.Helper()
	if os.Getenv("TREE_LIFECYCLE_TEST") == t.Name() {
		return true
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^"+regexp.QuoteMeta(t.Name())+"$", "-test.count=1")
	cmd.Env = append(os.Environ(), "TREE_LIFECYCLE_TEST="+t.Name())
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("isolated test: %v\n%s", err, output)
	}
	return false
}

// TestGlobalShutdownOrder 验证所有动态 Actor 退出后才逆序关闭固定服务。
func TestGlobalShutdownOrder(t *testing.T) {
	if !isolatedGlobalTest(t) {
		return
	}
	var order []string
	var database PID
	result := make(chan error, 1)
	SpawnOne(&shutdownActor{name: "player", stop: func(ctx Context) {
		_, err := ctx.Request(database, "save").AwaitTimeout(time.Second)
		result <- err
		order = append(order, "player")
	}})
	fixed := Spawn(
		&shutdownActor{name: "database", handle: func(ctx Context, _ interface{}) { ctx.Response(nil, nil) }, stop: func(Context) { order = append(order, "database") }},
		&shutdownActor{name: "gate", stop: func(Context) { order = append(order, "gate") }},
	)
	database = fixed[0]
	Shutdown()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if len(order) != 3 || order[0] != "player" || order[1] != "gate" || order[2] != "database" {
		t.Fatalf("shutdown order: %v", order)
	}
	if SpawnCount() != 0 {
		t.Fatal("actors remain after shutdown")
	}
	for _, pid := range fixed {
		if Send(pid, "late") {
			t.Fatal("stopped fixed actor accepts messages")
		}
	}
}

// TestGlobalResponseRouting 验证移除上下文及信封中的 Tree 引用后响应仍回到发起 Actor。
func TestGlobalResponseRouting(t *testing.T) {
	if !isolatedGlobalTest(t) {
		return
	}
	responses := make(chan interface{}, 2)
	callbacks := make(chan PID, 1)
	receiver := SpawnOne(&shutdownActor{name: "receiver", handle: func(ctx Context, msg interface{}) { ctx.Response(msg, nil) }})
	sender := SpawnOne(&shutdownActor{name: "sender", handle: func(ctx Context, msg interface{}) {
		if msg == "begin" {
			ctx.RequestCallback(receiver, "callback", func(cbCtx Context, value interface{}, err error) {
				if err != nil {
					responses <- err
				} else {
					responses <- value
				}
				callbacks <- cbCtx.Self()
			})
			ctx.RequestAsMessage(receiver, "response-message")
		} else {
			responses <- msg
		}
	}})
	Send(sender, "begin")
	seen := make(map[interface{}]bool)
	for i := 0; i < 2; i++ {
		select {
		case value := <-responses:
			seen[value] = true
		case <-time.After(time.Second):
			t.Fatal("response did not reach sender")
		}
	}
	if !seen["callback"] || !seen["response-message"] {
		t.Fatalf("responses: %v", seen)
	}
	if pid := <-callbacks; pid != sender {
		t.Fatal("callback ran in wrong actor")
	}
	Shutdown()
}

// TestFixedFirstLookup 验证同名时固定优先，固定退出后按名称回退动态，PID 路由仍严格匹配身份。
func TestFixedFirstLookup(t *testing.T) {
	if !isolatedGlobalTest(t) {
		return
	}
	defer Shutdown()
	fixed := Spawn(&shutdownActor{name: "shared", handle: func(ctx Context, _ interface{}) { ctx.Response("fixed", nil) }})[0]
	dynamic := SpawnOne(&shutdownActor{name: "shared", handle: func(ctx Context, _ interface{}) { ctx.Response("dynamic", nil) }})
	if pid, ok := Lookup("shared"); !ok || pid != fixed {
		t.Fatal("fixed actor was not preferred")
	}
	for _, test := range []struct {
		pid  PID
		want string
	}{{fixed, "fixed"}, {dynamic, "dynamic"}} {
		value, err := Request(test.pid, "probe").AwaitTimeout(time.Second)
		if err != nil || value != test.want {
			t.Fatalf("PID route: value=%v err=%v", value, err)
		}
	}
	if Send(PID{ID: fixed.ID, Name: "wrong"}, "probe") {
		t.Fatal("mismatched PID was accepted")
	}
	Stop(fixed)
	if pid, ok := Lookup("shared"); !ok || pid != dynamic {
		t.Fatal("lookup did not fall back to dynamic actor")
	}
	if Send(fixed, "late") {
		t.Fatal("stopped fixed PID was routed to same-name dynamic actor")
	}
	Stop(dynamic)
	if _, ok := Lookup("shared"); ok {
		t.Fatal("stopped actors remain addressable")
	}
}
