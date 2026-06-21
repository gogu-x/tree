package actor

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// --- Test: Spawn and Send ---

type echoActor struct {
	t     *testing.T
	wg    sync.WaitGroup
	msgCh chan interface{}
}

func (a *echoActor) OnInit(ctx ActorContext) {}
func (a *echoActor) OnStop(ctx ActorContext) {}

func (a *echoActor) HandleMessage(ctx ActorContext, msg interface{}) {
	switch m := msg.(type) {
	case string:
		a.msgCh <- m
	}
}

func TestSpawnAndSend(t *testing.T) {
	sys := NewActorSystem()
	defer sys.Shutdown()

	msgCh := make(chan interface{}, 1)
	pid := sys.Spawn("echo", &echoActor{
		t:     t,
		msgCh: msgCh,
	})

	if !sys.Send(pid, "hello") {
		t.Fatal("Send returned false")
	}

	select {
	case msg := <-msgCh:
		if msg != "hello" {
			t.Fatalf("expected 'hello', got %v", msg)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for message")
	}
}

// --- Test: Send to unknown PID returns false ---

func TestSendUnknownPID(t *testing.T) {
	sys := NewActorSystem()
	defer sys.Shutdown()

	fakePID := PID{ID: 99999, Name: "ghost"}
	if sys.Send(fakePID, "hello") {
		t.Fatal("expected Send to unknown PID to return false")
	}
}

// --- Test: TrySend ---

func TestTrySend(t *testing.T) {
	sys := NewActorSystem()
	defer sys.Shutdown()

	msgCh := make(chan interface{}, 1)
	pid := sys.Spawn("echo", &echoActor{t: t, msgCh: msgCh}, WithMailboxSize(1))

	// First TrySend should succeed
	if !sys.TrySend(pid, "msg1") {
		t.Fatal("first TrySend should succeed")
	}

	// TrySend to unknown PID should return false
	fakePID := PID{ID: 99999, Name: "ghost"}
	if sys.TrySend(fakePID, "msg") {
		t.Fatal("TrySend to unknown PID should return false")
	}
}

// --- Test: Request/Response ---

type pingActor struct {
	t *testing.T
}

func (a *pingActor) Name() {
	//TODO implement me
	panic("implement me")
}

func (a *pingActor) OnInit(ctx ActorContext) {}
func (a *pingActor) OnStop(ctx ActorContext) {}

func (a *pingActor) HandleMessage(ctx ActorContext, msg interface{}) {
	switch msg.(type) {
	case string:
		ctx.Response("pong", nil)
	}
}

func TestRequestResponse(t *testing.T) {
	sys := NewActorSystem()
	defer sys.Shutdown()

	pid := sys.Spawn("ping", &pingActor{t: t})

	result, err := sys.Request(pid, "ping").AwaitTimeout(time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "pong" {
		t.Fatalf("expected 'pong', got %v", result)
	}
}

// --- Test: Actor Stop ---

type stopActor struct {
	stopped bool
	mu      sync.Mutex
}

func (a *stopActor) Name() {
	//TODO implement me
	panic("implement me")
}

func (a *stopActor) OnInit(ctx ActorContext) {}
func (a *stopActor) OnStop(ctx ActorContext) {
	a.mu.Lock()
	a.stopped = true
	a.mu.Unlock()
}
func (a *stopActor) HandleMessage(ctx ActorContext, msg interface{}) {
	if _, ok := msg.(string); ok {
		ctx.Stop()
	}
}

func TestActorStop(t *testing.T) {
	sys := NewActorSystem()
	a := &stopActor{}
	pid := sys.Spawn("stop", a)

	sys.Send(pid, "stop-now")
	time.Sleep(100 * time.Millisecond)

	a.mu.Lock()
	stopped := a.stopped
	a.mu.Unlock()

	if !stopped {
		t.Fatal("expected actor to be stopped")
	}
}

// --- Test: System Shutdown ---

func TestSystemShutdown(t *testing.T) {
	sys := NewActorSystem()

	pid := sys.Spawn("worker", &echoActor{
		t:     t,
		msgCh: make(chan interface{}, 1),
	})

	sys.Send(pid, "before-shutdown")

	sys.Shutdown()

	if n := sys.SpawnCount(); n != 0 {
		t.Fatalf("expected 0 actors after shutdown, got %d", n)
	}
}

// --- Test: Mailbox Priority ---

func TestMailboxPriority(t *testing.T) {
	mb := NewMailbox(2)

	mb.PushUser("user-msg")
	mb.PushSystem(systemStop)
	if got := mb.Receive(); got != systemStop {
		t.Fatalf("expected system message first, got %v", got)
	}
	if got := mb.Receive(); got != "user-msg" {
		t.Fatalf("expected user message second, got %v", got)
	}
}

// --- Test: Pipe (async callback, zero extra goroutine) ---

type requesterActor struct {
	target   PID
	resultCh chan PipeResult
}

func (a *requesterActor) Name() {
	//TODO implement me
	panic("implement me")
}

func (a *requesterActor) OnInit(ctx ActorContext) {}
func (a *requesterActor) OnStop(ctx ActorContext) {}

func (a *requesterActor) HandleMessage(ctx ActorContext, msg interface{}) {
	switch m := msg.(type) {
	case string:
		ctx.Request(a.target, m).Pipe(ctx)
	case PipeResult:
		a.resultCh <- m
	}
}

func TestPipe(t *testing.T) {
	sys := NewActorSystem()
	defer sys.Shutdown()

	target := sys.Spawn("target", &pingActor{t: t})

	resultCh := make(chan PipeResult, 1)
	requester := sys.Spawn("requester", &requesterActor{
		target:   target,
		resultCh: resultCh,
	})

	sys.Send(requester, "ping")

	select {
	case r := <-resultCh:
		if r.Err != nil {
			t.Fatalf("unexpected error: %v", r.Err)
		}
		if r.Value != "pong" {
			t.Fatalf("expected 'pong', got %v", r.Value)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for pipe result")
	}
}

// --- Test: PipeWithTag (distinguish multiple concurrent requests) ---

type multiRequestActor struct {
	targetA  PID
	targetB  PID
	resultCh chan PipeResult
}

func (a *multiRequestActor) Name() {
	//TODO implement me
	panic("implement me")
}

func (a *multiRequestActor) OnInit(ctx ActorContext) {}
func (a *multiRequestActor) OnStop(ctx ActorContext) {}

func (a *multiRequestActor) HandleMessage(ctx ActorContext, msg interface{}) {
	switch m := msg.(type) {
	case string:
		if m == "go" {
			ctx.Request(a.targetA, "ping").PipeWithTag(ctx, "from-A")
			ctx.Request(a.targetB, "ping").PipeWithTag(ctx, "from-B")
		}
	case PipeResult:
		a.resultCh <- m
	}
}

func TestPipeWithTag(t *testing.T) {
	sys := NewActorSystem()
	defer sys.Shutdown()

	targetA := sys.Spawn("targetA", &pingActor{t: t})
	targetB := sys.Spawn("targetB", &pingActor{t: t})

	resultCh := make(chan PipeResult, 2)
	requester := sys.Spawn("requester", &multiRequestActor{
		targetA:  targetA,
		targetB:  targetB,
		resultCh: resultCh,
	})

	sys.Send(requester, "go")

	tags := make(map[interface{}]bool)
	for i := 0; i < 2; i++ {
		select {
		case r := <-resultCh:
			if r.Err != nil {
				t.Fatalf("unexpected error: %v", r.Err)
			}
			if r.Value != "pong" {
				t.Fatalf("expected 'pong', got %v", r.Value)
			}
			tags[r.Tag] = true
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for pipe result")
		}
	}

	if !tags["from-A"] || !tags["from-B"] {
		t.Fatalf("expected both tags, got %v", tags)
	}
}

// --- Test: Router-based message dispatch ---

type LoginRequest struct{ User string }
type MoveRequest struct{ X, Y int }

type gameActor struct {
	router   Router
	resultCh chan string
}

func (a *gameActor) Name() {
	//TODO implement me
	panic("implement me")
}

func (a *gameActor) OnInit(ctx ActorContext) {
	a.router.Register(&LoginRequest{}, a.handleLogin)
	a.router.Register(&MoveRequest{}, a.handleMove)
	a.router.SetFallback(func(ctx ActorContext, msg interface{}) {
		a.resultCh <- "fallback"
	})
}

func (a *gameActor) OnStop(ctx ActorContext) {}

func (a *gameActor) HandleMessage(ctx ActorContext, msg interface{}) {
	a.router.Route(ctx, msg)
}

func (a *gameActor) handleLogin(ctx ActorContext, msg interface{}) {
	req := msg.(*LoginRequest)
	a.resultCh <- "login:" + req.User
}

func (a *gameActor) handleMove(ctx ActorContext, msg interface{}) {
	req := msg.(*MoveRequest)
	a.resultCh <- fmt.Sprintf("move:%d,%d", req.X, req.Y)
}

func TestRouter(t *testing.T) {
	sys := NewActorSystem()
	defer sys.Shutdown()

	resultCh := make(chan string, 3)
	pid := sys.Spawn("game", &gameActor{resultCh: resultCh})

	sys.Send(pid, &LoginRequest{User: "alice"})
	sys.Send(pid, &MoveRequest{X: 10, Y: 20})
	sys.Send(pid, 12345) // unmatched ?fallback

	expected := []string{"login:alice", "move:10,20", "fallback"}
	for _, want := range expected {
		select {
		case got := <-resultCh:
			if got != want {
				t.Fatalf("expected %q, got %q", want, got)
			}
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for %q", want)
		}
	}
}

// --- Test: Callback (closure-based async) ---

type callbackActor struct {
	target   PID
	resultCh chan string
}

func (a *callbackActor) Name() {
	//TODO implement me
	panic("implement me")
}

func (a *callbackActor) OnInit(ctx ActorContext) {}
func (a *callbackActor) OnStop(ctx ActorContext) {}

func (a *callbackActor) HandleMessage(ctx ActorContext, msg interface{}) {
	switch m := msg.(type) {
	case string:
		userID := m // captured by closure
		ctx.Request(a.target, m).Callback(ctx, func(ret interface{}, err error) {
			// This closure runs in callbackActor's goroutine
			a.resultCh <- fmt.Sprintf("user=%s result=%v", userID, ret)
		})
	}
}

func TestCallback(t *testing.T) {
	sys := NewActorSystem()
	defer sys.Shutdown()

	target := sys.Spawn("ping", &pingActor{t: t})

	resultCh := make(chan string, 1)
	requester := sys.Spawn("requester", &callbackActor{
		target:   target,
		resultCh: resultCh,
	})

	sys.Send(requester, "alice")

	select {
	case r := <-resultCh:
		if r != "user=alice result=pong" {
			t.Fatalf("unexpected: %s", r)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

// --- Test: Multiple Callbacks (like chanrpc AsynCall) ---

type multiCallbackActor struct {
	target   PID
	resultCh chan string
}

func (a *multiCallbackActor) Name() {
	//TODO implement me
	panic("implement me")
}

func (a *multiCallbackActor) OnInit(ctx ActorContext) {}
func (a *multiCallbackActor) OnStop(ctx ActorContext) {}

func (a *multiCallbackActor) HandleMessage(ctx ActorContext, msg interface{}) {
	switch msg.(type) {
	case string:
		// Two concurrent requests with different closures
		ctx.Request(a.target, "ping").Callback(ctx, func(ret interface{}, err error) {
			a.resultCh <- "first:" + ret.(string)
		})
		ctx.Request(a.target, "ping").Callback(ctx, func(ret interface{}, err error) {
			a.resultCh <- "second:" + ret.(string)
		})
	}
}

func TestMultipleCallbacks(t *testing.T) {
	sys := NewActorSystem()
	defer sys.Shutdown()

	target := sys.Spawn("ping", &pingActor{t: t})

	resultCh := make(chan string, 2)
	requester := sys.Spawn("requester", &multiCallbackActor{
		target:   target,
		resultCh: resultCh,
	})

	sys.Send(requester, "go")

	results := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case r := <-resultCh:
			results[r] = true
		case <-time.After(time.Second):
			t.Fatal("timeout")
		}
	}

	if !results["first:pong"] || !results["second:pong"] {
		t.Fatalf("unexpected results: %v", results)
	}
}
