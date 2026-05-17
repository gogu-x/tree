package actor

import (
	"fmt"
	"testing"
	"time"
)

// ==========================================================================
// Test: Lookup-based cross-module (no PID injection)
//
// Gate and Game don't hold each other's PID at construction time.
// They discover each other by name at runtime via ctx.Lookup.
// ==========================================================================

type CrossLoginReq struct{ UserID string }
type CrossLoginResp struct{ OK bool; Message string }

// --- Gate Actor (discovers game by name) ---

type crossGateActor struct {
	resultCh chan string
}

func (a *crossGateActor) OnInit(ctx ActorContext) {}
func (a *crossGateActor) OnStop(ctx ActorContext) {}

func (a *crossGateActor) HandleMessage(ctx ActorContext, msg interface{}) {
	switch m := msg.(type) {
	case string:
		gamePID, ok := ctx.Lookup("game")
		if !ok {
			a.resultCh <- "error:game not found"
			return
		}
		ctx.Request(gamePID, &CrossLoginReq{UserID: m}).PipeWithTag(ctx, "login:"+m)

	case PipeResult:
		if m.Err != nil {
			a.resultCh <- fmt.Sprintf("error:%v", m.Err)
			return
		}
		resp := m.Value.(*CrossLoginResp)
		a.resultCh <- fmt.Sprintf("tag=%v ok=%v msg=%s", m.Tag, resp.OK, resp.Message)
	}
}

// --- Game Actor (discovers db by name) ---

type crossGameActor struct {
	router  Router
	pending map[string]*Future
}

func (a *crossGameActor) OnInit(ctx ActorContext) {
	a.pending = make(map[string]*Future)
	a.router.Register(&CrossLoginReq{}, a.handleLogin)
	a.router.Register(PipeResult{}, a.handleDBResult)
}
func (a *crossGameActor) OnStop(ctx ActorContext) {}
func (a *crossGameActor) HandleMessage(ctx ActorContext, msg interface{}) {
	a.router.Route(ctx, msg)
}

func (a *crossGameActor) handleLogin(ctx ActorContext, msg interface{}) {
	req := msg.(*CrossLoginReq)
	a.pending[req.UserID] = ctx.Future()

	dbPID, ok := ctx.Lookup("db")
	if !ok {
		a.pending[req.UserID].Respond(nil, ErrActorNotFound)
		delete(a.pending, req.UserID)
		return
	}
	ctx.Request(dbPID, &CrossDBQueryReq{UserID: req.UserID}).PipeWithTag(ctx, req.UserID)
}

func (a *crossGameActor) handleDBResult(ctx ActorContext, msg interface{}) {
	pr := msg.(PipeResult)
	userID := pr.Tag.(string)
	dbResp := pr.Value.(*CrossDBQueryResp)

	if f, ok := a.pending[userID]; ok {
		delete(a.pending, userID)
		f.Respond(&CrossLoginResp{
			OK:      true,
			Message: fmt.Sprintf("welcome %s lv%d", userID, dbResp.Level),
		}, nil)
	}
}

// --- DB Actor ---

type CrossDBQueryReq struct{ UserID string }
type CrossDBQueryResp struct{ Level int }

type crossDBActor struct {
	router Router
}

func (a *crossDBActor) OnInit(ctx ActorContext) {
	a.router.Register(&CrossDBQueryReq{}, a.handleQuery)
}
func (a *crossDBActor) OnStop(ctx ActorContext) {}
func (a *crossDBActor) HandleMessage(ctx ActorContext, msg interface{}) {
	a.router.Route(ctx, msg)
}
func (a *crossDBActor) handleQuery(ctx ActorContext, msg interface{}) {
	req := msg.(*CrossDBQueryReq)
	level := 10
	if req.UserID == "bob" {
		level = 20
	}
	ctx.Response(&CrossDBQueryResp{Level: level}, nil)
}

// --- Test: three modules, zero PID injection ---

func TestLookupCrossModule(t *testing.T) {
	sys := NewActorSystem()

	// Spawn order doesn't matter for the actors themselves ?	// they discover each other by name at message-processing time.
	sys.Spawn("db", &crossDBActor{})
	sys.Spawn("game", &crossGameActor{})

	resultCh := make(chan string, 2)
	sys.Spawn("gate", &crossGateActor{resultCh: resultCh})

	gatePID, _ := sys.Lookup("gate")
	sys.Send(gatePID, "alice")
	sys.Send(gatePID, "bob")

	expected := map[string]bool{
		"tag=login:alice ok=true msg=welcome alice lv10": true,
		"tag=login:bob ok=true msg=welcome bob lv20":     true,
	}

	for i := 0; i < 2; i++ {
		select {
		case r := <-resultCh:
			if !expected[r] {
				t.Fatalf("unexpected result: %s", r)
			}
			delete(expected, r)
		case <-time.After(time.Second):
			t.Fatal("timeout")
		}
	}

	sys.Shutdown()
}

// --- Test: Lookup returns false for unknown name ---

func TestLookupNotFound(t *testing.T) {
	sys := NewActorSystem()
	defer sys.Shutdown()

	_, ok := sys.Lookup("nonexistent")
	if ok {
		t.Fatal("expected Lookup to return false for unknown name")
	}
}

// --- Test: Registry cleanup after actor stops ---

func TestRegistryCleanupOnStop(t *testing.T) {
	sys := NewActorSystem()

	sys.Spawn("temp", &crossDBActor{})

	pid, ok := sys.Lookup("temp")
	if !ok {
		t.Fatal("expected temp to be registered")
	}

	sys.stop(pid)
	time.Sleep(50 * time.Millisecond)

	_, ok = sys.Lookup("temp")
	if ok {
		t.Fatal("expected temp to be unregistered after stop")
	}

	sys.Shutdown()
}

// --- Test: Bidirectional via Lookup (no PID fields) ---

type crossBroadcastEvent struct{ Content string }
type crossRegisterGate struct{}

type crossGateActorV2 struct {
	eventCh chan string
}

func (a *crossGateActorV2) OnInit(ctx ActorContext) {
	// Register self with game by sending a message ?game uses ctx.Sender()
	if gamePID, ok := ctx.Lookup("game2"); ok {
		ctx.Send(gamePID, &crossRegisterGate{})
	}
}
func (a *crossGateActorV2) OnStop(ctx ActorContext) {}
func (a *crossGateActorV2) HandleMessage(ctx ActorContext, msg interface{}) {
	switch m := msg.(type) {
	case string:
		if gamePID, ok := ctx.Lookup("game2"); ok {
			ctx.Send(gamePID, m)
		}
	case *crossBroadcastEvent:
		a.eventCh <- m.Content
	}
}

type crossGameActorV3 struct {
	router Router
	gates  []PID
}

func (a *crossGameActorV3) OnInit(ctx ActorContext) {
	a.router.Register(&crossRegisterGate{}, func(ctx ActorContext, msg interface{}) {
		a.gates = append(a.gates, ctx.Sender())
	})
	a.router.SetFallback(func(ctx ActorContext, msg interface{}) {
		if action, ok := msg.(string); ok {
			for _, gate := range a.gates {
				ctx.Send(gate, &crossBroadcastEvent{Content: "broadcast:" + action})
			}
		}
	})
}
func (a *crossGameActorV3) OnStop(ctx ActorContext) {}
func (a *crossGameActorV3) HandleMessage(ctx ActorContext, msg interface{}) {
	a.router.Route(ctx, msg)
}

func TestBidirectionalLookup(t *testing.T) {
	sys := NewActorSystem()

	sys.Spawn("game2", &crossGameActorV3{})

	eventCh := make(chan string, 4)
	sys.Spawn("gate1", &crossGateActorV2{eventCh: eventCh})
	sys.Spawn("gate2", &crossGateActorV2{eventCh: eventCh})

	// Wait for registration messages to be processed
	time.Sleep(50 * time.Millisecond)

	gate1PID, _ := sys.Lookup("gate1")
	sys.Send(gate1PID, "player-attack")

	received := 0
	for i := 0; i < 2; i++ {
		select {
		case e := <-eventCh:
			if e != "broadcast:player-attack" {
				t.Fatalf("unexpected event: %s", e)
			}
			received++
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for broadcast")
		}
	}

	if received != 2 {
		t.Fatalf("expected 2 broadcasts, got %d", received)
	}

	sys.Shutdown()
}
