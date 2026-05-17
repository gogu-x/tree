# bigTree

A lightweight Go toolkit for building concurrent game servers, built around the Actor model.

## Packages

| Package | Description |
|---------|-------------|
| `actor` (root) | Actor model — `ActorSystem`, `PID`, `Router`, `Future` |
| `timer` | Timer dispatcher, cron expressions, time wheel |
| `log` | 4-level logger (debug / release / error / fatal) |
| `db/mongodb` | MongoDB connection pool with auto-increment counters |
| `tools/` | Code generation scripts |

## Installation

```bash
go get github.com/gogu-x/bigTree@latest
```

## Quick Start

```go
import actor "github.com/gogu-x/bigTree"

type HelloActor struct{}

func (a *HelloActor) OnInit(ctx actor.ActorContext)                        {}
func (a *HelloActor) OnStop(ctx actor.ActorContext)                        {}
func (a *HelloActor) HandleMessage(ctx actor.ActorContext, msg interface{}) {
    fmt.Println("received:", msg)
}

func main() {
    sys := actor.NewActorSystem()
    pid := sys.Spawn("hello", &HelloActor{})
    sys.Send(pid, "world")
    sys.Start() // blocks until Ctrl+C
}
```

## Actor

### Define an Actor

```go
type MyActor struct{}

func (a *MyActor) OnInit(ctx actor.ActorContext)    { /* startup */ }
func (a *MyActor) OnStop(ctx actor.ActorContext)    { /* cleanup */ }
func (a *MyActor) HandleMessage(ctx actor.ActorContext, msg interface{}) {
    switch m := msg.(type) {
    case *MyMsg:
        // handle
    }
}
```

### ActorSystem

```go
sys := actor.NewActorSystem()

// Spawn
pid := sys.Spawn("worker", &MyActor{})

// Send (async, non-blocking)
sys.Send(pid, &MyMsg{})

// Lookup by name
pid, ok := sys.Lookup("worker")

// Request / Response
future := sys.Request(pid, &QueryMsg{})
result, err := future.Result(3 * time.Second)

// Graceful shutdown
sys.Shutdown()
```

### Router

```go
var r actor.Router
r.Register(&LoginReq{},  handleLogin)
r.Register(&ChatReq{},   handleChat)
r.SetFallback(handleUnknown)

// inside HandleMessage:
r.Route(ctx, msg)
```

## Timer

```go
disp := timer.NewDispatcher(128)

// one-shot
t := disp.AfterFunc(5*time.Second, func() {
    fmt.Println("fired")
})

// cron
expr, _ := timer.NewCronExpr("0 * * * *") // every hour
c := disp.CronFunc(expr, func() {
    fmt.Println("hourly tick")
})

// drain in your actor's select loop
select {
case t := <-disp.ChanTimer:
    t.Cb()
}
```

## Log

```go
import "github.com/gogu-x/bigTree/log"

log.Debug("connecting to %s", addr)
log.Release("server started")
log.Error("unexpected error: %v", err)
log.Fatal("cannot continue") // calls os.Exit(1)
```

Configure output:

```go
log.SetLevel("release")          // debug | release | error | fatal
log.SetOutput("server.log")      // "" = stdout
```

## MongoDB

```go
import "github.com/gogu-x/bigTree/db/mongodb"

ctx, err := mongodb.Dial("mongodb://localhost:27017", 10)
if err != nil {
    log.Fatal("%v", err)
}
defer ctx.Close()

s := ctx.Ref()
defer ctx.UnRef(s)

err = s.DB("game").C("players").Insert(bson.M{"uid": 1001})
```

## tools/gen-register.lua

Generates a `register.go` file that calls `codec.RegisterMsg` for every
`message` in a `.proto` file. Run after `protoc`:

```bash
lua tools/gen-register.lua protobuf/game.proto pb/game game
```

Output `pb/game/register.go`:

```go
func init() {
    codec.RegisterMsg(&LoginReq{}, &LoginResp{}, ...)
}
```

## License

Apache 2.0
