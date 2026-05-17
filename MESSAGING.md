# Actor 消息通信与路由机制

## 一、消息传递全景

```
Actor A                                          Actor B
  │                                                │
  │  ① Send (单向，不需要回复)                       │
  │──────────── msg ──────────────────────────────►│
  │                                                │
  │  ② Request + Response (一问一答)                 │
  │──────────── msg ──────────────────────────────►│
  │◄─────────── response ─────────────────────────│
  │                                                │
  │  ③ Request + Pipe (异步回调，不阻塞)              │
  │──────────── msg ──────────────────────────────►│
  │                                                │
  │  B 调用 ctx.Response(val, nil)                  │
  │  结果作为 PipeResult 投递回 A 的邮箱              │
  │◄─────────── PipeResult{Value, Err, Tag} ──────│
  │  A 在 HandleMessage 中处理                      │
```

---

## 二、所有可用的消息传递方法

### 1. `ctx.Send(pid, msg) bool` — 单向发送

```go
func (a *MyActor) HandleMessage(ctx ActorContext, msg interface{}) {
    ctx.Send(otherPID, "hello")  // 发出去就完了，不等回复
}
```

- 异步，不阻塞
- 邮箱满时**阻塞**直到有空间
- 接收方通过 `ctx.Sender()` 知道是谁发的
- 目标不存在返回 `false`

### 2. `ctx.TrySend(pid, msg) bool` — 非阻塞单向发送

```go
if !ctx.TrySend(otherPID, "hello") {
    // 目标不存在 或 邮箱满，消息丢弃
}
```

- 邮箱满时**不阻塞**，直接返回 `false`
- 适合"发后即忘"场景，等价于 chanrpc 的 `Go`

### 3. `ctx.Request(pid, msg) *Future` — 请求/响应

调用方发起请求，接收方通过 `ctx.Response` 回复：

```go
// Actor A：发起请求
func (a *ActorA) HandleMessage(ctx ActorContext, msg interface{}) {
    future := ctx.Request(pidB, "get-data")
    // 方式一：阻塞等待（会卡住当前 Actor，慎用）
    val, err := future.Await()
    // 方式二：带超时等待
    val, err := future.AwaitTimeout(time.Second)
}

// Actor B：处理并回复
func (b *ActorB) HandleMessage(ctx ActorContext, msg interface{}) {
    switch msg.(type) {
    case string:
        ctx.Response("result-data", nil)  // 回复给请求方
    }
}
```

- `Await` / `AwaitTimeout` 会**阻塞当前 Actor**，阻塞期间该 Actor 无法处理其他消息
- 目标不存在时 Future 立即返回 `ErrActorNotFound`

### 4. `future.Pipe(ctx)` — 异步回调（不阻塞）

```go
func (a *ActorA) HandleMessage(ctx ActorContext, msg interface{}) {
    switch m := msg.(type) {
    case string:
        ctx.Request(pidB, m).Pipe(ctx)  // 不阻塞，结果稍后到达

    case PipeResult:
        // 结果回来了，在这里处理
        fmt.Println(m.Value, m.Err)
    }
}
```

- **不阻塞**当前 Actor，不创建额外 goroutine
- 结果作为 `PipeResult` 消息投递回自己的邮箱
- 在 `HandleMessage` 中统一处理，天然线程安全

### 5. `future.PipeWithTag(ctx, tag)` — 带标签的异步回调

```go
func (a *ActorA) HandleMessage(ctx ActorContext, msg interface{}) {
    switch m := msg.(type) {
    case string:
        ctx.Request(pidB, "get-user").PipeWithTag(ctx, "user")
        ctx.Request(pidC, "get-order").PipeWithTag(ctx, "order")

    case PipeResult:
        switch m.Tag {
        case "user":
            // 用户数据
        case "order":
            // 订单数据
        }
    }
}
```

- 解决多个并发 Pipe 结果无法区分的问题

### 6. `ctx.Response(value, err)` — 回复请求

```go
func (b *ActorB) HandleMessage(ctx ActorContext, msg interface{}) {
    ctx.Response("done", nil)
}
```

- 只在收到 Request 消息时有效，普通 Send 消息调用此方法是空操作
- 一个请求只应 Response 一次

### 7. 从 ActorSystem 外部发送

非 Actor 代码（如 HTTP handler）也可以通过 `ActorSystem` 直接发送：

```go
sys.Send(pid, "hello")                          // 单向
sys.TrySend(pid, "hello")                       // 非阻塞单向
val, err := sys.Request(pid, "query").Await()   // 同步等待结果
```

---

## 三、方法选择指南

| 场景 | 方法 | 阻塞？ |
|------|------|--------|
| 通知，不关心结果 | `Send` | 邮箱满时阻塞 |
| 通知，允许丢弃 | `TrySend` | 不阻塞 |
| 需要结果，可以等 | `Request` + `Await` | 阻塞 |
| 需要结果，不能阻塞 Actor | `Request` + `Pipe` | 不阻塞 |
| 多个并发请求，需区分结果 | `Request` + `PipeWithTag` | 不阻塞 |
| 回复请求方 | `Response` | 不阻塞 |

---

## 四、消息路由机制

### 消息从 A 到 B 的完整路径

```
Actor A                          ActorSystem                        Actor B
   │                                │                                  │
   │ ctx.Send(pidB, &LoginRequest{})│                                  │
   │───────────────────────────────►│                                  │
   │                                │  查 map 找到 B 的 Mailbox         │
   │                                │──── PushUser(envelope) ────────►│
   │                                │                                  │
   │                                │              run() 主循环从 Inbox 取出
   │                                │              ↓
   │                                │         HandleMessage(ctx, msg)
   │                                │              ↓
   │                                │         router.Route(ctx, msg)
   │                                │              ↓
   │                                │         reflect.TypeOf(msg) == *LoginRequest
   │                                │              ↓
   │                                │         handleLogin(ctx, msg)
```

### 不使用 Router（大 switch）

```go
func (a *MyActor) HandleMessage(ctx ActorContext, msg interface{}) {
    switch m := msg.(type) {
    case *LoginRequest:   a.handleLogin(ctx, m)
    case *MoveRequest:    a.handleMove(ctx, m)
    case *ChatMessage:    a.handleChat(ctx, m)
    case PipeResult:      a.handlePipeResult(ctx, m)
    // 消息类型越多，这里越长...
    }
}
```

### 使用 Router（注册式路由）

```go
type GameActor struct {
    router actor.Router
}

func (a *GameActor) OnInit(ctx ActorContext) {
    // 注册：消息类型 → 处理函数，类似 chanrpc 的 Register
    a.router.Register(&LoginRequest{}, a.handleLogin)
    a.router.Register(&MoveRequest{},  a.handleMove)
    a.router.Register(&ChatMessage{},  a.handleChat)
    a.router.Register(&TradeRequest{}, a.handleTrade)
    a.router.Register(PipeResult{},    a.handlePipeResult)

    // 可选：未匹配消息的兜底处理
    a.router.SetFallback(func(ctx ActorContext, msg interface{}) {
        log.Printf("unknown message: %T", msg)
    })
}

// HandleMessage 只需一行
func (a *GameActor) HandleMessage(ctx ActorContext, msg interface{}) {
    a.router.Route(ctx, msg)
}

// 每个处理函数独立、清晰
func (a *GameActor) handleLogin(ctx ActorContext, msg interface{}) {
    req := msg.(*LoginRequest)
    // 验证登录...
    ctx.Response("ok", nil)
}

func (a *GameActor) handleMove(ctx ActorContext, msg interface{}) {
    req := msg.(*MoveRequest)
    // 处理移动...
}
```

### 与 chanrpc 的路由对比

| | chanrpc | actor + Router |
|---|---|---|
| 注册 | `server.Register("login", fn)` | `router.Register(&LoginRequest{}, fn)` |
| 路由键 | 任意 `interface{}` 作为 id | 消息的 `reflect.Type` |
| 分发 | Server.Exec 内部查 map | `router.Route` 查 map |
| 未注册消息 | panic | `SetFallback` 或静默忽略 |
| 类型安全 | 运行时检查函数签名 | 运行时类型断言 |

Router 用**消息的 Go 类型**作为路由键，不需要手动定义字符串 id。定义一个 struct 类型就是定义一种消息，发送时直接发 struct 实例，接收方自动路由到对应 handler。
