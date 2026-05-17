# Leaf Actor 模块代码分析文档

## 1. 概述

`actor` 包为 Leaf 框架提供了一个轻量级的 **Actor 模型**实现，用于构建基于消息传递的并发系统。它被设计为 Leaf 中 `chanrpc + skeleton` 模式的替代方案，提供更清晰的抽象和更简洁的 API。

### 核心设计理念

- **每个 Actor 运行在独立的 goroutine 中**，通过消息传递通信，避免共享状态
- **Mailbox 优先级机制**确保系统消息（如停止信号）不会被用户消息阻塞
- **Future 模式**统一了请求/响应的异步交互方式
- **Panic 恢复**保证单个 Actor 崩溃不会影响整个系统

---

## 2. 文件结构与职责

| 文件 | 职责 |
|------|------|
| `actor.go` | 核心类型定义：Actor 接口、PID、消息信封 |
| `mailbox.go` | 邮箱实现：双通道合并、系统消息优先 |
| `options.go` | Spawn 配置项：邮箱大小、日志器 |
| `system.go` | ActorSystem：Actor 生命周期管理、消息路由、panic 恢复 |
| `context.go` | ActorContext 接口与实现：Actor 运行时上下文 |
| `future.go` | Future/Promise 模式：异步请求响应 |
| `actor_test.go` | 单元测试 |

---

## 3. 各文件详细分析

### 3.1 actor.go — 核心类型定义

#### Actor 接口

```go
type Actor interface {
    OnInit(ctx ActorContext)
    HandleMessage(ctx ActorContext, message interface{})
    OnStop(ctx ActorContext)
}
```

这是所有 Actor 必须实现的接口，定义了三个生命周期方法：

| 方法 | 调用时机 | 用途 |
|------|---------|------|
| `OnInit` | Actor 启动后、处理消息前 | 初始化资源 |
| `HandleMessage` | 每收到一条消息时 | 业务逻辑处理 |
| `OnStop` | 收到停止信号时 | 清理资源 |

#### PID（进程标识符）

```go
type PID struct {
    ID   uint64  // 原子自增的唯一 ID
    Name string  // 人类可读名称
}
```

- 值类型，可安全跨 goroutine 复制
- 通过 `atomic.AddUint64` 全局分配，保证唯一性
- `String()` 输出格式为 `name#id`，如 `echo#1`

#### 消息信封

包内定义了三种消息包装类型：

| 类型 | 用途 |
|------|------|
| `systemMessage` | 内部生命周期消息（如 `systemStop`） |
| `messageEnvelope` | 普通消息 + 发送者 PID |
| `requestEnvelope` | 请求消息 + 发送者 PID + Future（用于响应） |

---

### 3.2 mailbox.go — 优先级邮箱

Mailbox 是 Actor 模型的核心组件，负责消息的接收与排序。

#### 架构设计

```
PushUser(msg)   → userChan (buffered)   ─┐
                                          ├→ merge goroutine → inbox (merged output)
PushSystem(msg)  → systemChan (unbuffered)─┘     (系统消息优先)
```

#### 关键字段

| 字段 | 类型 | 说明 |
|------|------|------|
| `userChan` | `chan interface{}` (有缓冲) | 用户消息通道，缓冲大小可配置 |
| `systemChan` | `chan interface{}` (无缓冲) | 系统消息通道，无缓冲确保即时处理 |
| `inbox` | `chan interface{}` | 合并后的输出通道，Actor 从此读取 |
| `closeCh` | `chan struct{}` | 关闭信号 |

#### merge 合并算法

```go
func (mb *Mailbox) merge() {
    // 1. 先检查是否关闭
    // 2. 优先尝试读取 systemChan（default 分支跳过）
    // 3. 若无系统消息，同时监听 systemChan 和 userChan
}
```

这是一个经典的**优先级 select 模式**：通过两层 `select` 实现系统消息优先于用户消息。当系统消息和用户消息同时就绪时，系统消息会被优先消费。

#### 公开方法

| 方法 | 说明 |
|------|------|
| `PushUser(msg)` | 发送用户消息，缓冲满时阻塞（背压） |
| `TryPushUser(msg) bool` | 非阻塞发送，缓冲满时返回 `false` |
| `PushSystem(msg)` | 发送系统消息，阻塞直到被消费 |
| `Inbox()` | 返回只读的合并消息通道 |
| `Close()` | 关闭邮箱，`sync.Once` 保证幂等 |

---

### 3.3 options.go — Spawn 配置

使用**函数选项模式（Functional Options）**配置 Actor 的启动参数。

#### 默认配置

```go
spawnConfig{
    mailboxSize: 64,    // 用户消息缓冲区默认 64
    logger:      defaultLogger,
}
```

#### 可用选项

| 选项函数 | 说明 |
|---------|------|
| `WithMailboxSize(size)` | 设置用户消息通道缓冲大小 |
| `WithLogger(l)` | 设置自定义日志器 |

#### Logger 接口

```go
type Logger interface {
    Error(format string, a ...interface{})
}
```

最小化的日志接口，仅要求 `Error` 方法，便于与各种日志库集成。

---

### 3.4 system.go — ActorSystem 核心

ActorSystem 是整个 Actor 模型的中枢，负责 Actor 的注册、运行、消息路由和生命周期管理。

#### 数据结构

```go
type ActorSystem struct {
    mu     sync.RWMutex           // 读写锁保护 actors map
    actors map[PID]*actorProcess  // PID → 进程映射
    wg     sync.WaitGroup         // 等待所有 Actor 退出
}

type actorProcess struct {
    pid     PID
    actor   Actor
    mailbox *Mailbox
    logger  Logger
}
```

#### Spawn 流程

```
Spawn(name, actor, opts...)
  ├→ 应用配置选项
  ├→ 分配 PID
  ├→ 创建 Mailbox
  ├→ 注册到 actors map
  └→ 启动 goroutine 执行 run()
```

#### run 主循环

```
run(proc)
  ├→ defer: 关闭 Mailbox + 从 map 中移除
  ├→ 调用 OnInit
  └→ 循环读取 Inbox:
       ├→ systemMessage(stop) → 调用 OnStop → 退出
       ├→ requestEnvelope     → 构建带 Future 的 context → HandleMessage
       ├→ messageEnvelope     → 构建带 sender 的 context → HandleMessage
       └→ 其他               → 构建基础 context → HandleMessage
```

#### Panic 恢复

`safeCall` 方法包装所有 Actor 回调，捕获 panic 并记录堆栈信息（4096 字节），防止单个 Actor 崩溃导致系统级故障。

#### 消息发送

| 方法 | 说明 |
|------|------|
| `Send(pid, msg)` | 异步发送，目标不存在时静默忽略 |
| `Request(pid, msg) *Future` | 发送并返回 Future，目标不存在时 Future 立即返回 `ErrActorNotFound` |
| `stop(pid)` | 向目标发送系统停止信号 |
| `Shutdown()` | 向所有 Actor 发送停止信号并等待全部退出 |
| `SpawnCount() int` | 返回当前运行中的 Actor 数量 |

---

### 3.5 context.go — Actor 上下文

#### ActorContext 接口

```go
type ActorContext interface {
    Self() PID
    Sender() PID
    Message() interface{}
    Send(pid PID, msg interface{})
    Request(pid PID, msg interface{}) *Future
    Response(value interface{}, err error)
    Stop()
}
```

这是 Actor 与外界交互的唯一入口，提供了：

| 方法 | 说明 |
|------|------|
| `Self()` | 获取自身 PID |
| `Sender()` | 获取消息发送者 PID（无发送者时为零值） |
| `Message()` | 获取当前消息（OnInit/OnStop 时为 nil） |
| `Send()` | 向其他 Actor 发送消息（自动附带自身 PID 作为 sender） |
| `Request()` | 向其他 Actor 发送请求并获取 Future |
| `Response()` | 回复当前请求（非 Request 消息时为空操作） |
| `Stop()` | 停止自身 |

#### localContext 实现

`localContext` 是 `ActorContext` 的本地实现，持有 `self`、`system`、`sender`、`msg`、`future` 五个字段。每次消息处理时创建新的 context 实例，确保上下文隔离。

---

### 3.6 future.go — 异步请求/响应

Future 模式替代了原 `chanrpc` 包中的 `Call0/Call1/CallN` 三种调用变体和三种回调签名，统一为单一模式。

#### 核心结构

```go
type Future struct {
    resultCh chan FutureResult  // 容量为 1 的缓冲通道
}

type FutureResult struct {
    Value interface{}
    Err   error
}
```

#### 方法

| 方法 | 说明 |
|------|------|
| `NewFuture()` | 创建新 Future（1 容量缓冲通道） |
| `Respond(value, err)` | 写入结果（由被调用 Actor 调用） |
| `Await()` | 阻塞等待结果 |
| `AwaitTimeout(d)` | 带超时等待，超时返回 `ErrTimeout` |

#### 预定义错误

| 错误 | 说明 |
|------|------|
| `ErrTimeout` | Future 等待超时 |
| `ErrActorNotFound` | 目标 PID 未注册 |

---

### 3.7 actor_test.go — 单元测试

覆盖了 5 个核心场景：

| 测试 | 验证内容 |
|------|---------|
| `TestSpawnAndSend` | 基本的 Spawn + Send 消息传递 |
| `TestRequestResponse` | Request/Response 模式（Future 获取结果） |
| `TestActorStop` | Actor 自我停止（ctx.Stop()）及 OnStop 回调 |
| `TestSystemShutdown` | 系统级 Shutdown 后所有 Actor 被清理 |
| `TestMailboxPriority` | 系统消息优先于用户消息被消费 |

---

## 4. 消息流转全景图

```
                          ┌─────────────────────────────────────────┐
                          │            ActorSystem                  │
                          │  actors: map[PID]*actorProcess          │
                          └──────┬──────────────────────────────────┘
                                 │
         ┌───────────────────────┼───────────────────────┐
         │                       │                       │
    ┌────▼────┐            ┌─────▼─────┐           ┌─────▼─────┐
    │ Actor A │            │  Actor B  │           │  Actor C  │
    │ (goroutine)          │ (goroutine)           │ (goroutine)
    └────┬────┘            └─────┬─────┘           └───────────┘
         │                       │
         │  ctx.Send(B, msg)     │
         │──────────────────────►│
         │                       │
         │  ctx.Request(B, msg)  │
         │──────────────────────►│
         │                       │ ctx.Response(val, nil)
         │◄──────────────────────│
         │  future.Await()       │
         │  → (val, nil)         │

每个 Actor 内部:
    ┌──────────────────────────────────────────┐
    │  Mailbox                                 │
    │                                          │
    │  userChan ──┐                            │
    │             ├→ merge goroutine → inbox   │
    │  systemChan ┘   (system 优先)            │
    │                                          │
    │  inbox → run() 主循环 → HandleMessage    │
    └──────────────────────────────────────────┘
```

---

## 5. 与 chanrpc 的对比

| 特性 | chanrpc (旧) | actor (新) |
|------|-------------|-----------|
| 并发模型 | channel + skeleton 调度 | Actor + Mailbox |
| 调用方式 | Call0/Call1/CallN 三种变体 | 统一的 Request + Future |
| 回调签名 | 三种不同签名 | 统一的 `HandleMessage` |
| 消息优先级 | 无 | 系统消息优先 |
| Panic 恢复 | 有 | 有（带堆栈日志） |
| 生命周期 | 手动管理 | OnInit/HandleMessage/OnStop |
| 配置方式 | 结构体字段 | 函数选项模式 |

---

## 6. 设计亮点

1. **优先级 Mailbox**：双层 select 模式确保 Stop 等系统消息不被用户消息队列阻塞，这在高负载场景下尤为重要。

2. **Context 隔离**：每条消息处理时创建独立的 `localContext`，避免状态泄漏。

3. **Future 统一模式**：用单一的 `Future` 类型替代多种回调变体，大幅简化了请求/响应的使用方式。

4. **Panic 安全**：`safeCall` 包装所有 Actor 回调，单个 Actor 的 panic 不会传播到其他 Actor 或系统。

5. **优雅关闭**：`Shutdown()` 通过系统消息通知所有 Actor 停止，并通过 `WaitGroup` 等待全部退出，确保资源正确释放。

6. **背压机制**：用户消息通道有缓冲限制，满时发送方阻塞，天然实现了流量控制。

---

## 7. 潜在改进方向

1. **监督树（Supervision）**：当前 Actor panic 后仅记录日志，可考虑添加重启策略（如 Erlang 的 one-for-one / one-for-all）。

2. **Actor 发现**：目前只能通过 PID 寻址，可考虑添加基于名称的注册表（Registry）。

3. **消息超时**：`PushUser` 在缓冲满时会无限阻塞，可考虑添加带超时的发送方法。

4. **指标监控**：可添加邮箱深度、消息处理延迟等运行时指标。

5. **泛型支持**：当前消息类型为 `interface{}`，Go 1.18+ 可考虑使用泛型提供类型安全的消息处理。
