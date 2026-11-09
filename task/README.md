# task：各系统共用的任务组件

日常、成就、活动等系统用它管理玩家任务。计数规则（累加、取最大值、按参数匹配等）是提前写好的方法，按任务类型注册一次；每个任务的目标值和条件参数来自系统自己的配置表，在创建任务时带上。创建哪些任务由系统根据自己的参数决定，可以在周期开始时生成，也可以随时追加；其他系统发布事件推进进度，任务完成时通知所属系统；每日、每周等周期刷新由库判断并执行。库只负责任务数据、事件分发、状态变化和周期刷新，不依赖玩家、Actor、数据库或具体玩法。

完整可运行示例见 [example_test.go](example_test.go)（每天刷新、按玩家等级生成任务的日常，加上不刷新的成就，接入同一个玩家），执行 `go test ./task -run Example -v`。

## 概念

| 概念 | 是什么 | 数量 |
| --- | --- | --- |
| `Goals` | 任务类型表：任务类型编号 → 计数规则（提前写好的方法） | 全服一个，启动时创建 |
| `Registry` | 一个系统的名字、回调（`Hooks`）和可选的刷新周期，不保存任务配置 | 每个系统一个，启动时创建 |
| `Instance` | 一条任务：任务类型、目标值、条件参数（创建时从配置带上），以及进度和状态 | 系统创建 |
| `State` | 一个玩家在一个系统里的任务存档，随玩家文档存盘 | 每个玩家每个系统一份 |
| `Component` | 绑定 Registry 和 State 后的运行时对象，提供 Add、Claim、Reset 等操作 | 每个玩家每个系统一个 |
| `Hub` | 玩家的任务事件入口，把事件分发给所有挂载的 Component | 每个玩家一个 |

```text
启动：Goals ──┬── Registry(SystemDaily, 日常 Hooks, 每天 5 点刷新)
              └── Registry(SystemAchieve, 成就 Hooks)

玩家：Hub ──┬── Component(daily)   ── 日常 State
            └── Component(achieve) ── 成就 State

战斗系统 hub.Publish(击杀事件) → 订阅了该类型的任务推进 → 所属系统的 Changed 收到通知
```

## 接入步骤

### 1. 启动：注册计数规则（全服一次）

```go
goals, err := task.NewGoals(map[uint32]task.Handler{
    GoalKillMonster: task.AccumulateMatch, // 击杀指定怪物：Params[0] 等于事件参数时累加
    GoalLevel:       task.Maximum,         // 等级达到：取最大值
})
```

任务类型编号是全服约定，配置表的"任务类型"字段直接填这个编号。计数规则只决定"事件来了进度怎么变"，目标值和条件参数在任务上。内置规则不够用时，自己写一个 `Handler` 纯函数，从 `current.Target`、`current.Param(i)` 读取任务自己的规则。

### 2. 启动：每个系统建注册表

```go
// 系统名称统一定义为常量
const (
    SystemDaily       task.System = "daily"
    SystemGuildDaily  task.System = "guild_daily"
    SystemGuildWeekly task.System = "guild_weekly"
)

dailyRegistry, err := task.NewRegistry(SystemDaily, goals, dailyHooks{},
    task.WithPeriod(task.Daily(loc, 5*time.Hour))) // 不刷新的系统省略 WithPeriod
```

注册表不保存任务配置，配置表由系统自己读。

### 3. 写系统回调

```go
type dailyHooks struct{ task.BaseHooks } // 只覆盖需要的方法

// Changed：任务变化后调用，owner 就是 Bind 时传入的玩家。
func (dailyHooks) Changed(owner task.Owner, change task.Change) {
    if change.FinishedNow() {
        owner.(*Player).SendDailyTaskNtf(change.After)
    }
}

// Initial（可选）：Add、Reset 时求初始进度，例如"等级达到 30"直接读当前等级。
func (dailyHooks) Initial(owner task.Owner, instance task.Instance) (int64, bool) {
    if instance.Goal == GoalLevel {
        return owner.(*Player).Level, true
    }
    return 0, true
}

// Generate（可选，带刷新周期的系统用）：进入新周期时返回本周期要创建的任务。
func (dailyHooks) Generate(owner task.Owner, cycle task.Cycle) []task.Instance { ... }
```

### 4. 玩家加载：Bind 并挂到 Hub

```go
p.Daily.tasks, dropped, err = task.Bind(dailyRegistry, &p.Daily.State, p)
p.TaskHub.Attach(p.Daily.tasks)
p.TaskHub.Refresh(now) // 带周期的系统在这里补上离线期间错过的刷新
```

玩家对象可选实现 `MarkDirty()`（任务变化时安排存盘）和 `LogID() uint64`（日志里带玩家 ID）。

任务自带规则，配置表修改不影响已经创建的任务。Bind 只检查存档本身：任务类型的计数规则已从代码中删除的任务，从存档移除并放进 `dropped`；存档损坏时返回 error，不修改存档。

### 5. 创建任务：什么时候、创建哪些，都由系统决定

任务库不读配置表。系统读自己的配置，把任务类型、目标值、条件参数填进 `Instance`；任务在创建时按任务类型订阅事件。系统有两种方式创建：

- **周期开始时生成**：注册时加 `task.WithPeriod(...)`，并实现 `Generate`。进入新周期时，库删除上一周期的全部任务，然后调用 `Generate`，由系统根据自己的参数（玩家等级、联盟等级、开服天数、随机池等）返回本周期的任务。
- **任意时刻追加**：活动开启、解锁新功能、完成前置任务等，系统直接调用 `Add`。带周期的系统中途追加的任务，同样会在下次刷新时删除。

```go
// 每天 5 点刷新；每周一 0 点用 task.Weekly(loc, time.Monday, 0)
dailyRegistry, err := task.NewRegistry(SystemDaily, goals, dailyHooks{},
    task.WithPeriod(task.Daily(loc, 5*time.Hour)))

// 系统自己的转换函数：配置表一行 → 一条任务
func newDailyTask(cfg *gdconf.DailyTaskCfg) task.Instance {
    return task.Instance{
        ID: uint64(cfg.ID), ConfigID: cfg.ID, // ConfigID 库不读取，供系统发奖、展示时查表
        Goal: cfg.TaskType, Target: cfg.Target, Params: cfg.Params,
    }
}

func (dailyHooks) Generate(owner task.Owner, cycle task.Cycle) []task.Instance {
    p := owner.(*Player)
    var tasks []task.Instance
    for _, cfg := range gdconf.GetDailyTaskPool(p.Level) { // 系统自己的筛选规则
        tasks = append(tasks, newDailyTask(cfg))
    }
    return tasks
}

// 周期中途追加
p.Daily.tasks.Add(newDailyTask(cfg), now)
```

实例 ID 在系统内唯一，常见做法是直接用配置 ID；同一个配置需要多个实例时，由系统自己分配 ID。

### 6. 其他系统发布事件

```go
p.TaskHub.Publish(task.Event{Goal: GoalKillMonster, Count: 1, Param: monsterType}, now)
```

发布方只需要拿到玩家，不需要知道哪些系统关心这个事件。没有任务订阅该类型的系统直接跳过。

### 7. 领取

```go
switch p.Daily.tasks.Claim(id, now, func(owner task.Owner, t task.Instance) bool {
    return owner.(*Player).AddItems(rewardOf(t.ConfigID)) // 失败时必须保证没有加任何东西
}) {
case task.ClaimOK:
case task.ClaimNotFound, task.ClaimNotFinished:
    return ErrCode_TASK_NOT_FINISH
case task.ClaimRewarded:
    return ErrCode_TASK_REWARDED
// ...
}
```

### 8. 刷新

带 `WithPeriod` 的系统由库判断周期：`Add`、`Reset`、`Publish`、`Claim`、`Expire` 会先检查是否进入新周期，进入了就刷新。刷新时删除旧任务（通知 `Expired`，系统可以在 Changed 里给完成未领取的任务补发），然后调用 `Generate`。当前周期的开始时间存在 `State.CycleStart`，所以停服跨天、玩家离线多天后上线，都只刷新一次。

`Get`、`Snapshot` 没有 now 参数，不会检查周期。玩家登录、下发任务列表之前先调用 `Refresh(now)` 或 `Hub.Refresh(now)`。库没有定时器；需要在刷新时刻主动推送的系统，自己用定时器在到点时调用 `Refresh`。

不需要按周期整体刷新的系统（成就、主线）不加 `WithPeriod`，需要时手动操作：
- `Reset(id, expireAt, now)`：让单个任务重新开始，ID 不变，通知 `Refreshed`；
- `Remove(id)` 后再 `Add`：换成别的任务；
- `Expire(now)`：删除所有到达 `ExpireAt` 的任务。

同一个系统里有不同周期的任务（例如联盟每日、每周），分别建两个 Registry、两份 State，挂到同一个 Hub。

### 9. 玩家下线

```go
p.TaskHub.Detach(p.Daily.tasks)
p.Daily.tasks.Close() // 不存盘、不删存档；State 由玩家文档保存
```

## API 一览

| 方法 | 作用和返回值 |
| --- | --- |
| `NewGoals(map)` | 创建任务类型表；编号为零或计数规则为空时报错 |
| `NewRegistry(name, goals, hooks, opts...)` | 创建系统注册表，name 为 `task.System` 类型的系统名称；`WithPeriod(Daily/Weekly/自定义 Period)` 开启周期刷新 |
| `Bind(registry, state, owner)` | 返回 `(组件, 被移除的任务, error)`；不调用任何回调 |
| `Add(instance, now)` | 按传入的任务类型、目标值、条件参数创建任务并订阅，调用 Initial，通知 Added；初始进度达标直接 Finished |
| `Refresh(now)` | 带周期的系统检查并执行刷新；其他系统什么也不做 |
| `Reset(id, expireAt, now)` | 让任务按原规则重新开始，调用 Initial，通知 Refreshed；换规则用 Remove + Add |
| `Get(id)` / `Snapshot()` | 返回副本（含 Params），不清理过期任务 |
| `Publish(event, now)` | 只分发给本组件；跨系统用 `Hub.Publish` |
| `Claim(id, now, grant)` | 返回 `ClaimResult`；成功后 Rewarded，通知 Claimed |
| `Remove(id)` | 删除任意状态的任务，通知 Removed；可重复调用 |
| `Expire(now)` | 删除所有到期任务，通知 Expired，包括完成未领取的 |
| `Close()` | 释放运行时引用；重复关闭返回 false |
| `ListenerCount(goal)` | 诊断用：订阅该类型的进行中任务数 |
| `Hub.Attach / Detach / Publish / Refresh` | 挂载、卸载组件，向所有挂载组件分发事件或检查刷新 |
| `Change.FinishedNow()` | 本次变化是否让任务达成（含 Add、Reset 时直接达成） |

生成的 `GetXxx/SetXxx` 只是字段访问，Set 不校验、不退订、不通知。Bind 之后通过组件方法修改任务。

## 行为边界

- 所有 now 是服务端秒时间戳。`ExpireAt=0` 不过期，`now >= ExpireAt` 视为到期。带周期的系统一般不用 `ExpireAt`，否则任务到期和周期刷新之间会出现空档。
- 服务器时间调回到已刷新的周期之前时，不会重复刷新，等时间走过记录的周期后才再次刷新。
- 同一个 Component、Hub 只能在玩家的串行上下文里使用；Goals、Registry 创建后只读，可以并发共享。同一份 State 只能绑定一个活跃组件。
- Handler、Initial 必须是只读计算。Changed、Handler 收到的 `Params` 与任务共用，不得修改。Changed 里可以继续 Add、Remove、Reset、Publish；新任务不参加正在进行的分发。
- Finished 之后不再推进，即使当前值下降也不会回到 Going。
- Grant 收到的实例状态已是 Rewarded，期间禁止修改任务。返回 false 或 panic 时恢复 Finished（panic 继续传播）；库不回滚业务资产。
- Expire 会删除完成未领取的任务，不自动补发；需要补发的系统在 Changed 里处理 `Expired`。
- 日志：新增、重置、领取、删除、过期、Bind 修正记 Info，带系统名和玩家 ID。进度推进是高频操作，库不记日志，需要时在 Changed 里记。玩家可触发的领取失败不记日志。

## 内置计数规则

| 函数 | 规则 |
| --- | --- |
| `Accumulate` | 累加正数 Count，溢出饱和；非正数不匹配 |
| `AccumulateMatch` | 任务的 `Params[0]` 等于 `Event.Param` 时累加 |
| `Maximum` | 取当前进度和非负 Count 的较大值 |
| `Current` | 使用非负 Count，完成前允许下降 |
| `Once` | 收到事件就把进度设为 1，配合 `Target=1` |

## 生成与验证

在 tree 根目录：

```sh
make tools-build # 首次或生成工具源码更新后执行
make accessor deepcopy
go test -race ./comm ./task
go test ./task -run '^$' -bench Task -benchmem
```
