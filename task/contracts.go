// 本文件定义系统接入时需要实现或传入的接口，不包含任务状态存储。

package task

// Owner 是透传给回调的业务上下文，通常是玩家，也可以为 nil。
// 底层只检查它是否实现 DirtyMarker、Identifier，不读取其他数据。
type Owner = any

// DirtyMarker 是可选接口：任务数据有变化时调用，供业务安排存盘；底层不做数据库操作。
type DirtyMarker interface {
	MarkDirty()
}

// Identifier 是可选接口：返回写入日志的宿主编号（例如玩家 ID），便于定位问题。
type Identifier interface {
	LogID() uint64
}

// Handler 是任务类型的计数规则：接收任务快照和事件，返回新的绝对进度以及事件是否匹配。
// 目标值和条件参数从 current.Target、current.Param 读取。返回 false 或负进度时不修改任务。
// 必须是纯计算：不得修改 current.Params、业务数据，不得保存事件引用或调用组件方法。
type Handler func(current Instance, event Event) (progress int64, matched bool)

// Hooks 是系统级回调，同一系统的所有玩家共享，单个玩家的数据通过 owner 获取。
type Hooks interface {
	// Initial 在 Add、Reset 时调用，返回非负的绝对初始进度；返回 false 时中止操作，不得产生副作用。
	Initial(owner Owner, instance Instance) (progress int64, ok bool)
	// Changed 在任务变化写入内存、订阅更新、MarkDirty 之后调用；可继续新增、删除、重置或发布。
	Changed(owner Owner, change Change)
	// Generate 在进入新的刷新周期时调用（仅 WithPeriod 的系统），返回本周期要创建的任务。
	// 系统可根据 owner 的当前数据（等级、联盟等级、开服天数等）和自己的配置表决定任务，
	// 返回的每个任务需填好 Goal、Target、Params，按 Add 规则创建。
	// 返回空表示本周期不预先创建，系统可在周期中途自行 Add。
	Generate(owner Owner, cycle Cycle) []Instance
}

// Grant 在 Claim 中同步发奖；返回 false 时必须保证业务资产没有变化。
// 执行期间禁止修改任务；不支持异步发奖。
type Grant func(owner Owner, instance Instance) (success bool)

// BaseHooks 提供默认回调，系统嵌入后只覆盖需要的方法。
type BaseHooks struct{}

// Initial 默认从零开始；状态类任务（如等级达到 N）可覆盖为读取当前业务数值。
func (BaseHooks) Initial(Owner, Instance) (int64, bool) { return 0, true }

// Changed 默认不处理。
func (BaseHooks) Changed(Owner, Change) {}

// Generate 默认不在周期开始时创建任务。
func (BaseHooks) Generate(Owner, Cycle) []Instance { return nil }
