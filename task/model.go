// 本文件定义任务存档、事件、变更快照和领取结果；系统接入接口见 contracts.go。

package task

// Status 表示任务实例的持久化状态。
type Status uint8

const (
	Going    Status = iota // 任务进行中。
	Finished               // 任务已达成，等待领取。
	Rewarded               // 任务奖励已领取。
)

// String 返回日志使用的状态名。
func (s Status) String() string {
	switch s {
	case Going:
		return "going"
	case Finished:
		return "finished"
	case Rewarded:
		return "rewarded"
	default:
		return "unknown"
	}
}

// Kind 表示一次任务数据变化的原因。
type Kind uint8

const (
	Added     Kind = iota // 新增任务，可能由初始进度直接达成。
	Advanced              // 进度更新，可能在本次更新达成。
	Claimed               // 已成功发放奖励。
	Removed               // 主动删除任务。
	Expired               // 任务到达 ExpireAt，或刷新周期结束，被删除。
	Refreshed             // 任务被 Reset 重新开始。
)

// String 返回日志使用的变化原因名。
func (k Kind) String() string {
	switch k {
	case Added:
		return "added"
	case Advanced:
		return "advanced"
	case Claimed:
		return "claimed"
	case Removed:
		return "removed"
	case Expired:
		return "expired"
	case Refreshed:
		return "refreshed"
	default:
		return "unknown"
	}
}

// ClaimResult 是 Claim 的结果，系统据此返回自己的错误码。
type ClaimResult uint8

const (
	ClaimOK          ClaimResult = iota // 发奖成功，任务变为 Rewarded。
	ClaimUnavailable                    // 组件已关闭、正在发奖或参数非法，属于服务端调用错误。
	ClaimNotFound                       // 任务不存在。
	ClaimNotFinished                    // 任务进行中，尚未达成。
	ClaimRewarded                       // 奖励已领取过。
	ClaimExpired                        // 任务已过期，已删除并通知 Expired。
	ClaimGrantFailed                    // Grant 返回 false，任务保持 Finished。
)

// String 返回日志使用的结果名。
func (r ClaimResult) String() string {
	switch r {
	case ClaimOK:
		return "ok"
	case ClaimUnavailable:
		return "unavailable"
	case ClaimNotFound:
		return "not_found"
	case ClaimNotFinished:
		return "not_finished"
	case ClaimRewarded:
		return "rewarded"
	case ClaimExpired:
		return "expired"
	case ClaimGrantFailed:
		return "grant_failed"
	default:
		return "unknown"
	}
}

// Instance 保存一个任务实例，不存储配置副本和事件回调。
// +deepcopy-gen=true
//
//go:generate make -C .. accessor GEN_DIR=task STRUCTS="Instance State Event Change"
//go:generate make -C .. deepcopy GEN_DIR=task
type Instance struct {
	ID       uint64  `bson:"id"`                  // 系统内唯一的实例编号，由系统分配；常见做法是直接使用配置编号。
	ConfigID uint32  `bson:"config_id,omitempty"` // 系统自己的配置编号，库不读取，供发奖、展示时查表。
	Goal     uint32  `bson:"goal"`                // 任务类型，决定订阅哪类事件、使用哪个计数规则。
	Target   int64   `bson:"target"`              // 达成目标，必须大于零，创建时从配置带上。
	Params   []int64 `bson:"params,omitempty"`    // 计数规则使用的条件参数，创建时从配置带上，之后只读。
	Progress int64   `bson:"progress"`            // 当前进度，范围为零到目标值。
	Status   Status  `bson:"status"`              // 当前任务状态。
	ExpireAt int64   `bson:"expire_at,omitempty"` // 过期秒时间戳，零表示不限制。
}

// Param 读取条件参数；越界返回零和 false。
func (i Instance) Param(index int) (int64, bool) {
	if index < 0 || index >= len(i.Params) {
		return 0, false
	}
	return i.Params[index], true
}

// State 是一个系统的任务存档，随宿主文档加载和存盘；空任务集合不预先分配。
// +deepcopy-gen=true
type State struct {
	Tasks      map[uint64]*Instance `bson:"tasks,omitempty"`       // 系统持有的任务实例。
	CycleStart int64                `bson:"cycle_start,omitempty"` // 最近一次刷新所在周期的开始时间，仅 WithPeriod 的系统使用。
}

// Event 是服务端发布的任务事件，不直接接受客户端提供的进度。
type Event struct {
	Goal  uint32 // 任务类型。
	Count int64  // 数量或当前状态值。
	Param int64  // 常用匹配参数，例如物品或怪物类型。
	Data  any    // 扩展载荷，算法负责类型校验，不得保留其引用。
}

// Change 以值快照通知系统任务变化；Params 与底层实例共用，回调不得修改。
type Change struct {
	Kind   Kind     // 变化原因。
	Before Instance // 变化前快照，新增时为零值。
	After  Instance // 变化后快照，删除时为零值。
}

// FinishedNow 判断本次变化是否让任务达成，包含新增或重置时初始进度直接达标的情况。
func (c Change) FinishedNow() bool {
	return c.After.Status == Finished && (c.Kind == Refreshed || c.Before.Status != Finished)
}
