// 本文件负责组件的创建、存档恢复、关闭和日志上下文。

package task

import (
	"fmt"

	"github.com/gogu-x/tree/tlog"
)

// Component 管理一个宿主（通常是玩家）在一个系统里的任务，运行时订阅不写入存档。
// 只能在宿主的串行执行上下文中使用，关闭后不可复用；所有 now 参数是服务端秒时间戳。
type Component struct {
	registry *Registry              // 系统共享的只读配置。
	state    *State                 // 宿主持有的任务存档。
	owner    Owner                  // 透传给回调的业务上下文。
	going    map[uint32][]*Instance // 按任务类型分组的进行中任务，只存指针；完成后移出。
	depth    int                    // 正在进行的分发层数，大于零时只置空、不压缩 going。
	holes    bool                   // going 中是否有分发期间留下的空位。
	cycleEnd int64                  // 当前刷新周期的结束时间，零表示尚未计算。
	granting bool                   // Grant 回调期间禁止修改任务。
	closed   bool                   // 是否已经关闭。
}

// Bind 用系统注册表和一份独立的任务存档创建组件，owner 原样透传给回调。
// 新玩家传 &State{}；老玩家传加载出的存档。成功时只恢复 Going 任务的订阅，不调用任何回调。
// 每个任务自带目标值和条件参数，配置表修改不影响已创建的任务；需要生效时由系统 Reset 或 Remove 后重新 Add，
// 带刷新周期的系统在下一周期 Generate 时自然使用新配置。
//
// 任务类型已不在 goals 中（计数规则被删除）的任务从 State 删除，并在 dropped 中返回，由系统决定是否补偿；
// 此时调用 owner 的 MarkDirty（如果实现），不触发 Changed。
// 实例编号不一致、进度或目标值非法、状态未知等存档损坏情况返回错误，且不修改 State。
// 同一 State 只能绑定一个活跃组件。Bind 不检查过期和刷新周期：带周期的系统在下一次
// Add、Reset、Publish、Claim、Expire 或 Refresh 时自动刷新；玩家登录后、下发任务列表前建议调用 Refresh。
func Bind(registry *Registry, state *State, owner Owner) (component *Component, dropped []Instance, err error) {
	if registry == nil || state == nil {
		return nil, nil, fmt.Errorf("绑定任务数据需要注册表和存档")
	}
	// 先整体校验，损坏时不修改 State。
	for id, instance := range state.Tasks {
		if instance == nil || id == 0 || instance.ID != id || !validInstance(instance) {
			return nil, nil, fmt.Errorf("任务存档损坏, system=%v, owner=%v, task=%v, instance=%+v", registry.name, logID(owner), id, instance)
		}
	}
	for id, instance := range state.Tasks {
		if registry.goals.handler(instance.Goal) == nil {
			dropped = append(dropped, *instance)
			delete(state.Tasks, id)
			tlog.Info("[task/Bind] 任务类型已删除, 移除任务, system=%v, owner=%v, before=%+v", registry.name, logID(owner), *instance)
		}
	}
	if len(state.Tasks) == 0 {
		state.Tasks = nil
	}
	component = &Component{registry: registry, state: state, owner: owner}
	for _, instance := range state.Tasks {
		if instance.Status == Going {
			component.subscribe(instance)
		}
	}
	if marker, ok := owner.(DirtyMarker); ok && len(dropped) > 0 {
		marker.MarkDirty()
	}
	return component, dropped, nil
}

// validInstance 检查任务数据自身是否一致：目标值为正，进度与状态匹配。
func validInstance(instance *Instance) bool {
	if instance.Target <= 0 || instance.ExpireAt < 0 {
		return false
	}
	switch instance.Status {
	case Going:
		return instance.Progress >= 0 && instance.Progress < instance.Target
	case Finished, Rewarded:
		return instance.Progress == instance.Target
	default:
		return false
	}
}

// Close 释放订阅、配置和业务上下文引用，保留调用方持有的 State，不触发回调、不存盘。
// 重复关闭、nil 组件或 Grant 执行期间返回 false。继续使用原数据时需重新 Bind。
func (m *Component) Close() bool {
	if !m.writable() {
		return false
	}
	m.going, m.holes = nil, false
	m.registry, m.state, m.owner = nil, nil, nil
	m.closed = true
	return true
}

// ListenerCount 返回订阅指定任务类型的进行中任务数量，供诊断使用。
func (m *Component) ListenerCount(goal uint32) int {
	if !m.readable() {
		return 0
	}
	count := 0
	for _, instance := range m.going[goal] {
		if instance != nil {
			count++
		}
	}
	return count
}

// readable 只允许活跃组件读取任务。
func (m *Component) readable() bool { return m != nil && !m.closed }

// writable 只允许活跃且不在 Grant 回调中的组件修改任务。
func (m *Component) writable() bool { return m.readable() && !m.granting }

// logID 返回日志使用的宿主编号；owner 未实现 Identifier 时为零。
func logID(owner Owner) uint64 {
	if identifier, ok := owner.(Identifier); ok {
		return identifier.LogID()
	}
	return 0
}
