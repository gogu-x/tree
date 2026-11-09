// 本文件负责带刷新周期的系统在进入新周期时删除旧任务、生成新任务。

package task

import "github.com/gogu-x/tree/tlog"

// Refresh 检查是否进入新的刷新周期，是则立即刷新；未设置 WithPeriod 的系统什么也不做。
// Add、Reset、Publish、Claim、Expire 会自动检查，Get、Snapshot 不检查，
// 所以玩家登录、下发任务列表前应先调用 Refresh。组件不可写或 now 为负时返回 false。
func (m *Component) Refresh(now int64) bool {
	return m.writable() && m.sync(now)
}

// sync 在进入新周期时刷新任务，同一周期内只比较一次时间。
// 返回 false 表示 now 非法，或刷新过程中回调关闭了组件。
func (m *Component) sync(now int64) bool {
	if now < 0 {
		return false
	}
	period := m.registry.period
	if period == nil || now < m.cycleEnd {
		return true
	}
	start, end := period.Bounds(now)
	m.cycleEnd = end
	// 同一周期（例如刚 Bind），或服务器时间被调回到已刷新周期之前，都不重复刷新。
	if start <= m.state.CycleStart {
		return true
	}
	m.refresh(Cycle{Start: start, End: end}, now)
	return m.writable()
}

// refresh 删除上一周期的全部任务（通知 Expired），再按 Generate 的结果创建新任务。
// 先写入新周期，回调中再次调用组件方法时不会重复刷新。
func (m *Component) refresh(cycle Cycle, now int64) {
	previous := m.state.CycleStart
	m.state.CycleStart = cycle.Start
	if marker, ok := m.owner.(DirtyMarker); ok {
		marker.MarkDirty()
	}
	tlog.Info("[task/refresh] 进入新刷新周期, system=%v, owner=%v, previous=%v, start=%v, end=%v, tasks=%v",
		m.registry.name, logID(m.owner), previous, cycle.Start, cycle.End, len(m.state.Tasks))
	old := make([]*Instance, 0, len(m.state.Tasks))
	for _, instance := range m.state.Tasks {
		old = append(old, instance)
	}
	for _, instance := range old {
		if !m.writable() {
			return
		}
		if m.state.Tasks[instance.ID] == instance {
			m.remove(instance, Expired)
		}
	}
	if !m.writable() || m.registry.hooks == nil {
		return
	}
	for _, instance := range m.registry.hooks.Generate(m.owner, cycle) {
		if !m.writable() {
			return
		}
		m.Add(instance, now)
	}
}
