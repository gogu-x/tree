// 本文件负责任务实例的创建、查询、重置、删除和过期清理。

package task

import "github.com/gogu-x/tree/tlog"

// Add 创建任务：系统从自己的配置表填好 ID、Goal、Target、Params，以及可选的 ConfigID、ExpireAt；
// Progress、Status 保持零值。Params 会被复制。任务在此时按 Goal 订阅事件。
// 先调用 Initial 求初始进度，再保存、订阅，最后通知 Added；初始进度达标时直接 Finished。
// 返回 false 表示组件不可写、参数无效、任务类型未注册、ID 已存在、已过期或 Initial 失败，此时不新增任务。
func (m *Component) Add(instance Instance, now int64) bool {
	if !m.writable() || !m.sync(now) {
		return false
	}
	if instance.ID == 0 || instance.Target <= 0 || instance.Progress != 0 || instance.Status != Going ||
		!validExpireAt(instance.ExpireAt, now) || m.registry.goals.handler(instance.Goal) == nil {
		tlog.Error("[task/Add] 新建任务参数无效, system=%v, owner=%v, instance=%+v, now=%v",
			m.registry.name, logID(m.owner), instance, now)
		return false
	}
	if _, exists := m.state.Tasks[instance.ID]; exists {
		tlog.Error("[task/Add] 任务实例已存在, system=%v, owner=%v, task=%v", m.registry.name, logID(m.owner), instance.ID)
		return false
	}
	instance.Params = append([]int64(nil), instance.Params...) // 不与配置表共用底层数组。
	if !m.initial(&instance) {
		return false
	}
	// Initial 应为只读计算；防止接入方意外重入后重复添加或关闭组件。
	if !m.writable() || m.state.Tasks[instance.ID] != nil {
		return false
	}
	if m.state.Tasks == nil {
		m.state.Tasks = make(map[uint64]*Instance)
	}
	m.state.Tasks[instance.ID] = &instance
	if instance.Status == Going {
		m.subscribe(&instance)
	}
	m.changed(Change{Kind: Added, After: instance})
	return true
}

// Reset 让已有任务重新开始：保留任务类型、目标值和条件参数，重新调用 Initial 计算进度，更新过期时间，
// 任意状态都变为 Going（初始达标则 Finished），然后通知 Refreshed。实例 ID 保持不变。
// 需要换成新配置时，Remove 后重新 Add。
// expireAt 为零表示不过期，非零时必须晚于 now。任务不存在、参数无效或 Initial 失败时返回 false，任务不变。
func (m *Component) Reset(id uint64, expireAt int64, now int64) bool {
	if !m.writable() || !validExpireAt(expireAt, now) || !m.sync(now) {
		return false
	}
	instance := m.state.Tasks[id]
	if instance == nil {
		return false
	}
	next := Instance{ID: id, ConfigID: instance.ConfigID, Goal: instance.Goal, Target: instance.Target, Params: instance.Params, ExpireAt: expireAt}
	if !m.initial(&next) {
		return false
	}
	if !m.writable() || m.state.Tasks[id] != instance {
		return false
	}
	before := *instance
	// 原地修改，订阅中保存的指针保持有效；先退订再按新状态订阅。
	m.unsubscribe(instance)
	*instance = next
	if instance.Status == Going {
		m.subscribe(instance)
	}
	m.changed(Change{Kind: Refreshed, Before: before, After: next})
	return true
}

// initial 调用 Initial 写入初始进度和状态；失败时记录错误并返回 false。
func (m *Component) initial(instance *Instance) bool {
	if hooks := m.registry.hooks; hooks != nil {
		progress, ok := hooks.Initial(m.owner, *instance)
		if !ok || progress < 0 {
			tlog.Error("[task/initial] 初始进度计算失败, system=%v, owner=%v, task=%v, progress=%v, ok=%v",
				m.registry.name, logID(m.owner), instance.ID, progress, ok)
			return false
		}
		instance.Progress = min(progress, instance.Target)
	}
	if instance.Progress == instance.Target {
		instance.Status = Finished
	}
	return true
}

// Get 按实例 ID 返回副本（含 Params），修改返回值不影响任务；不存在或组件已关闭时返回 false。不检查过期和刷新。
func (m *Component) Get(id uint64) (Instance, bool) {
	if !m.readable() {
		return Instance{}, false
	}
	if instance := m.state.Tasks[id]; instance != nil {
		return *instance.DeepCopy(), true
	}
	return Instance{}, false
}

// Snapshot 返回所有任务的副本（含 Params），不保证顺序，不检查过期和刷新；空集合或组件已关闭时返回 nil。
func (m *Component) Snapshot() []Instance {
	if !m.readable() || len(m.state.Tasks) == 0 {
		return nil
	}
	result := make([]Instance, 0, len(m.state.Tasks))
	for _, instance := range m.state.Tasks {
		result = append(result, *instance.DeepCopy())
	}
	return result
}

// Remove 删除任意状态的任务，取消订阅并通知 Removed；不发奖。
// 组件不可写或任务不存在时返回 false，可重复调用。
func (m *Component) Remove(id uint64) bool {
	if !m.writable() {
		return false
	}
	instance := m.state.Tasks[id]
	if instance == nil {
		return false
	}
	m.remove(instance, Removed)
	return true
}

// remove 统一执行主动删除与过期删除，先退订、删记录，再通知。调用方保证 instance 在 State 中。
func (m *Component) remove(instance *Instance, kind Kind) {
	m.unsubscribe(instance)
	delete(m.state.Tasks, instance.ID)
	if len(m.state.Tasks) == 0 {
		m.state.Tasks = nil // 最后一条删除后释放 map，存盘时省略空字段。
	}
	m.changed(Change{Kind: kind, Before: *instance})
}

// Expire 删除扫描开始时已到期（now >= ExpireAt）的任务，逐个通知 Expired。
// 已完成未领取的任务同样删除，不自动补发；需要补发的系统在 Changed 中处理 Expired。
// 回调中新建的任务留到下次扫描；回调关闭组件时返回 false，已完成的删除不回滚。
func (m *Component) Expire(now int64) bool {
	if !m.writable() || !m.sync(now) {
		return false
	}
	var candidates []*Instance
	for _, instance := range m.state.Tasks {
		if expired(instance, now) {
			candidates = append(candidates, instance)
		}
	}
	for _, instance := range candidates {
		if !m.writable() {
			return false
		}
		// 前一个回调可能已删除或替换同 ID 记录，只处理仍是原记录且仍到期的任务。
		if m.state.Tasks[instance.ID] == instance && expired(instance, now) {
			m.remove(instance, Expired)
		}
	}
	return true
}

// expired 判断任务是否已到期，ExpireAt 为零表示不限制。
func expired(instance *Instance, now int64) bool {
	return instance.ExpireAt > 0 && now >= instance.ExpireAt
}

// validExpireAt 判断新任务的过期时间是否可用：零表示不过期，否则必须晚于 now。
func validExpireAt(expireAt, now int64) bool { return expireAt == 0 || expireAt > now }
