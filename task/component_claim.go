// 本文件负责领取校验、发奖回调和失败时的状态恢复。

package task

import "github.com/gogu-x/tree/tlog"

// Claim 对未过期的 Finished 任务同步调用一次 Grant，成功后变为 Rewarded 并通知 Claimed。
// 返回值说明失败原因，系统据此返回自己的错误码；玩家可触发的失败（不存在、未完成、已领取、已过期）不打错误日志。
// 过期任务会被删除并通知 Expired；上一刷新周期的任务在刷新时已删除，返回 ClaimNotFound。Grant 返回 false 或 panic 时恢复 Finished，panic 继续向上传播。
// Grant 收到的实例状态已是 Rewarded，期间禁止修改任务；业务资产的原子性由 Grant 保证。
func (m *Component) Claim(id uint64, now int64, grant Grant) ClaimResult {
	if !m.writable() || grant == nil || !m.sync(now) {
		return ClaimUnavailable
	}
	instance := m.state.Tasks[id]
	switch {
	case instance == nil:
		return ClaimNotFound
	case instance.Status == Going:
		return ClaimNotFinished
	case instance.Status == Rewarded:
		return ClaimRewarded
	case expired(instance, now):
		m.remove(instance, Expired)
		return ClaimExpired
	}
	before := *instance
	if !m.grant(instance, grant) {
		tlog.Warn("[task/Claim] 发奖回调返回失败, system=%v, owner=%v, task=%v", m.registry.name, logID(m.owner), id)
		return ClaimGrantFailed
	}
	m.changed(Change{Kind: Claimed, Before: before, After: *instance})
	return ClaimOK
}

// grant 在回调期间禁止写入并临时标记 Rewarded，失败或 panic 时恢复 Finished。
func (m *Component) grant(instance *Instance, grant Grant) (success bool) {
	m.granting = true
	instance.Status = Rewarded
	defer func() {
		m.granting = false
		if !success {
			instance.Status = Finished
		}
	}()
	return grant(m.owner, *instance)
}
