// 本文件统一处理任务变化后的脏标记、日志和 Changed 通知。

package task

import "github.com/gogu-x/tree/tlog"

// changed 在数据和订阅更新后，依次执行 MarkDirty、记录日志、调用 Changed。
// 进度推进是高频操作，不记日志（日志参数装箱在级别关闭时也会分配内存），需要时由系统在 Changed 中记录；
// 新增、重置、领取、删除、过期记 Info。
// owner 和 registry 先取出，MarkDirty 即使关闭组件也不影响本次通知。
func (m *Component) changed(change Change) {
	owner, registry := m.owner, m.registry
	if marker, ok := owner.(DirtyMarker); ok {
		marker.MarkDirty()
	}
	if change.Kind != Advanced {
		tlog.Info("[task/changed] 任务状态变化, system=%v, owner=%v, kind=%v, before=%+v, after=%+v",
			registry.name, logID(owner), change.Kind, change.Before, change.After)
	}
	if registry.hooks != nil {
		registry.hooks.Changed(owner, change)
	}
}
