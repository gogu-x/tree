// 本文件负责任务事件分发、进度计算和达成后的退订。

package task

// Publish 把任务事件同步分发给本组件中任务类型等于 Event.Goal 的进行中任务；跨系统分发请用 Hub.Publish。
// 计数规则返回新的绝对进度，组件负责限幅、达成退订并通知 Advanced；已过期的任务删除并通知 Expired。
// 没有任务属于该类型时什么也不做。只有组件不可写、now 为负或 Goal 为零时返回 false。
// true 不代表有任务推进或完成。事件必须来自服务端的可信结算。
func (m *Component) Publish(event Event, now int64) bool {
	if !m.writable() || event.Goal == 0 || !m.sync(now) {
		return false
	}
	handler := m.registry.goals.handler(event.Goal)
	if handler == nil || len(m.going[event.Goal]) == 0 {
		return true
	}
	// 只处理分发开始时已有的任务：回调中新增的任务追加在末尾，不参与本次分发；
	// 回调中删除的任务位置置空并立即跳过，最外层分发结束后再压缩，保证遍历下标稳定。
	// 每轮重新读取切片，因为回调中的追加可能让切片换了底层数组。
	m.depth++
	defer m.endPublish()
	for i, n := 0, len(m.going[event.Goal]); i < n && !m.closed; i++ {
		if instance := m.going[event.Goal][i]; instance != nil {
			m.advance(instance, handler, &event, now)
		}
	}
	return true
}

// endPublish 在最外层分发结束后压缩分发期间留下的空位。
func (m *Component) endPublish() {
	m.depth--
	if m.depth > 0 || !m.holes {
		return
	}
	for goal, list := range m.going {
		kept := list[:0]
		for _, instance := range list {
			if instance != nil {
				kept = append(kept, instance)
			}
		}
		clear(list[len(kept):])
		if len(kept) == 0 {
			delete(m.going, goal)
		} else {
			m.going[goal] = kept
		}
	}
	m.holes = false
}

// advance 处理一个进行中任务：先检查过期，再计算进度，达标时先退订再通知。
func (m *Component) advance(instance *Instance, handler Handler, event *Event, now int64) {
	if expired(instance, now) {
		m.remove(instance, Expired)
		return
	}
	before := *instance
	progress, matched := handler(before, *event)
	if !matched || progress < 0 {
		return
	}
	progress = min(progress, instance.Target)
	if progress == before.Progress {
		return
	}
	instance.Progress = progress
	if progress == instance.Target {
		instance.Status = Finished
		m.unsubscribe(instance)
	}
	m.changed(Change{Kind: Advanced, Before: before, After: *instance})
}

// subscribe 把一个进行中任务加入其任务类型的分组；调用方保证任务类型已注册。
func (m *Component) subscribe(instance *Instance) {
	if m.going == nil {
		m.going = make(map[uint32][]*Instance)
	}
	m.going[instance.Goal] = append(m.going[instance.Goal], instance)
}

// unsubscribe 把任务移出其任务类型的分组，不在分组中时什么也不做。
// 分发期间只置空并标记，结束后统一压缩；否则与末尾交换后删除，空分组直接释放。
func (m *Component) unsubscribe(instance *Instance) {
	list := m.going[instance.Goal]
	for i, current := range list {
		if current != instance {
			continue
		}
		if m.depth > 0 {
			list[i], m.holes = nil, true
			return
		}
		last := len(list) - 1
		list[i], list[last] = list[last], nil
		if last == 0 {
			delete(m.going, instance.Goal)
		} else {
			m.going[instance.Goal] = list[:last]
		}
		return
	}
}
