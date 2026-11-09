package task

// Hub 是一个宿主（通常是玩家）的任务事件入口，把事件分发给挂载的所有系统组件。
// 发布方只需要拿到玩家的 Hub，不需要知道哪些系统关心这个事件。零值可用，只能在宿主的串行上下文中使用。
type Hub struct {
	components []*Component // 挂载的系统组件，按挂载顺序分发。
}

// Attach 挂载一个系统组件；nil、已关闭或已挂载时返回 false。
// 在分发过程中挂载的组件不参与本次分发。
func (h *Hub) Attach(component *Component) bool {
	if !component.readable() || h.index(component) >= 0 {
		return false
	}
	h.components = append(h.components[:len(h.components):len(h.components)], component)
	return true
}

// Detach 卸载一个系统组件，未挂载时返回 false。系统关闭组件前应先卸载。
// 在分发过程中卸载的组件，如果还没轮到，不再收到本次事件。
func (h *Hub) Detach(component *Component) bool {
	at := h.index(component)
	if at < 0 {
		return false
	}
	// 复制后再删除，正在进行的分发继续遍历旧切片。
	components := make([]*Component, 0, len(h.components)-1)
	components = append(components, h.components[:at]...)
	h.components = append(components, h.components[at+1:]...)
	return true
}

// Publish 把事件依次分发给所有挂载的系统组件；没有任务订阅该类型的系统直接跳过。
// 系统之间不保证除挂载顺序以外的执行顺序；Goal 为零、now 为负时不分发。
func (h *Hub) Publish(event Event, now int64) {
	if event.Goal == 0 || now < 0 {
		return
	}
	for _, component := range h.components {
		if h.index(component) >= 0 {
			component.Publish(event, now)
		}
	}
}

// index 返回组件的挂载位置，未挂载时返回 -1。系统数量很少，线性查找即可。
func (h *Hub) index(component *Component) int {
	for i, current := range h.components {
		if current == component {
			return i
		}
	}
	return -1
}

// Refresh 对所有挂载的系统组件调用 Refresh，适合玩家登录或跨天定时器统一触发。
func (h *Hub) Refresh(now int64) {
	for _, component := range h.components {
		if h.index(component) >= 0 {
			component.Refresh(now)
		}
	}
}
