package task

import "math"

// Accumulate 累加非负数量，溢出时饱和；最终进度由组件限制到目标值。
func Accumulate(current Instance, event Event) (int64, bool) {
	count := event.GetCount()
	progress := current.GetProgress()
	if count <= 0 {
		return progress, false
	}
	if count > math.MaxInt64-progress {
		return math.MaxInt64, true
	}
	return progress + count, true
}

// Maximum 记录历史最大状态值。
func Maximum(current Instance, event Event) (int64, bool) {
	return max(current.GetProgress(), event.GetCount()), event.GetCount() >= 0
}

// Current 使用事件提供的当前状态值，完成前允许进度降低。
func Current(_ Instance, event Event) (int64, bool) {
	return event.GetCount(), event.GetCount() >= 0
}

// AccumulateMatch 在事件参数精确匹配任务首个条件参数时累加，不隐式使用零值通配。
func AccumulateMatch(current Instance, event Event) (int64, bool) {
	parameter, ok := current.Param(0)
	if !ok || parameter != event.GetParam() {
		return current.GetProgress(), false
	}
	return Accumulate(current, event)
}

// Once 在收到事件时把进度设置为一，忽略事件数量和参数；通常配合 Target=1。
func Once(Instance, Event) (int64, bool) { return 1, true }
