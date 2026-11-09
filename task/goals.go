package task

import "fmt"

// Goals 是任务类型表：任务类型编号 → 计数规则。计数规则是提前写好的方法，启动时注册一次；
// 每个任务的目标值和条件参数来自系统配置，在创建任务时提供。创建后只读，可并发共享。
type Goals struct {
	handlers map[uint32]Handler // 任务类型对应的计数规则。
}

// NewGoals 复制传入的任务类型表；编号为零或计数规则为空时返回错误。
func NewGoals(handlers map[uint32]Handler) (*Goals, error) {
	copied := make(map[uint32]Handler, len(handlers))
	for goal, handler := range handlers {
		if goal == 0 || handler == nil {
			return nil, fmt.Errorf("任务类型无效, goal=%v, handler为空=%v", goal, handler == nil)
		}
		copied[goal] = handler
	}
	return &Goals{handlers: copied}, nil
}

// handler 返回任务类型对应的计数规则，未注册时返回 nil。
func (g *Goals) handler(goal uint32) Handler { return g.handlers[goal] }
