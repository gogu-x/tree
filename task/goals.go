package task

import "fmt"

// Goals 是任务类型表：任务类型编号 → 计数规则。计数规则是提前写好的方法，启动时注册一次；
// 每个任务的目标值和条件参数来自系统配置，在创建任务时提供。创建后只读，可并发共享。
type Goals struct {
	handlers       map[uint32]Handler // 任务类型对应的计数规则。
	defaultHandler Handler            // 可选公共规则，由任务自身的非零Goal选择类型。
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
func (g *Goals) handler(goal uint32) Handler {
	if goal == 0 {
		return nil
	}
	if handler := g.handlers[goal]; handler != nil {
		return handler
	}
	return g.defaultHandler
}

// NewGoalsWithDefault 使用公共规则处理任务自身声明的非零类型。
// Add 根据实例Goal订阅事件，Bind 根据存档Goal恢复订阅，无需预先列举类型。
// 与NewGoals的显式类型模式独立，不改变已有系统对未注册类型的处理。
func NewGoalsWithDefault(handler Handler) (*Goals, error) {
	if handler == nil {
		return nil, fmt.Errorf("公共任务规则不能为空")
	}
	return &Goals{defaultHandler: handler}, nil
}
