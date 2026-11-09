package task

import "fmt"

// System 是任务系统的名称，例如日常、成就、联盟每日。
// 由业务定义为常量统一管理，用于日志定位；同一进程内各系统应使用不同的名称。
type System string

// Registry 是一个系统（日常、成就、联盟任务等）的回调和可选刷新周期，创建后只读，可并发共享。
// 不保存任务配置：每个任务的目标值和条件参数由系统在创建任务时从自己的配置表带上。
type Registry struct {
	name   System // 系统名称，用于日志定位。
	goals  *Goals // 任务类型到计数规则的映射。
	hooks  Hooks  // 系统共享的回调，可为 nil。
	period Period // 刷新周期，nil 表示不自动刷新。
}

// Option 是创建 Registry 时的可选设置。
type Option func(*Registry)

// WithPeriod 让系统任务按周期自动刷新：进入新周期时删除上一周期的全部任务（通知 Expired），
// 再调用 Hooks.Generate 创建新周期的任务。周期中途仍可随时 Add。
// 同一系统里需要不同周期的任务（如每日、每周）时，分别建两个 Registry。
func WithPeriod(period Period) Option {
	return func(r *Registry) { r.period = period }
}

// NewRegistry 用系统名称、计数规则表、可选回调和设置创建注册表；名称为空或 goals 为空时返回错误。
func NewRegistry(name System, goals *Goals, hooks Hooks, options ...Option) (*Registry, error) {
	if name == "" || goals == nil {
		return nil, fmt.Errorf("创建任务注册表需要系统名和任务类型表, name=%q", name)
	}
	registry := &Registry{name: name, goals: goals, hooks: hooks}
	for _, option := range options {
		option(registry)
	}
	return registry, nil
}

// Name 返回系统名称。
func (r *Registry) Name() System { return r.name }
