// Package task 为日常、成就、活动等系统提供共用的任务组件。
//
// # 接入方式
//
// 启动时用 NewGoals 按任务类型注册计数规则，每个系统用 NewRegistry 声明名字、回调和刷新周期。
// 任务的目标值和条件参数来自系统配置表，创建任务时写入 Instance，并在此时订阅事件。
// 玩家加载时，每个系统 Bind 自己的 State 得到 Component，再挂到玩家的 Hub。
// 创建哪些任务由系统决定：带 WithPeriod 的系统在每个周期开始时由 Hooks.Generate 返回，任何时候也可以直接 Add。
// 其他系统调用 Hub.Publish 发布事件；任务变化通过 Hooks.Changed 通知所属系统；领取用 Claim。
// 周期刷新由库自动判断，其他刷新由系统调用 Reset、Remove 或 Expire。完整示例见 Example。
//
// # 职责边界
//
// Goals、Registry 创建后只读，可并发共享。Component、Hub 保存单个玩家的运行时订阅，只能串行使用。
// State 是任务存档；持久化、事件来源、创建和刷新规则、奖励内容都由系统负责。
//
// # 调用约定
//
// now 是服务端秒时间戳。Going 达标后变为 Finished，领取成功变为 Rewarded。
// Bind 不补算历史事件、不调用回调；Close 释放运行时引用，不删除或保存 State。
package task
