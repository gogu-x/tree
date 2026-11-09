package task_test

import (
	"fmt"
	"time"

	"github.com/gogu-x/tree/task"
)

// 任务类型由全服统一定义，配置表里的任务类型字段直接使用这些编号。
const (
	GoalKillMonster uint32 = 1 // 击杀指定怪物，Params[0] 为怪物类型。
	GoalLevel       uint32 = 2 // 玩家等级达到 Target。
)

// 任务系统名称由业务统一定义为常量。
const (
	SystemDaily   task.System = "daily"   // 日常任务。
	SystemAchieve task.System = "achieve" // 成就。
)

// taskConfig 模拟某个系统的配置表行：任务类型、目标值、条件参数都由配置决定。
type taskConfig struct {
	ID       uint32
	TaskType uint32
	Target   int64
	Params   []int64
	MinLevel int64 // 系统自己的字段：解锁等级，任务库不读取。
}

// newTask 按配置创建任务实例；实例 ID 直接使用配置 ID。
func newTask(cfg taskConfig) task.Instance {
	return task.Instance{ID: uint64(cfg.ID), ConfigID: cfg.ID, Goal: cfg.TaskType, Target: cfg.Target, Params: cfg.Params}
}

// dailyConfigs 模拟日常任务配置表。
var dailyConfigs = []taskConfig{
	{ID: 101, TaskType: GoalKillMonster, Target: 3, Params: []int64{7}},               // 击杀 3 只 7 号怪。
	{ID: 102, TaskType: GoalKillMonster, Target: 5, Params: []int64{8}, MinLevel: 30}, // 30 级解锁。
	{ID: 103, TaskType: GoalKillMonster, Target: 1, Params: []int64{9}},               // 活动期间中途追加。
}

// achieveConfigs 模拟成就配置表。
var achieveConfigs = []taskConfig{
	{ID: 1, TaskType: GoalKillMonster, Target: 10, Params: []int64{7}}, // 累计击杀 10 只 7 号怪。
}

// player 模拟玩家：持有各系统的任务存档、组件和一个事件入口 Hub。
type player struct {
	uid          uint64
	level        int64
	dailyState   task.State // 日常任务存档，随玩家文档存盘。
	achieveState task.State // 成就任务存档。
	daily        *task.Component
	achieve      *task.Component
	tasks        task.Hub // 其他系统通过它发布任务事件。
	dirty        bool
	notices      []string
}

func (p *player) LogID() uint64 { return p.uid }   // 日志里带上玩家 ID。
func (p *player) MarkDirty()    { p.dirty = true } // 任务变化时安排存盘。

// dailyHooks 是日常系统的回调。
type dailyHooks struct{ task.BaseHooks }

// Generate 在每天刷新时决定今天的任务：按玩家当前等级筛选配置，规则完全由日常系统决定。
func (dailyHooks) Generate(owner task.Owner, _ task.Cycle) []task.Instance {
	p := owner.(*player)
	var tasks []task.Instance
	for _, cfg := range dailyConfigs {
		if cfg.ID != 103 && p.level >= cfg.MinLevel {
			tasks = append(tasks, newTask(cfg))
		}
	}
	return tasks
}

// Changed 在任务首次完成时通知客户端。
func (dailyHooks) Changed(owner task.Owner, change task.Change) {
	if change.FinishedNow() {
		p := owner.(*player)
		p.notices = append(p.notices, fmt.Sprintf("日常任务 %d 完成", change.After.ConfigID))
	}
}

// achieveHooks 是成就系统的回调：成就不刷新，只在达成时通知。
type achieveHooks struct{ task.BaseHooks }

func (achieveHooks) Changed(owner task.Owner, change task.Change) {
	if change.FinishedNow() {
		p := owner.(*player)
		p.notices = append(p.notices, fmt.Sprintf("成就 %d 达成", change.After.ConfigID))
	}
}

// todayIDs 返回日常任务的配置 ID 列表，按 ID 排序便于输出。
func todayIDs(component *task.Component) []uint32 {
	var ids []uint32
	for _, cfg := range dailyConfigs {
		if _, ok := component.Get(uint64(cfg.ID)); ok {
			ids = append(ids, cfg.ID)
		}
	}
	return ids
}

// Example 展示两个系统接入同一个玩家：每天自动刷新并按玩家数据生成任务、周期中途追加任务、
// 跨系统发布事件、完成通知、领取，以及不刷新的成就系统。
func Example() {
	loc := time.FixedZone("UTC+8", 8*3600)

	// ===== 启动：计数规则是提前写好的方法，按任务类型注册一次 =====
	goals, err := task.NewGoals(map[uint32]task.Handler{
		GoalKillMonster: task.AccumulateMatch,
		GoalLevel:       task.Maximum,
	})
	if err != nil {
		panic(err)
	}
	// ===== 启动：每个系统声明名字、回调和刷新周期，不需要提交配置表；日常每天 5 点刷新 =====
	dailyRegistry, err := task.NewRegistry(SystemDaily, goals, dailyHooks{}, task.WithPeriod(task.Daily(loc, 5*time.Hour)))
	if err != nil {
		panic(err)
	}
	achieveRegistry, err := task.NewRegistry(SystemAchieve, goals, achieveHooks{})
	if err != nil {
		panic(err)
	}

	// ===== 玩家加载：每个系统 Bind 自己的存档，挂到玩家的 Hub，再统一检查刷新 =====
	p := &player{uid: 10001, level: 20}
	if p.daily, _, err = task.Bind(dailyRegistry, &p.dailyState, p); err != nil {
		panic(err)
	}
	if p.achieve, _, err = task.Bind(achieveRegistry, &p.achieveState, p); err != nil {
		panic(err)
	}
	p.tasks.Attach(p.daily)
	p.tasks.Attach(p.achieve)

	day1 := time.Date(2026, 11, 9, 12, 0, 0, 0, loc).Unix()
	p.tasks.Refresh(day1) // 新玩家：日常进入第一个周期，Generate 按等级生成今天的任务。
	p.achieve.Add(newTask(achieveConfigs[0]), day1)
	fmt.Println("今天的日常:", todayIDs(p.daily))

	// ===== 周期中途：活动开启，日常系统自己追加一个任务，明天刷新时一起清掉 =====
	p.daily.Add(newTask(dailyConfigs[2]), day1)
	fmt.Println("追加后:", todayIDs(p.daily))

	// ===== 战斗系统结算后发布事件，不关心哪些系统在监听 =====
	p.tasks.Publish(task.Event{Goal: GoalKillMonster, Count: 3, Param: 7}, day1)
	fmt.Println(p.notices)

	// ===== 客户端领取：系统把结果映射成自己的错误码 =====
	grant := func(task.Owner, task.Instance) bool { return true } // 在这里加物品。
	fmt.Println("领取日常:", p.daily.Claim(101, day1, grant))
	fmt.Println("重复领取:", p.daily.Claim(101, day1, grant))
	fmt.Println("领取成就:", p.achieve.Claim(1, day1, grant))

	// ===== 玩家升到 30 级，第二天 6 点上线：自动刷新，按新等级生成；成就不受影响 =====
	p.level = 30
	day2 := time.Date(2026, 11, 10, 6, 0, 0, 0, loc).Unix()
	p.tasks.Refresh(day2)
	achieve, _ := p.achieve.Get(1)
	fmt.Println("第二天的日常:", todayIDs(p.daily), "成就进度:", achieve.Progress)

	// ===== 展示：任务自带目标值 =====
	fmt.Printf("成就进度: %d/%d, 需要存盘: %v\n", achieve.Progress, achieve.Target, p.dirty)

	// ===== 玩家下线：先从 Hub 卸载再关闭；存档由玩家自己保存 =====
	p.tasks.Detach(p.daily)
	p.tasks.Detach(p.achieve)
	p.daily.Close()
	p.achieve.Close()
	// Output:
	// 今天的日常: [101]
	// 追加后: [101 103]
	// [日常任务 101 完成]
	// 领取日常: ok
	// 重复领取: rewarded
	// 领取成就: not_finished
	// 第二天的日常: [101 102] 成就进度: 3
	// 成就进度: 3/10, 需要存盘: true
}
