package task

import (
	"testing"
	"time"
)

var shanghai = time.FixedZone("UTC+8", 8*3600)

// at 返回上海时区的秒时间戳。
func at(year int, month time.Month, day, hour int) int64 {
	return time.Date(year, month, day, hour, 0, 0, 0, shanghai).Unix()
}

func TestPeriodBounds(t *testing.T) {
	daily := Daily(shanghai, 5*time.Hour)
	// 2026-11-09 是周一。
	cases := []struct {
		name       string
		period     Period
		now        int64
		start, end int64
	}{
		{"每日刷新点之后", daily, at(2026, 11, 9, 12), at(2026, 11, 9, 5), at(2026, 11, 10, 5)},
		{"每日刷新点之前算前一天", daily, at(2026, 11, 9, 4), at(2026, 11, 8, 5), at(2026, 11, 9, 5)},
		{"每日跨月", daily, at(2026, 12, 1, 3), at(2026, 11, 30, 5), at(2026, 12, 1, 5)},
		{"每周周中", Weekly(shanghai, time.Monday, 0), at(2026, 11, 12, 10), at(2026, 11, 9, 0), at(2026, 11, 16, 0)},
		{"每周周日", Weekly(shanghai, time.Monday, 0), at(2026, 11, 15, 23), at(2026, 11, 9, 0), at(2026, 11, 16, 0)},
		{"每周刷新点之前算上一周", Weekly(shanghai, time.Monday, 5*time.Hour), at(2026, 11, 9, 4), at(2026, 11, 2, 5), at(2026, 11, 9, 5)},
	}
	for _, c := range cases {
		if start, end := c.period.Bounds(c.now); start != c.start || end != c.end {
			t.Errorf("%s: got [%v,%v) want [%v,%v)", c.name, start, end, c.start, c.end)
		}
	}
}

// dailyTask 模拟日常系统从配置表读出的任务：任务类型 4（累加），目标值来自配置。
func dailyTask(id uint64, target int64) Instance {
	return Instance{ID: id, ConfigID: uint32(id), Goal: 4, Target: target}
}

// levelOwner 模拟按玩家等级生成任务的系统参数。
type levelOwner struct {
	testOwner
	level int64
}

func periodComponent(t *testing.T, hooks *testHooks, state *State, owner Owner) *Component {
	t.Helper()
	r, err := NewRegistry(systemDaily, testGoals(t), hooks, WithPeriod(Daily(shanghai, 5*time.Hour)))
	if err != nil {
		t.Fatal(err)
	}
	return mustBind(t, r, state, owner)
}

func TestPeriodRefreshGeneratesByOwnerData(t *testing.T) {
	var cycles []Cycle
	var kinds []Kind
	hooks := &testHooks{
		generate: func(owner Owner, cycle Cycle) []Instance {
			cycles = append(cycles, cycle)
			// 系统按自己的参数决定本周期的任务：等级 10 以上多一个难任务。
			tasks := []Instance{dailyTask(1, 3)}
			if owner.(*levelOwner).level >= 10 {
				tasks = append(tasks, dailyTask(2, 10))
			}
			return tasks
		},
		changed: func(_ Owner, change Change) { kinds = append(kinds, change.Kind) },
	}
	owner := &levelOwner{level: 5}
	state := &State{}
	component := periodComponent(t, hooks, state, owner)

	day1 := at(2026, 11, 9, 12)
	if !component.Refresh(day1) || len(cycles) != 1 || len(component.Snapshot()) != 1 || state.CycleStart != at(2026, 11, 9, 5) {
		t.Fatalf("首次刷新错误: cycles=%v tasks=%v", cycles, component.Snapshot())
	}
	// 同一周期内重复调用不刷新；周期中途按需新增任务。
	component.Refresh(day1 + 3600)
	if len(cycles) != 1 || !component.Add(dailyTask(3, 1), day1+3600) {
		t.Fatal("同周期重复刷新或中途新增失败")
	}
	component.Publish(Event{Goal: 4, Count: 3}, day1)
	if component.Claim(1, day1, grantOK) != ClaimOK {
		t.Fatal("领取失败")
	}

	// 玩家升级后进入下一天：Publish 自动刷新，旧任务全部删除，新任务按当前等级生成，事件推进新任务。
	owner.level = 12
	kinds = nil
	day2 := at(2026, 11, 10, 6)
	component.Publish(Event{Goal: 4, Count: 1}, day2)
	requireTask(t, component, Instance{ID: 1, ConfigID: 1, Progress: 1})
	requireTask(t, component, Instance{ID: 2, ConfigID: 2, Progress: 1})
	if _, exists := component.Get(3); exists || len(cycles) != 2 || cycles[1].Start != at(2026, 11, 10, 5) {
		t.Fatalf("跨周期刷新错误: cycles=%v", cycles)
	}
	expired := 0
	for _, kind := range kinds {
		if kind == Expired {
			expired++
		}
	}
	if expired != 2 {
		t.Fatalf("旧任务未通知 Expired: %v", kinds)
	}

	// 重新加载同一周期的存档不会重复生成；时间回拨也不会重复刷新。
	reloaded := periodComponent(t, hooks, state, owner)
	reloaded.Refresh(day2 + 60)
	reloaded.Refresh(day1)
	if len(cycles) != 2 {
		t.Fatal("重新加载或时间回拨后重复刷新")
	}
}

func TestPeriodRefreshClaimAfterCycleAndReentry(t *testing.T) {
	var component *Component
	hooks := &testHooks{
		generate: func(Owner, Cycle) []Instance { return []Instance{dailyTask(1, 3)} },
		changed: func(_ Owner, change Change) {
			// 回调中再次调用组件不会重复刷新。
			if change.Kind == Expired {
				component.Publish(Event{Goal: 4, Count: 1}, at(2026, 11, 10, 6))
			}
		},
	}
	owner := &testOwner{id: 1}
	component = periodComponent(t, hooks, &State{}, owner)
	day1 := at(2026, 11, 9, 12)
	component.Publish(Event{Goal: 4, Count: 3}, day1)
	// 第二天领取前一天完成未领的任务：周期已切换，旧任务已删除。
	if got := component.Claim(1, at(2026, 11, 10, 6), grantOK); got != ClaimNotFinished {
		t.Fatalf("跨周期领取结果=%v", got)
	}
	requireTask(t, component, Instance{ID: 1, ConfigID: 1})
}

func TestHubRefreshAndNoPeriod(t *testing.T) {
	generated := 0
	hooks := &testHooks{generate: func(Owner, Cycle) []Instance { generated++; return nil }}
	periodic := periodComponent(t, hooks, &State{}, nil)
	plainRegistry, _ := NewRegistry(systemPlain, testGoals(t), hooks)
	plain := mustBind(t, plainRegistry, &State{}, nil)
	var hub Hub
	hub.Attach(periodic)
	hub.Attach(plain)
	hub.Refresh(at(2026, 11, 9, 12))
	if generated != 1 || !plain.Refresh(at(2026, 11, 9, 12)) || plain.Refresh(-1) {
		t.Fatalf("Hub 刷新或无周期系统行为错误: generated=%v", generated)
	}
}
