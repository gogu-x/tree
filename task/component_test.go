package task

import (
	"math"
	"reflect"
	"testing"

	"github.com/gogu-x/tree/tlog"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type testOwner struct {
	id    uint64 // 测试玩家编号。
	dirty int    // 脏标记次数。
}

func (o *testOwner) LogID() uint64 { return o.id }
func (o *testOwner) MarkDirty()    { o.dirty++ }

type testHooks struct {
	initial  func(Owner, Instance) (int64, bool) // 测试初始进度计算。
	changed  func(Owner, Change)                 // 测试变更处理。
	generate func(Owner, Cycle) []Instance       // 测试周期任务生成。
}

func (h *testHooks) Generate(owner Owner, cycle Cycle) []Instance {
	if h.generate != nil {
		return h.generate(owner, cycle)
	}
	return nil
}

func (h *testHooks) Initial(owner Owner, instance Instance) (int64, bool) {
	if h.initial != nil {
		return h.initial(owner, instance)
	}
	return 0, true
}

func (h *testHooks) Changed(owner Owner, change Change) {
	if h.changed != nil {
		h.changed(owner, change)
	}
}

func init() { _ = tlog.SetLevel("error") }

// 测试使用的任务系统名称。
const (
	systemTest    System = "test"
	systemDaily   System = "daily"
	systemCustom  System = "custom"
	systemEmpty   System = "empty"
	systemFirst   System = "first"
	systemSecond  System = "second"
	systemPlain   System = "plain"
	systemAchieve System = "achieve"
)

func testGoals(t testing.TB) *Goals {
	t.Helper()
	goals, err := NewGoals(map[uint32]Handler{1: AccumulateMatch, 2: Current, 3: Maximum, 4: Accumulate})
	if err != nil {
		t.Fatal(err)
	}
	return goals
}

// testConfigs 模拟系统配置表：配置编号 → 任务类型、目标值、条件参数。
var testConfigs = map[uint32]Instance{
	1: {Goal: 1, Target: 3, Params: []int64{7}}, // 击杀 3 只 7 号怪。
	2: {Goal: 2, Target: 100},                   // 当前值达到 100。
	3: {Goal: 3, Target: 10},                    // 历史最大值达到 10。
}

// spec 模拟系统创建任务时从配置表带上规则。
func spec(instance Instance) Instance {
	cfg := testConfigs[instance.ConfigID]
	instance.Goal, instance.Target, instance.Params = cfg.Goal, cfg.Target, cfg.Params
	return instance
}

func testRegistry(t testing.TB, hooks Hooks) *Registry {
	t.Helper()
	r, err := NewRegistry(systemTest, testGoals(t), hooks)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func mustBind(t testing.TB, r *Registry, state *State, owner Owner) *Component {
	t.Helper()
	component, dropped, err := Bind(r, state, owner)
	if err != nil || len(dropped) != 0 {
		t.Fatalf("绑定失败: err=%v dropped=%v", err, dropped)
	}
	return component
}

func testComponent(t testing.TB, hooks Hooks) (*Component, *State, *testOwner) {
	t.Helper()
	state := &State{}
	owner := &testOwner{id: 9}
	return mustBind(t, testRegistry(t, hooks), state, owner), state, owner
}

// requireTask 比较任务的编号、进度、状态和过期时间；规则字段由 TestAddStoresRule 单独检查。
func requireTask(t testing.TB, component *Component, expected Instance) {
	t.Helper()
	actual, exists := component.Get(expected.ID)
	actual.Goal, actual.Target, actual.Params = expected.Goal, expected.Target, expected.Params
	if !exists || !reflect.DeepEqual(actual, expected) {
		t.Fatalf("任务不匹配: got=%+v want=%+v", actual, expected)
	}
}

func grantOK(Owner, Instance) bool { return true }

func TestLifecycleAndReward(t *testing.T) {
	component, state, owner := testComponent(t, nil)
	if state.Tasks != nil || owner.dirty != 0 {
		t.Fatal("空绑定提前分配或修改存档")
	}
	if !component.Add(spec(Instance{ID: 1, ConfigID: 1}), 100) || component.Add(spec(Instance{ID: 1, ConfigID: 1}), 100) {
		t.Fatal("添加或重复编号校验失败")
	}
	if got := component.Claim(1, 100, grantOK); got != ClaimNotFinished {
		t.Fatalf("未完成任务领取结果=%v", got)
	}
	component.Publish(Event{Goal: 1, Count: 10, Param: 8}, 100)
	if owner.dirty != 1 {
		t.Fatal("不匹配事件标记了脏数据")
	}
	component.Publish(Event{Goal: 1, Count: 2, Param: 7}, 100)
	requireTask(t, component, Instance{ID: 1, ConfigID: 1, Progress: 2})
	component.Publish(Event{Goal: 1, Count: math.MaxInt64, Param: 7}, 100)
	finished := Instance{ID: 1, ConfigID: 1, Progress: 3, Status: Finished}
	requireTask(t, component, finished)
	if component.ListenerCount(1) != 0 {
		t.Fatal("完成任务未退订")
	}
	component.Publish(Event{Goal: 1, Count: 1, Param: 7}, 100)
	if owner.dirty != 3 {
		t.Fatal("完成任务仍被推进")
	}
	if got := component.Claim(1, 100, func(Owner, Instance) bool { return false }); got != ClaimGrantFailed {
		t.Fatalf("失败发奖结果=%v", got)
	}
	requireTask(t, component, finished)
	granted := 0
	grant := func(_ Owner, instance Instance) bool { granted++; return instance.Status == Rewarded }
	if component.Claim(1, 100, grant) != ClaimOK || component.Claim(1, 100, grant) != ClaimRewarded || granted != 1 {
		t.Fatal("领取幂等失败")
	}
	if component.Claim(99, 100, grant) != ClaimNotFound || component.Claim(1, -1, grant) != ClaimUnavailable {
		t.Fatal("领取结果分类错误")
	}
	if !component.Remove(1) || state.Tasks != nil || component.Remove(1) {
		t.Fatal("删除后任务表未释放或重复删除成功")
	}
}

func TestRegistryValidationAndIsolation(t *testing.T) {
	goals := testGoals(t)
	if _, err := NewGoals(map[uint32]Handler{0: Accumulate}); err == nil {
		t.Fatal("零编号任务类型被接受")
	}
	if _, err := NewRegistry("", goals, nil); err == nil {
		t.Fatal("空系统名被接受")
	}
	r, err := NewRegistry(systemDaily, goals, nil)
	if err != nil || r.Name() != systemDaily {
		t.Fatal("创建注册表失败")
	}
	one := mustBind(t, r, &State{}, &testOwner{id: 1})
	two := mustBind(t, r, &State{}, &testOwner{id: 2})
	one.Add(spec(Instance{ID: 1, ConfigID: 1}), 0)
	two.Add(spec(Instance{ID: 1, ConfigID: 1}), 0)
	one.Publish(Event{Goal: 1, Count: 3, Param: 7}, 0)
	requireTask(t, one, Instance{ID: 1, ConfigID: 1, Progress: 3, Status: Finished})
	requireTask(t, two, Instance{ID: 1, ConfigID: 1})
}

func TestAddStoresRule(t *testing.T) {
	component, state, _ := testComponent(t, nil)
	params := []int64{7}
	if !component.Add(Instance{ID: 1, Goal: 1, Target: 2, Params: params}, 0) {
		t.Fatal("不带配置编号的任务创建失败")
	}
	params[0] = 99 // 修改配置表不影响已创建的任务。
	got, _ := component.Get(1)
	if got.Goal != 1 || got.Target != 2 || !reflect.DeepEqual(got.Params, []int64{7}) {
		t.Fatalf("任务规则未保存或未复制: %+v", got)
	}
	got.Params[0] = 100 // 修改查询结果不影响任务。
	if state.Tasks[1].Params[0] != 7 {
		t.Fatal("Get 返回了底层参数切片")
	}
	invalid := []Instance{
		{ID: 2, Goal: 99, Target: 1}, // 任务类型未注册。
		{ID: 2, Goal: 1, Target: 0},  // 目标值非正。
		{ID: 0, Goal: 1, Target: 1},  // 编号为零。
	}
	for _, instance := range invalid {
		if component.Add(instance, 0) {
			t.Fatalf("无效任务被接受: %+v", instance)
		}
	}
	component.Publish(Event{Goal: 1, Count: 2, Param: 7}, 0)
	requireTask(t, component, Instance{ID: 1, Progress: 2, Status: Finished})
}

func TestBSONRestoreAndIndependentSnapshot(t *testing.T) {
	initialCalls := 0
	hooks := &testHooks{initial: func(Owner, Instance) (int64, bool) { initialCalls++; return 1, true }}
	component, state, _ := testComponent(t, hooks)
	component.Add(spec(Instance{ID: 1, ConfigID: 1}), 0)
	component.Add(spec(Instance{ID: 2, ConfigID: 1}), 0)
	component.Publish(Event{Goal: 1, Count: 2, Param: 7}, 0)
	component.Add(spec(Instance{ID: 3, ConfigID: 1}), 0)
	snapshot := state.DeepCopy()
	component.Publish(Event{Goal: 1, Count: 1, Param: 7}, 0)
	if snapshot.Tasks[3].Progress != 1 {
		t.Fatal("快照引用了活跃进度")
	}
	encoded, err := bson.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	var restored State
	if err := bson.Unmarshal(encoded, &restored); err != nil {
		t.Fatal(err)
	}
	owner := &testOwner{id: 10}
	runtime := mustBind(t, component.registry, &restored, owner)
	if initialCalls != 3 || runtime.ListenerCount(1) != 1 || owner.dirty != 0 {
		t.Fatal("恢复时重复初始化、订阅了完成任务或误标脏")
	}
	if !component.Close() || component.Publish(Event{Goal: 1}, 0) || component.Close() {
		t.Fatal("关闭后仍能发布或重复关闭")
	}
	if len(state.Tasks) != 3 {
		t.Fatal("关闭误删了持久化任务")
	}
	runtime.Publish(Event{Goal: 1, Count: 2, Param: 7}, 0)
	requireTask(t, runtime, Instance{ID: 3, ConfigID: 1, Progress: 3, Status: Finished})
}

func TestBindRejectsCorruptState(t *testing.T) {
	r := testRegistry(t, nil)
	valid := spec(Instance{ID: 1, ConfigID: 1})
	corrupt := []*Instance{nil}
	for _, change := range []func(*Instance){
		func(i *Instance) { i.ID = 2 },
		func(i *Instance) { i.Status = Status(99) },
		func(i *Instance) { i.Progress = -1 },
		func(i *Instance) { i.Progress = 3 },                     // 进行中却已达标。
		func(i *Instance) { i.Status, i.Progress = Finished, 1 }, // 完成但进度不足。
		func(i *Instance) { i.Target = 0 },
		func(i *Instance) { i.ExpireAt = -1 },
	} {
		instance := valid
		change(&instance)
		corrupt = append(corrupt, &instance)
	}
	for _, instance := range corrupt {
		owner := &testOwner{id: 1}
		unknown := Instance{ID: 5, Goal: 99, Target: 1}
		state := &State{Tasks: map[uint64]*Instance{1: instance, 5: &unknown}}
		if component, _, err := Bind(r, state, owner); err == nil || component != nil || owner.dirty != 0 || len(state.Tasks) != 2 {
			t.Fatalf("损坏存档被加载或修改: %+v", instance)
		}
	}
	if _, _, err := Bind(nil, &State{}, nil); err == nil {
		t.Fatal("空注册表绑定成功")
	}
}

func TestBindDropsUnknownGoal(t *testing.T) {
	r := testRegistry(t, nil)
	kept := spec(Instance{ID: 1, ConfigID: 1, Progress: 1})
	removed := Instance{ID: 2, ConfigID: 8, Goal: 99, Target: 5, Progress: 1} // 计数规则已从代码中删除。
	state := &State{Tasks: map[uint64]*Instance{1: &kept, 2: &removed}}
	owner := &testOwner{id: 1}
	component, dropped, err := Bind(r, state, owner)
	if err != nil || len(dropped) != 1 || dropped[0].ID != 2 || owner.dirty != 1 || len(state.Tasks) != 1 {
		t.Fatalf("删除未知任务类型错误: err=%v dropped=%v dirty=%v", err, dropped, owner.dirty)
	}
	requireTask(t, component, Instance{ID: 1, ConfigID: 1, Progress: 1})
	if component.ListenerCount(1) != 1 {
		t.Fatal("保留的任务未恢复订阅")
	}
}

func TestResetRestartsTask(t *testing.T) {
	var changes []Change
	initial := int64(0)
	hooks := &testHooks{
		initial: func(Owner, Instance) (int64, bool) { return initial, true },
		changed: func(_ Owner, change Change) { changes = append(changes, change) },
	}
	component, _, _ := testComponent(t, hooks)
	component.Add(spec(Instance{ID: 1, ConfigID: 1, ExpireAt: 50}), 0)
	component.Publish(Event{Goal: 1, Count: 3, Param: 7}, 0)
	component.Claim(1, 0, grantOK)
	if component.Reset(1, 10, 10) || component.Reset(99, 0, 10) {
		t.Fatal("非法过期时间或不存在的任务重置成功")
	}
	changes = nil
	if !component.Reset(1, 100, 60) {
		t.Fatal("重置失败")
	}
	requireTask(t, component, Instance{ID: 1, ConfigID: 1, ExpireAt: 100})
	if len(changes) != 1 || changes[0].Kind != Refreshed || changes[0].Before.Status != Rewarded || component.ListenerCount(1) != 1 {
		t.Fatalf("重置通知或订阅错误: %+v", changes)
	}
	component.Publish(Event{Goal: 1, Count: 1, Param: 7}, 60)
	requireTask(t, component, Instance{ID: 1, ConfigID: 1, Progress: 1, ExpireAt: 100})
	// 进行中任务重置时，初始进度直接达标。
	initial = 9
	changes = nil
	if !component.Reset(1, 0, 60) || component.ListenerCount(1) != 0 || !changes[0].FinishedNow() {
		t.Fatal("重置后直接完成未退订或未标记完成")
	}
	requireTask(t, component, Instance{ID: 1, ConfigID: 1, Progress: 3, Status: Finished})
	// Finished 任务重置为 Finished 也算重新达成。
	if !component.Reset(1, 0, 60) || !changes[1].FinishedNow() {
		t.Fatal("完成任务重置后未标记重新达成")
	}
}

func TestReentrantCompletionAndNewTask(t *testing.T) {
	var component *Component
	hooks := &testHooks{changed: func(_ Owner, change Change) {
		if change.Kind == Advanced && change.After.ID == 1 {
			if component.ListenerCount(1) != 1 {
				t.Fatal("完成通知前没有退订")
			}
			component.Remove(2)
			component.Add(spec(Instance{ID: 3, ConfigID: 1}), 0)
			component.Publish(Event{Goal: 1, Count: 1, Param: 7}, 0)
		}
	}}
	component, _, _ = testComponent(t, hooks)
	component.Add(spec(Instance{ID: 1, ConfigID: 1}), 0)
	component.Add(spec(Instance{ID: 2, ConfigID: 1}), 0)
	component.Publish(Event{Goal: 1, Count: 3, Param: 7}, 0)
	if _, exists := component.Get(2); exists {
		t.Fatal("被回调删除的任务仍存在")
	}
	requireTask(t, component, Instance{ID: 3, ConfigID: 1, Progress: 1})
}

func TestGrantReentryAndPanicRollback(t *testing.T) {
	component, _, owner := testComponent(t, nil)
	component.Add(spec(Instance{ID: 1, ConfigID: 1}), 0)
	component.Publish(Event{Goal: 1, Count: 3, Param: 7}, 0)
	func() {
		defer func() {
			if recover() == nil {
				t.Error("缺少发奖异常")
			}
		}()
		component.Claim(1, 0, func(Owner, Instance) bool {
			if component.Remove(1) || component.Publish(Event{Goal: 1, Count: 1}, 0) || component.Close() ||
				component.Add(spec(Instance{ID: 2, ConfigID: 1}), 0) || component.Reset(1, 0, 0) || component.Claim(1, 0, grantOK) != ClaimUnavailable {
				t.Fatal("发奖期间允许重入任务修改")
			}
			panic("发奖异常")
		})
	}()
	requireTask(t, component, Instance{ID: 1, ConfigID: 1, Progress: 3, Status: Finished})
	if owner.dirty != 2 || component.Claim(1, 0, grantOK) != ClaimOK {
		t.Fatal("异常后状态或锁未恢复")
	}
}

func TestExpirationAndInitialFailure(t *testing.T) {
	component, _, _ := testComponent(t, nil)
	if component.Add(spec(Instance{ID: 1, ConfigID: 1, ExpireAt: 10}), 10) {
		t.Fatal("接受过期任务")
	}
	component.Add(spec(Instance{ID: 1, ConfigID: 1, ExpireAt: 10}), 0)
	component.Publish(Event{Goal: 1, Count: 3, Param: 7}, 10)
	if _, exists := component.Get(1); exists || component.ListenerCount(1) != 0 {
		t.Fatal("过期任务推进或未退订")
	}
	component.Add(spec(Instance{ID: 2, ConfigID: 1, ExpireAt: 10}), 0)
	component.Publish(Event{Goal: 1, Count: 3, Param: 7}, 9)
	if got := component.Claim(2, 10, func(Owner, Instance) bool { t.Fatal("过期任务发奖"); return true }); got != ClaimExpired {
		t.Fatalf("过期任务领取结果=%v", got)
	}
	component.Add(spec(Instance{ID: 3, ConfigID: 1, ExpireAt: 20}), 10)
	component.Expire(20)
	if len(component.Snapshot()) != 0 {
		t.Fatal("批量过期未删除任务")
	}
	failed, state, owner := testComponent(t, &testHooks{initial: func(Owner, Instance) (int64, bool) { return 0, false }})
	if failed.Add(spec(Instance{ID: 1, ConfigID: 1}), 0) || state.Tasks != nil || owner.dirty != 0 {
		t.Fatal("初始进度失败留下了任务")
	}
}

func TestHandlersAndCustomPayload(t *testing.T) {
	component, _, _ := testComponent(t, nil)
	component.Add(spec(Instance{ID: 1, ConfigID: 2}), 0)
	component.Add(spec(Instance{ID: 2, ConfigID: 3}), 0)
	component.Publish(Event{Goal: 2, Count: 10}, 0)
	component.Publish(Event{Goal: 2, Count: 5}, 0)
	component.Publish(Event{Goal: 3, Count: 5}, 0)
	component.Publish(Event{Goal: 3, Count: 1}, 0)
	requireTask(t, component, Instance{ID: 1, ConfigID: 2, Progress: 5})
	requireTask(t, component, Instance{ID: 2, ConfigID: 3, Progress: 5})
	if len(component.Snapshot()) != 2 || !component.Publish(Event{Goal: 999, Count: 1}, 0) {
		t.Fatal("快照数量错误或未订阅类型发布失败")
	}
	goals, _ := NewGoals(map[uint32]Handler{99: func(_ Instance, event Event) (int64, bool) {
		value, ok := event.Data.(int64)
		return value, ok
	}})
	registry, _ := NewRegistry(systemCustom, goals, nil)
	custom := mustBind(t, registry, &State{}, nil)
	custom.Add(Instance{ID: 1, ConfigID: 1, Goal: 99, Target: 5}, 0)
	custom.Publish(Event{Goal: 99, Data: "错误参数"}, 0)
	custom.Publish(Event{Goal: 99, Data: int64(5)}, 0)
	requireTask(t, custom, Instance{ID: 1, ConfigID: 1, Progress: 5, Status: Finished})
}

func BenchmarkTaskPublishNoMatch(b *testing.B) {
	component, _, _ := testComponent(b, nil)
	for id := uint64(1); id <= 100; id++ {
		component.Add(spec(Instance{ID: id, ConfigID: 1}), 0)
	}
	b.ReportAllocs()
	for b.Loop() {
		component.Publish(Event{Goal: 1, Count: 1, Param: 8}, 0)
	}
}

func BenchmarkTaskProgress(b *testing.B) {
	component, _, _ := testComponent(b, nil)
	for id := uint64(1); id <= 100; id++ {
		component.Add(spec(Instance{ID: id, ConfigID: 2}), 0)
	}
	b.ReportAllocs()
	value := int64(1)
	for b.Loop() {
		component.Publish(Event{Goal: 2, Count: value}, 0)
		value = 3 - value
	}
}
