package task

import "testing"

func TestHubDispatchAcrossSystems(t *testing.T) {
	goals := testGoals(t)
	owner := &testOwner{id: 1}
	var completed []System
	makeSystem := func(name System, target int64) *Component {
		hooks := &testHooks{changed: func(_ Owner, change Change) {
			if change.FinishedNow() {
				completed = append(completed, name)
			}
		}}
		registry, err := NewRegistry(name, goals, hooks)
		if err != nil {
			t.Fatal(err)
		}
		component := mustBind(t, registry, &State{}, owner)
		component.Add(Instance{ID: 1, ConfigID: 1, Goal: 4, Target: target}, 0)
		return component
	}
	daily, achievement := makeSystem(systemDaily, 2), makeSystem(systemAchieve, 5)
	// 第三个系统没有订阅任何任务类型，分发时不报错。
	empty, _ := NewRegistry(systemEmpty, goals, nil)
	idle := mustBind(t, empty, &State{}, owner)

	var hub Hub
	if !hub.Attach(daily) || !hub.Attach(achievement) || !hub.Attach(idle) || hub.Attach(daily) || hub.Attach(nil) {
		t.Fatal("挂载结果错误")
	}
	hub.Publish(Event{Goal: 4, Count: 2}, 0)
	requireTask(t, daily, Instance{ID: 1, ConfigID: 1, Progress: 2, Status: Finished})
	requireTask(t, achievement, Instance{ID: 1, ConfigID: 1, Progress: 2})
	if len(completed) != 1 || completed[0] != systemDaily {
		t.Fatalf("完成通知错误: %v", completed)
	}
	if !hub.Detach(achievement) || hub.Detach(achievement) {
		t.Fatal("卸载结果错误")
	}
	hub.Publish(Event{Goal: 4, Count: 3}, 0)
	requireTask(t, achievement, Instance{ID: 1, ConfigID: 1, Progress: 2})
}

func TestHubDetachDuringPublish(t *testing.T) {
	goals := testGoals(t)
	var hub Hub
	var second *Component
	hooks := &testHooks{changed: func(Owner, Change) { hub.Detach(second) }}
	firstRegistry, _ := NewRegistry(systemFirst, goals, hooks)
	secondRegistry, _ := NewRegistry(systemSecond, goals, nil)
	first := mustBind(t, firstRegistry, &State{}, nil)
	second = mustBind(t, secondRegistry, &State{}, nil)
	first.Add(Instance{ID: 1, ConfigID: 1, Goal: 4, Target: 9}, 0)
	second.Add(Instance{ID: 1, ConfigID: 1, Goal: 4, Target: 9}, 0)
	hub.Attach(first)
	hub.Attach(second)
	hub.Publish(Event{Goal: 4, Count: 1}, 0)
	requireTask(t, second, Instance{ID: 1, ConfigID: 1})
}

func TestSharedRegistryAcrossComponents(t *testing.T) {
	registry := testRegistry(t, nil)
	for range 16 {
		t.Run("独立组件", func(t *testing.T) {
			t.Parallel()
			component := mustBind(t, registry, &State{}, nil)
			if !component.Add(spec(Instance{ID: 1, ConfigID: 1}), 0) {
				t.Fatal("创建失败")
			}
			component.Publish(Event{Goal: 1, Count: 3, Param: 7}, 0)
			requireTask(t, component, Instance{ID: 1, ConfigID: 1, Progress: 3, Status: Finished})
			component.Close()
		})
	}
}
