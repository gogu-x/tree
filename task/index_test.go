package task

import (
	"fmt"
	"runtime"
	"testing"
)

// TestPublishReentrySafety 覆盖分组切片在回调重入时的边界：关闭、重置、删除同组任务、嵌套发布。
func TestPublishReentrySafety(t *testing.T) {
	t.Run("回调中关闭组件后停止分发", func(t *testing.T) {
		var component *Component
		visited := 0
		hooks := &testHooks{changed: func(_ Owner, change Change) {
			if change.Kind == Advanced {
				visited++
				component.Close()
			}
		}}
		component = mustBind(t, testRegistry(t, hooks), &State{}, nil)
		for id := uint64(1); id <= 3; id++ {
			component.Add(Instance{ID: id, Goal: 4, Target: 9}, 0)
		}
		visited = 0
		if !component.Publish(Event{Goal: 4, Count: 1}, 0) || visited != 1 || component.ListenerCount(4) != 0 {
			t.Fatalf("关闭后仍继续分发: visited=%v", visited)
		}
	})
	t.Run("回调中重置同组任务不会被重复推进", func(t *testing.T) {
		var component *Component
		hooks := &testHooks{changed: func(_ Owner, change Change) {
			if change.Kind == Advanced && change.After.ID == 1 {
				component.Reset(1, 0, 0)
			}
		}}
		component = mustBind(t, testRegistry(t, hooks), &State{}, nil)
		component.Add(Instance{ID: 1, Goal: 4, Target: 9}, 0)
		component.Add(Instance{ID: 2, Goal: 4, Target: 9}, 0)
		component.Publish(Event{Goal: 4, Count: 1}, 0)
		requireTask(t, component, Instance{ID: 1})
		requireTask(t, component, Instance{ID: 2, Progress: 1})
		if component.ListenerCount(4) != 2 {
			t.Fatalf("重置后订阅数错误: %v", component.ListenerCount(4))
		}
	})
	t.Run("回调中删除同组和其他组任务后立即跳过并压缩", func(t *testing.T) {
		var component *Component
		nested := false
		hooks := &testHooks{changed: func(_ Owner, change Change) {
			if change.Kind == Advanced && change.After.ID == 1 && !nested {
				nested = true
				component.Remove(2)
				component.Remove(10)
				component.Publish(Event{Goal: 4, Count: 1}, 0) // 嵌套发布同一类型。
			}
		}}
		component = mustBind(t, testRegistry(t, hooks), &State{}, nil)
		for id := uint64(1); id <= 3; id++ {
			component.Add(Instance{ID: id, Goal: 4, Target: 9}, 0)
		}
		component.Add(Instance{ID: 10, Goal: 3, Target: 9}, 0)
		component.Publish(Event{Goal: 4, Count: 1}, 0)
		requireTask(t, component, Instance{ID: 1, Progress: 2})
		requireTask(t, component, Instance{ID: 3, Progress: 2})
		if _, exists := component.Get(2); exists || component.ListenerCount(4) != 2 || component.ListenerCount(3) != 0 {
			t.Fatal("删除的任务仍被推进或分组未压缩")
		}
		if len(component.going[4]) != 2 || component.going[3] != nil || component.holes {
			t.Fatalf("分组切片未压缩: %v", component.going)
		}
	})
}

// manyGoalsComponent 创建一个有 tasks 个进行中任务、平均分布在 goals 种类型上的组件；类型 goals+1 已注册但没有任务。
func manyGoalsComponent(tb testing.TB, tasks, goals int) *Component {
	tb.Helper()
	handlers := map[uint32]Handler{}
	for goal := 1; goal <= goals+1; goal++ {
		handlers[uint32(goal)] = Accumulate
	}
	g, err := NewGoals(handlers)
	if err != nil {
		tb.Fatal(err)
	}
	r, _ := NewRegistry(systemTest, g, nil)
	component, _, _ := Bind(r, &State{}, nil)
	for id := 1; id <= tasks; id++ {
		component.Add(Instance{ID: uint64(id), Goal: uint32(id%goals + 1), Target: 1 << 40}, 0)
	}
	return component
}

// BenchmarkTask500Going 模拟成就类系统：一个玩家 500 个长期进行中的任务。
func BenchmarkTask500Going(b *testing.B) {
	for _, goals := range []int{1, 20, 100} {
		b.Run(fmt.Sprintf("goals=%d/matched", goals), func(b *testing.B) {
			component := manyGoalsComponent(b, 500, goals)
			b.ReportAllocs()
			for b.Loop() {
				component.Publish(Event{Goal: 1, Count: 1}, 0)
			}
		})
		b.Run(fmt.Sprintf("goals=%d/no_task", goals), func(b *testing.B) {
			component := manyGoalsComponent(b, 500, goals)
			b.ReportAllocs()
			for b.Loop() {
				component.Publish(Event{Goal: uint32(goals + 1), Count: 1}, 0)
			}
		})
	}
}

// BenchmarkTaskMemory 报告每个任务的常驻内存（存档 + 运行时），用于回归对比。
func BenchmarkTaskMemory(b *testing.B) {
	for _, c := range []struct{ tasks, goals int }{{5, 5}, {20, 10}, {100, 30}, {500, 100}} {
		b.Run(fmt.Sprintf("tasks=%d/goals=%d", c.tasks, c.goals), func(b *testing.B) {
			const players = 200
			var perTask float64
			for b.Loop() {
				b.StopTimer()
				runtime.GC()
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				b.StartTimer()
				components := make([]*Component, players)
				for i := range components {
					components[i] = manyGoalsComponent(b, c.tasks, c.goals)
				}
				b.StopTimer()
				runtime.GC()
				runtime.ReadMemStats(&after)
				perTask = float64(after.HeapAlloc-before.HeapAlloc) / float64(players*c.tasks)
				runtime.KeepAlive(components)
				b.StartTimer()
			}
			b.ReportMetric(perTask, "B/task")
		})
	}
}
