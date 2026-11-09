package comm

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"
)

func TestSubscriptionsIdentityAndRelease(t *testing.T) {
	var bus Subscriptions[int, int, int]
	if !bus.Subscribe(1, 10, 100) || bus.Subscribe(1, 10, 200) || !bus.Subscribe(2, 10, 300) {
		t.Fatal("订阅身份校验失败")
	}
	if bus.Count(1) != 1 || bus.Count(2) != 1 {
		t.Fatal("主题未隔离")
	}
	bus.Publish(1, func(value int) {
		if value != 100 {
			t.Fatal("重复注册覆盖了原值")
		}
	})
	if !bus.Unsubscribe(1, 10) || bus.Unsubscribe(1, 10) {
		t.Fatal("退订未幂等")
	}
	bus.Unsubscribe(2, 10)
	if bus.topics != nil {
		t.Fatal("空主题索引没有释放")
	}
	bus.Publish(99, func(int) { t.Fatal("空主题被调用") })
	if bus.topics != nil {
		t.Fatal("空发布创建了主题")
	}
}

func TestSubscriptionsReentrantSnapshot(t *testing.T) {
	var bus Subscriptions[int, int, int]
	bus.Subscribe(1, 1, 1)
	bus.Subscribe(1, 2, 2)
	var outer, inner []int
	bus.Publish(1, func(value int) {
		outer = append(outer, value)
		if value == 1 {
			bus.Unsubscribe(1, 2)
			bus.Subscribe(1, 3, 3)
			bus.Publish(1, func(nested int) { inner = append(inner, nested) })
		}
	})
	if !slices.Equal(outer, []int{1}) || !slices.Equal(inner, []int{1, 3}) {
		t.Fatalf("外层=%v, 内层=%v", outer, inner)
	}
	if bus.Count(1) != 2 {
		t.Fatal("嵌套发布损坏索引")
	}
	bus.Publish(1, func(value int) { bus.Unsubscribe(1, value) })
	if bus.topics != nil {
		t.Fatal("自退订后仍然持有索引")
	}
}

func TestSubscriptionsClearAndRecreate(t *testing.T) {
	var bus Subscriptions[int, int, int]
	bus.Subscribe(1, 1, 1)
	bus.Subscribe(1, 2, 2)
	if calls := bus.Publish(1, func(int) { bus.Clear(); bus.Subscribe(1, 9, 9) }); calls != 1 {
		t.Fatal(calls)
	}
	if bus.Count(1) != 1 {
		t.Fatal("旧主题收尾删除了新主题")
	}
	bus.Publish(1, func(value int) {
		if value != 9 {
			t.Fatal(value)
		}
	})
}

func TestSubscriptionsReplaceSameIdentity(t *testing.T) {
	var bus Subscriptions[int, int, int]
	bus.Subscribe(1, 1, 1)
	bus.Publish(1, func(int) { bus.Unsubscribe(1, 1); bus.Subscribe(1, 1, 2) })
	var values []int
	bus.Publish(1, func(value int) { values = append(values, value) })
	if !slices.Equal(values, []int{2}) {
		t.Fatal(values)
	}
}

func TestSubscriptionsPanicRecovery(t *testing.T) {
	var bus Subscriptions[int, int, int]
	bus.Subscribe(1, 1, 1)
	bus.Subscribe(1, 2, 2)
	func() {
		defer func() {
			if recover() == nil {
				t.Error("缺少预期 panic")
			}
		}()
		bus.Publish(1, func(value int) { bus.Unsubscribe(1, value); panic("预期回调异常") })
	}()
	if bus.Count(1) != 1 || bus.Publish(1, func(int) {}) != 1 {
		t.Fatal("panic 后索引不可用")
	}
	bus.Clear()
}

func TestSubscriptionsRandomLifecycle(t *testing.T) {
	var bus Subscriptions[int, int, int]
	model := make(map[int]bool)
	rng := rand.New(rand.NewSource(42))
	for range 10000 {
		id := rng.Intn(128)
		if rng.Intn(2) == 0 {
			if added := bus.Subscribe(1, id, id); added == model[id] {
				t.Fatal("添加与模型不符")
			}
			model[id] = true
		} else {
			if removed := bus.Unsubscribe(1, id); removed != model[id] {
				t.Fatal("删除与模型不符")
			}
			delete(model, id)
		}
		seen := make(map[int]bool)
		bus.Publish(1, func(value int) {
			if seen[value] || !model[value] {
				t.Fatal("重复或过期订阅")
			}
			seen[value] = true
		})
		if len(seen) != len(model) {
			t.Fatal("遗漏订阅")
		}
	}
}

func TestSubscriptionsShrinkAndNoPublishAllocation(t *testing.T) {
	var bus Subscriptions[int, int, int]
	for id := range 2048 {
		bus.Subscribe(1, id, id)
	}
	for id := range 2040 {
		bus.Unsubscribe(1, id)
	}
	if cap(bus.topics[1].order) > 64 {
		t.Fatal("历史峰值容量未压缩")
	}
	if allocs := testing.AllocsPerRun(1000, func() { bus.Publish(1, func(int) {}) }); allocs != 0 {
		t.Fatalf("发布存在分配: %v", allocs)
	}
}

func BenchmarkSubscriptionsPublish(b *testing.B) {
	for _, count := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			var bus Subscriptions[int, int, int]
			for id := range count {
				bus.Subscribe(1, id, id)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				bus.Publish(1, func(int) {})
			}
		})
	}
}

func BenchmarkSnapshotCopyPublish(b *testing.B) {
	for _, count := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			ids := make([]int, count)
			callbacks := make(map[int]func(), count)
			for id := range count {
				ids[id] = id
				callbacks[id] = func() {}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				snapshot := append([]int(nil), ids...)
				for _, id := range snapshot {
					if callback := callbacks[id]; callback != nil {
						callback()
					}
				}
			}
		})
	}
}
