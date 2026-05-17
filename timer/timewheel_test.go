package timer_test

import (
	"fmt"
	"goAcotr/timer"
	"sync/atomic"
	"testing"
	"time"
)

// 基本触发测试
func TestTimeWheelAfterFunc(t *testing.T) {
	tw := timer.NewTimeWheel(64)
	defer tw.Stop()

	done := make(chan struct{})
	tw.AfterFunc(5*time.Millisecond, func() {
		fmt.Println("My name is Leaf")
		close(done)
	})

	select {
	case cb := <-tw.ChanTimer:
		cb()
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timer did not fire")
	}
	<-done
}

// Stop 后不应触发
func TestTimeWheelStop(t *testing.T) {
	tw := timer.NewTimeWheel(64)
	defer tw.Stop()

	fired := make(chan struct{}, 1)
	wt := tw.AfterFunc(2*time.Second, func() {
		//fired <- struct{}{}
		fmt.Println("stop no timer")
	})
	wt.Stop()

	for {
		select {
		case cb := <-tw.ChanTimer:
			cb()
		case <-fired:
			t.Fatal("stopped timer should not fire")
		case <-time.After(200 * time.Millisecond):
		}
	}

}

// 大量定时器并发测试（模拟10000玩家）
func TestTimeWheelMassTimers(t *testing.T) {
	const n = 10000
	tw := timer.NewTimeWheel(n + 1024)
	defer tw.Stop()

	var count int64
	for i := 0; i < n; i++ {
		tw.AfterFunc(100*time.Millisecond, func() {
			atomic.AddInt64(&count, 1)
			fmt.Println("count", atomic.LoadInt64(&count))
		})
	}

	// 从 ChanTimer 消费（模拟 Skeleton Run 循环）
	deadline := time.After(2 * time.Second)
	for {
		select {
		case cb := <-tw.ChanTimer:
			cb()
		case <-deadline:
			t.Fatalf("only %d/%d timers fired", atomic.LoadInt64(&count), n)
		}
	}
}

// 精度测试：误差应在 2 个 tick（20ms）以内
func TestTimeWheelPrecision(t *testing.T) {
	tw := timer.NewTimeWheel(64)
	defer tw.Stop()

	want := 100 * time.Millisecond
	start := time.Now()
	done := make(chan struct{})
	tw.AfterFunc(want, func() { close(done) })

	select {
	case cb := <-tw.ChanTimer:
		cb()
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timer did not fire")
	}
	<-done

	elapsed := time.Since(start)
	if elapsed < want || elapsed > want+20*time.Millisecond {
		t.Fatalf("precision out of range: elapsed=%v want=%v~%v", elapsed, want, want+20*time.Millisecond)
	}
}
