package tree

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"
)

type orderedActor struct {
	name string

	mu    *sync.Mutex
	order *[]string
	query string
}

func (a *orderedActor) Name() string { return a.name }
func (a *orderedActor) OnInit(_ Context) {
	if a.query != "" {
		pid := MustLookup(a.query)
		value, err := Request(pid, "ready?").AwaitTimeout(time.Second)
		if err != nil || value != "ready" {
			panic(fmt.Sprintf("dependency request failed: value=%v err=%v", value, err))
		}
	}
	a.mu.Lock()
	*a.order = append(*a.order, a.name)
	a.mu.Unlock()
}
func (a *orderedActor) HandleMessage(ctx Context, _ interface{}) { ctx.Response("ready", nil) }
func (a *orderedActor) OnStop(Context)                           {}

func TestSpawnInitializesInArgumentOrder(t *testing.T) {
	if !isolatedGlobalTest(t) {
		return
	}
	var mu sync.Mutex
	var order []string
	Spawn(
		&orderedActor{name: "database", mu: &mu, order: &order},
		&orderedActor{name: "http", mu: &mu, order: &order, query: "database"},
	)
	if want := []string{"database", "http"}; !reflect.DeepEqual(order, want) {
		t.Fatalf("init order = %#v, want %#v", order, want)
	}
	Shutdown()
}

// TestConcurrentSpawnAcceptsOneBatch 验证并发固定注册只允许一批成功。
func TestConcurrentSpawnAcceptsOneBatch(t *testing.T) {
	if !isolatedGlobalTest(t) {
		return
	}
	defer Shutdown()
	var mu sync.Mutex
	var order []string
	start := make(chan struct{})
	results := make(chan bool, 2)
	var wg sync.WaitGroup
	for _, prefix := range []string{"a", "b"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if recover() != nil {
					results <- false
				}
			}()
			<-start
			Spawn(
				&orderedActor{name: prefix + "1", mu: &mu, order: &order},
				&orderedActor{name: prefix + "2", mu: &mu, order: &order},
			)
			results <- true
		}()
	}
	close(start)
	wg.Wait()
	if first, second := <-results, <-results; first == second {
		t.Fatalf("expected one accepted batch, got %v, %v", first, second)
	}
	valid := reflect.DeepEqual(order, []string{"a1", "a2"}) || reflect.DeepEqual(order, []string{"b1", "b2"})
	if !valid || SpawnCount() != 2 {
		t.Fatalf("unexpected registered batch: %v, count=%d", order, SpawnCount())
	}
}
