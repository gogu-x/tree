package tree

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"
)

type orderedActor struct {
	name   string
	system *Tree
	mu     *sync.Mutex
	order  *[]string
	query  string
}

func (a *orderedActor) Name() string { return a.name }
func (a *orderedActor) OnInit(_ Context) {
	if a.query != "" {
		pid := a.system.MustLookup(a.query)
		value, err := a.system.Request(pid, "ready?").AwaitTimeout(time.Second)
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
	system := NewTree()
	var mu sync.Mutex
	var order []string
	system.Spawn(
		&orderedActor{name: "database", system: system, mu: &mu, order: &order},
		&orderedActor{name: "http", system: system, mu: &mu, order: &order, query: "database"},
	)
	if want := []string{"database", "http"}; !reflect.DeepEqual(order, want) {
		t.Fatalf("init order = %#v, want %#v", order, want)
	}
	system.Shutdown()
}

func TestConcurrentSpawnBatchesDoNotInterleaveInitialization(t *testing.T) {
	system := NewTree()
	var mu sync.Mutex
	var order []string
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, prefix := range []string{"a", "b"} {
		prefix := prefix
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			system.Spawn(
				&orderedActor{name: prefix + "1", system: system, mu: &mu, order: &order},
				&orderedActor{name: prefix + "2", system: system, mu: &mu, order: &order},
			)
		}()
	}
	close(start)
	wg.Wait()
	mu.Lock()
	got := append([]string(nil), order...)
	mu.Unlock()
	valid := reflect.DeepEqual(got, []string{"a1", "a2", "b1", "b2"}) || reflect.DeepEqual(got, []string{"b1", "b2", "a1", "a2"})
	if !valid {
		t.Fatalf("spawn batches interleaved: %#v", got)
	}
	system.Shutdown()
}
