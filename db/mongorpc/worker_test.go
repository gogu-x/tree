package mongorpc

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/gogu-x/tree"
	"github.com/gogu-x/tree/tlog"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// useTestLogger 为测试设置静默日志，避免 tlog.Log 为空导致 panic。
func useTestLogger(t *testing.T) {
	t.Helper()
	logger, err := tlog.New("error", "", tlog.NoFlags)
	if err != nil {
		t.Fatal(err)
	}
	tlog.Log = logger
}

// drainPool 通过屏障等待池内此前提交的全部任务完成。
func drainPool(t *testing.T, pool *workerPool) {
	t.Helper()
	done := make(chan struct{})
	pool.broadcast(func() { close(done) })
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("broadcast barrier timed out")
	}
}

func TestWorkerPoolSameShardKeepsOrder(t *testing.T) {
	useTestLogger(t)
	pool := newWorkerPool(4, 16)
	defer pool.stop(time.Second)

	var got []int
	for index := 0; index < 1000; index++ {
		value := index
		pool.submit(2, func() { got = append(got, value) })
	}
	drainPool(t, pool)
	for index, value := range got {
		if value != index {
			t.Fatalf("order broken at %d: got %d", index, value)
		}
	}
	if len(got) != 1000 {
		t.Fatalf("expected 1000 tasks, got %d", len(got))
	}
}

func TestWorkerPoolShardsRunInParallel(t *testing.T) {
	useTestLogger(t)
	pool := newWorkerPool(2, 4)
	release := make(chan struct{})
	pool.submit(0, func() { <-release }) // 分片 0 被阻塞。

	finished := make(chan struct{})
	pool.submit(1, func() { close(finished) })
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("shard 1 should not wait for blocked shard 0")
	}
	close(release)
	if !pool.stop(time.Second) {
		t.Fatal("pool did not stop")
	}
}

func TestWorkerPoolBroadcastWaitsAllShards(t *testing.T) {
	useTestLogger(t)
	pool := newWorkerPool(4, 4)
	defer pool.stop(time.Second)

	var mu sync.Mutex
	completed := 0
	for shard := 0; shard < 4; shard++ {
		delay := time.Duration(shard*20) * time.Millisecond
		pool.submit(shard, func() {
			time.Sleep(delay)
			mu.Lock()
			completed++
			mu.Unlock()
		})
	}
	drainPool(t, pool)
	mu.Lock()
	defer mu.Unlock()
	if completed != 4 {
		t.Fatalf("barrier fired before all shards finished, completed=%d", completed)
	}
}

func TestWorkerPoolStopRunsQueuedTasks(t *testing.T) {
	useTestLogger(t)
	pool := newWorkerPool(3, 64)
	var mu sync.Mutex
	count := 0
	for index := 0; index < 150; index++ {
		pool.submit(index%3, func() {
			mu.Lock()
			count++
			mu.Unlock()
		})
	}
	if !pool.stop(5 * time.Second) {
		t.Fatal("pool did not stop in time")
	}
	if count != 150 {
		t.Fatalf("queued tasks lost on stop, executed=%d", count)
	}
}

func TestWorkerPoolStopTimeout(t *testing.T) {
	useTestLogger(t)
	pool := newWorkerPool(1, 1)
	release := make(chan struct{})
	defer close(release)
	pool.submit(0, func() { <-release })
	if pool.stop(50 * time.Millisecond) {
		t.Fatal("stop should time out while a task is blocked")
	}
}

func TestWorkerPoolRecoversPanic(t *testing.T) {
	useTestLogger(t)
	pool := newWorkerPool(1, 4)
	defer pool.stop(time.Second)
	pool.submit(0, func() { panic("boom") })
	ran := make(chan struct{})
	pool.submit(0, func() { close(ran) })
	select {
	case <-ran:
	case <-time.After(2 * time.Second):
		t.Fatal("worker died after panic")
	}
}

func TestShardOf(t *testing.T) {
	if shardOf("player", uint64(42), 1) != 0 {
		t.Fatal("single worker must always use shard 0")
	}
	first := shardOf("player", uint64(42), 8)
	for round := 0; round < 10; round++ {
		if shardOf("player", uint64(42), 8) != first {
			t.Fatal("shard must be stable for the same key")
		}
	}
	if shardOf("player", int64(42), 8) != first {
		t.Fatal("int64 and uint64 keys with the same value must share a shard")
	}
	if shardOf("activity", nil, 8) != shardOf("activity", nil, 8) {
		t.Fatal("nil key must shard by collection")
	}
	used := make(map[int]bool)
	for key := uint64(0); key < 1000; key++ {
		shard := shardOf("player", key, 8)
		if shard < 0 || shard >= 8 {
			t.Fatalf("shard out of range: %d", shard)
		}
		used[shard] = true
	}
	if len(used) != 8 {
		t.Fatalf("keys should spread over all shards, used=%d", len(used))
	}
}

func TestActorDrainAndPanicResponse(t *testing.T) {
	useTestLogger(t)
	system := tree.NewTree()
	defer system.Shutdown()
	// db 为空：执行任何数据库请求都会 panic，用于验证 panic 时请求方仍能收到错误回复。
	pid := system.SpawnOne(NewActor("mongo_nil_test", nil, WithWorkers(4)))

	if _, err := system.Request(pid, &FindOne{Collection: "docs", Key: uint64(1)}).AwaitTimeout(2 * time.Second); err == nil {
		t.Fatal("panicking operation should respond with an error")
	}
	if _, err := system.Request(pid, &Drain{}).AwaitTimeout(2 * time.Second); err != nil {
		t.Fatalf("drain failed: %v", err)
	}
}

// TestActorConcurrentUpserts 需要本地 Mongo：MONGO_TEST_URI=mongodb://127.0.0.1:27017 go test ./db/mongorpc/
// 验证多工作协程并发写入后数据完整，且同主键多次写入以最后一次为准。
func TestActorConcurrentUpserts(t *testing.T) {
	uri := os.Getenv("MONGO_TEST_URI")
	if uri == "" {
		t.Skip("MONGO_TEST_URI not set")
	}
	useTestLogger(t)
	dbName := fmt.Sprintf("tree_worker_test_%d", time.Now().UnixNano())
	db := Connect(uri, "", "", dbName)
	defer func() { _ = db.Drop(context.Background()) }()

	system := tree.NewTree()
	defer system.Shutdown()
	pid := system.SpawnOne(NewActor("mongo_worker_test", db, WithWorkers(8)))

	const docs = 500
	const rounds = 5
	envelopes := make([]*tree.Envelope, 0, docs*rounds)
	for round := 1; round <= rounds; round++ {
		for id := 0; id < docs; id++ {
			envelopes = append(envelopes, system.Request(pid, &UpdateOne{
				Collection: "docs",
				Filter:     bson.M{"_id": id},
				Update:     bson.M{"$set": bson.M{"round": round}},
				Upsert:     true,
				Key:        id,
			}))
		}
	}
	for _, envelope := range envelopes {
		if _, err := envelope.AwaitTimeout(30 * time.Second); err != nil {
			t.Fatalf("upsert failed: %v", err)
		}
	}
	if _, err := system.Request(pid, &Drain{}).AwaitTimeout(10 * time.Second); err != nil {
		t.Fatalf("drain failed: %v", err)
	}
	count, err := db.Collection("docs").CountDocuments(context.Background(), bson.M{"round": rounds})
	if err != nil {
		t.Fatal(err)
	}
	if count != docs {
		t.Fatalf("expected %d docs at final round, got %d", docs, count)
	}
}
