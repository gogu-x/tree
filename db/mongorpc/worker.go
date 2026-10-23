package mongorpc

import (
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogu-x/tree/tlog"
)

// workerTask 是交给工作协程执行的一次任务。
type workerTask func()

// workerPool 维护固定数量的工作协程，每个协程独占一个有界任务队列。
// 同一分片的任务按提交顺序串行执行；不同分片之间并发执行、互不保证顺序。
// submit、broadcast、stop 只能由所属 Actor 协程调用。
type workerPool struct {
	queues []chan workerTask // 每个工作协程的任务队列，下标即分片号。
	wg     sync.WaitGroup    // 等待全部工作协程退出。
}

// newWorkerPool 创建并启动 count 个工作协程，每个队列缓冲 queueSize 个任务。
func newWorkerPool(count int, queueSize int) *workerPool {
	pool := &workerPool{queues: make([]chan workerTask, count)}
	for index := range pool.queues {
		queue := make(chan workerTask, queueSize)
		pool.queues[index] = queue
		pool.wg.Add(1)
		go pool.run(index, queue)
	}
	return pool
}

// size 返回工作协程数量。
func (pool *workerPool) size() int { return len(pool.queues) }

// run 是工作协程主循环，队列关闭且排空后退出。
func (pool *workerPool) run(index int, queue <-chan workerTask) {
	defer pool.wg.Done()
	for task := range queue {
		pool.execute(index, task)
	}
}

// execute 执行单个任务并兜底捕获 panic，避免一个任务拖垮整个工作协程。
func (pool *workerPool) execute(index int, task workerTask) {
	defer func() {
		if recovered := recover(); recovered != nil {
			tlog.Log.Error("[mongorpc/workerPool.execute] 工作协程任务异常, worker=%v panic=%v", index, recovered)
		}
	}()
	task()
}

// submit 将任务投递到指定分片；队列满时阻塞，形成对上游的背压。
func (pool *workerPool) submit(shard int, task workerTask) {
	pool.queues[shard] <- task
}

// broadcast 向每个分片投递一个屏障任务，全部分片执行到屏障后调用 done。
// done 在最后到达屏障的工作协程中执行。
func (pool *workerPool) broadcast(done func()) {
	remaining := int32(pool.size())
	for shard := range pool.queues {
		pool.submit(shard, func() {
			if atomic.AddInt32(&remaining, -1) == 0 {
				done()
			}
		})
	}
}

// stop 关闭全部队列，等待已提交任务执行完毕；超时返回 false。
func (pool *workerPool) stop(timeout time.Duration) bool {
	for _, queue := range pool.queues {
		close(queue)
	}
	finished := make(chan struct{})
	go func() {
		pool.wg.Wait()
		close(finished)
	}()
	select {
	case <-finished:
		return true
	case <-time.After(timeout):
		return false
	}
}

// shardOf 根据集合名和分片键计算分片号。key 为空时只按集合分片，
// 保证同一集合内无键请求（批量查询、建索引等）之间的顺序。
func shardOf(collection string, key interface{}, count int) int {
	if count <= 1 {
		return 0
	}
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(collection))
	if key != nil {
		_, _ = hasher.Write([]byte{0})
		writeShardKey(hasher, key)
	}
	return int(hasher.Sum32() % uint32(count))
}

// writeShardKey 将常见主键类型写入哈希；有符号与无符号整数按相同数值映射到相同分片。
func writeShardKey(hasher hash.Hash32, key interface{}) {
	var buffer [8]byte
	switch value := key.(type) {
	case string:
		_, _ = hasher.Write([]byte(value))
		return
	case int:
		binary.LittleEndian.PutUint64(buffer[:], uint64(value))
	case int32:
		binary.LittleEndian.PutUint64(buffer[:], uint64(value))
	case int64:
		binary.LittleEndian.PutUint64(buffer[:], uint64(value))
	case uint:
		binary.LittleEndian.PutUint64(buffer[:], uint64(value))
	case uint32:
		binary.LittleEndian.PutUint64(buffer[:], uint64(value))
	case uint64:
		binary.LittleEndian.PutUint64(buffer[:], value)
	default:
		_, _ = fmt.Fprint(hasher, value)
		return
	}
	_, _ = hasher.Write(buffer[:])
}
