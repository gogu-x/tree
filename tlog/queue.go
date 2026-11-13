package tlog

import "sync/atomic"

const (
	logQueueSize    = 4096 // 每个输出目标最多缓存的日志条数，必须是 2 的幂。
	enqueueAttempts = 8    // 限制生产者 CAS 重试次数，竞争过大时计数丢弃而不自旋等待。
)

// logSlot 通过序号发布数据并标识槽位可复用时机。
type logSlot struct {
	sequence atomic.Uint64
	record   atomic.Pointer[logRecord]
}

// logQueue 是多生产者、单消费者的有界原子队列，不使用互斥锁或发送通道。
// 成功预留位置的生产者只执行原子写入，不调用用户代码；消费者只由输出协程调用。
type logQueue struct {
	head  atomic.Uint64
	next  uint64
	slots [logQueueSize]logSlot
}

// newLogQueue 初始化每个槽位的首轮序号。
func newLogQueue() *logQueue {
	queue := &logQueue{}
	for i := range queue.slots {
		queue.slots[i].sequence.Store(uint64(i))
	}
	return queue
}

// push 最多尝试有限次；满队列或激烈竞争都直接返回 false。
func (queue *logQueue) push(record *logRecord) bool {
	for attempt := 0; attempt < enqueueAttempts; attempt++ {
		pos := queue.head.Load()
		slot := &queue.slots[pos&(logQueueSize-1)]
		diff := int64(slot.sequence.Load() - pos)
		if diff < 0 {
			return false
		}
		if diff != 0 || !queue.head.CompareAndSwap(pos, pos+1) {
			continue
		}
		slot.record.Store(record)
		slot.sequence.Store(pos + 1)
		return true
	}
	return false
}

// pop 只读取已发布的数据；生产者尚未发布时立即返回 nil，稍后由消费者重试。
func (queue *logQueue) pop() *logRecord {
	pos := queue.next
	slot := &queue.slots[pos&(logQueueSize-1)]
	if slot.sequence.Load() != pos+1 {
		return nil
	}
	record := slot.record.Swap(nil)
	slot.sequence.Store(pos + logQueueSize)
	queue.next++
	return record
}
