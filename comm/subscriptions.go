package comm

// Subscriptions 是供单个 Actor 使用的带身份订阅索引，零值可用。
// 每个主题内身份唯一，保存订阅值而不是逐项回调；不保证调用顺序，不支持并发访问。
// 发布过程中删除的订阅立即失效，新增订阅不参与当前发布，但可参与嵌套发布。
// Publish 不复制订阅列表，普通增删均摊 O(1)，空主题会释放索引。
type Subscriptions[K comparable, I comparable, V any] struct {
	topics map[K]*subscriptionTopic[I, V]
}

type subscriptionEntry[I comparable, V any] struct {
	id     I
	value  V
	index  int
	active bool
}

type subscriptionTopic[I comparable, V any] struct {
	entries map[I]*subscriptionEntry[I, V]
	order   []*subscriptionEntry[I, V]
	depth   int
	dirty   bool
}

// Subscribe 注册一个身份，重复注册不覆盖已有订阅。
func (s *Subscriptions[K, I, V]) Subscribe(topic K, id I, value V) bool {
	if s.topics == nil {
		s.topics = make(map[K]*subscriptionTopic[I, V])
	}
	bucket := s.topics[topic]
	if bucket == nil {
		bucket = &subscriptionTopic[I, V]{entries: make(map[I]*subscriptionEntry[I, V])}
		s.topics[topic] = bucket
	}
	if _, exists := bucket.entries[id]; exists {
		return false
	}
	entry := &subscriptionEntry[I, V]{id: id, value: value, index: len(bucket.order), active: true}
	bucket.entries[id] = entry
	bucket.order = append(bucket.order, entry)
	return true
}

// Unsubscribe 取消身份订阅，并释放其持有的值。
func (s *Subscriptions[K, I, V]) Unsubscribe(topic K, id I) bool {
	bucket := s.topics[topic]
	if bucket == nil {
		return false
	}
	entry := bucket.entries[id]
	if entry == nil {
		return false
	}
	delete(bucket.entries, id)
	entry.active = false
	var zero V
	entry.value = zero
	if bucket.depth > 0 {
		bucket.dirty = true
		return true
	}
	last := len(bucket.order) - 1
	moved := bucket.order[last]
	bucket.order[entry.index] = moved
	moved.index = entry.index
	bucket.order[last] = nil
	bucket.order = bucket.order[:last]
	s.release(topic, bucket)
	return true
}

// Publish 同步访问发布开始时存在且尚未退订的值；即使回调 panic 也恢复索引状态。
func (s *Subscriptions[K, I, V]) Publish(topic K, visit func(V)) int {
	bucket := s.topics[topic]
	if bucket == nil || visit == nil {
		return 0
	}
	bucket.depth++
	defer func() {
		bucket.depth--
		if bucket.depth == 0 {
			bucket.compact()
			s.release(topic, bucket)
		}
	}()
	count := 0
	for _, entry := range bucket.order {
		if entry.active {
			count++
			visit(entry.value)
		}
	}
	return count
}

func (bucket *subscriptionTopic[I, V]) compact() {
	if !bucket.dirty {
		return
	}
	write := 0
	for _, entry := range bucket.order {
		if entry.active {
			entry.index = write
			bucket.order[write] = entry
			write++
		}
	}
	clear(bucket.order[write:])
	bucket.order = bucket.order[:write]
	bucket.dirty = false
}

func (s *Subscriptions[K, I, V]) release(topic K, bucket *subscriptionTopic[I, V]) {
	if len(bucket.entries) == 0 {
		// Clear 后可能已有同名新主题，旧发布不能删除新主题。
		if s.topics[topic] == bucket {
			delete(s.topics, topic)
			if len(s.topics) == 0 {
				s.topics = nil
			}
		}
		bucket.order = nil
		return
	}
	// 大量删除后压缩数组和哈希索引，避免一直保留历史峰值容量。
	if capacity := cap(bucket.order); capacity > 64 && len(bucket.order) < capacity/4 {
		bucket.order = append([]*subscriptionEntry[I, V](nil), bucket.order...)
		entries := make(map[I]*subscriptionEntry[I, V], len(bucket.order))
		for _, entry := range bucket.order {
			entries[entry.id] = entry
		}
		bucket.entries = entries
	}
}

// Count 返回指定主题当前有效订阅数，不创建空主题。
func (s *Subscriptions[K, I, V]) Count(topic K) int {
	if bucket := s.topics[topic]; bucket != nil {
		return len(bucket.entries)
	}
	return 0
}

// Clear 取消所有订阅，正在发布的剩余回调不会继续执行。
func (s *Subscriptions[K, I, V]) Clear() {
	for _, bucket := range s.topics {
		for _, entry := range bucket.entries {
			entry.active = false
			var zero V
			entry.value = zero
		}
		clear(bucket.entries)
		bucket.dirty = true
		if bucket.depth == 0 {
			bucket.order = nil
		}
	}
	s.topics = nil
}
