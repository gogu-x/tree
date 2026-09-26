package mongorpc

import (
	"fmt"
	"reflect"
	"time"

	"github.com/gogu-x/tree"
)

type recordKey struct {
	typeOf reflect.Type
	id     interface{}
}

type saveState struct {
	pending   interface{}
	callbacks []func(tree.Context, error)
}

// Lifecycle 统一协调不同文档模型的加载、快照存盘和批量存盘。
// 与 Store 相同，其方法应在所属 Actor goroutine 中调用。
type Lifecycle struct {
	store  *Store
	saving map[recordKey]*saveState
}

// NewLifecycle 创建基于通用 Mongo Store 的数据生命周期组件。
func NewLifecycle(store *Store) *Lifecycle {
	return &Lifecycle{store: store, saving: make(map[recordKey]*saveState)}
}

// Register 注册模型及其内存快照、遍历和存盘回调策略。
func (lifecycle *Lifecycle) Register(model Model) error {
	if lifecycle == nil || lifecycle.store == nil {
		return fmt.Errorf("database lifecycle has no store")
	}
	if model == nil {
		return fmt.Errorf("database model is nil")
	}
	return lifecycle.store.Register(model)
}

// LoadOne 加载已注册模型的一条数据。
func (lifecycle *Lifecycle) LoadOne(
	ctx tree.Context,
	prototype interface{},
	id interface{},
	callback func(tree.Context, interface{}, error),
) bool {
	if lifecycle == nil || lifecycle.store == nil {
		return false
	}
	return lifecycle.store.LoadOne(ctx, prototype, id, callback)
}

// LoadAll 加载已注册模型的全部匹配数据。
func (lifecycle *Lifecycle) LoadAll(
	ctx tree.Context,
	prototype interface{},
	filter interface{},
	callback func(tree.Context, interface{}, error),
) bool {
	if lifecycle == nil || lifecycle.store == nil {
		return false
	}
	return lifecycle.store.LoadAll(ctx, prototype, filter, callback)
}

// Save 对已注册模型创建快照并存盘；同一模型主键的并发写入会合并串行执行。
func (lifecycle *Lifecycle) Save(
	ctx tree.Context,
	prototype interface{},
	data interface{},
	callback func(tree.Context, error),
) bool {
	if lifecycle == nil || lifecycle.store == nil {
		return false
	}
	model, ok := lifecycle.store.modelFor(prototype)
	if !ok || data == nil || reflect.TypeOf(data) != modelType(prototype) {
		return false
	}
	id := model.GetKey(data)
	if id == nil {
		return false
	}
	snapshot := model.Clone(data)
	if snapshot == nil || reflect.TypeOf(snapshot) != modelType(prototype) {
		return false
	}
	key := recordKey{typeOf: modelType(prototype), id: id}
	if !reflect.TypeOf(id).Comparable() {
		return false
	}
	state, saving := lifecycle.saving[key]
	if !saving {
		state = &saveState{}
		lifecycle.saving[key] = state
	}
	if callback != nil {
		state.callbacks = append(state.callbacks, callback)
	}
	if saving {
		state.pending = snapshot
		return true
	}
	lifecycle.saveSnapshot(ctx, model, id, snapshot, state, key)
	return true
}

// SaveAll 存盘所有已注册且配置了内存遍历器的模型数据。
func (lifecycle *Lifecycle) SaveAll(ctx tree.Context, callback func(tree.Context, error)) {
	if lifecycle == nil || lifecycle.store == nil {
		if callback != nil {
			callback(ctx, fmt.Errorf("database lifecycle is not initialized"))
		}
		return
	}
	type saveItem struct {
		model Model
		data  interface{}
	}
	items := make([]saveItem, 0)
	var firstErr error
	for _, model := range lifecycle.store.models {
		model.Range(func(data interface{}) bool {
			items = append(items, saveItem{model: model, data: data})
			return true
		})
	}
	remaining := len(items)
	if remaining == 0 {
		if callback != nil {
			callback(ctx, nil)
		}
		return
	}
	completed := false
	finish := func(callbackCtx tree.Context) {
		if remaining != 0 || completed {
			return
		}
		completed = true
		if callback != nil {
			callback(callbackCtx, firstErr)
		}
	}
	for _, item := range items {
		if lifecycle.Save(ctx, item.model.GetPrototype(), item.data, func(callbackCtx tree.Context, err error) {
			if err != nil && firstErr == nil {
				firstErr = err
			}
			remaining--
			finish(callbackCtx)
		}) {
			continue
		}
		if firstErr == nil {
			firstErr = fmt.Errorf("save request could not be scheduled, collection=%s", item.model.GetCollection())
		}
		remaining--
		finish(ctx)
	}
	finish(ctx)
}

// SaveInterval 返回注册模型中最短的非零自动存盘周期。
func (lifecycle *Lifecycle) SaveInterval() time.Duration {
	if lifecycle == nil || lifecycle.store == nil {
		return 0
	}
	var interval time.Duration
	for _, model := range lifecycle.store.models {
		modelInterval := model.GetSaveInterval()
		if modelInterval > 0 && (interval == 0 || modelInterval < interval) {
			interval = modelInterval
		}
	}
	return interval
}

func (lifecycle *Lifecycle) saveSnapshot(
	ctx tree.Context,
	model Model,
	id interface{},
	snapshot interface{},
	state *saveState,
	key recordKey,
) {
	if !lifecycle.store.Save(ctx, model.GetPrototype(), id, snapshot, func(callbackCtx tree.Context, err error) {
		if state.pending != nil {
			pending := state.pending
			state.pending = nil
			lifecycle.saveSnapshot(callbackCtx, model, id, pending, state, key)
			return
		}
		if err == nil {
			if afterSaveErr := model.AfterSave(callbackCtx, snapshot); afterSaveErr != nil {
				err = afterSaveErr
			}
		}
		delete(lifecycle.saving, key)
		for _, pendingCallback := range state.callbacks {
			pendingCallback(callbackCtx, err)
		}
	}) {
		delete(lifecycle.saving, key)
		for _, pendingCallback := range state.callbacks {
			pendingCallback(ctx, fmt.Errorf("save request could not be sent, collection=%s", model.GetCollection()))
		}
	}
}
