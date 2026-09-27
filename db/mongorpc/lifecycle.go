package mongorpc

import (
	"fmt"
	"reflect"
	"time"

	"github.com/gogu-x/tree"
	"github.com/gogu-x/tree/timer"
	"github.com/gogu-x/tree/tlog"
)

type recordKey struct {
	typeOf reflect.Type
	id     interface{}
}

type saveState struct {
	pending   interface{}
	callbacks []func(tree.Context, error)
}

type saveItem struct {
	model Model
	data  interface{}
}

// Lifecycle 统一协调不同文档模型的加载、快照存盘和批量存盘。
// 与 Store 相同，其方法应在所属 Actor goroutine 中调用。
type Lifecycle struct {
	store   *Store
	saving  map[recordKey]*saveState
	Ticker  *timer.TimeWheel // Ticker 在所属 Actor 上调度已注册模型的自动存盘。
	context tree.Context
	stopped bool
}

// NewLifecycle 创建基于通用 Mongo Store 的数据生命周期组件。
func NewLifecycle(
	store *Store,
	ticker *timer.TimeWheel,
	ctx tree.Context,
) *Lifecycle {
	lifecycle := &Lifecycle{
		store:   store,
		saving:  make(map[recordKey]*saveState),
		Ticker:  ticker,
		context: ctx,
	}
	return lifecycle
}

// Register 注册模型及其内存快照、遍历和存盘回调策略。
func (lifecycle *Lifecycle) Register(model Model) error {
	if lifecycle == nil || lifecycle.store == nil {
		return fmt.Errorf("database lifecycle has no store")
	}
	if model == nil {
		return fmt.Errorf("database model is nil")
	}
	lifecycle.Ticker.Register(model.GetTickerType(), lifecycle.onModelTimer)
	lifecycle.scheduleModel(model)
	return lifecycle.store.Register(model)
}

func (lifecycle *Lifecycle) scheduleModel(model Model) error {
	interval := model.GetSaveInterval()
	if interval <= 0 {
		return nil
	}
	ticker := lifecycle.Ticker
	if lifecycle.stopped || ticker == nil || lifecycle.context == nil {
		return fmt.Errorf("model %s has save interval but lifecycle timer is not configured", model.GetCollection())
	}
	dataType := modelType(model.GetPrototype())
	if ticker.After(model.GetTickerType(), interval, dataType) == nil {
		return fmt.Errorf("could not schedule model save timer, collection=%s", model.GetCollection())
	}
	return nil
}

func (lifecycle *Lifecycle) onModelTimer(data interface{}) {
	if lifecycle.stopped {
		return
	}
	dataType, ok := data.(reflect.Type)
	if !ok {
		return
	}
	model, exists := lifecycle.store.models[dataType]
	if !exists {
		return
	}
	lifecycle.saveModel(lifecycle.context, model, func(_ tree.Context, err error) {
		if err != nil {
			tlog.Log.Error("[db/Lifecycle.onModelTimer] 模型定时存盘失败, collection=%s err=%v", model.GetCollection(), err)
		} else {
			tlog.Log.Info("[db/Lifecycle.onModelTimer] 模型定时存盘完成, collection=%s", model.GetCollection())
		}
		if interval := model.GetSaveInterval(); !lifecycle.stopped && interval > 0 {
			if lifecycle.Ticker.After(model.GetTickerType(), interval, dataType) == nil {
				tlog.Log.Error("[db/Lifecycle.onModelTimer] 重设模型存盘定时器失败, collection=%s", model.GetCollection())
			}
		}
	})
}

// Stop 停止生命周期组件后续的自动存盘调度。
func (lifecycle *Lifecycle) Stop() {
	if lifecycle != nil {
		lifecycle.stopped = true
	}
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
	dirty := model.MackDirty(data)
	if !dirty {
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
	if lifecycle == nil {
		if callback != nil {
			callback(ctx, fmt.Errorf("database lifecycle is not initialized"))
		}
		return
	}
	store := lifecycle.store
	if store == nil {
		if callback != nil {
			callback(ctx, fmt.Errorf("database lifecycle is not initialized"))
		}
		return
	}
	models := make([]Model, 0, len(store.models))
	for _, model := range store.models {
		models = append(models, model)
	}
	items := make([]saveItem, 0)
	for _, model := range models {
		model.Range(func(data interface{}) bool {
			items = append(items, saveItem{model: model, data: data})
			return true
		})
	}
	lifecycle.saveItems(ctx, items, callback)
}

func (lifecycle *Lifecycle) saveModel(ctx tree.Context, model Model, callback func(tree.Context, error)) {
	items := make([]saveItem, 0)
	model.Range(func(data interface{}) bool {
		items = append(items, saveItem{model: model, data: data})
		return true
	})
	lifecycle.saveItems(ctx, items, callback)
}

func (lifecycle *Lifecycle) saveItems(ctx tree.Context, items []saveItem, callback func(tree.Context, error)) {
	var firstErr error
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
