// Package database 提供可注册多种文档模型的通用 Mongo 数据访问库。
package mongorpc

import (
	"fmt"
	"reflect"
	"time"

	"github.com/gogu-x/tree"
	"github.com/gogu-x/tree/tlog"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Model 描述一个 Mongo 集合中的文档类型和主键字段。
type Model interface {
	GetCollection() string                     // Mongo 集合名称。
	GetPrototype() interface{}                 // 指向文档结构体的指针。
	GetIDField() string                        // Mongo 主键字段名称。
	GetKey(interface{}) interface{}            // 从文档提取主键。
	Clone(interface{}) interface{}             // 创建可异步存盘的快照。
	Range(func(interface{}) bool)              // 遍历当前内存中的文档。
	GetSaveInterval() time.Duration            // 自动存盘周期；零表示不参与周期调度。
	AfterSave(tree.Context, interface{}) error // 单条存盘完成后的模型回调。
	MackDirty() bool                           // 是否持有脏标记
}

// Store 按 Go 文档类型管理集合映射，并提供通用加载和存盘操作。
type Store struct {
	mongoActorName string
	models         map[reflect.Type]Model
}

// NewStore 构造绑定指定 Mongo Actor 的数据访问库。
func NewStore(mongoActorName string) *Store {
	return &Store{mongoActorName: mongoActorName, models: make(map[reflect.Type]Model)}
}

// Register 注册一个文档模型。
func (store *Store) Register(model Model) error {
	if model == nil {
		return fmt.Errorf("invalid database model")
	}
	dataType := reflect.TypeOf(model.GetPrototype())
	if model.GetCollection() == "" || dataType == nil || dataType.Kind() != reflect.Pointer || dataType.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("invalid database model")
	}
	if model.GetIDField() == "" {
		return fmt.Errorf("model id field is required")
	}
	store.models[dataType] = model
	return nil
}

// LoadOne 按已注册模型的主键加载单条文档。
func (store *Store) LoadOne(
	ctx tree.Context,
	prototype interface{},
	id interface{},
	callback func(tree.Context, interface{}, error),
) bool {
	model, ok := store.modelFor(prototype)
	if !ok || callback == nil {
		return false
	}
	if mongoPID, ok := ctx.Lookup(store.mongoActorName); ok {
		result := reflect.New(modelType(prototype).Elem()).Interface()
		request := &FindOne{
			Collection: model.GetCollection(),
			Filter:     bson.M{model.GetIDField(): id},
			Result:     result,
		}
		if envelope := ctx.RequestCallback(mongoPID, request, callback); envelope != nil {
			return true
		}
	}
	tlog.Log.Error("[db/Store.LoadOne] Mongo Actor 不可用或单条加载请求未发送, collection=%v", model.GetCollection())
	return false
}

// LoadAll 按过滤条件加载已注册模型的所有匹配文档。
// callback 收到的结果是指向 []T 的指针，其中 T 是 Prototype 对应的文档类型。
func (store *Store) LoadAll(
	ctx tree.Context,
	prototype interface{},
	filter interface{},
	callback func(tree.Context, interface{}, error),
) bool {
	model, ok := store.modelFor(prototype)
	if !ok || callback == nil {
		return false
	}
	if mongoPID, ok := ctx.Lookup(store.mongoActorName); ok {
		result := reflect.New(reflect.SliceOf(modelType(prototype).Elem())).Interface()
		request := &FindMany{
			Collection: model.GetCollection(),
			Filter:     filter,
			Result:     result,
		}
		if envelope := ctx.RequestCallback(mongoPID, request, callback); envelope != nil {
			return true
		}
	}
	tlog.Log.Error("[db/Store.LoadAll] Mongo Actor 不可用或批量加载请求未发送, collection=%v", model.GetCollection())
	return false
}

// Save 按主键保存已注册模型的数据，并在集合中不存在时插入文档。
func (store *Store) Save(
	ctx tree.Context,
	prototype interface{},
	id interface{},
	data interface{},
	callback func(tree.Context, error),
) bool {
	model, ok := store.modelFor(prototype)
	if !ok || callback == nil || reflect.TypeOf(data) != modelType(prototype) {
		return false
	}
	if mongoPID, ok := ctx.Lookup(store.mongoActorName); ok {
		encoded, err := bson.Marshal(data)
		if err != nil {
			tlog.Log.Error("[db/Store.Save] 数据 BSON 序列化失败, collection=%v err=%v", model.GetCollection(), err)
			return false
		}
		document := bson.M{}
		if err := bson.Unmarshal(encoded, &document); err != nil {
			tlog.Log.Error("[db/Store.Save] BSON 文档解析失败, collection=%v err=%v", model.GetCollection(), err)
			return false
		}
		delete(document, model.GetIDField())
		request := &UpdateOne{
			Collection: model.GetCollection(),
			Filter:     bson.M{model.GetIDField(): id},
			Update:     bson.M{"$set": document},
			Upsert:     true,
		}
		if envelope := ctx.RequestCallback(mongoPID, request, func(callbackCtx tree.Context, _ interface{}, err error) {
			callback(callbackCtx, err)
		}); envelope != nil {
			return true
		}
	}
	tlog.Log.Error("[db/Store.Save] Mongo Actor 不可用或保存请求未发送, collection=%v", model.GetCollection())
	return false
}

func (store *Store) modelFor(prototype interface{}) (Model, bool) {
	if store == nil || prototype == nil {
		return nil, false
	}
	model, ok := store.models[modelType(prototype)]
	return model, ok
}

func modelType(prototype interface{}) reflect.Type {
	if prototype == nil {
		return nil
	}
	return reflect.TypeOf(prototype)
}
