// Package mongorpc Package mongo provides a common MongoDB Actor that serializes all database
// operations to prevent concurrency issues. Any process needing MongoDB should
// Spawn one Actor and interact via messages.
//
// Actor 协程只负责分发：每个请求按 (集合, Key) 哈希投递到固定的工作协程执行并回复。
// 顺序保证：同一集合内 Key 相同的请求、以及同一集合内 Key 为空的请求，按到达顺序串行执行；
// 其余请求之间并发执行、不保证顺序。需要“先写后读”的调用方必须等待写请求回调后再发读请求。
// 工作协程数为 1（默认）时退化为全局串行，与旧行为一致。
package mongorpc

import (
	"context"
	"fmt"
	"time"

	"github.com/gogu-x/tree"
	"github.com/gogu-x/tree/tlog"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// InsertOne inserts a document. Response: (insertedID, error)
type InsertOne struct {
	Collection string
	Doc        interface{}
	Key        interface{} // 分片键，通常为文档主键；为空时按集合分片。
}

// FindOne queries a single document, decoding into Result. Response: error
type FindOne struct {
	Collection string
	Filter     interface{}
	Result     interface{}
	Key        interface{} // 分片键，通常为文档主键；为空时按集合分片。
}

// FindMany 根据查询条件加载全部匹配文档。
type FindMany struct {
	Collection string      // Mongo 集合名称。
	Filter     interface{} // Mongo 查询条件。
	Result     interface{} // 指向文档切片的指针。
	Key        interface{} // 分片键；为空时按集合分片。
}

// UpdateOne updates a single document. Response: error
type UpdateOne struct {
	Collection string
	Filter     interface{}
	Update     interface{}
	Upsert     bool
	Key        interface{} // 分片键，通常为文档主键；为空时按集合分片。
}

// DeleteOne deletes a single document. Response: error
type DeleteOne struct {
	Collection string
	Filter     interface{}
	Key        interface{} // 分片键，通常为文档主键；为空时按集合分片。
}

// CreateIndexes 为集合创建索引；定义相同的已有索引会被跳过。Response: ([]string 索引名, error)
// 按集合分片，与同集合的无键请求（如 FindMany）保持提交顺序。
type CreateIndexes struct {
	Collection string             // Mongo 集合名称。
	Indexes    []mongo.IndexModel // 要创建的索引定义。
}

// Drain 等待此前已提交到本 Actor 的全部请求执行完成后回复。Response: (nil, nil)
// 用于关服前确认写请求已落库。
type Drain struct{}

const (
	// opTimeout 表示普通数据库请求的超时时间。
	opTimeout = 8 * time.Second
	// indexTimeout 表示建索引请求的超时时间；首次在大集合上建索引耗时较长。
	indexTimeout = 60 * time.Second
	// defaultWorkerCount 表示默认工作协程数量，为 1 时与旧版全局串行行为一致。
	defaultWorkerCount = 1
	// defaultWorkerQueueSize 表示每个工作协程任务队列的默认缓冲大小。
	defaultWorkerQueueSize = 1024
	// defaultStopTimeout 表示 Actor 停止时等待已提交任务完成的默认时长。
	defaultStopTimeout = 30 * time.Second
	// mailboxSize 表示 Mongo Actor 邮箱缓冲大小。
	mailboxSize = 20 * 1024
)

// operation 是可由工作协程执行的数据库请求。
type operation interface {
	collectionName() string                          // 请求所属集合。
	shardKey() interface{}                           // 分片键，为空时按集合分片。
	execute(db *mongo.Database) (interface{}, error) // 在工作协程中执行请求。
}

// actorOptions 保存 Mongo Actor 的可选配置。
type actorOptions struct {
	workers     int           // 工作协程数量。
	queueSize   int           // 每个工作协程的任务队列缓冲大小。
	stopTimeout time.Duration // 停止时等待已提交任务完成的时长。
}

// Option 配置 Mongo Actor。
type Option func(*actorOptions)

// WithWorkers 设置工作协程数量；小于 1 时忽略。建议不超过驱动连接池上限（默认 100）。
func WithWorkers(count int) Option {
	return func(opts *actorOptions) {
		if count > 0 {
			opts.workers = count
		}
	}
}

// WithWorkerQueueSize 设置每个工作协程的任务队列缓冲大小；小于 1 时忽略。
func WithWorkerQueueSize(size int) Option {
	return func(opts *actorOptions) {
		if size > 0 {
			opts.queueSize = size
		}
	}
}

// WithStopTimeout 设置停止时等待已提交任务完成的时长；不大于 0 时忽略。
func WithStopTimeout(timeout time.Duration) Option {
	return func(opts *actorOptions) {
		if timeout > 0 {
			opts.stopTimeout = timeout
		}
	}
}

// Actor 是 MongoDB 访问 Actor：Actor 协程分发请求，工作协程执行并回复。
type Actor struct {
	name    string          // Actor 注册名称。
	router  tree.Router     // 请求类型路由。
	db      *mongo.Database // 目标数据库，驱动对象可并发使用。
	options actorOptions    // 工作协程配置。
	pool    *workerPool     // 工作协程池，OnInit 中创建。
}

// NewActor 创建 MongoDB Actor。name 由调用方指定（如 constant.ActorGameMongo /
// constant.ActorPlatformMongo），因为同一进程可能需要多个库各一个 Actor。
func NewActor(name string, db *mongo.Database, opts ...Option) *Actor {
	config := actorOptions{
		workers:     defaultWorkerCount,
		queueSize:   defaultWorkerQueueSize,
		stopTimeout: defaultStopTimeout,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&config)
		}
	}
	return &Actor{name: name, db: db, options: config}
}

// Name 返回 Actor 注册名称。
func (a *Actor) Name() string { return a.name }

// MailboxSize 返回 Actor 邮箱缓冲大小。
func (a *Actor) MailboxSize() int { return mailboxSize }

// OnInit 注册请求路由并启动工作协程。
func (a *Actor) OnInit(_ tree.Context) {
	a.router.Register(&InsertOne{}, a.dispatch)
	a.router.Register(&FindOne{}, a.dispatch)
	a.router.Register(&FindMany{}, a.dispatch)
	a.router.Register(&UpdateOne{}, a.dispatch)
	a.router.Register(&DeleteOne{}, a.dispatch)
	a.router.Register(&CreateIndexes{}, a.dispatch)
	a.router.Register(&Drain{}, a.onDrain)
	a.pool = newWorkerPool(a.options.workers, a.options.queueSize)
	tlog.Log.Info("[mongorpc/Actor.OnInit] Mongo Actor 已就绪, name=%v db=%v workers=%v queueSize=%v",
		a.name, a.dbName(), a.options.workers, a.options.queueSize)
}

// HandleMessage 将请求交给路由分发。
func (a *Actor) HandleMessage(ctx tree.Context, msg interface{}) {
	a.router.Route(ctx, msg)
}

// OnStop 关闭任务队列并等待已分发任务执行完毕。邮箱中尚未分发的消息不会被处理，
// 关服前应先通过 Drain 确认写请求已完成。
func (a *Actor) OnStop(_ tree.Context) {
	if a.pool == nil {
		return
	}
	if !a.pool.stop(a.options.stopTimeout) {
		tlog.Log.Error("[mongorpc/Actor.OnStop] 等待工作协程退出超时, name=%v timeout=%v", a.name, a.options.stopTimeout)
		return
	}
	tlog.Log.Info("[mongorpc/Actor.OnStop] 工作协程已全部退出, name=%v", a.name)
}

// dispatch 按 (集合, 分片键) 将请求投递到对应工作协程，回复由工作协程完成。
func (a *Actor) dispatch(ctx tree.Context, msg interface{}) {
	op, ok := msg.(operation)
	if !ok {
		tlog.Log.Error("[mongorpc/Actor.dispatch] 不支持的请求类型, type=%T", msg)
		return
	}
	envelope := ctx.RequestEnvelope()
	shard := shardOf(op.collectionName(), op.shardKey(), a.pool.size())
	a.pool.submit(shard, func() {
		a.runOperation(op, envelope)
	})
}

// onDrain 在全部工作协程处理完此前已分发的请求后回复。
func (a *Actor) onDrain(ctx tree.Context, _ interface{}) {
	envelope := ctx.RequestEnvelope()
	a.pool.broadcast(func() {
		if envelope != nil {
			envelope.Respond(nil, nil)
		}
	})
}

// runOperation 在工作协程中执行请求并回复；执行 panic 时回复错误，避免请求方永久等待。
func (a *Actor) runOperation(op operation, envelope *tree.Envelope) {
	defer func() {
		if recovered := recover(); recovered != nil {
			tlog.Log.Error("[mongorpc/Actor.runOperation] 数据库请求执行异常, collection=%v type=%T panic=%v",
				op.collectionName(), op, recovered)
			if envelope != nil {
				envelope.Respond(nil, fmt.Errorf("mongo operation panic: %v", recovered))
			}
		}
	}()
	value, err := op.execute(a.db)
	if envelope != nil {
		envelope.Respond(value, err)
	}
}

// dbName 返回数据库名称，数据库未配置时返回空串。
func (a *Actor) dbName() string {
	if a.db == nil {
		return ""
	}
	return a.db.Name()
}

func (m *InsertOne) collectionName() string { return m.Collection }
func (m *InsertOne) shardKey() interface{}  { return m.Key }

// execute 插入单条文档并返回插入的主键。
func (m *InsertOne) execute(db *mongo.Database) (interface{}, error) {
	dbCtx, cancel := bg()
	defer cancel()
	res, err := db.Collection(m.Collection).InsertOne(dbCtx, m.Doc)
	if err != nil {
		tlog.Log.Error("[mongorpc/InsertOne] 插入文档失败, collection=%v err=%v", m.Collection, err)
		return nil, err
	}
	return res.InsertedID, nil
}

func (m *FindOne) collectionName() string { return m.Collection }
func (m *FindOne) shardKey() interface{}  { return m.Key }

// execute 查询单条文档并解码到 Result。
func (m *FindOne) execute(db *mongo.Database) (interface{}, error) {
	dbCtx, cancel := bg()
	defer cancel()
	err := db.Collection(m.Collection).FindOne(dbCtx, m.Filter).Decode(m.Result)
	return m.Result, err
}

func (m *FindMany) collectionName() string { return m.Collection }
func (m *FindMany) shardKey() interface{}  { return m.Key }

// execute 查询全部匹配文档并解码到 Result。
func (m *FindMany) execute(db *mongo.Database) (interface{}, error) {
	dbCtx, cancel := bg()
	defer cancel()
	cursor, err := db.Collection(m.Collection).Find(dbCtx, m.Filter)
	if err != nil {
		tlog.Log.Error("[mongorpc/FindMany] 集合查询失败, collection=%v err=%v", m.Collection, err)
		return nil, err
	}
	defer cursor.Close(dbCtx)
	if err := cursor.All(dbCtx, m.Result); err != nil {
		tlog.Log.Error("[mongorpc/FindMany] 查询结果解码失败, collection=%v err=%v", m.Collection, err)
		return nil, err
	}
	return m.Result, nil
}

func (m *UpdateOne) collectionName() string { return m.Collection }
func (m *UpdateOne) shardKey() interface{}  { return m.Key }

// execute 更新单条文档，Upsert 为 true 时不存在则插入。
func (m *UpdateOne) execute(db *mongo.Database) (interface{}, error) {
	opts := options.UpdateOne()
	if m.Upsert {
		opts.SetUpsert(true)
	}
	dbCtx, cancel := bg()
	defer cancel()
	_, err := db.Collection(m.Collection).UpdateOne(dbCtx, m.Filter, m.Update, opts)
	if err != nil {
		tlog.Log.Error("[mongorpc/UpdateOne] 更新文档失败, collection=%v err=%v", m.Collection, err)
	}
	return nil, err
}

func (m *DeleteOne) collectionName() string { return m.Collection }
func (m *DeleteOne) shardKey() interface{}  { return m.Key }

// execute 删除单条匹配文档。
func (m *DeleteOne) execute(db *mongo.Database) (interface{}, error) {
	dbCtx, cancel := bg()
	defer cancel()
	_, err := db.Collection(m.Collection).DeleteOne(dbCtx, m.Filter)
	if err != nil {
		tlog.Log.Error("[mongorpc/DeleteOne] 删除文档失败, collection=%v err=%v", m.Collection, err)
	}
	return nil, err
}

func (m *CreateIndexes) collectionName() string { return m.Collection }
func (m *CreateIndexes) shardKey() interface{}  { return nil }

// execute 为集合创建索引并返回索引名。
func (m *CreateIndexes) execute(db *mongo.Database) (interface{}, error) {
	dbCtx, cancel := context.WithTimeout(context.Background(), indexTimeout)
	defer cancel()
	names, err := db.Collection(m.Collection).Indexes().CreateMany(dbCtx, m.Indexes)
	if err != nil {
		tlog.Log.Error("[mongorpc/CreateIndexes] 创建索引失败, collection=%v err=%v", m.Collection, err)
		return names, err
	}
	tlog.Log.Info("[mongorpc/CreateIndexes] 索引已就绪, collection=%v indexes=%v", m.Collection, names)
	return names, nil
}

// bg returns a background context bounded by a timeout. The caller is
// responsible for invoking the returned cancel function once the context is
// no longer needed, to release the timer promptly instead of waiting for it
// to fire.
func bg() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), opTimeout)
}

// Connect connects to MongoDB and returns the named database. Empty credentials
// leave authentication disabled.
func Connect(uri, username, password, dbName string) *mongo.Database {
	clientOptions := options.Client().ApplyURI(uri)
	if username != "" || password != "" {
		clientOptions.SetAuth(options.Credential{
			Username: username,
			Password: password,
		})
	}

	client, err := mongo.Connect(clientOptions)
	if err != nil {
		tlog.Log.Info("rpc/mongo.Connect: %v", err)
	}
	return client.Database(dbName)
}
