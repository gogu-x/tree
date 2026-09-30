// Package mongorpc Package mongo provides a common MongoDB Actor that serializes all database
// operations to prevent concurrency issues. Any process needing MongoDB should
// Spawn one Actor and interact via messages.
package mongorpc

import (
	"context"
	"log"
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
}

// FindOne queries a single document, decoding into Result. Response: error
type FindOne struct {
	Collection string
	Filter     interface{}
	Result     interface{}
}

// FindMany 根据查询条件加载全部匹配文档。
type FindMany struct {
	Collection string      // Mongo 集合名称。
	Filter     interface{} // Mongo 查询条件。
	Result     interface{} // 指向文档切片的指针。
}

// UpdateOne updates a single document. Response: error
type UpdateOne struct {
	Collection string
	Filter     interface{}
	Update     interface{}
	Upsert     bool
}

// DeleteOne deletes a single document. Response: error
type DeleteOne struct {
	Collection string
	Filter     interface{}
}

type Actor struct {
	name   string
	router tree.Router
	db     *mongo.Database
}

// NewActor 创建 MongoDB Actor。name 由调用方指定（如 constant.ActorGameMongo /
// constant.ActorPlatformMongo），因为同一进程可能需要多个库各一个 Actor。
func NewActor(name string, db *mongo.Database) *Actor { return &Actor{name: name, db: db} }

func (a *Actor) Name() string { return a.name }

func (a *Actor) MailboxSize() int { return 20 * 1024 }

func (a *Actor) OnInit(_ tree.Context) {
	a.router.Register(&InsertOne{}, a.onInsert)
	a.router.Register(&FindOne{}, a.onFind)
	a.router.Register(&FindMany{}, a.onFindMany)
	a.router.Register(&UpdateOne{}, a.onUpdate)
	a.router.Register(&DeleteOne{}, a.onDelete)
	tlog.Log.Info("rpc/mongo: ready, db=%s", a.db.Name())
}

func (a *Actor) HandleMessage(ctx tree.Context, msg interface{}) {
	a.router.Route(ctx, msg)
}

func (a *Actor) OnStop(_ tree.Context) {}

func (a *Actor) onInsert(ctx tree.Context, msg interface{}) {
	m := msg.(*InsertOne)
	f := ctx.RequestEnvelope()
	dbCtx, cancel := bg()
	defer cancel()
	res, err := a.db.Collection(m.Collection).InsertOne(dbCtx, m.Doc)
	if f == nil {
		return
	}
	if err != nil {
		f.Respond(nil, err)
		return
	}
	f.Respond(res.InsertedID, nil)
}

func (a *Actor) onFind(ctx tree.Context, msg interface{}) {
	m := msg.(*FindOne)
	f := ctx.RequestEnvelope()
	dbCtx, cancel := bg()
	defer cancel()
	err := a.db.Collection(m.Collection).FindOne(dbCtx, m.Filter).Decode(m.Result)
	if f == nil {
		return
	}
	f.Respond(m.Result, err)
}

func (a *Actor) onFindMany(ctx tree.Context, msg interface{}) {
	m := msg.(*FindMany)
	f := ctx.RequestEnvelope()
	dbCtx, cancel := bg()
	defer cancel()
	cursor, err := a.db.Collection(m.Collection).Find(dbCtx, m.Filter)
	if err != nil {
		tlog.Log.Error("[rpc/mongo/FindMany] 集合查询失败, collection=%v err=%v", m.Collection, err)
		if f != nil {
			f.Respond(nil, err)
		}
		return
	}
	defer cursor.Close(dbCtx)
	if err := cursor.All(dbCtx, m.Result); err != nil {
		tlog.Log.Error("[rpc/mongo/FindMany] 查询结果解码失败, collection=%v err=%v", m.Collection, err)
		if f != nil {
			f.Respond(nil, err)
		}
		return
	}
	if f != nil {
		f.Respond(m.Result, nil)
	}
}
func (a *Actor) onUpdate(ctx tree.Context, msg interface{}) {
	m := msg.(*UpdateOne)
	f := ctx.RequestEnvelope()
	opts := options.UpdateOne()
	if m.Upsert {
		opts.SetUpsert(true)
	}
	dbCtx, cancel := bg()
	defer cancel()
	_, err := a.db.Collection(m.Collection).UpdateOne(dbCtx, m.Filter, m.Update, opts)
	if err != nil {
		log.Printf("rpc/mongo: UpdateOne [%s] error: %v", m.Collection, err)
	}
	if f == nil {
		return
	}
	f.Respond(nil, err)
}

func (a *Actor) onDelete(ctx tree.Context, msg interface{}) {
	m := msg.(*DeleteOne)
	f := ctx.RequestEnvelope()
	dbCtx, cancel := bg()
	defer cancel()
	_, err := a.db.Collection(m.Collection).DeleteOne(dbCtx, m.Filter)
	if f == nil {
		return
	}
	f.Respond(nil, err)
}

// bg returns a background context bounded by a timeout. The caller is
// responsible for invoking the returned cancel function once the context is
// no longer needed, to release the timer promptly instead of waiting for it
// to fire.
func bg() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 8*time.Second)
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
